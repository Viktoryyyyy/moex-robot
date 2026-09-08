"""Replay weekly CPI estimates from a frozen verified Rosstat receipt."""
import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from html.parser import HTMLParser
import json
from pathlib import Path
import re

from .rosstat_https import CERTIFICATES, MAX_BYTES, POLICY, _freeze, validate_url

SCHEMA = 'rosstat_weekly_cpi_document.v1'
MONTHS = dict(zip(('января февраля марта апреля мая июня июля августа сентября '
                   'октября ноября декабря').split(), range(1, 13)))
PERIOD = r'со? (\d{1,2})(?: ([а-я]+))? по (\d{1,2}) ([а-я]+) (\d{4})'
INDEX = r'(\d{1,3},\d{2})'


def period_dates(value):
    """Explicit same-year periods, including holidays and adjacent months."""
    match = re.fullmatch(PERIOD, value)
    if not match: raise ValueError('unsupported CPI period')
    day1, month1, day2, month2, year = match.groups()
    if month2 not in MONTHS or (month1 is not None and month1 not in MONTHS):
        raise ValueError('unknown observation month')
    start = date(int(year), MONTHS[month1 or month2], int(day1))
    end = date(int(year), MONTHS[month2], int(day2))
    if not 0 <= (end - start).days <= 13:
        raise ValueError('invalid observation period')
    return start, end


def _text(parts):
    return ' '.join(''.join(parts).split())


class Document(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.titles, self.paragraphs, self.rows = [], [], []
        self.title = self.paragraph = self.cell = self.row = None
        self.ignored = 0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self.ignored += 1
        if self.ignored:
            return
        if tag == 'title': self.title = []
        elif tag == 'p': self.paragraph = []
        elif tag == 'tr': self.row = []
        elif tag in ('td', 'th'): self.cell = []
        elif tag == 'br': self.handle_data(' ')

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self.ignored = max(0, self.ignored - 1)
            return
        if self.ignored:
            return
        if tag == 'title' and self.title is not None:
            self.titles.append(_text(self.title)); self.title = None
        elif tag == 'p' and self.paragraph is not None:
            self.paragraphs.append(_text(self.paragraph)); self.paragraph = None
        elif tag in ('td', 'th') and self.cell is not None:
            if self.row is not None: self.row.append(_text(self.cell))
            self.cell = None
        elif tag == 'tr' and self.row is not None:
            self.rows.append(self.row); self.row = None

    def handle_data(self, data):
        if not self.ignored:
            for target in (self.title, self.paragraph, self.cell):
                if target is not None: target.append(data)


def _utc(value):
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.utcoffset() is None:
        raise ValueError('aware receipt and consumption timestamps required')
    return result.astimezone(timezone.utc)


def parse(raw, *, received_at):
    """Weekly estimates with explicit source bases, not final monthly CPI."""
    if not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid document size')
    doc = Document()
    doc.feed(raw.decode('utf-8-sig'))
    doc.close()
    if len(doc.titles) != 1:
        raise ValueError('one CPI title required')
    title = re.fullmatch('Об оценке индекса потребительских цен ' + PERIOD + ' года', doc.titles[0])
    if not title:
        raise ValueError('unsupported CPI title or period format')
    start, end = period_dates(title[0].removeprefix('Об оценке индекса потребительских цен ').removesuffix(' года'))
    month, year = title.groups()[-2:]
    if end > _utc(received_at).date():
        raise ValueError('invalid or future observation period')
    candidates = [p for p in doc.paragraphs if p.startswith('За период ') and 'индекс потребительских цен' in p]
    if len(candidates) != 1:
        raise ValueError('one unambiguous CPI summary required')
    summary = re.match('За период (' + PERIOD + r') г\. индекс потребительских цен(?:\d+)?'
        r', по оценке Росстата, составил ' + INDEX + r'%, с начала месяца [–—-] '
        + INDEX + r'%, с начала года [–—-] ' + INDEX + r'%(?: \(справочно:|\.)', candidates[0])
    january = False
    if not summary and start == date(start.year, 1, 1):
        summary = re.match('За период (' + PERIOD + r') г\. индекс потребительских цен(?:\d+)?'
            r', по оценке Росстата, составил ' + INDEX + r'% \(справочно:', candidates[0])
        january = True
    if not summary or period_dates(summary[1]) != (start, end):
        raise ValueError('CPI summary identity or units mismatch')
    values = summary.groups()[6:]
    rows = [r for r in doc.rows if r and r[0] == 'Индекс потребительских цен (оценка)']
    if len(rows) != 1 or len(rows[0]) != (5 if january else 6) or rows[0][1:(2 if january else 3)] != list(values[:2]):
        raise ValueError('CPI summary and table disagree')
    headers = [r for r in doc.rows if len(r) == (3 if january else 4) and
               r[1] == (f'С начала {month} {year} г.' if january else 'К предыдущей дате регистрации')]
    if len(headers) != 1 or (not january and headers[0][2] != f'С начала {month} {year} г.'):
        raise ValueError('CPI table period or column identity mismatch')
    numbers = [Decimal(v.replace(',', '.')) for v in values]
    if any(v <= 0 for v in numbers):
        raise ValueError('nonpositive CPI index')
    return {'series_id': 'ROSSTAT_WEEKLY_CPI_ESTIMATE', 'geography': 'RU',
        'observation_start': start.isoformat(), 'observation_end': end.isoformat(),
        'units': 'index_percent_base_100',
        'indices': ({'previous_registration': None, 'month_start': str(numbers[0]), 'year_start': None}
                    if january else dict(zip(('previous_registration', 'month_start', 'year_start'), map(str, numbers)))),
        'weekly_change_percent': None if january else str(numbers[0] - Decimal('100')),
        'document_format': 'january_initial_month_index' if january else 'three_explicit_bases',
        'monthly_final': False}


def _read(path, expected, suffix):
    path = Path(path)
    if not isinstance(expected, str) or not re.fullmatch('[0-9a-f]{64}', expected):
        raise ValueError('invalid evidence hash')
    if path.name != expected + suffix or path.is_symlink():
        raise ValueError('invalid evidence path')
    with path.open('rb') as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or sha256(raw).hexdigest() != expected:
        raise ValueError('evidence hash mismatch')
    return raw


def replay(manifest, *, manifest_sha256, now):
    manifest = Path(manifest)
    receipt = json.loads(_read(manifest, manifest_sha256, '.json'))
    validate_url(receipt['source_url'])
    if not re.fullmatch(r'https://rosstat\.gov\.ru/storage/mediabank/[^/]+\.html', receipt['source_url']):
        raise ValueError('CPI media document required')
    if (receipt.get('policy') != POLICY or receipt.get('certificate_sha256') != CERTIFICATES
        or receipt.get('tls_chain_and_hostname_verified') is not True
        or receipt.get('semantic_validation_status') != 'NOT_PARSED'
        or any(receipt.get(k) is not False for k in ('factual_authority', 'historical_pit_acceptance', 'action_authority'))):
        raise ValueError('verified acquisition receipt required')
    requested, received = (_utc(receipt[k]) for k in ('requested_at_utc', 'received_at_utc'))
    if not requested <= received <= _utc(now):
        raise ValueError('invalid receipt causal order')
    raw = _read(manifest.parent / (receipt['raw_sha256'] + '.html'), receipt['raw_sha256'], '.html')
    fact = parse(raw, received_at=received)
    return {'schema_version': SCHEMA, **fact, 'source_url': receipt['source_url'],
        'raw_sha256': receipt['raw_sha256'], 'receipt_sha256': manifest_sha256,
        'system_available_at': received.isoformat(), 'source_publication_time': None,
        'semantic_validation_status': 'DOCUMENT_PARSED', 'scope': 'received_document_only',
        'latest_publication_verified': False, 'factual_authority': False,
        'consumer_factual_use_allowed': False, 'historical_pit_acceptance': False,
        'action_authority': False, 'consensus': None, 'surprise': None,
        'revision_policy': 'freeze_received_vintage_no_backdating',
        'remaining_admission': ['latest_release_selection', 'release_calendar', 'forecast_horizon_alignment']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--as-of', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = replay(args.manifest, manifest_sha256=args.manifest_sha256, now=args.as_of)
    encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    if args.output.is_symlink(): raise ValueError('output symlink refused')
    args.output.mkdir(parents=True, exist_ok=True)
    output = args.output / (sha256(encoded).hexdigest() + '.json')
    _freeze(output, encoded)
    print(json.dumps({'output': str(output), **result}, ensure_ascii=False, indent=2))
