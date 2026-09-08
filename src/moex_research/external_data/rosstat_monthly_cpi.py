"""Received official monthly CPI; printed indices, not a weekly estimate."""
from calendar import monthrange
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import re
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from . import rosstat_https as transport
from . import rosstat_cpi_factual as weekly
from . import rosstat_weekly_cpi as document

COMPONENT = 'rosstat_monthly_cpi'
POLICY = 'rosstat_monthly_cpi_received.v1'
INDEX_URL = weekly.INDEX_URL
TITLE = 'Об индексе потребительских цен (ежемесячная)'
LOCATIVE = 'январе феврале марте апреле мае июне июле августе сентябре октябре ноябре декабре'.split()
NOMINATIVE = 'январь февраль март апрель май июнь июль август сентябрь октябрь ноябрь декабрь'.split()
DATIVE = 'январю февралю марту апрелю маю июню июлю августу сентябрю октябрю ноябрю декабрю'.split()
INSTRUMENTAL = 'январем февралем мартом апрелем маем июнем июлем августом сентябрем октябрем ноябрем декабрем'.split()
MAX_OBSERVATION_DAYS = 62
MAX_RECEIPT_SECONDS = weekly.MAX_RECEIPT_SECONDS
DENIED = ('historical_pit_acceptance', 'action_authority', 'forecast_alignment_accepted',
          'full_rosstat_macro_accepted', 'calendar_accepted', 'intraday_use_allowed')


def _month(label):
    match = re.fullmatch(r'в ([а-я]+) (\d{4}) года', label)
    if not match or match[1] not in LOCATIVE: raise ValueError('unsupported monthly period')
    return date(int(match[2]), LOCATIVE.index(match[1]) + 1, 1)


class _Index(weekly.Index):
    def __init__(self):
        super().__init__()
        self.anchors, self.anchor = [], None

    def handle_starttag(self, tag, attrs):
        super().handle_starttag(tag, attrs)
        if tag == 'a' and not self.ignored:
            if self.anchor is not None: raise ValueError('nested archive anchor')
            self.anchor = {'url': dict(attrs).get('href', ''), 'text': [], 'ancestors': self.stack.copy()}

    def handle_data(self, text):
        super().handle_data(text)
        if not self.ignored and self.anchor is not None: self.anchor['text'].append(text)

    def handle_endtag(self, tag):
        if tag == 'a' and self.anchor is not None:
            self.anchors.append(self.anchor)
            self.anchor = None
        super().handle_endtag(tag)


def select(raw, *, now):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= transport.MAX_BYTES:
        raise ValueError('invalid monthly archive body')
    parser = _Index(); parser.feed(raw.decode('utf-8-sig')); parser.close()
    titles = [n for n in parser.nodes if 'toggle-card__title' in n['classes'] and weekly._text(n) == TITLE]
    if len(titles) != 1: raise ValueError('one monthly archive required')
    sections = [n for n in titles[0]['ancestors'] if 'toggle-card' in n['classes']]
    if not sections: raise ValueError('monthly archive scope absent')
    candidates = []
    for anchor in parser.anchors:
        if not any(n is sections[-1] for n in anchor['ancestors']): continue
        label = document._text(anchor['text'])
        if not label: continue
        if label == TITLE: continue
        month = _month(label)
        candidates.append({'observation_month': month.strftime('%Y-%m'),
            'source_url': urljoin(INDEX_URL, anchor['url']), 'archive_period_label': label})
    if not candidates: raise ValueError('empty monthly archive')
    newest = max(c['observation_month'] for c in candidates)
    selected = [c for c in candidates if c['observation_month'] == newest]
    if len(selected) != 1: raise ValueError('ambiguous latest monthly release')
    selected = selected[0]
    transport.validate_url(selected['source_url'])
    if not re.fullmatch(r'https://rosstat\.gov\.ru/storage/mediabank/[^/]+\.html', selected['source_url']):
        raise ValueError('newest monthly document unsupported')
    _age(_month(selected['archive_period_label']), now)
    return selected


def _age(month, now):
    end = date(month.year, month.month, monthrange(month.year, month.month)[1])
    today = document._utc(now).astimezone(ZoneInfo('Europe/Moscow')).date()
    if not 0 <= (today - end).days <= MAX_OBSERVATION_DAYS:
        raise ValueError('future or expired monthly observation')
    return end


class _Document(document.Document):
    """Keep table identities so column headings cannot validate another table."""
    def __init__(self):
        super().__init__()
        self.tables, self.table_stack = [], []

    def handle_starttag(self, tag, attrs):
        if tag == 'table' and not self.ignored:
            table = []
            self.tables.append(table); self.table_stack.append(table)
        super().handle_starttag(tag, attrs)

    def handle_endtag(self, tag):
        row = self.row if tag == 'tr' and not self.ignored else None
        super().handle_endtag(tag)
        if row is not None and self.table_stack: self.table_stack[-1].append(row)
        if tag == 'table' and not self.ignored and self.table_stack: self.table_stack.pop()


def parse(raw, *, received_at):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= transport.MAX_BYTES:
        raise ValueError('invalid monthly document')
    doc = _Document(); doc.feed(raw.decode('utf-8-sig')); doc.close()
    if len(doc.titles) != 1: raise ValueError('one monthly title required')
    prefix = 'Об индексе потребительских цен '
    if not doc.titles[0].startswith(prefix): raise ValueError('monthly title identity mismatch')
    month = _month(doc.titles[0][len(prefix):]); end = _age(month, received_at)
    m, y = month.month, month.year
    previous_month, previous_year = (12, y - 1) if m == 1 else (m - 1, y)
    headings = [f'{DATIVE[previous_month-1]} {previous_year} г.', f'декабрю {y-1} г.', f'{DATIVE[m-1]} {y-1} г.']
    base_count = 2 if m in (1, 12) else 3
    expected_headers = [headings[0], headings[2]] if m == 1 else headings[:base_count]
    tables = [table for table in doc.tables if any(r and r[0] == 'Индекс потребительских цен' for r in table)]
    if len(tables) != 1: raise ValueError('one total CPI table required')
    rows = [r for r in tables[0] if r and r[0] == 'Индекс потребительских цен']
    first = [r for r in tables[0] if len(r) >= 2 and r[1] == f'{NOMINATIVE[m-1].capitalize()} {y} г. к']
    column_rows = [r for r in tables[0] if r[:base_count] == expected_headers]
    if len(rows) != 1 or len(first) != 1 or len(column_rows) != 1 or len(rows[0]) != (5 if m == 1 else 7 if m == 12 else 9):
        raise ValueError('monthly CPI table identity/bases unsupported')
    values = rows[0][1:1+base_count]
    if any(not re.fullmatch(r'\d{1,3},\d{2}', v) for v in values): raise ValueError('monthly printed precision unsupported')
    numbers = [Decimal(v.replace(',', '.')) for v in values]
    if any(v <= 0 for v in numbers): raise ValueError('nonpositive monthly index')
    candidates = [p for p in doc.paragraphs if ('индекс потребительских цен' in p.lower()) and
        (p.startswith(f'Индекс потребительских цен в {LOCATIVE[m-1]} {y} г.') or p.startswith(f'В {LOCATIVE[m-1]} {y} г.'))]
    if len(candidates) != 1: raise ValueError('one monthly CPI summary required')
    summary = candidates[0].split('(в ', 1)[0]
    # Both actual source introductory formats identify the preceding month/year.
    if not any(term in summary for term in (f'к {headings[0]}', f'с {INSTRUMENTAL[previous_month-1]} {previous_year} г.')):
        raise ValueError('summary previous-month identity mismatch')
    summary_values = re.findall(r'(\d{1,3},\d{2})%', summary)
    expected_values = values[:1] if m == 1 else values[:2]
    if summary_values != expected_values: raise ValueError('monthly summary/table mismatch')
    if m != 1 and not any(term in summary for term in (f'к декабрю {y-1} г.', f'с декабрем {y-1} г.')):
        raise ValueError('summary previous-December identity mismatch')
    indices = dict(zip(('previous_month', 'previous_december', 'same_month_previous_year'),
        map(str, [numbers[0], numbers[0], numbers[1]] if m == 1 else [numbers[0], numbers[1], numbers[1]] if m == 12 else numbers)))
    return {'series_id': 'ROSSTAT_MONTHLY_CPI', 'geography': 'RU', 'observation_month': month.strftime('%Y-%m'),
        'observation_start': month.isoformat(), 'observation_end': end.isoformat(), 'units': 'index_percent_base_100',
        'indices': indices, 'changes_percent': {k: str(Decimal(v)-100) for k,v in indices.items()},
        'changes_derivation': 'printed_index_minus_100', 'decimal_places': 2,
        'precision_scope': 'printed_granularity_not_error_bound', 'frequency': 'MONTHLY',
        'estimate_kind': 'PUBLISHED_MONTHLY_INDEX',
        'revision_status': 'received_vintage_may_be_revised'}


def _replay(refs, *, now):
    index, raw = weekly._receipt(refs['index_manifest_path'], refs['index_manifest_sha256'], now=now, expected_url=INDEX_URL)
    selected = select(raw, now=now)
    receipt, raw = weekly._receipt(refs['document_manifest_path'], refs['document_manifest_sha256'], now=now, expected_url=selected['source_url'])
    if document._utc(index['received_at_utc']) > document._utc(receipt['requested_at_utc']):
        raise ValueError('document acquired before archive')
    parsed = parse(raw, received_at=receipt['received_at_utc'])
    if parsed['observation_month'] != selected['observation_month']: raise ValueError('archive/document month mismatch')
    return {**parsed, **selected, **refs, 'policy': POLICY, 'raw_sha256': receipt['raw_sha256'],
        'received_at': receipt['received_at_utc'], 'system_available_at': receipt['received_at_utc'],
        'listed_publication_date': None, 'source_publication_time': None,
        'index_received_at': index['received_at_utc'], 'scope': 'latest_listed_monthly_cpi_received_reference',
        'latest_selection_scope': 'official_monthly_archive_at_receipt', 'factual_authority': True,
        'consumer_factual_use_allowed': True, 'revision_policy': 'freeze_each_received_vintage_no_backdating',
        'limitations': ['publication_date_and_hour_unverified', 'calendar_not_required_for_dated_context',
            'maximum_observation_age_62_calendar_days_not_release_timeliness_proof'], **dict.fromkeys(DENIED, False)}


def load(*, root):
    root = Path(root).resolve(); output = root / 'raw/external/rosstat_monthly_cpi'
    if not output.resolve().is_relative_to(root): raise ValueError('evidence directory escapes root')
    index = transport.capture(INDEX_URL, output=output)
    _, raw = weekly._receipt(index['manifest_path'], index['manifest_sha256'], now=datetime.now(timezone.utc), expected_url=INDEX_URL)
    selected = select(raw, now=datetime.now(timezone.utc))
    receipt = transport.capture(selected['source_url'], output=output)
    refs = {'index_manifest_path': index['manifest_path'], 'index_manifest_sha256': index['manifest_sha256'],
        'document_manifest_path': receipt['manifest_path'], 'document_manifest_sha256': receipt['manifest_sha256']}
    return _replay(refs, now=datetime.now(timezone.utc))


def reconcile(component, *, now):
    result = deepcopy(component) if isinstance(component, dict) else {}
    data = result.get('data'); data = data if isinstance(data, dict) else {}; result['data'] = data
    try:
        if result.get('status') != 'READY' or result.get('refresh_error') or data.get('factual_authority') is not True or data.get('consumer_factual_use_allowed') is not True:
            raise ValueError('monthly component admission blocked')
        refs = {k: data[k] for k in ('index_manifest_path', 'index_manifest_sha256', 'document_manifest_path', 'document_manifest_sha256')}
        expected = _replay(refs, now=now)
        if set(data)-{'read_freshness_reason'} != set(expected) or any(data[k] != v for k,v in expected.items()):
            raise ValueError('monthly normalized evidence mismatch')
        data['read_freshness_reason'] = None
    except (ValueError, TypeError, KeyError, OSError, OverflowError, AttributeError) as exc:
        result['status'] = 'UNAVAILABLE'; data.update(dict.fromkeys(DENIED, False))
        data['factual_authority'] = data['consumer_factual_use_allowed'] = False
        data['read_freshness_reason'] = str(exc)
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components', {})
    if isinstance(components, dict) and COMPONENT in components:
        components[COMPONENT] = reconcile(components[COMPONENT], now=now)
