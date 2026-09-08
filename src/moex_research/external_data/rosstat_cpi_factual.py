"""Latest listed weekly CPI: receipt-bound dated context, not full Rosstat macro."""
from copy import deepcopy
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from . import rosstat_https as transport
from . import rosstat_weekly_cpi as document

INDEX_URL = 'https://rosstat.gov.ru/compendium/document/50798'
TITLE = 'Об оценке индекса потребительских цен (еженедельная)'
POLICY = 'rosstat_latest_listed_weekly_cpi.v1'
MAX_RECEIPT_SECONDS = 1200
MAX_PUBLICATION_DAYS = 10


class Index(HTMLParser):
    """Track div ancestry so calendar/announcements cannot supply archive rows."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.nodes = [], []
        self.tables, self.table = [], None
        self.ignored = 0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'): self.ignored += 1
        if self.ignored: return
        if tag == 'table':
            if self.table is not None: raise ValueError('nested calendar table unsupported')
            self.table = document.Document()
            self.tables.append((self.stack.copy(), self.table))
        if self.table is not None: self.table.handle_starttag(tag, attrs)
        attrs = dict(attrs)
        if tag == 'div':
            node = {'classes': attrs.get('class', '').split(), 'text': [], 'links': [],
                    'ancestors': self.stack.copy()}
            self.nodes.append(node)
            self.stack.append(node)
        elif tag == 'a':
            for node in self.stack: node['links'].append(attrs.get('href', ''))
        elif tag == 'br': self.handle_data(' ')

    def handle_endtag(self, tag):
        if not self.ignored and self.table is not None:
            self.table.handle_endtag(tag)
            if tag == 'table': self.table = None
        if tag in ('script', 'style'):
            self.ignored = max(0, self.ignored - 1)
        elif not self.ignored and tag == 'div' and self.stack:
            self.stack.pop()

    def handle_data(self, text):
        if not self.ignored:
            if self.table is not None: self.table.handle_data(text)
            for node in self.stack: node['text'].append(text)


def _text(node):
    return ' '.join(''.join(node['text']).split())


def calendar(raw, *, selected, now):
    """A dated, bounded weekly publication schedule; no invented release hour."""
    parser = Index()
    parser.feed(raw.decode('utf-8-sig'))
    parser.close()
    today = document._utc(now).astimezone(ZoneInfo('Europe/Moscow')).date()
    titles = [n for n in parser.nodes if 'toggle-card__title' in n['classes'] and
              re.fullmatch(r'ГРАФИК размещения срочных информаций и справок на сайте Росстата (?:в I|во II) полугодии ' + str(today.year) + ' года', _text(n))]
    if not titles: raise ValueError('current-year official weekly calendar missing')
    events = []
    for title in titles:
        sections = [a for a in title['ancestors'] if 'toggle-card' in a['classes']]
        if not sections: raise ValueError('calendar scope missing')
        tables = [t for ancestors, t in parser.tables if any(a is sections[-1] for a in ancestors)]
        if len(tables) != 1: raise ValueError('one scoped calendar table required')
        for row in tables[0].rows:
            if not any('Об оценке индекса потребительских цен' in cell for cell in row): continue
            if len(row) != 3: raise ValueError('invalid weekly calendar row')
            match = re.fullmatch(r'Об оценке индекса потребительских цен (' + document.PERIOD + ') года', row[1])
            if not match: raise ValueError('unsupported weekly calendar period')
            start, end = document.period_dates(match[1])
            due = re.fullmatch(r'(\d{1,2}) ([а-я]+)', row[2])
            if not due or due[2] not in document.MONTHS: raise ValueError('invalid calendar publication date')
            day = datetime(today.year, document.MONTHS[due[2]], int(due[1])).date()
            if start.year != today.year or end > day or (day - end).days > 7:
                raise ValueError('calendar observation/publication dates disagree')
            events.append({'observation_start': start.isoformat(), 'observation_end': end.isoformat(),
                           'scheduled_publication_date': day.isoformat()})
    events.sort(key=lambda e: e['observation_end'])
    if not events or len({e['observation_end'] for e in events}) != len(events):
        raise ValueError('empty or ambiguous weekly calendar')
    for previous, current in zip(events, events[1:]):
        if ((datetime.fromisoformat(current['observation_start']) -
             datetime.fromisoformat(previous['observation_end'])).days != 1 or
            current['scheduled_publication_date'] <= previous['scheduled_publication_date']):
            raise ValueError('gapped, overlapping or unordered weekly calendar')
    start, end = document.period_dates(selected['archive_period_label'].removesuffix(' года'))
    matching = [e for e in events if (e['observation_start'], e['observation_end']) == (start.isoformat(), end.isoformat())]
    if len(matching) != 1 or matching[0]['scheduled_publication_date'] != selected['listed_publication_date']:
        raise ValueError('archive and scheduled release disagree')
    future = [e for e in events if e['observation_end'] > end.isoformat()]
    if not future: raise ValueError('weekly calendar next release coverage missing')
    upcoming = future[0]
    if today.isoformat() > upcoming['scheduled_publication_date']:
        raise ValueError('scheduled weekly publication overdue; latest archive still old')
    return {'weekly_release_calendar_accepted': True, 'weekly_calendar_scope': 'official_dated_schedule_only',
            'next_scheduled_release': upcoming, 'scheduled_release_time': None,
            'calendar_timezone': 'Europe/Moscow', 'calendar_overdue_policy': 'after_scheduled_date_end',
            'weekly_calendar_coverage_end': events[-1]['scheduled_publication_date']}


def select(raw, *, now):
    if not 0 < len(raw) <= transport.MAX_BYTES: raise ValueError('invalid archive size')
    parser = Index()
    parser.feed(raw.decode('utf-8-sig'))
    parser.close()
    titles = [n for n in parser.nodes if 'toggle-card__title' in n['classes'] and _text(n) == TITLE]
    if len(titles) != 1: raise ValueError('one weekly CPI archive required')
    sections = [n for n in titles[0]['ancestors'] if 'toggle-card' in n['classes']]
    if not sections: raise ValueError('weekly archive scope missing')
    section = sections[-1]
    rows = [n for n in parser.nodes if 'document-list__item--row' in n['classes']
            and any(a is section for a in n['ancestors'])]
    if not rows: raise ValueError('empty weekly archive')
    candidates = []
    for row in rows:
        children = [n for n in parser.nodes if any(a is row for a in n['ancestors'])]
        labels = [_text(n) for n in children if 'document-list__item-title' in n['classes']]
        infos = [_text(n) for n in children if 'document-list__item-info' in n['classes']]
        if len(labels) != 1 or len(infos) != 1 or len(row['links']) != 1:
            raise ValueError('ambiguous weekly archive row')
        match = re.search(r', (\d{2}\.\d{2}\.\d{4})$', infos[0])
        if not match: raise ValueError('archive publication date missing')
        day = datetime.strptime(match[1], '%d.%m.%Y').date()
        # Even unsupported newest formats remain candidates; never skip to older HTML.
        candidates.append({'source_url': urljoin(INDEX_URL, row['links'][0]),
            'archive_period_label': labels[0], 'listed_publication_date': day.isoformat()})
    latest_day = max(c['listed_publication_date'] for c in candidates)
    latest = [c for c in candidates if c['listed_publication_date'] == latest_day]
    if len(latest) != 1: raise ValueError('ambiguous newest weekly release')
    selected = latest[0]
    age = (document._utc(now).astimezone(ZoneInfo('Europe/Moscow')).date()
           - datetime.fromisoformat(latest_day).date()).days
    if not 0 <= age <= MAX_PUBLICATION_DAYS: raise ValueError('future or expired latest release')
    transport.validate_url(selected['source_url'])
    if not re.fullmatch(r'https://rosstat\.gov\.ru/storage/mediabank/[^/]+\.html', selected['source_url']):
        raise ValueError('newest weekly release format unsupported')
    if not re.fullmatch(document.PERIOD + ' года', selected['archive_period_label']):
        raise ValueError('newest weekly period unsupported')
    document.period_dates(selected['archive_period_label'].removesuffix(' года'))
    return selected


def _receipt(path, digest, *, now, expected_url=None):
    path = Path(path)
    value = json.loads(document._read(path, digest, '.json'))
    transport.validate_url(value['source_url'])
    if expected_url is not None and value['source_url'] != expected_url:
        raise ValueError('receipt source mismatch')
    if (value.get('policy') != transport.POLICY or value.get('certificate_sha256') != transport.CERTIFICATES
        or value.get('tls_chain_and_hostname_verified') is not True
        or value.get('semantic_validation_status') != 'NOT_PARSED'
        or any(value.get(k) is not False for k in ('factual_authority', 'historical_pit_acceptance', 'action_authority'))):
        raise ValueError('verified transport receipt required')
    requested, received = (document._utc(value[k]) for k in ('requested_at_utc', 'received_at_utc'))
    now = document._utc(now)
    if not requested <= received <= now or (now - received).total_seconds() > MAX_RECEIPT_SECONDS:
        raise ValueError('expired receipt or invalid causal order')
    raw = document._read(path.parent / (value['raw_sha256'] + '.html'), value['raw_sha256'], '.html')
    return value, raw


def _replay(refs, *, now):
    index, raw = _receipt(refs['index_manifest_path'], refs['index_manifest_sha256'], now=now, expected_url=INDEX_URL)
    selected = select(raw, now=now)
    scheduled = calendar(raw, selected=selected, now=now)
    receipt, _ = _receipt(refs['document_manifest_path'], refs['document_manifest_sha256'], now=now,
                          expected_url=selected['source_url'])
    if document._utc(index['received_at_utc']) > document._utc(receipt['requested_at_utc']):
        raise ValueError('document acquired before index selection')
    parsed = document.replay(refs['document_manifest_path'], manifest_sha256=refs['document_manifest_sha256'], now=now)
    expected = [d.isoformat() for d in document.period_dates(selected['archive_period_label'].removesuffix(' года'))]
    if expected != [parsed['observation_start'], parsed['observation_end']]:
        raise ValueError('archive and document periods disagree')
    if parsed['observation_end'] > selected['listed_publication_date']:
        raise ValueError('publication precedes observation end')
    return {**parsed, **selected, **scheduled, **refs, 'policy': POLICY,
        'scope': 'latest_listed_weekly_estimate_dated_context',
        'latest_publication_verified': True, 'latest_verification_scope': 'official_weekly_archive_at_receipt',
        'factual_authority': True, 'consumer_factual_use_allowed': True,
        'received_at': receipt['received_at_utc'], 'index_received_at': index['received_at_utc'],
        'calendar_accepted': False, 'full_rosstat_macro_accepted': False,
        'forecast_alignment_accepted': False,
        'remaining_admission': ['full_rosstat_macro', 'release_calendar', 'forecast_horizon_alignment']}


def load(*, root):
    root = Path(root).resolve()
    output = root / 'raw/external/rosstat_weekly_cpi'
    if not output.resolve().is_relative_to(root): raise ValueError('archive escapes data root')
    index = transport.capture(INDEX_URL, output=output)
    _, raw = _receipt(index['manifest_path'], index['manifest_sha256'], now=datetime.now(timezone.utc), expected_url=INDEX_URL)
    selected = select(raw, now=datetime.now(timezone.utc))
    receipt = transport.capture(selected['source_url'], output=output)
    refs = {'index_manifest_path': index['manifest_path'], 'index_manifest_sha256': index['manifest_sha256'],
            'document_manifest_path': receipt['manifest_path'], 'document_manifest_sha256': receipt['manifest_sha256']}
    return _replay(refs, now=datetime.now(timezone.utc))


def reconcile(component, *, now):
    result = deepcopy(component)
    data = result.get('data')
    if not isinstance(data, dict): return result
    try:
        if result.get('status') != 'READY' or result.get('refresh_error') or data.get('factual_authority') is not True or data.get('consumer_factual_use_allowed') is not True:
            raise ValueError('previous admission or latest refresh blocked')
        refs = {k: data[k] for k in ('index_manifest_path', 'index_manifest_sha256', 'document_manifest_path', 'document_manifest_sha256')}
        replay = _replay(refs, now=now)
        if any(data.get(k) != v for k, v in replay.items()): raise ValueError('received evidence replay mismatch')
        data['read_freshness_reason'] = None
    except (ValueError, TypeError, KeyError, OSError, OverflowError) as exc:
        result['status'] = 'UNAVAILABLE'
        for key in ('factual_authority', 'consumer_factual_use_allowed', 'latest_publication_verified',
                    'historical_pit_acceptance', 'action_authority', 'calendar_accepted',
                    'full_rosstat_macro_accepted', 'forecast_alignment_accepted'):
            data[key] = False
        data['weekly_release_calendar_accepted'] = False
        data['read_freshness_reason'] = str(exc)
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components', {})
    if isinstance(components.get('rosstat_cpi'), dict):
        components['rosstat_cpi'] = reconcile(components['rosstat_cpi'], now=now)
