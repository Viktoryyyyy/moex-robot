"""Receipt-bound published meeting dates; never decision or publication evidence."""
from copy import deepcopy
from datetime import date, datetime, timezone
from hashlib import sha256
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from . import cbr_rates_factual as evidence

SOURCE_URL = 'https://www.cbr.ru/dkp/cal_mp/'
POLICY = 'cbr_meeting_calendar.v1'
SCOPE = 'PUBLISHED_CBR_MEETING_SCHEDULE_ONLY'
COMPONENT = 'cbr_meeting_calendar'
MAX_BYTES = 2_000_000
MAX_RECEIPT_SECONDS = 1200
MOSCOW = ZoneInfo('Europe/Moscow')
MEETING = 'Заседание Совета директоров Банка России по ключевой ставке'
MONTHS = dict(zip(('января','февраля','марта','апреля','мая','июня','июля','августа','сентября','октября','ноября','декабря'), range(1,13)))
DENIED = ('factual_authority', 'consumer_factual_use_allowed', 'actual_decision_proven',
    'source_publication_time_proven', 'historical_pit_acceptance', 'forecast_use_allowed',
    'action_authority', 'full_calendar_accepted', 'full_macro_complete')


def _text(text):
    return ' '.join(text.split())


class _DOM(HTMLParser):
    """Only tracked div/a ancestry is used for year and day selection."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.nodes, self.stack, self.ignored = [], [], 0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self.ignored += 1
        if not self.ignored and tag in ('div', 'a'):
            node = {'tag': tag, 'attrs': dict(attrs), 'text': '', 'parents': tuple(self.stack)}
            self.nodes.append(node)
            self.stack.append(len(self.nodes)-1)

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self.ignored = max(0, self.ignored-1)
            return
        if not self.ignored and tag in ('div', 'a'):
            if not self.stack or self.nodes[self.stack[-1]]['tag'] != tag:
                raise ValueError('unbalanced calendar structure')
            self.stack.pop()

    def handle_data(self, data):
        if not self.ignored:
            for index in self.stack:
                self.nodes[index]['text'] += data


def parse(raw, *, year):
    if type(year) is not int or not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid year or raw body')
    dom = _DOM()
    dom.feed(raw.decode('utf-8'))
    dom.close()
    tabs = [n for n in dom.nodes if n['tag'] == 'a' and _text(n['text']) == f'{year} год' and n['attrs'].get('data-tabs-tab')]
    if len(tabs) != 1:
        raise ValueError('unique current year tab required')
    panels = [(i,n) for i,n in enumerate(dom.nodes) if n['attrs'].get('role') == 'tabpanel' and n['attrs'].get('data-tabs-content') == tabs[0]['attrs']['data-tabs-tab']]
    if len(panels) != 1:
        raise ValueError('unique year panel required')
    panel_id, panel = panels[0]
    if panel_id in dom.stack:
        raise ValueError('unclosed selected calendar panel')
    def has(node, name):
        return name in node['attrs'].get('class', '').split()
    days = [(i,n) for i,n in enumerate(dom.nodes) if panel_id in n['parents'] and has(n,'main-events_day')]
    if not 1 <= len(days) <= 100:
        raise ValueError('invalid calendar day count')
    events, previous = [], None
    panel_text = _text(panel['text'])
    release = 'Предполагаемое время публикации пресс-релиза — 13:30 по московскому времени'
    conference = 'Предполагаемое время начала пресс-конференции — 15:00 по московскому времени'
    # Fail closed when the source changes the explicitly tentative time policy.
    if release not in panel_text or conference not in panel_text:
        raise ValueError('unrecognized tentative time policy')
    for index, node in days:
        children = [n for n in dom.nodes if index in n['parents']]
        dates = [n for n in children if has(n,'date')]
        if len(dates) != 1:
            raise ValueError('unique day date required')
        match = re.fullmatch(r'(\d{1,2}) ([а-я]+) (\d{4}) года', _text(dates[0]['text']))
        if not match or match[2] not in MONTHS:
            raise ValueError('invalid scheduled date')
        day = date(int(match[3]), MONTHS[match[2]], int(match[1]))
        if day.year != year or (previous is not None and day <= previous):
            raise ValueError('calendar year/order/duplicate mismatch')
        previous = day
        titles = [n for n in children if has(n,'title')]
        if titles:
            if len(titles) != 1 or _text(titles[0]['text']) != MEETING:
                raise ValueError('unknown meeting title')
            events.append({'event_id': 'cbr_key_rate_meeting:' + day.isoformat(),
                'scheduled_date': day.isoformat(), 'scheduled_time': None,
                'event_status': 'SCHEDULED', 'timezone': 'Europe/Moscow',
                'planned_press_release_time': '13:30', 'planned_press_conference_time': '15:00',
                'planned_times_kind': 'TENTATIVE_SOURCE_SCHEDULE',
                'actual_event_time': None, 'source_publication_time': None})
        elif 'Резюме обсуждения ключевой ставки' not in _text(node['text']) or MEETING in _text(node['text']):
            raise ValueError('unrecognized nonmeeting calendar day')
    if not events:
        raise ValueError('no scheduled meetings')
    return events


def _normalized(raw, *, receipt, now):
    now = evidence._utc(now)
    if not isinstance(receipt, dict) or set(receipt) != {'source_url','requested_at','received_at','raw_sha256'}:
        raise ValueError('invalid receipt shape')
    received, requested = evidence._utc(receipt['received_at']), evidence._utc(receipt['requested_at'])
    today = now.astimezone(MOSCOW).date()
    if not requested <= received <= now or (now-received).total_seconds() > MAX_RECEIPT_SECONDS:
        raise ValueError('receipt expired or chronology invalid')
    if any(t.astimezone(MOSCOW).year != today.year for t in (requested,received)):
        raise ValueError('calendar reference year changed')
    if receipt['source_url'] != SOURCE_URL or sha256(raw).hexdigest() != evidence._digest(receipt['raw_sha256']):
        raise ValueError('source identity or hash mismatch')
    events = parse(raw, year=today.year)
    return {'policy': POLICY, 'scope': SCOPE, 'year': today.year, 'events': events,
        'receipt': receipt, 'source_url': SOURCE_URL, 'raw_sha256': receipt['raw_sha256'],
        'received_at': received.isoformat(), 'system_available_at': received.isoformat(),
        'source_publication_time': None, 'calendar_schedule_usable': True,
        **dict.fromkeys(DENIED, False)}


def load(*, root, now_fn=lambda: datetime.now(timezone.utc), fetch=evidence._fetch):
    requested = evidence._utc(now_fn())
    raw = fetch(SOURCE_URL)
    received = evidence._utc(now_fn())
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid raw response')
    digest = sha256(raw).hexdigest()
    receipt = {'source_url': SOURCE_URL, 'requested_at': requested.isoformat(),
        'received_at': received.isoformat(), 'raw_sha256': digest}
    data = _normalized(raw, receipt=receipt, now=received)
    directory = Path(root) / 'raw/external/cbr_meeting_calendar'
    if any(p.is_symlink() for p in (directory,*directory.parents)):
        raise ValueError('symlink evidence directory refused')
    directory.mkdir(parents=True, exist_ok=True)
    evidence._freeze(directory / (digest+'.html'),raw)
    encoded = json.dumps(data,sort_keys=True,separators=(',',':')).encode()
    digest = sha256(encoded).hexdigest()
    path = directory / (digest+'.json')
    evidence._freeze(path,encoded)
    return {**data,'manifest_path':str(path),'manifest_sha256':digest,
        'upcoming_events': _upcoming(data,received)}


def _upcoming(data, now):
    today = evidence._utc(now).astimezone(MOSCOW).date().isoformat()
    return [deepcopy(e) for e in data['events'] if e['scheduled_date'] >= today]


def reconcile(component, *, now):
    result = deepcopy(component) if isinstance(component,dict) else {}
    data = result.get('data')
    if not isinstance(data,dict):
        data = result['data'] = {}
    try:
        if result.get('status') != 'READY' or result.get('refresh_error'):
            raise ValueError('latest refresh not ready')
        digest = evidence._digest(data['manifest_sha256'])
        path = Path(data['manifest_path'])
        if path.name != digest+'.json':
            raise ValueError('manifest name mismatch')
        frozen = json.loads(evidence._read(path,digest))
        if not isinstance(frozen,dict) or set(data)-{'manifest_path','manifest_sha256','read_freshness_reason','upcoming_events'} != set(frozen):
            raise ValueError('manifest shape mismatch')
        if any(json.dumps(data[k],sort_keys=True) != json.dumps(value,sort_keys=True) for k,value in frozen.items()):
            raise ValueError('normalized evidence modified')
        if not isinstance(frozen.get('receipt'),dict):
            raise ValueError('receipt object required')
        raw_hash = evidence._digest(frozen['receipt']['raw_sha256'])
        raw = evidence._read(path.parent/(raw_hash+'.html'),raw_hash)
        replayed = _normalized(raw,receipt=frozen['receipt'],now=now)
        if json.dumps(replayed,sort_keys=True) != json.dumps(frozen,sort_keys=True):
            raise ValueError('raw replay mismatch')
        data['upcoming_events'] = _upcoming(replayed,now)
        data['read_freshness_reason'] = None
    except (ValueError,TypeError,KeyError,OSError,OverflowError) as exc:
        result['status'] = 'UNAVAILABLE'
        data.update(dict.fromkeys(DENIED,False))
        data['calendar_schedule_usable'] = False
        data['upcoming_events'] = []
        data['read_freshness_reason'] = str(exc)
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components',{})
    if isinstance(components,dict) and COMPONENT in components:
        components[COMPONENT] = reconcile(components[COMPONENT],now=now)
