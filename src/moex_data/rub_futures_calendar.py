"""Receipt-bound published futures calendar; never proof of an actual completed session."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

POLICY = 'rub_futures_published_calendar.v1'
SCOPE = 'PUBLISHED_CALENDAR_ONLY'
BASE_URL = 'https://apim.moex.com/iss/calendars/futures.json'
MAX_BYTES = 2_000_000
MAX_RECEIPT_SECONDS = 1200
MOSCOW = ZoneInfo('Europe/Moscow')
COLUMNS = ['tradedate', 'is_traded', 'trade_session_date', 'reason', 'updatetime']
DENIED = ('factual_authority', 'consumer_factual_use_allowed', 'session_completion_proven',
          'forecast_trading_targets_accepted', 'historical_pit_acceptance', 'full_forecast_ready', 'action_authority')


def _utc(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('aware timestamp required')
    return value.astimezone(timezone.utc)


def _date(value):
    if not isinstance(value, str):
        raise ValueError('canonical date required')
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError('canonical date required')
    return result


def interval(now):
    today = _utc(now).astimezone(MOSCOW).date()
    return max(date(today.year, 1, 1), today - timedelta(days=7)), min(date(today.year, 12, 31), today + timedelta(days=14))


def source_url(start, end):
    if not isinstance(start, date) or not isinstance(end, date) or start > end or (end - start).days > 28 or start.year != end.year:
        raise ValueError('bounded same-year calendar interval required')
    return BASE_URL + '?' + urlencode({'show_all_days': 1, 'from': start.isoformat(), 'till': end.isoformat()})


def _json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON value')))


def _rows(raw, *, start, end, received_at, mapping_end):
    source_url(start, end)
    received = _utc(received_at)
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid calendar response size')
    payload = _json(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get('off_days'), dict):
        raise ValueError('off_days object required')
    block = payload['off_days']
    if block.get('columns') != COLUMNS or not isinstance(block.get('data'), list):
        raise ValueError('calendar schema mismatch')
    expected = {start + timedelta(days=i) for i in range((end - start).days + 1)}
    days = {}
    for cells in block['data']:
        if not isinstance(cells, list) or len(cells) != len(COLUMNS):
            raise ValueError('invalid calendar row')
        civil, traded, session, reason, updated = cells
        day = _date(civil)
        if day not in expected or civil in days or type(traded) is not int or traded not in (0, 1):
            raise ValueError('duplicate/out-of-range date or invalid is_traded')
        if reason not in ('H', 'W', 'N', 'T'):
            raise ValueError('unsupported calendar reason')
        if (reason == 'H' and traded != 0) or (reason == 'T' and traded != 1):
            raise ValueError('calendar reason and trading flag conflict')
        if updated is not None:
            if not isinstance(updated, str):
                raise ValueError('invalid source update time')
            stamp = datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
            if stamp.strftime('%Y-%m-%d %H:%M:%S') != updated or stamp.date() > received.astimezone(MOSCOW).date():
                raise ValueError('noncanonical or future source update date')
        if reason == 'W':
            destination = _date(session)
            if traded != 1 or not day < destination <= mapping_end or destination.year != start.year:
                raise ValueError('weekend mapping outside allowed forward boundary')
        elif session is not None:
            raise ValueError('session mapping allowed only for W rows')
        days[civil] = {'civil_date': civil, 'is_traded': traded, 'trade_session_date': session,
            'reason': reason, 'source_update_time': updated,
            'trading_date': (session or civil) if traded else None}
    if len(days) != len(expected):
        raise ValueError('calendar coverage is incomplete')
    return days


def _destinations(days, *, allow_missing_after=None):
    for item in days.values():
        if item['reason'] == 'W':
            target = days.get(item['trade_session_date'])
            if target is None and allow_missing_after is not None and _date(item['trade_session_date']) > allow_missing_after:
                continue
            if target is None or target['is_traded'] != 1 or target['reason'] == 'W':
                raise ValueError('weekend destination is not a normal trading date')


def parse(raw, *, start, end, received_at):
    days = _rows(raw, start=start, end=end, received_at=received_at, mapping_end=end)
    _destinations(days)
    return [days[key] for key in sorted(days)]


def _expansion_cap(now):
    today = _utc(now).astimezone(MOSCOW).date()
    return min(date(today.year, 12, 31), today + timedelta(days=21))


def _expanded_end(raw, *, start, end, received_at):
    # The first response is inspected, never admitted. Only a fully structured W
    # destination can justify a bounded second request; no permissive fallback.
    days = _rows(raw, start=start, end=end, received_at=received_at, mapping_end=_expansion_cap(received_at))
    _destinations(days, allow_missing_after=end)
    return max([end, *[_date(item['trade_session_date']) for item in days.values() if item['reason'] == 'W']])


def _fetch(url, *, env):
    import requests
    match = re.fullmatch(re.escape(BASE_URL) + r'\?show_all_days=1&from=(\d{4}-\d{2}-\d{2})&till=(\d{4}-\d{2}-\d{2})', url)
    if not match or source_url(_date(match[1]), _date(match[2])) != url:
        raise ValueError('calendar route outside authenticated scope')
    token = env.get('MOEX_API_KEY', '')
    if not isinstance(token, str) or not token.strip():
        raise ValueError('MOEX_API_KEY required for authenticated calendar')
    ca = env.get('REQUESTS_CA_BUNDLE') or env.get('CURL_CA_BUNDLE') or True
    if ca is not True and (not isinstance(ca, str) or not ca.strip()):
        raise ValueError('invalid configured CA bundle')
    try:
        with requests.get(url, headers={'Authorization': 'Bearer ' + token.strip(),
                'Accept': 'application/json', 'User-Agent': 'moex-robot-futures-calendar/1'},
                timeout=(5, 10), allow_redirects=False, stream=True, verify=ca) as response:
            if response.status_code != 200 or response.url != url or response.headers.get('Content-Type', '').split(';')[0].strip() != 'application/json':
                raise ValueError('unexpected calendar response route/status/type')
            raw = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                raw.extend(chunk)
                if len(raw) > MAX_BYTES:
                    raise ValueError('calendar response too large')
        return bytes(raw)
    except requests.RequestException:
        raise ValueError('calendar HTTPS request failed') from None


def _freeze(path, raw):
    if path.is_symlink():
        raise ValueError('evidence symlink refused')
    try:
        with path.open('xb') as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError('evidence collision')


def load(*, root, env=None, now_fn=lambda: datetime.now(timezone.utc), fetch=None):
    started = _utc(now_fn())
    start, end = interval(started)
    url = source_url(start, end)
    raw = (fetch or _fetch)(url, env=os.environ if env is None else env)
    received = _utc(now_fn())
    if not started <= received or interval(received) != (start, end):
        raise ValueError('calendar acquisition crossed clock/date boundary')
    original_end = end
    expanded = _expanded_end(raw, start=start, end=end, received_at=received)
    attempts = 1
    if expanded > end:
        end = expanded
        url = source_url(start, end)
        raw = (fetch or _fetch)(url, env=os.environ if env is None else env)
        final_received = _utc(now_fn())
        if not received <= final_received or interval(final_received) != (start, original_end):
            raise ValueError('calendar extension crossed clock/date boundary')
        received = final_received
        attempts = 2
    days = parse(raw, start=start, end=end, received_at=received)
    directory = Path(root) / 'raw/external/moex_futures_calendar'
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise ValueError('evidence directory symlink refused')
    directory.mkdir(parents=True, exist_ok=True)
    raw_hash = sha256(raw).hexdigest()
    _freeze(directory / (raw_hash + '.json'), raw)
    data = {'policy': POLICY, 'scope': SCOPE, 'source_url': url,
        'coverage_start': start.isoformat(), 'coverage_end': end.isoformat(),
        'initial_query_end': original_end.isoformat(), 'fetch_attempts': attempts,
        'requested_at': started.isoformat(), 'received_at': received.isoformat(),
        'system_available_at': received.isoformat(), 'raw_sha256': raw_hash, 'days': days,
        'source_publication_time': None, 'source_update_timezone_verified': False,
        'calendar_plan_usable': True, 'actual_session_state': 'UNKNOWN', **dict.fromkeys(DENIED, False)}
    encoded = json.dumps(data, sort_keys=True, separators=(',', ':')).encode()
    digest = sha256(encoded).hexdigest()
    manifest = directory / (digest + '.manifest.json')
    _freeze(manifest, encoded)
    return {**data, 'manifest_path': str(manifest), 'manifest_sha256': digest}


def _digest(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('invalid digest')
    return value


def _read(path, digest):
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('evidence symlink refused')
    with path.open('rb') as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or sha256(raw).hexdigest() != digest:
        raise ValueError('evidence hash mismatch')
    return raw


def reconcile(component, *, now):
    result = deepcopy(component)
    data = result.get('data')
    if not isinstance(data, dict):
        return result
    try:
        now = _utc(now)
        if result.get('status') != 'READY' or result.get('refresh_error') or data.get('calendar_plan_usable') is not True:
            raise ValueError('latest calendar refresh not admitted')
        if data.get('policy') != POLICY or data.get('scope') != SCOPE or any(data.get(k) is not False for k in DENIED):
            raise ValueError('calendar scope or authority mismatch')
        if data.get('actual_session_state') != 'UNKNOWN' or data.get('source_publication_time') is not None or data.get('source_update_timezone_verified') is not False:
            raise ValueError('unsupported calendar timestamp or state claim')
        requested, received = _utc(data['requested_at']), _utc(data['received_at'])
        if not requested <= received <= now or (now - received).total_seconds() > MAX_RECEIPT_SECONDS or data.get('system_available_at') != data['received_at']:
            raise ValueError('expired calendar receipt or invalid chronology')
        start, original_end = interval(requested)
        end = _date(data['coverage_end'])
        attempts = data.get('fetch_attempts')
        if (interval(received) != (start, original_end) or data.get('coverage_start') != start.isoformat()
                or data.get('initial_query_end') != original_end.isoformat()
                or not original_end <= end <= _expansion_cap(requested)
                or type(attempts) is not int or attempts != (2 if end > original_end else 1)
                or data.get('source_url') != source_url(start, end)):
            raise ValueError('calendar query identity mismatch')
        digest = _digest(data['manifest_sha256'])
        path = Path(data['manifest_path'])
        if path.name != digest + '.manifest.json':
            raise ValueError('manifest name mismatch')
        evidence = _json(_read(path, digest))
        if not isinstance(evidence, dict) or set(evidence) != set(data) - {'manifest_path', 'manifest_sha256', 'read_freshness_reason'} or any(data[k] != value for k, value in evidence.items()):
            raise ValueError('normalized evidence mismatch')
        raw_hash = _digest(data['raw_sha256'])
        raw = _read(path.parent / (raw_hash + '.json'), raw_hash)
        if parse(raw, start=start, end=end, received_at=received) != data['days']:
            raise ValueError('calendar raw replay mismatch')
        data['read_freshness_reason'] = None
    except (ValueError, TypeError, KeyError, OSError, OverflowError) as exc:
        result['status'] = 'UNAVAILABLE'
        data.update(calendar_plan_usable=False, actual_session_state='UNKNOWN', **dict.fromkeys(DENIED, False))
        data['read_freshness_reason'] = str(exc)
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components', {})
    if isinstance(components.get('futures_calendar'), dict):
        components['futures_calendar'] = reconcile(components['futures_calendar'], now=now)
