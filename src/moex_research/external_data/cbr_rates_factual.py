"""Current receipt-bound CBR rates with immutable replay; no historical/model admission."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import ssl
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from zoneinfo import ZoneInfo

from . import cbr

POLICY = 'cbr_rates_received_reference.v1'
MAX_BYTES = 2_000_000
MAX_RECEIPT_SECONDS = 1200
MAX_RUONIA_DAYS = 7
MOSCOW = ZoneInfo('Europe/Moscow')
SOURCES = {'ruonia': (cbr.RUONIA_ROUTE, 30), 'key_rate': (cbr.KEY_RATE_ROUTE, 3650)}
KEY_RATE_WIDE_HEADER = ('Date effective', 'Key rate', 'Key rate changes (p.p.)',
    'Liquidity absorption', 'Liquidity provision', 'Refinancing rate')
# Exact grouped headings observed in the archived official response on 2026-09-08.
KEY_RATE_SUBHEADERS = (
    ('Rates on deposit standing facilities1', 'Rates on open market oprations (auctions)2', 'Rates on lending standing facilities'),
    ('Max bid rate on deposit auctions3', 'Min bid rate on repo auctions4', 'Min bid rate on loan auctions5'),
    ('Main and fine-tuning', 'for 1 month', 'for 1 year', 'Primary mechanism6', 'Supplementary mechanism7',
     '1-day lending standing facilities (loans, repos, FX swaps8)',
     'Lombard loans9 and loans secured by non-marketable assets10, for terms from 2 to 90 days',
     'Loans secured by non-marketable assets10, for terms over 90 days', 'Loans secured by gold11, for terms over 1 day'),
)


def _utc(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('aware timestamp required')
    return value.astimezone(timezone.utc)


def _url(source, day):
    route, days = SOURCES[source]
    return cbr._url(route, day - timedelta(days=days), day)


def parse(raw, *, source, url, received_at, now):
    received, now = _utc(received_at), _utc(now)
    if received > now or not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid raw or receipt chronology')
    day = received.astimezone(MOSCOW).date()
    if source not in SOURCES or url != _url(source, day):
        raise ValueError('source query identity mismatch')
    if source == 'ruonia':
        tables = cbr._TableParser()
        tables.feed(raw.decode('utf-8'))
        matches = [(table, index) for table in tables.tables for index, cells in enumerate(table)
                   if cells == cbr.RUONIA_HEADERS]
        if len(matches) != 1:
            raise ValueError('ambiguous RUONIA table')
        table, index = matches[0]
        for cells in table[index + 1:]:
            if len(cells) != len(cbr.RUONIA_HEADERS):
                raise ValueError('malformed RUONIA table row')
            cbr.parse_date(cells[0], field='observation_date')
    else:
        tables = cbr._TableParser()
        tables.feed(raw.decode('utf-8'))
        matches = [(table, index) for table in tables.tables for index, cells in enumerate(table)
                   if cells[:2] == cbr.KEY_RATE_HEADERS]
        if len(matches) != 1:
            raise ValueError('ambiguous key-rate table')
        table, index = matches[0]
        header, rows = table[index], table[index + 1:]
        if header == KEY_RATE_WIDE_HEADER:
            if rows[:len(KEY_RATE_SUBHEADERS)] != KEY_RATE_SUBHEADERS:
                raise ValueError('unsupported key-rate grouped headings')
            rows = rows[len(KEY_RATE_SUBHEADERS):]
        elif header != cbr.KEY_RATE_HEADERS:
            raise ValueError('unsupported key-rate header')
        if not rows:
            raise ValueError('key-rate table has no observations')
        for cells in rows:
            if len(cells) < 2:
                raise ValueError('malformed key-rate row')
            effective = cbr.parse_date(cells[0], field='effective_date')
            if not day - timedelta(days=SOURCES[source][1]) <= effective <= day:
                raise ValueError('raw key-rate row outside requested dates')
    parser = cbr.parse_ruonia_html if source == 'ruonia' else cbr.parse_key_rate_html
    records = parser(raw, retrieved_at_utc=received, source_route=url)
    field = 'observation_date' if source == 'ruonia' else 'effective_date'
    for row in records:
        if not day - timedelta(days=SOURCES[source][1]) <= date.fromisoformat(row[field]) <= day:
            raise ValueError('row outside requested dates')
        if source == 'ruonia' and date.fromisoformat(row['publication_date']) > day:
            raise ValueError('future RUONIA publication date')
    row = max(records, key=lambda item: item[field])
    value = row['ruonia_rate_pct' if source == 'ruonia' else 'key_rate_pct']
    if isinstance(value, bool) or not math.isfinite(value) or not 0 <= value <= 100:
        raise ValueError('invalid annual percent rate')
    if source == 'ruonia':
        if row['calculation_status'] != 'Standard':
            raise ValueError('latest RUONIA calculation status not accepted')
        if (now.astimezone(MOSCOW).date() - date.fromisoformat(row[field])).days > MAX_RUONIA_DAYS:
            raise ValueError('RUONIA observation expired')
    return {'metric_id': 'cbr_ruonia_rate_pct' if source == 'ruonia' else 'cbr_key_rate_pct',
        'value': value, 'unit': 'PERCENT_PER_ANNUM', field: row[field],
        'source_publication_date': row['publication_date'] if source == 'ruonia' else None,
        'source_publication_time': None, 'system_available_at': received.isoformat(),
        'scope': 'latest_received_published_rate', 'historical_pit_acceptance': False}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('CBR redirect refused')


def _fetch(url):
    opener = build_opener(_NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))
    with opener.open(Request(url, headers={'Accept': 'text/html', 'User-Agent': 'moex-robot-cbr-factual/1'}), timeout=10) as response:
        if response.status != 200 or response.geturl() != url or response.headers.get_content_type() != 'text/html':
            raise ValueError('unexpected CBR response')
        raw = response.read(MAX_BYTES + 1)
    if not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('CBR response size invalid')
    return raw


def _freeze(path, raw):
    if path.is_symlink():
        raise ValueError('symlink evidence refused')
    try:
        with path.open('xb') as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError('evidence collision')


def load(*, root, now_fn=lambda: datetime.now(timezone.utc), fetch=_fetch):
    root = Path(root)
    directory = root / 'raw/external/cbr_rates'
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise ValueError('symlink evidence directory refused')
    directory.mkdir(parents=True, exist_ok=True)
    receipts, facts = {}, {}
    for source in SOURCES:
        started = _utc(now_fn())
        url = _url(source, started.astimezone(MOSCOW).date())
        raw = fetch(url)
        received = _utc(now_fn())
        if received < started or received.astimezone(MOSCOW).date() != started.astimezone(MOSCOW).date():
            raise ValueError('acquisition crossed date or clock boundary')
        digest = sha256(raw).hexdigest()
        _freeze(directory / (digest + '.html'), raw)
        facts[source] = parse(raw, source=source, url=url, received_at=received, now=received)
        receipts[source] = {'source_url': url, 'requested_at': started.isoformat(),
            'received_at': received.isoformat(), 'raw_sha256': digest}
    completed = _utc(now_fn())
    if any(not _utc(r['received_at']) <= completed or (completed - _utc(r['received_at'])).total_seconds() > MAX_RECEIPT_SECONDS for r in receipts.values()):
        raise ValueError('acquisition receipt expired')
    evidence = {'policy': POLICY, 'observations': facts, 'receipts': receipts, 'received_at': completed.isoformat(),
        'system_available_at': completed.isoformat(), 'scope': 'cbr_key_rate_and_ruonia_received_reference',
        'factual_authority': True, 'consumer_factual_use_allowed': True,
        'full_macro_complete': False, 'historical_pit_acceptance': False,
        'forecast_use_allowed': False, 'action_authority': False}
    encoded = json.dumps(evidence, sort_keys=True, separators=(',', ':')).encode()
    digest = sha256(encoded).hexdigest()
    manifest = directory / (digest + '.json')
    _freeze(manifest, encoded)
    return {**evidence, 'manifest_path': str(manifest), 'manifest_sha256': digest}


def _digest(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('invalid evidence digest')
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
        if result.get('status') != 'READY' or result.get('refresh_error'):
            raise ValueError('latest refresh not ready')
        if data.get('policy') != POLICY or data.get('scope') != 'cbr_key_rate_and_ruonia_received_reference':
            raise ValueError('policy identity mismatch')
        if any(data.get(k) is not False for k in ('full_macro_complete', 'historical_pit_acceptance', 'forecast_use_allowed', 'action_authority')):
            raise ValueError('unsupported authority')
        if data.get('factual_authority') is not True or data.get('consumer_factual_use_allowed') is not True:
            raise ValueError('factual admission absent')
        digest = _digest(data['manifest_sha256'])
        path = Path(data['manifest_path'])
        if path.name != digest + '.json':
            raise ValueError('manifest name mismatch')
        evidence = json.loads(_read(path, digest))
        if not isinstance(evidence, dict):
            raise ValueError('manifest object required')
        if set(evidence) != set(data) - {'manifest_path', 'manifest_sha256', 'read_freshness_reason'} or any(data[k] != value for k, value in evidence.items()):
            raise ValueError('normalized evidence mismatch')
        completed = _utc(data['received_at'])
        if not completed <= now or data['system_available_at'] != data['received_at']:
            raise ValueError('invalid completion chronology')
        if not isinstance(data.get('receipts'), dict) or not isinstance(data.get('observations'), dict):
            raise ValueError('receipts and observations objects required')
        if set(data['receipts']) != set(SOURCES) or set(data['observations']) != set(SOURCES):
            raise ValueError('required source absent')
        for source, receipt in data['receipts'].items():
            if not isinstance(receipt, dict) or not isinstance(data['observations'][source], dict):
                raise ValueError('source receipt and observation objects required')
            received = _utc(receipt['received_at'])
            if not _utc(receipt['requested_at']) <= received <= completed <= now or (now - received).total_seconds() > MAX_RECEIPT_SECONDS:
                raise ValueError('source receipt expired or chronology invalid')
            raw_hash = _digest(receipt['raw_sha256'])
            raw = _read(path.parent / (raw_hash + '.html'), raw_hash)
            fact = parse(raw, source=source, url=receipt['source_url'], received_at=received, now=now)
            if fact != data['observations'][source]:
                raise ValueError('raw replay mismatch')
        data['read_freshness_reason'] = None
    except (ValueError, TypeError, KeyError, OSError, OverflowError) as exc:
        result['status'] = 'UNAVAILABLE'
        data['factual_authority'] = data['consumer_factual_use_allowed'] = False
        for key in ('full_macro_complete', 'historical_pit_acceptance', 'forecast_use_allowed', 'action_authority'):
            data[key] = False
        data['read_freshness_reason'] = str(exc)
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components', {})
    if isinstance(components.get('cbr_rates_verified'), dict):
        components['cbr_rates_verified'] = reconcile(components['cbr_rates_verified'], now=now)
