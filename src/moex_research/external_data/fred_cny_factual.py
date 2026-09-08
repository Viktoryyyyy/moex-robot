"""Receipt-bound FRED DEXCHUS daily reference; never live CNH or historical PIT."""
from copy import deepcopy
import csv
from datetime import date, datetime, timezone
from hashlib import sha256
import io
import json
import math
from pathlib import Path
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXCHUS'
POLICY = 'fred_dexchus_dated_reference.v1'
MAX_BYTES = 1_000_000
MAX_RECEIPT_SECONDS = 1200
MAX_OBSERVATION_DAYS = 14


def _utc(value):
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.utcoffset() is None:
        raise ValueError('aware timestamp required')
    return parsed.astimezone(timezone.utc)


def parse(raw, *, now):
    if not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid CSV size')
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))
    if reader.fieldnames != ['observation_date', 'DEXCHUS']:
        raise ValueError('DEXCHUS CSV identity mismatch')
    previous, latest = None, None
    today = _utc(now).astimezone(ZoneInfo('America/New_York')).date()
    for row in reader:
        if set(row) != {'observation_date', 'DEXCHUS'}:
            raise ValueError('malformed CSV row')
        day = date.fromisoformat(row['observation_date'])
        if day.isoformat() != row['observation_date'] or day > today or (previous and day <= previous):
            raise ValueError('unordered, duplicate or future observation date')
        previous = day
        if row['DEXCHUS'] in ('', '.'):
            continue  # Official missing markers are not zero or a new observation.
        rate = float(row['DEXCHUS'])
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError('invalid CNY per USD rate')
        latest = {'observation_date': day.isoformat(), 'value': rate}
    if latest is None or (today - date.fromisoformat(latest['observation_date'])).days > MAX_OBSERVATION_DAYS:
        raise ValueError('no sufficiently recent published observation')
    return latest


def _freeze(path, raw):
    if path.is_symlink():
        raise ValueError('symlink evidence refused')
    try:
        with path.open('xb') as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError('evidence collision')


def load(*, root, now_fn=lambda: datetime.now(timezone.utc), opener=urlopen):
    started = _utc(now_fn())
    with opener(Request(URL, headers={'Accept': 'text/csv', 'User-Agent': 'moex-robot-factual/1'}), timeout=8) as response:
        if response.geturl() != URL:
            raise ValueError('FRED redirect refused')
        raw = response.read(MAX_BYTES + 1)
    received = _utc(now_fn())
    if received < started:
        raise ValueError('receipt precedes request')
    fact = parse(raw, now=received)
    root = Path(root).resolve()
    directory = root / 'raw/external/fred_dexchus'
    if not directory.resolve().is_relative_to(root):
        raise ValueError('evidence directory escapes data root')
    directory.mkdir(parents=True, exist_ok=True)
    raw_hash = sha256(raw).hexdigest()
    _freeze(directory / (raw_hash + '.csv'), raw)
    evidence = {'policy': POLICY, 'source_url': URL, 'series_id': 'DEXCHUS',
        'units': 'CNY_per_USD', 'requested_at': started.isoformat(),
        'received_at': received.isoformat(), 'raw_sha256': raw_hash, **fact}
    encoded = json.dumps(evidence, sort_keys=True, separators=(',', ':')).encode()
    manifest_hash = sha256(encoded).hexdigest()
    manifest = directory / (manifest_hash + '.json')
    _freeze(manifest, encoded)
    return {**evidence, 'manifest_path': str(manifest), 'manifest_sha256': manifest_hash,
        'scope': 'latest_published_daily_reference', 'source_event_time': None,
        'source_publication_time': None, 'system_available_at': received.isoformat(),
        'factual_authority': True, 'consumer_factual_use_allowed': True,
        'intraday_use_allowed': False, 'cnh_quote': False, 'historical_pit_acceptance': False,
        'action_authority': False, 'revision_policy': 'freeze_each_received_vintage_no_backdating'}


def reconcile(component, *, now):
    result = deepcopy(component)
    data = result.get('data')
    if not isinstance(data, dict):
        return result
    try:
        now = _utc(now)
        if result.get('status') != 'READY' or result.get('refresh_error') or data.get('consumer_factual_use_allowed') is not True or data.get('factual_authority') is not True:
            raise ValueError('persisted admission or latest refresh blocked')
        if data.get('policy') != POLICY or data.get('source_url') != URL or data.get('series_id') != 'DEXCHUS' or data.get('units') != 'CNY_per_USD':
            raise ValueError('source identity mismatch')
        received = _utc(data['received_at'])
        if not _utc(data['requested_at']) <= received <= now or (now - received).total_seconds() > MAX_RECEIPT_SECONDS:
            raise ValueError('receipt expired or causal order invalid')
        if data.get('system_available_at') != data['received_at']:
            raise ValueError('availability must equal observed receipt')
        manifest = Path(data['manifest_path'])
        digest = data['manifest_sha256']
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest) or manifest.name != digest + '.json' or manifest.is_symlink():
            raise ValueError('invalid manifest reference')
        with manifest.open('rb') as stream:
            encoded = stream.read(MAX_BYTES + 1)
        if len(encoded) > MAX_BYTES or sha256(encoded).hexdigest() != digest:
            raise ValueError('manifest hash mismatch')
        evidence = json.loads(encoded)
        if any(data.get(key) != value for key, value in evidence.items()):
            raise ValueError('fact differs from received evidence')
        raw_hash = evidence['raw_sha256']
        if len(raw_hash) != 64 or any(c not in '0123456789abcdef' for c in raw_hash):
            raise ValueError('invalid raw hash')
        raw_path = manifest.parent / (raw_hash + '.csv')
        if raw_path.is_symlink():
            raise ValueError('symlink raw evidence refused')
        with raw_path.open('rb') as stream:
            raw = stream.read(MAX_BYTES + 1)
        if sha256(raw).hexdigest() != raw_hash or parse(raw, now=now) != {key: data[key] for key in ('observation_date', 'value')}:
            raise ValueError('raw replay mismatch')
        if data.get('scope') != 'latest_published_daily_reference' or data.get('source_event_time') is not None or data.get('source_publication_time') is not None or any(data.get(key) is not False for key in ('intraday_use_allowed', 'cnh_quote', 'historical_pit_acceptance', 'action_authority')):
            raise ValueError('unsupported authority scope')
        data['read_freshness_reason'] = None
    except (ValueError, TypeError, KeyError, OSError, OverflowError) as exc:
        result['status'] = 'UNAVAILABLE'
        data['factual_authority'] = data['consumer_factual_use_allowed'] = False
        for key in ('intraday_use_allowed', 'cnh_quote', 'historical_pit_acceptance', 'action_authority'):
            data[key] = False
        data['read_freshness_reason'] = str(exc)
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components', {})
    if isinstance(components.get('external_cny'), dict):
        components['external_cny'] = reconcile(components['external_cny'], now=now)
