from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from moex_research.external_data import fred_cny_factual as fred
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

NOW = datetime(2026, 9, 8, 4, 0, tzinfo=timezone.utc)
RAW = b'observation_date,DEXCHUS\n2026-08-27,6.7198\n2026-08-28,6.7260\n2026-08-31,.\n'


class Response:
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def geturl(self): return fred.URL
    def read(self, limit): return RAW[:limit]


def loaded(tmp_path):
    return fred.load(root=tmp_path, now_fn=lambda: NOW, opener=lambda *a, **k: Response())


def test_frozen_reference_replays_and_never_becomes_live_or_historical(tmp_path):
    data = loaded(tmp_path)
    assert data['value'] == 6.726
    assert data['observation_date'] == '2026-08-28'
    assert data['system_available_at'] == NOW.isoformat()
    assert data['source_publication_time'] is None
    assert data['historical_pit_acceptance'] is False
    value = {'components': {'external_cny': {'status': 'READY', 'data': data}}}
    before = deepcopy(value)
    view = apply_read_freshness(value, now=NOW)
    assert value == before
    assert view['components']['external_cny']['data']['consumer_factual_use_allowed'] is True


@pytest.mark.parametrize('defect', ['wrong_series', 'negative', 'nan', 'duplicate', 'future', 'old', 'html'])
def test_invalid_source_cannot_supply_dated_fact(defect):
    raw = {
        'wrong_series': RAW.replace(b'DEXCHUS', b'DEXJPUS'),
        'negative': RAW.replace(b'6.7260', b'-1'),
        'nan': RAW.replace(b'6.7260', b'NaN'),
        'duplicate': RAW + b'2026-08-31,6.7\n',
        'future': RAW + b'2026-09-09,6.7\n',
        'old': b'observation_date,DEXCHUS\n2026-08-01,6.7\n',
        'html': b'<html>unavailable</html>',
    }[defect]
    with pytest.raises(ValueError): fred.parse(raw, now=NOW)


@pytest.mark.parametrize('defect', ['expired', 'future_receipt', 'retained', 'failed', 'changed_fact', 'raw', 'manifest', 'scope', 'blocked', 'availability'])
def test_read_time_rejection_never_falls_back_or_promotes(tmp_path, defect):
    data = loaded(tmp_path)
    component = {'status': 'READY', 'data': data}
    now = NOW
    if defect == 'expired': now += timedelta(seconds=1201)
    elif defect == 'future_receipt': now -= timedelta(seconds=1)
    elif defect == 'retained': component['status'] = 'RETAINED_PREVIOUS'
    elif defect == 'failed': component['refresh_error'] = 'timeout'
    elif defect == 'changed_fact': data['value'] = 7
    elif defect == 'raw': Path(data['manifest_path']).with_name(data['raw_sha256'] + '.csv').write_bytes(b'bad')
    elif defect == 'manifest': Path(data['manifest_path']).write_bytes(b'{}')
    elif defect == 'scope': data['cnh_quote'] = True
    elif defect == 'blocked': data['consumer_factual_use_allowed'] = False
    elif defect == 'availability': data['system_available_at'] = '2000-01-01T00:00:00+00:00'
    view = fred.reconcile(component, now=now)
    assert view['status'] == 'UNAVAILABLE'
    assert view['data']['consumer_factual_use_allowed'] is False
    assert view['data']['action_authority'] is False


def test_new_receipt_does_not_rewrite_prior_vintage(tmp_path):
    first = loaded(tmp_path)
    second = fred.load(root=tmp_path, now_fn=lambda: NOW + timedelta(seconds=1), opener=lambda *a, **k: Response())
    assert first['raw_sha256'] == second['raw_sha256']
    assert first['manifest_path'] != second['manifest_path']
    assert Path(first['manifest_path']).exists()


def test_failed_producer_revokes_retained_factual_flags(tmp_path):
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    prior = {'components': {'external_cny': {'status': 'READY', 'data': loaded(tmp_path)}}}
    def fail(now): raise ValueError('source timeout')
    result = runner._component_payload('external_cny', fail, now=NOW, previous=prior)
    assert result['status'] == 'UNAVAILABLE'
    assert result['data']['consumer_factual_use_allowed'] is False


@pytest.mark.parametrize('malformed', [None, [], 'bad', 7, True, {},
    {'status': 'READY', 'data': None}, {'status': 'READY', 'data': []},
    {'status': 'READY', 'data': 'bad'}, {'status': 'READY', 'data': 7}])
def test_malformed_components_fail_closed_at_reconcile_and_readtime(malformed):
    before = deepcopy(malformed)
    view = fred.reconcile(malformed, now=NOW)
    assert view['status'] == 'UNAVAILABLE'
    for flag in ('factual_authority', 'consumer_factual_use_allowed', 'intraday_use_allowed',
                 'cnh_quote', 'historical_pit_acceptance', 'action_authority'):
        assert view['data'][flag] is False
    assert malformed == before
    snapshot = {'components': {'external_cny': malformed}}
    read = apply_read_freshness(snapshot, now=NOW)
    assert read['components']['external_cny']['status'] == 'UNAVAILABLE'
    assert snapshot['components']['external_cny'] == before


@pytest.mark.parametrize('manifest_value', [None, [], 'bad', 7])
def test_valid_hash_does_not_make_nonobject_manifest_acceptable(tmp_path, manifest_value):
    from hashlib import sha256
    import json
    data = loaded(tmp_path)
    raw = json.dumps(manifest_value).encode()
    digest = sha256(raw).hexdigest()
    path = Path(data['manifest_path']).with_name(digest + '.json')
    path.write_bytes(raw)
    data.update(manifest_path=str(path), manifest_sha256=digest)
    view = fred.reconcile({'status': 'READY', 'data': data}, now=NOW)
    assert view['status'] == 'UNAVAILABLE'
    assert view['data']['consumer_factual_use_allowed'] is False
    assert view['data']['read_freshness_reason'] == 'manifest object required'
