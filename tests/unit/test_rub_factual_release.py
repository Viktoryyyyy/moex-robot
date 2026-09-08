from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import pytest
from moex_data.rub_factual_release import build, export

NOW = datetime(2026, 9, 8, 4, 0, tzinfo=timezone.utc)
COMMIT = 'a' * 40


def snapshot():
    return {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {}, 'authority': {'broker_execution': False}}


def test_missing_required_input_never_becomes_neutral_or_complete(tmp_path):
    value = snapshot()
    before = deepcopy(value)
    release = build(value, now=NOW, code_revision=COMMIT)
    assert value == before
    assert release['status'] == 'INCOMPLETE'
    assert 'external_cny' in release['blocking_required_factors']
    for horizon in ('D1', 'W1'):
        assert release['horizons'][horizon]['model_probability'] is None
        assert release['horizons'][horizon]['target_date_proven'] is False
    directory = export(value, now=NOW, code_revision=COMMIT, output=tmp_path)
    manifest = json.loads((directory / 'manifest.json').read_text())
    assert sha256((directory / 'release.json').read_bytes()).hexdigest() == manifest['release_sha256']
    assert sha256((directory / 'input_snapshot.json').read_bytes()).hexdigest() == manifest['input_snapshot_sha256']
    replay = build(json.loads((directory / 'input_snapshot.json').read_text()), now=NOW, code_revision=COMMIT)
    assert replay == release


def test_expired_current_pair_is_not_presented_as_a_usable_fact():
    value = snapshot()
    stamp = (NOW - timedelta(seconds=1201)).isoformat()
    value['components']['futoi_live'] = {'status': 'READY', 'data': {
        'factual_authority': True, 'consumer_factual_use_allowed': True,
        'current_intraday': {'status': 'FRESH', 'last_success_at': stamp,
            'factual': {'snapshot_ts': stamp, 'source_publication_time': stamp,
                'availability_ts_utc': stamp, 'ingest_ts_utc': stamp}}}}
    release = build(value, now=NOW, code_revision=COMMIT)
    assert not any(f['factor'] == 'futoi_live' for f in release['facts'])
    assert 'futoi_live' in release['blocking_required_factors']


def test_future_snapshot_or_unpinned_revision_cannot_be_exported():
    with pytest.raises(ValueError): build(snapshot(), now=NOW - timedelta(seconds=1), code_revision=COMMIT)
    with pytest.raises(ValueError): build(snapshot(), now=NOW, code_revision='main')


def test_source_native_oi_field_is_preserved():
    from moex_data.rub_factual_release import describe
    value = snapshot()
    value['components']['synchronized_live_market_oi'] = {'data': {'instruments': {
        'si_front': {'secid': 'SiU6', 'timestamp': NOW.isoformat(), 'price_oi_usable': True,
                     'last': 86748., 'oi': 123456}}}}
    fact = describe(value)['facts'][0]
    assert fact['values'] == {'last': 86748., 'oi': 123456}


def test_missing_components_are_incomplete_without_breaking_legacy_read():
    value = snapshot()
    del value['components']
    result = build(value, now=NOW, code_revision=COMMIT)
    assert result['facts'] == []
    assert result['status'] == 'INCOMPLETE'
    assert 'components' not in value


def _external_cny(tmp_path):
    from moex_research.external_data import fred_cny_factual as fred
    class Response:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def geturl(self): return fred.URL
        def read(self, limit): return b'observation_date,DEXCHUS\n2026-08-28,6.7260\n'
    return {'status': 'READY', 'data': fred.load(root=tmp_path, now_fn=lambda: NOW,
        opener=lambda *args, **kwargs: Response())}


@pytest.mark.parametrize('defect', ['none', 'expired', 'raw_changed', 'manifest_changed',
    'missing_evidence', 'changed_value', 'retained', 'refresh_failed'])
def test_direct_describe_external_fact_agrees_with_evidence_matrix(tmp_path, defect):
    from pathlib import Path
    from moex_data.rub_factual_release import describe
    value = snapshot()
    component = _external_cny(tmp_path)
    value['components']['external_cny'] = component
    value['components']['synchronized_live_market_oi'] = {'data': {'instruments': {
        'si_front': {'price_oi_usable': True, 'last': 80000.0, 'oi': 123}}}}
    data = component['data']
    path = Path(data['manifest_path'])
    if defect == 'expired':
        value['live_read_freshness'] = {'read_at_utc': (NOW + timedelta(seconds=1201)).isoformat()}
    elif defect == 'raw_changed': path.with_name(data['raw_sha256'] + '.csv').write_bytes(b'changed')
    elif defect == 'manifest_changed': path.write_bytes(b'{}')
    elif defect == 'missing_evidence': path.unlink()
    elif defect == 'changed_value': data['value'] = 99.0
    elif defect == 'retained': component['status'] = 'RETAINED_PREVIOUS'
    elif defect == 'refresh_failed': component['refresh_error'] = 'failed'
    before = deepcopy(value)
    release = describe(value)
    row = next(row for row in release['matrix'] if row['block_id'] == 'external_cny')
    facts = [fact for fact in release['facts'] if fact['factor'] == 'external_cny']
    assert row['factual_context_usable'] is (defect == 'none')
    assert bool(facts) is row['factual_context_usable']
    assert row['usable_for_full_forecast'] is False
    if facts:
        assert facts[0]['values']['units'] == 'CNY_per_USD'
        assert facts[0]['values']['value'] == 6.726
    assert any(fact['factor'] == 'si_front' for fact in release['facts'])
    assert value == before


@pytest.mark.parametrize('malformed', [None, [], 'bad', 7, True,
    {'status': 'READY', 'data': []}, {'status': 'READY', 'data': 'bad'},
    {'status': 'READY', 'data': True}, {'status': 'READY', 'data': None}])
def test_malformed_external_shapes_do_not_break_release_or_matrix(malformed):
    from moex_data.rub_factual_release import describe
    from moex_data.rub_production_source_matrix import build as matrix_build
    value = snapshot()
    value['components']['external_cny'] = malformed
    before = deepcopy(value)
    row = next(row for row in matrix_build(value)['rows'] if row['block_id'] == 'external_cny')
    assert row['factual_context_usable'] is False
    for result in (describe(value), build(value, now=NOW, code_revision=COMMIT)):
        assert not any(fact['factor'] == 'external_cny' for fact in result['facts'])
        assert 'external_cny' in result['blocking_required_factors']
    assert value == before
