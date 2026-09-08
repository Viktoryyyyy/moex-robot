from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest
from moex_data import rub_factual_release as release
from moex_data.rub_factual_release_acceptance import projection_completeness
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

NOW = datetime(2026, 9, 8, 13, tzinfo=timezone.utc)
COMMIT = 'a' * 40


def snapshot(status='PARTIAL'):
    stamp = NOW.isoformat()
    instruments = {key: {'secid': key, 'last': 12.8, 'oi': 0, 'timestamp': stamp,
        'stale': False, 'price_oi_usable': key != 'cnyrub_tom'} for key in ('cnyrub_tom', 'cr_front')}
    metric = {'metric_id': 'cny.basis', 'pair_id': 'CNY/RUB', 'status': 'READY', 'value': -0.2,
        'units': 'RUB_per_CNY', 'legs': ['cr_front', 'cnyrub_tom'], 'formula': 'a-b',
        'source_refs': {'cr_front': 'official'}, 'source_timestamps': {'cr_front': stamp},
        'freshness': {'status': 'READY'}, 'synchronized': True, 'data_as_of': stamp,
        'action_authority': False}
    return {'identity': {'generated_at_utc': stamp}, 'components': {
        'synchronized_live_market_oi': {'status': 'READY', 'data': {'instruments': instruments,
            'quality': {'spot_price_usable': True}, 'synchronization': {}}},
        'live_basis_carry': {'status': status, 'data': {'status': status, 'current_live_scope_status': status,
            'pairs': {'cny': {'pair_id': 'CNY/RUB', 'metrics': [metric]}}}}}}


def facts(value):
    return {item['factor']: item for item in value['facts']}


@pytest.mark.parametrize('status', ['READY', 'PARTIAL'])
def test_complete_projection_same_asof_and_immutable(status, tmp_path):
    original = snapshot(status); before = deepcopy(original)
    built = release.build(original, now=NOW, code_revision=COMMIT)
    direct = release.describe(original)
    canonical = release.describe(apply_read_freshness(original, now=NOW))
    for value in (built, direct, canonical):
        projection_completeness(original, value, now=NOW)
        assert facts(value)['cnyrub_tom']['values'] == {'last': 12.8}
        assert facts(value)['basis_carry'] == facts(built)['basis_carry']
        assert value['horizons']['D1']['target_date_proven'] is False
    directory = release.export(original, now=NOW, code_revision=COMMIT, output=tmp_path)
    replay = release.build(json.loads((directory/'input_snapshot.json').read_text()), now=NOW, code_revision=COMMIT)
    assert replay == built
    assert original == before


@pytest.mark.parametrize('defect', ['item_false', 'quality_false', 'quality_missing', 'stale', 'expired',
    'missing', 'none', 'nan', 'bool', 'negative', 'future'])
def test_spot_refusal_and_no_usd_tom_invention(defect):
    original = snapshot(); data = original['components']['synchronized_live_market_oi']['data']
    spot = data['instruments']['cnyrub_tom']
    if defect == 'item_false': spot['spot_price_usable'] = False
    elif defect == 'quality_false': data['quality']['spot_price_usable'] = False
    elif defect == 'quality_missing': del data['quality']['spot_price_usable']
    elif defect == 'stale': spot['stale'] = True
    elif defect == 'expired': spot['timestamp'] = (NOW-timedelta(seconds=61)).isoformat()
    elif defect == 'future': spot['timestamp'] = (NOW+timedelta(seconds=6)).isoformat()
    elif defect == 'missing': del data['instruments']['cnyrub_tom']
    else: spot['last'] = {'none': None, 'nan': float('nan'), 'bool': True, 'negative': -1}[defect]
    value = release.describe(original)
    assert 'cnyrub_tom' not in facts(value)
    assert not any(factor in facts(value) for factor in ('usd_tom', 'usd_spot'))
    projection_completeness(original, value, now=NOW)


@pytest.mark.parametrize('defect', ['expired', 'missing'])
def test_only_dependent_metric_expires(defect):
    original = snapshot(); components = original['components']
    metrics = components['live_basis_carry']['data']['pairs']['cny']['metrics']
    other = deepcopy(metrics[0]); other.update(metric_id='cny.independent', legs=['cr_front'], value=0)
    metrics.append(other)
    instruments = components['synchronized_live_market_oi']['data']['instruments']
    if defect == 'expired': instruments['cnyrub_tom']['timestamp'] = (NOW-timedelta(seconds=61)).isoformat()
    else: del instruments['cnyrub_tom']
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    assert [m['values']['metric_id'] for m in facts(value)['basis_carry']['values']['metrics']] == ['cny.independent']


@pytest.mark.parametrize('conflict', [False, True])
def test_duplicates_deterministic_or_conflicts_excluded(conflict):
    original = snapshot(); metrics = original['components']['live_basis_carry']['data']['pairs']['cny']['metrics']
    metrics.append(deepcopy(metrics[0]))
    if conflict: metrics[1]['value'] = 1
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    assert ('basis_carry' in facts(value)) is not conflict
    if not conflict:
        entries = facts(value)['basis_carry']['values']['metrics']
        assert len(entries) == 1 and entries[0]['snapshot_path'].endswith('.metrics.0')


@pytest.mark.parametrize('value', [None, True, float('nan'), float('inf')])
def test_invalid_metric_values_never_exported(value):
    original = snapshot()
    original['components']['live_basis_carry']['data']['pairs']['cny']['metrics'][0]['value'] = value
    result = release.describe(original)
    assert 'basis_carry' not in facts(result)
    projection_completeness(original, result, now=NOW)


@pytest.mark.parametrize('factor', ['cnyrub_tom', 'basis_carry'])
def test_reverse_oracle_detects_silent_omission(factor):
    original = snapshot(); value = release.build(original, now=NOW, code_revision=COMMIT)
    value['facts'] = [fact for fact in value['facts'] if fact['factor'] != factor]
    with pytest.raises(AssertionError): projection_completeness(original, value, now=NOW)
