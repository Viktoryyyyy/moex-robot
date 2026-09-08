"""Read-time permissions require evidence; partial basis facts stay explicit."""
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from moex_data.rub_snapshot_read_freshness import apply_read_freshness
from moex_data.rub_production_source_matrix import build

NOW = datetime(2026, 9, 8, 9, tzinfo=timezone.utc)


def snapshot():
    stamp = NOW.isoformat()
    data = {'factual_authority': True, 'consumer_factual_use_allowed': True,
            'current_intraday': {'status': 'FRESH', 'last_success_at': stamp,
                'factual': dict(snapshot_ts=stamp, source_publication_time=stamp,
                               availability_ts_utc=stamp, ingest_ts_utc=stamp)}}
    return {'identity': {'generated_at_utc': stamp},
            'components': {name: {'status': 'READY', 'data': deepcopy(data)}
                           for name in ('futoi_live', 'futoi_live_cr')},
            'authority': {'futoi_factual_authority': True, 'futoi_by_instrument': {
                name: {'factual_authority': True}
                for name in ('si_futures_family', 'cr_futures_family')}}}


@pytest.mark.parametrize('name,instrument', [('futoi_live', 'si_futures_family'),
                                          ('futoi_live_cr', 'cr_futures_family')])
@pytest.mark.parametrize('defect', ['component_missing', 'data_missing', 'record_missing',
                                  'record_null', 'expired', 'persisted_block'])
def test_missing_current_evidence_revokes_flags(name, instrument, defect):
    source = snapshot()
    component = source['components'][name]
    if defect == 'component_missing':
        del source['components'][name]
    elif defect == 'data_missing':
        del component['data']
    elif defect == 'record_missing':
        del component['data']['current_intraday']
    elif defect == 'record_null':
        component['data']['current_intraday'] = None
    elif defect == 'expired':
        component['data']['current_intraday']['factual']['snapshot_ts'] = '2026-09-07T09:00:00+00:00'
    else:
        component['data']['consumer_factual_use_allowed'] = False
    original = deepcopy(source)
    result = apply_read_freshness(source, now=NOW)
    assert result['authority']['futoi_by_instrument'][instrument]['factual_authority'] is False
    if name == 'futoi_live':
        assert result['authority']['futoi_factual_authority'] is False
    surviving = result['components'].get(name, {})
    if surviving:
        assert surviving['status'] == 'UNAVAILABLE'
    if isinstance(surviving.get('data'), dict):
        assert surviving['data']['factual_authority'] is False
        assert surviving['data']['consumer_factual_use_allowed'] is False
    assert source == original


def test_fresh_current_permissions_are_preserved():
    result = apply_read_freshness(snapshot(), now=NOW)
    assert result['authority']['futoi_factual_authority'] is True
    assert all(item['factual_authority'] for item in result['authority']['futoi_by_instrument'].values())


@pytest.mark.parametrize('name', ['futoi_live', 'futoi_live_cr'])
@pytest.mark.parametrize('defect', ['record_missing', 'empty_fact'])
def test_component_permissions_revoke_even_without_top_level_authority(name, defect):
    source = snapshot()
    del source['authority']
    data = source['components'][name]['data']
    if defect == 'record_missing':
        del data['current_intraday']
    else:
        data['current_intraday']['factual'] = {}
    result = apply_read_freshness(source, now=NOW)
    component = result['components'][name]
    assert component['status'] == 'UNAVAILABLE'
    assert component['data']['factual_authority'] is False
    assert component['data']['consumer_factual_use_allowed'] is False
    assert 'authority' not in result


@pytest.mark.parametrize('status,metrics,factual,forecast', [
    ('PARTIAL', [{'metric_id': 'usd_rub.front_next', 'status': 'READY'},
                 {'metric_id': 'cny_rub.front_spot', 'status': 'UNAVAILABLE'}], True, False),
    ('UNAVAILABLE', [{'metric_id': 'usd_rub.front_next', 'status': 'READY'}], False, False),
    ('READY', [], False, False),
    ('READY', [{'metric_id': 'usd_rub.front_next', 'status': 'READY'}], True, True),
])
def test_basis_matrix_distinguishes_individual_facts_from_complete_scope(status, metrics, factual, forecast):
    source = snapshot()
    source['components']['synchronized_live_market_oi'] = {'data': {'instruments': {
        'si_front': {'timestamp': NOW.isoformat(), 'stale': False},
        'si_next': {'timestamp': NOW.isoformat(), 'stale': False}}}}
    metrics = [{**metric, 'value': 0.0, 'legs': ['si_front', 'si_next'],
                'units': 'RUB_per_USD', 'pair_id': 'USD/RUB'} for metric in metrics]
    source['components']['live_basis_carry'] = {'status': status, 'data': {
        'ready_metric_count': 100, 'pairs': {'usd_rub': {'metrics': metrics}}}}
    row = next(row for row in build(source)['rows'] if row['block_id'] == 'basis_carry')
    assert row['factual_context_usable'] is factual
    assert row['usable_for_full_forecast'] is forecast
    assert row['admitted_metric_ids'] == (['usd_rub.front_next'] if factual else [])
    assert row['factual_authority_scope'] == 'individual_READY_metrics_only'
