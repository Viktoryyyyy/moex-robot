import json
from pathlib import Path

import pytest

from moex_data import rub_macro_requirements as policy


def test_minimum_keeps_all_blocks_and_unresolved_liquidity():
    result = policy.describe()
    assert result['schema_version'] == 'rub_macro_requirements.v1'
    assert result['status'] == 'ENGINEERING_MINIMUM'
    assert set(result['required_blocks']) == {'cbr_rates', 'minfin_fx_operations', 'rosstat_macro', 'event_calendar'}
    rows = result['requirements']
    assert len(rows) == len({r['requirement_id'] for r in rows}) == 8
    assert all(r['required'] is True and r['admitted'] is False for r in rows)
    assert {r['metric_id'] for r in rows if r['kind'] == 'fact'} == {
        'cbr_key_rate_pct', 'cbr_ruonia_rate_pct', 'ROSSTAT_WEEKLY_CPI_ESTIMATE', 'ROSSTAT_MONTHLY_CPI'}
    liquidity = next(r for r in rows if r['source_id'] == 'cbr_banking_liquidity_daily')
    assert liquidity['metric_id'] is None
    assert liquidity['kind'] == 'fact_group'
    assert len(liquidity['metric_ids']) == 4
    assert 'row_level_publication_vintages' in liquidity['unresolved']


def test_policy_is_not_evidence_or_product_acceptance():
    result = policy.describe()
    assert all(result[key] is False for key in policy.DENIED)
    assert {'other_rosstat_series_and_historical_vintages', 'tax_cycle', 'global_macro_calendar',
            'h10_release_calendar', 'consensus_surprise', 'banking_liquidity_vintage',
            'exhaustive_product_series_and_event_definition'} <= set(result['unresolved_gaps'])
    assert all(r['availability_rule'] and r['revision_rule'] for r in result['requirements'])


def test_mutating_returned_nested_policy_cannot_remove_future_requirements():
    original = policy.describe()
    changed = policy.describe()
    changed['requirements'][4]['value_mapping']['fields'].clear()
    changed['requirements'][3]['unresolved'].clear()
    changed['required_blocks'].clear()
    changed['unresolved_gaps'].clear()
    changed['full_policy_complete'] = True
    assert policy.describe() == original


def test_registered_source_references_exist_without_new_provider():
    root = Path(__file__).resolve().parents[2]
    registry = json.loads((root / policy.NEWS_REGISTRY).read_text(encoding='utf-8'))
    known = {row['source_id'] for row in registry['primary_sources']}
    for row in policy.describe()['requirements']:
        assert (root / row['source_registry_ref'].split('#')[0]).is_file()
        if row['source_registry_ref'] == policy.NEWS_REGISTRY:
            assert row['source_id'] in known
        elif row['source_registry_ref'].endswith('#CURRENT_RECEIVED_SCHEDULE_REGISTRY'):
            assert row['source_id'] in policy.CURRENT_RECEIVED_SCHEDULE_REGISTRY
        elif row['source_registry_ref'] != policy.REGISTRY:
            assert row['source_id'] is None


@pytest.mark.parametrize('source', ['cbr_key_rate_daily', 'cbr_ruonia_daily', 'cbr_banking_liquidity_daily'])
def test_required_registry_source_removal_fails_closed(monkeypatch, source):
    monkeypatch.delitem(policy.SOURCE_REGISTRY, source)
    with pytest.raises(ValueError, match='missing from existing registry'):
        policy.describe()


def test_plans_do_not_claim_outcomes_or_numeric_series():
    rows = policy.describe()['requirements']
    plan = next(r for r in rows if r['kind'] == 'announcement_plan')
    assert plan['metric_id'] is None
    assert plan['scope'] == 'ANNOUNCED_PLAN_NOT_EXECUTION'
    assert plan['value_mapping']['asset_scope'] == 'FX_AND_GOLD'
    assert plan['value_mapping']['executed_operations_proven'] is False
    schedules = [r for r in rows if r['kind'] == 'scheduled_event']
    assert {r['event_family'] for r in schedules} == {'rosstat_weekly_cpi', 'runtime.cbr_meeting_calendar_current_received'}
    assert all(r['metric_id'] is None and r['admitted'] is False for r in schedules)


def test_cbr_schedule_tracks_runtime_adapter_without_claiming_historical_availability():
    row = next(r for r in policy.describe()['requirements']
               if r['requirement_id'] == 'cbr_key_rate_meeting_schedule')
    root = Path(__file__).resolve().parents[2]
    assert row['design_reference'] == 'contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml'
    assert row['design_reference'] != row['source_registry_ref']
    assert (root / row['runtime_contract_ref']).is_file()
    assert row['source_adapter_ref'] == 'src/moex_research/external_data/cbr_meeting_calendar.py'
    assert (root / row['source_adapter_ref']).is_file()
    assert row['availability_scope'] == 'current_receipt_not_historical'
    assert set(row['unresolved']) == {'historical_schedule_vintages', 'actual_decision_publication'}
    assert row['admitted'] is False
