from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import runpy

import pytest
from moex_data import rub_factual_release as release
from moex_data import rub_factual_package as package
from moex_data.rub_macro_requirements import describe as requirements

HELPERS = runpy.run_path(str(Path(__file__).with_name('test_rub_factual_projection.py')))
NOW = HELPERS['NOW']; COMMIT = 'a' * 40


def source():
    return HELPERS['core_snapshot']()


def test_compact_facts_source_dates_and_numbers_survive_without_audit_paths():
    original = source(); before = deepcopy(original)
    item = original['components']['synchronized_live_market_oi']['data']['instruments']['cr_front']
    item['normalization'] = {'reference': 1000, 'reference_price': 12.8, 'source_reference': 'https://example.org/source',
        'partition_ref': '/audit/partition', 'raw_sha256': 'b' * 64}
    before = deepcopy(original)
    value = release.compact(original, now=NOW, code_revision=COMMIT)
    fact = next(item for item in value['facts'] if item['factor'] == 'cr_front')
    assert fact['values']['normalization'] == {'reference': 1000, 'reference_price': 12.8, 'source_reference': 'https://example.org/source'}
    assert fact['values']['open'] == 12.7 and fact['values']['expiry_date'] == '2026-09-17'
    assert 'snapshot_path' not in json.dumps(value)
    assert value['as_of_utc'] == NOW.isoformat() and value['code_revision'] == COMMIT
    assert value['status'] == 'PARTIAL' and value['authority']['model_ready'] is False
    assert 'blocking_required_factors' not in value and 'matrix' not in value
    assert original == before
    assert release.compact(json.loads(json.dumps(original)), now=NOW, code_revision=COMMIT) == value


def admitted_coverage_fixture():
    """Synthetic admission vector for coverage logic, not source acceptance."""
    value = release.build(source(), now=NOW, code_revision=COMMIT)
    value['facts'] = [{'factor': key} for key in (*package.MARKETS, 'basis_carry', 'futoi_live', 'futoi_live_cr', 'oil', 'external_cny')]
    for item in value['market_usability'].values(): item.update(missing_metadata=[], price_oi_usable=True)
    for item in value['futoi_context'].values(): item.update(current_usable=True, reason=None)
    value['macro_evidence_inventory']['requirements_coverage'] = [
        {'requirement_id': item['requirement_id'], 'current_evidence_present': True} for item in requirements()['requirements']]
    value['news_context'].update(source_status='READY', acquisition_fresh=True,
        summary={'ok_source_count': 1, 'selection_audit': {'selected_ids': ['one'], 'candidate_count': 1}})
    return value


def test_all_mandatory_coverage_can_be_complete_without_model_permission():
    result = package.coverage(admitted_coverage_fixture())
    assert result['status'] == 'COMPLETE' and result['missing_required'] == []
    assert result['model_ready'] is False and result['external_blockers'] == []


@pytest.mark.parametrize('requirement', [item['requirement_id'] for item in requirements()['requirements']])
def test_every_engineering_macro_requirement_is_mandatory(requirement):
    value = admitted_coverage_fixture()
    next(item for item in value['macro_evidence_inventory']['requirements_coverage'] if item['requirement_id'] == requirement)['current_evidence_present'] = False
    result = package.coverage(value)
    assert result['missing_required'] == [requirement]
    assert result['status'] == 'PARTIAL'
    assert (requirement in result['external_blockers']) == (requirement == 'minfin_fx_operations_plan')


@pytest.mark.parametrize('factor', [*package.MARKETS, 'basis_carry', 'futoi_live', 'futoi_live_cr', 'oil', 'external_cny'])
def test_each_market_basis_futoi_reference_is_required(factor):
    value = admitted_coverage_fixture(); value['facts'] = [item for item in value['facts'] if item['factor'] != factor]
    assert package.coverage(value)['missing_required'] == [factor]


@pytest.mark.parametrize('defect', ['metadata', 'structure', 'hour', 'day', 'week', 'headline', 'failed_news', 'expired_news', 'empty_news', 'invalid_position'])
def test_coverage_cannot_hide_mandatory_context_gaps(defect):
    value = admitted_coverage_fixture()
    if defect == 'metadata': value['market_usability']['si_front']['missing_metadata'] = ['expiry_date']
    elif defect == 'structure': value['market_structure']['status'] = 'UNAVAILABLE'
    elif defect in ('hour', 'day', 'week'):
        tf = {'hour': '1H', 'day': '1D', 'week': '1W'}[defect]
        value['timeframe_context'] = [item for item in value['timeframe_context'] if item['values']['timeframe'] != tf]
    elif defect == 'headline': value['news_context']['events'][0]['content_status'] = 'HEADLINE_NOT_PRESERVED_BY_SOURCE'
    elif defect == 'failed_news': value['news_context']['source_status'] = 'RETAINED_PREVIOUS'
    elif defect == 'expired_news': value['news_context']['acquisition_fresh'] = False
    elif defect == 'empty_news': value['news_context']['events'] = []
    else: value['user_position_context'] = {'status': 'UNAVAILABLE', 'availability': 'INVALID_EXPLICIT_USER_INPUT'}
    assert package.coverage(value)['status'] == 'PARTIAL'


def test_valid_empty_news_and_explicit_no_position_are_usable_states():
    value = admitted_coverage_fixture()
    value['news_context']['events'] = []
    value['news_context']['summary']['selection_audit'] = {'selected_ids': [], 'candidate_count': 0}
    value['user_position_context'] = {'status': 'UNAVAILABLE', 'availability': 'NO_EXPLICIT_USER_INPUT'}
    assert package.coverage(value)['status'] == 'COMPLETE'


def test_empty_upcoming_cbr_calendar_still_proves_its_finite_scope(tmp_path):
    from moex_data.rub_macro_evidence_inventory import describe
    helper = runpy.run_path(str(Path(__file__).with_name('test_cbr_meeting_calendar.py')))
    now = datetime(2026, 12, 19, 7, tzinfo=timezone.utc)
    snapshot = {'components': {'cbr_meeting_calendar': helper['component'](tmp_path, now)}}
    value = describe(snapshot, now=now)
    assert value['scheduled_events'] == []
    row = next(item for item in value['requirements_coverage'] if item['requirement_id'] == 'cbr_key_rate_meeting_schedule')
    assert row['current_evidence_present'] is True
    assert value['calendar_coverage'][0]['coverage_year'] == 2026
    assert value['full_calendar_accepted'] is False


@pytest.mark.parametrize('stamp,day,monday,next_start,next_end,sunday', [
    ('2026-09-13T16:00:00+00:00', '2026-09-13', '2026-09-07', '2026-09-14', '2026-09-20', True),
    ('2026-09-13T21:01:00+00:00', '2026-09-14', '2026-09-14', '2026-09-21', '2026-09-27', False)])
def test_review_period_and_prospective_horizon_are_not_weekly_bar(stamp, day, monday, next_start, next_end, sunday):
    now = datetime.fromisoformat(stamp)
    result = package.review_horizons({'components': {}}, {'timeframe_context': []}, now=now)
    assert result['D1']['report_start'].startswith(day)
    assert result['W1']['report_start'].startswith(monday)
    assert result['W1']['prospective_start_date'] == next_start and result['W1']['prospective_end_date'] == next_end
    assert result['W1']['is_regular_preparation_day'] is sunday
    assert result['W1']['planned_trading_dates'] == []
    assert result['W1']['session_completion_proven'] is False


def test_plans_only_from_actual_calendar_rows_with_weekend_mapping():
    helper = runpy.run_path(str(Path(__file__).with_name('test_rub_futures_calendar.py')))
    days = helper['parse'](helper['payload']())
    result = package.review_horizons({'components': {'futures_calendar': {'status': 'READY', 'data': {
        'calendar_plan_usable': True, 'days': days}}}}, {'timeframe_context': []}, now=NOW)
    assert result['W1']['planned_calendar_coverage'] == 'COMPLETE'
    assert '2026-09-21' not in result['W1']['planned_trading_dates']
    assert any(item['civil_date'] == '2026-09-19' and item['trading_date'] == '2026-09-21'
        for item in result['W1']['planned_civil_date_mappings'])
    assert result['W1']['planned_dates_are_actual_sessions'] is False


def test_preceding_weekend_mapping_into_prospective_week_is_explicit():
    helper = runpy.run_path(str(Path(__file__).with_name('test_rub_futures_calendar.py')))
    payload = helper['payload']()
    row = next(item for item in payload['off_days']['data'] if item[0] == '2026-09-13')
    row[1:4] = [1, '2026-09-14', 'W']
    days = helper['parse'](payload)
    result = package.review_horizons({'components': {'futures_calendar': {'status': 'READY', 'data': {
        'calendar_plan_usable': True, 'days': days}}}}, {'timeframe_context': []}, now=NOW)
    assert any(item['civil_date'] == '2026-09-13' and item['trading_date'] == '2026-09-14'
        for item in result['W1']['planned_civil_date_mappings'])
    assert all('2026-09-14' <= day <= '2026-09-20' for day in result['W1']['planned_trading_dates'])


@pytest.mark.parametrize('defect', ['none', 'future', 'blocked', 'nan'])
def test_existing_ema_numeric_context_is_not_model_confidence(defect):
    original = source(); data = original['components']['live_market_structure']['data']
    data['trend'] = 'BULLISH_USD'
    data['ema_3_19'] = {'quality_status': 'OK', 'available_at': NOW.isoformat(), 'confidence': 1.0,
        'direction': 'BULLISH_USD', 'details': {'ema_fast': 81, 'ema_slow': 80, 'bar_count': 50, 'source': 'existing_closed_bars'}}
    if defect == 'future': data['ema_3_19']['available_at'] = (NOW + timedelta(seconds=1)).isoformat()
    elif defect == 'blocked': data['ema_3_19']['quality_status'] = 'BLOCKED'
    elif defect == 'nan': data['ema_3_19']['details']['ema_fast'] = None
    result = release.compact(original, now=NOW, code_revision=COMMIT)
    ema = result['market_structure']['deterministic_context']['ema_3_19']
    assert (ema['status'] == 'AVAILABLE') == (defect == 'none')
    assert 'confidence' not in ema
    if defect == 'none': assert ema['values']['ema_fast'] == 81
    assert result['market_structure']['deterministic_context'].get('trend') == ('BULLISH_USD' if defect == 'none' else None)
    assert result['market_structure']['status'] == 'AVAILABLE'


@pytest.mark.parametrize('dirty', [' M src/code.py', 'M  src/code.py', ''])
def test_executing_revision_rejects_tracked_edits_but_ignores_untracked_audit_files(monkeypatch, dirty):
    calls = []
    def git(args, **kwargs):
        calls.append(args)
        return dirty if args[1] == 'status' else COMMIT
    monkeypatch.setattr(release.subprocess, 'check_output', git)
    if dirty:
        with pytest.raises(ValueError, match='clean tracked'): release.executing_revision()
        assert len(calls) == 1
    else: assert release.executing_revision() == COMMIT
    assert calls[0] == ['git', 'status', '--porcelain', '--untracked-files=no']


def test_unavailable_ema_cannot_smuggle_values_or_direction_through_oracle():
    from moex_data.rub_factual_release_acceptance import projection_completeness
    original = source()
    value = release.build(original, now=NOW, code_revision=COMMIT)
    value['market_structure']['deterministic_context']['ema_3_19'].update(values={'ema_fast': 999}, relation='BULLISH_USD')
    with pytest.raises(AssertionError): projection_completeness(original, value, now=NOW)


def test_failed_current_keeps_only_independently_admitted_si_prior():
    original = HELPERS['futoi_snapshot']()
    for key in ('futoi_live', 'futoi_live_cr'):
        item = original['components'][key]['data']['current_intraday']
        item.update(status='RETAINED_STALE', refresh_error='FIZ/YUR net positions do not balance to zero')
    result = release.compact(original, now=NOW, code_revision=COMMIT)
    si = result['futoi_context']['futoi_live']; cr = result['futoi_context']['futoi_live_cr']
    assert si['previous_observation']['trade_date'] == '2026-09-07'
    assert si['current_usable'] is False and si['comparisons'] is None
    assert si['reason'] == 'FIZ/YUR net positions do not balance to zero'
    assert cr['previous_observation'] is None and cr['comparisons'] is None


def test_current_export_one_clock_exact_bytes_and_no_overwrite(tmp_path):
    pytest.importorskip('fcntl')
    from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import load_factual_release
    metadata = runpy.run_path(str(Path(__file__).with_name('test_rub_factual_snapshot_http_server.py')))['_snapshot']()
    original = source()
    for key in ('schema_version', 'refresh_policy', 'readiness', 'authority'): original[key] = metadata[key]
    original['identity']['project'] = 'MOEX_Bot'
    original['read_freshness'] = {'status': 'FRESH', 'snapshot_age_seconds': 0, 'read_at_utc': NOW.isoformat()}
    clocks = []
    def clock(): clocks.append(1); return NOW
    def reader(*, now_fn):
        assert now_fn() == NOW
        return deepcopy(original), Path('unused')
    expected = load_factual_release(now_fn=clock, reader=reader, code_revision=COMMIT)
    assert len(clocks) == 1
    path = release.export_current(output=tmp_path, now_fn=clock, reader=reader, code_revision=COMMIT)
    assert json.loads(path.read_bytes()) == expected
    before = path.read_bytes()
    with pytest.raises(FileExistsError): release.export_current(output=tmp_path, now_fn=clock, reader=reader, code_revision=COMMIT)
    assert path.read_bytes() == before
