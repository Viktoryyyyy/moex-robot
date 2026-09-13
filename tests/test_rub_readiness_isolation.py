"""Synthetic fixtures through production admission; no archived-source claims."""
from copy import deepcopy
from datetime import timedelta
import pytest

from moex_data.rub_factual_package import readiness_dimensions

MARKETS = ('usdrubf', 'si_front', 'si_next', 'cnyrubf', 'cr_front', 'cr_next', 'cnyrub_tom')
BASIS = (
    'usd_rub.front_perpetual_basis_abs', 'usd_rub.front_perpetual_basis_bps',
    'usd_rub.next_perpetual_basis_abs', 'usd_rub.next_perpetual_basis_bps',
    'usd_rub.front_next_spread_abs', 'usd_rub.front_next_spread_bps', 'usd_rub.front_next_term_carry_annualized',
    'cny_rub.perpetual_spot_basis_abs', 'cny_rub.perpetual_spot_basis_bps',
    'cny_rub.front_spot_basis_abs', 'cny_rub.front_spot_basis_bps', 'cny_rub.next_spot_basis_abs', 'cny_rub.next_spot_basis_bps',
    'cny_rub.front_perpetual_basis_abs', 'cny_rub.front_perpetual_basis_bps',
    'cny_rub.next_perpetual_basis_abs', 'cny_rub.next_perpetual_basis_bps',
    'cny_rub.front_next_spread_abs', 'cny_rub.front_next_spread_bps',
    'cny_rub.front_spot_implied_carry_annualized', 'cny_rub.next_spot_implied_carry_annualized',
    'cny_rub.front_next_term_carry_annualized',
)
COVERAGE = {'requirements': [], 'external_blockers': []}


def fixture(*, live=False, dated=False):
    """Synthetic post-admission release shape, not production/source replay."""
    facts = ([{'factor': key} for key in MARKETS] + [{'factor': 'basis_carry', 'values': {
        'metrics': [{'values': {'metric_id': key}} for key in BASIS]}}]) if live else []
    observations = ({'market:' + key: {} for key in MARKETS} | {'basis:' + key: {} for key in BASIS} |
                    {'structure:observed_range_levels.USDRUBF': {},
                     'timeframe:observed_1H.USDRUBF': {'values': {'values': {'timeframe': '1H'}}}}) if dated else {}
    timeframes = [{'scope': 'accepted_dated_observation_not_session_completion',
                   'values': {'timeframe': tf, 'stage': 7, 'status': 'ready', 'selected_causal_ts_utc': '2026-01-01T00:00:00+00:00'}} for tf in ('1D', '1W')]
    if live:
        timeframes.append({'scope': 'complete_observed_clock_hour_not_accepted_HTF_dataset_or_session_completion',
                           'values': {'timeframe': '1H'}})
    return {'facts': facts, 'dated_context': {'observations': observations}, 'timeframe_context': timeframes,
            'market_structure': {'status': 'AVAILABLE' if live else 'UNAVAILABLE'},
            'observed_range_levels': {'status': 'AVAILABLE' if dated else 'UNAVAILABLE'},
            'macro_evidence_inventory': {'requirements_coverage': []}}


def test_live_only_cannot_fill_empty_dated_preparation():
    result = readiness_dimensions(fixture(live=True), COVERAGE)
    assert result['preparation']['markets'] == {key: False for key in MARKETS}
    assert result['preparation']['basis']['available_metric_ids'] == []
    assert result['preparation']['observed_levels_available'] is False
    assert result['preparation']['timeframes'] == {'1H': False, '1D': True, '1W': True}
    assert result['preparation']['status'] == 'PARTIAL'
    assert result['current_live']['status'] == 'AVAILABLE'


@pytest.mark.parametrize('live', [False, True])
def test_complete_dated_independently_of_live(live):
    value = fixture(live=live, dated=True); before = deepcopy(value)
    result = readiness_dimensions(value, COVERAGE)
    assert result['preparation']['status'] == 'COMPLETE'
    assert result['preparation']['markets'] == {key: True for key in MARKETS}
    assert result['preparation']['basis']['available_metric_ids'] == sorted('basis:' + key for key in BASIS)
    assert result['preparation']['basis']['missing_required_metric_ids'] == []
    assert result['current_live']['status'] == ('AVAILABLE' if live else 'UNAVAILABLE')
    assert value == before


@pytest.mark.parametrize('missing', ['market', 'basis'])
def test_live_cannot_complete_missing_dated_instrument_or_basis(missing):
    value = fixture(live=True, dated=True); observations = value['dated_context']['observations']
    if missing == 'market': del observations['market:cnyrub_tom']
    else: del observations['basis:usd_rub.front_next_spread_abs']
    result = readiness_dimensions(value, COVERAGE)
    assert result['preparation']['status'] == 'PARTIAL'
    assert result['preparation']['markets']['cnyrub_tom'] is (missing != 'market')
    assert result['preparation']['basis']['missing_required_metric_ids'] == (['basis:usd_rub.front_next_spread_abs'] if missing == 'basis' else [])
    assert ('basis:usd_rub.front_next_spread_abs' in result['preparation']['basis']['available_metric_ids']) is (missing != 'basis')
    assert result['current_live']['status'] == 'AVAILABLE'


def test_absent_live_and_dated_remain_independently_unavailable():
    result = readiness_dimensions(fixture(), COVERAGE)
    assert result['preparation']['status'] == 'PARTIAL'
    assert result['preparation']['markets'] == {key: False for key in MARKETS}
    assert result['preparation']['basis']['available_metric_ids'] == []
    assert result['current_live']['status'] == 'UNAVAILABLE'


def test_live_levels_and_hour_do_not_substitute_and_origin_a_keys_remain_supported():
    value = fixture(live=True, dated=True); observations = value['dated_context']['observations']
    observations.pop('structure:observed_range_levels.USDRUBF')
    observations.pop('timeframe:observed_1H.USDRUBF')
    result = readiness_dimensions(value, COVERAGE)
    assert result['preparation']['observed_levels_available'] is False
    assert result['preparation']['timeframes']['1H'] is False
    observations['structure'] = {'values': {}, 'origin': 'previously_accepted_live'}
    observations['timeframe:observed_1H.USDRUBF'] = {'values': {'values': {'timeframe': '1H'}}}
    assert readiness_dimensions(value, COVERAGE)['preparation']['status'] == 'COMPLETE'


@pytest.mark.parametrize('origin', ['previously_accepted_live', 'source_observation_acquired_now'])
def test_native_stage7_hour_cannot_replace_saved_dated_hour(origin):
    value = fixture(dated=True)
    observations = value['dated_context']['observations']
    witness = observations.pop('timeframe:observed_1H.USDRUBF')
    witness['origin'] = origin
    native_hour = deepcopy(value['timeframe_context'][0])
    native_hour['values']['timeframe'] = '1H'
    value['timeframe_context'].append(native_hour)
    before = deepcopy(value)
    prep = readiness_dimensions(value, COVERAGE)['preparation']
    assert prep['timeframes'] == {'1H': False, '1D': True, '1W': True}
    assert prep['status'] == 'PARTIAL'
    assert value == before
    observations['timeframe:observed_1H.USDRUBF'] = witness
    assert readiness_dimensions(value, COVERAGE)['preparation']['status'] == 'COMPLETE'


def real_b_snapshot():
    from test_rub_fast_market import snapshot
    from test_rub_observed_range_levels import store, NOW
    from test_rub_dated_market_source import acquisitions
    from moex_data.rub_dated_market_source import capture
    value = snapshot(); value['identity']['generated_at_utc'] = NOW.isoformat()
    value['accepted_dated_market'] = capture(None, acquisitions(), now=NOW)
    value['accepted_dated_slow'] = store()
    value['components']['stage9_daily'] = {'status': 'READY', 'data': {'server_core': {'blocks': [
        {'block_id': 'native.' + tf, 'timeframe': tf, 'stage': 7, 'status': 'ready', 'selected_causal_ts_utc': '2026-01-01T00:00:00+00:00'}
        for tf in ('1D', '1W')]}}}
    return value, NOW


def test_real_b_native_hour_delivery_does_not_fill_dated_hour_gap():
    from moex_data import rub_factual_release as release
    view, now = real_b_snapshot()
    selections = view['accepted_dated_slow']['selections']
    witness = selections.pop('timeframe:observed_1H.USDRUBF')
    blocks = view['components']['stage9_daily']['data']['server_core']['blocks']
    native_hour = deepcopy(blocks[0])
    native_hour.update(block_id='native.1H', timeframe='1H')
    blocks.append(native_hour)
    before = deepcopy(view)
    package = release.compact(view, now=now, code_revision='a' * 40)
    assert 'timeframe:observed_1H.USDRUBF' not in package['dated_context']['observations']
    assert any(row['values'].get('timeframe') == '1H' for row in package['timeframe_context'])
    assert package['readiness_dimensions']['preparation']['timeframes'] == {'1H': False, '1D': True, '1W': True}
    assert package['readiness_dimensions']['preparation']['status'] == 'PARTIAL'
    assert view == before
    selections['timeframe:observed_1H.USDRUBF'] = witness
    assert release.compact(view, now=now, code_revision='a' * 40)['readiness_dimensions']['preparation']['status'] == 'COMPLETE'


def test_real_b_and_native_d1w1_repeated_compact_preserve_original_input():
    from moex_data import rub_factual_release as release
    view, now = real_b_snapshot(); before = deepcopy(view)
    first = release.compact(view, now=now, code_revision='a' * 40)
    second = release.compact(view, now=now, code_revision='a' * 40)
    assert first == second and view == before
    assert first['readiness_dimensions']['preparation']['status'] == 'COMPLETE'
    assert first['readiness_dimensions']['current_live']['status'] == 'UNAVAILABLE'
    assert first['readiness_dimensions']['preparation']['timeframes'] == {'1H': True, '1D': True, '1W': True}
    assert all(item['origin'] == 'source_observation_acquired_now' for item in first['dated_context']['observations'].values())


@pytest.mark.parametrize('defect', ['expired', 'digest', 'rehash_identity', 'missing'])
def test_present_rejected_dated_evidence_cannot_raise_readiness(defect):
    from moex_data import rub_factual_release as release, rub_dated_context as dated
    view, now = real_b_snapshot()
    if defect == 'expired': now += timedelta(hours=96)
    else:
        for name in ('accepted_dated_market', 'accepted_dated_slow'):
            stored = view[name]
            for old, frame in list(stored['frames'].items()):
                if defect == 'missing': stored['frames'].pop(old)
                elif defect == 'digest': frame['accepted_at_utc'] = '2001-01-01T00:00:00+00:00'
                else:
                    frame['identity'] = {'secid': 'ALIEN'}
                    ref = dated.digest(frame); stored['frames'][ref] = stored['frames'].pop(old)
                    stored['selections'] = {key: ref if value == old else value for key, value in stored['selections'].items()}
    before = deepcopy(view)
    package = release.compact(view, now=now, code_revision='a' * 40)
    prep = package['readiness_dimensions']['preparation']
    assert prep['status'] == 'PARTIAL' and prep['markets'] == {key: False for key in MARKETS}
    assert prep['basis']['available_metric_ids'] == [] and prep['observed_levels_available'] is False
    # Existing native D1/W1 are older than 96h and retain their own causal rules.
    assert prep['timeframes'] == {'1H': False, '1D': True, '1W': True}
    assert view == before


def test_real_origin_a_current_and_saved_witness_have_independent_admission():
    from test_rub_fast_market import snapshot, market, NOW
    from moex_data import rub_dated_context as dated, rub_factual_release as release
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
    view = snapshot(); view['identity']['generated_at_utc'] = NOW.isoformat(); raw = market()
    # The core market fixture omits APIM's native expiry attachment; without it
    # four carry metrics are correctly unavailable on both base and fixed code.
    from test_synchronized_live_market_oi_context import _payloads
    from moex_data.synchronized_live_market_oi_context_apim import _attach_expiry_metadata
    _attach_expiry_metadata(raw, _payloads()[0])
    live.attach_live_market_oi_context(view, raw, attempted_at_utc=NOW.isoformat())
    live.attach_live_basis_carry_context(view, raw, attempted_at_utc=NOW.isoformat())
    live_only = release.compact(view, now=NOW, code_revision='a' * 40)['readiness_dimensions']
    assert live_only['current_live']['status'] == 'AVAILABLE'
    assert live_only['preparation']['markets'] == {key: False for key in MARKETS}
    view['accepted_dated_market'] = dated.capture(None, components=view['components'], now=NOW, kind='market')
    before = deepcopy(view)
    both = release.compact(view, now=NOW, code_revision='a' * 40)
    assert both['readiness_dimensions']['preparation']['markets'] == {key: True for key in MARKETS}
    assert both['readiness_dimensions']['current_live']['status'] == 'AVAILABLE'
    assert all('origin' not in frame for frame in view['accepted_dated_market']['frames'].values())
    assert view == before


def test_real_original_hour_current_alone_then_saved_a():
    from test_rub_hourly_observation import component, NOW
    from moex_data import rub_dated_context as dated, rub_factual_release as release
    view = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {'live_market_structure': component()}}
    current = release.compact(view, now=NOW, code_revision='a' * 40)
    assert current['readiness_dimensions']['preparation']['timeframes']['1H'] is False
    dated.capture_slow(view, None, now=NOW)
    assert release.compact(view, now=NOW, code_revision='a' * 40)['readiness_dimensions']['preparation']['timeframes']['1H'] is True
