"""Neutral min/max oracle over explicit native observations, not session levels."""
from copy import deepcopy
from datetime import timedelta

import pytest

from moex_data import rub_observed_range_levels as levels, rub_dated_hour_source as hour, rub_dated_context as dated
from test_rub_dated_hour_source import NOW, DATE, acquisition


def store(raw=None, now=NOW):
    raw = acquisition(now=now) if raw is None else raw
    return levels.capture(hour.capture(None, raw, now=now), raw, now=now)


def test_neutral_boundaries_from_all_observed_rows_with_no_extra_acquisition():
    raw = acquisition(); raw['pages'][0]['payload']['data']['data'][20][4] = 83
    value = store(raw); accepted, _ = dated.validated(value, NOW)
    block = accepted[levels.PURPOSE][2]['values']
    assert block['levels'] == [{'level_id': 'observed_range_low', 'level_type': 'RANGE_BOUNDARY', 'side': 'LOW', 'price': 79},
                               {'level_id': 'observed_range_high', 'level_type': 'RANGE_BOUNDARY', 'side': 'HIGH', 'price': 83}]
    assert block['source_bar_count'] == 22 and block['source_page_count'] == 1
    assert block['current_usable'] is block['historical_pit_usable'] is block['model_usable'] is block['session_completion_proven'] is False
    assert 'interactions' not in block and 'support' not in block


def test_gaps_disclosed_and_never_filled():
    raw = acquisition(); raw['pages'][0]['payload']['data']['data'].pop(15)
    raw['pages'][0]['payload']['data.cursor']['data'][0][1] -= 1
    block = dated.validated(store(raw), NOW)[0][levels.PURPOSE][2]['values']
    assert block['gap_count'] == 1 and len(block['missing_5m_endpoints']) == 1 and block['gaps_filled'] is False
    assert block['source_bar_count'] == 21


@pytest.mark.parametrize('defect', ['duplicate', 'bool', 'negative', 'ohlc', 'identity', 'future'])
def test_invalid_outside_selected_hour_refuses_levels_without_erasing_h1(defect):
    raw = acquisition(); rows = raw['pages'][0]['payload']['data']['data']
    if defect == 'duplicate': rows[-1] = deepcopy(rows[-2])
    if defect == 'bool': rows[-1][3] = True
    if defect == 'negative': rows[-1][7] = -1
    if defect == 'ohlc': rows[-1][4] = 70
    if defect == 'identity': rows[-1][0] = 'ALIEN'
    if defect == 'future': raw['pages'][0]['received_at_utc'] = '2026-09-11T20:40:00+00:00'; raw['pages'][0]['request_started_at_utc'] = '2026-09-11T20:39:00+00:00'
    value = store(raw)
    assert levels.PURPOSE not in value['selections'] and value['last_observed_range_refusal']
    if defect != 'identity': assert hour.PURPOSE in value['selections']


def test_repeat_keeps_acceptance_range_revision_changes_independently_of_hour():
    first = store(); later = NOW + timedelta(minutes=1)
    raw = acquisition(now=later)
    second = levels.capture(hour.capture(first, raw, now=later), raw, now=later)
    assert second['frames'] == first['frames']
    raw['pages'][0]['payload']['data']['data'][-1][4] = 83
    third = levels.capture(hour.capture(second, raw, now=later), raw, now=later)
    assert third['selections'][hour.PURPOSE] == first['selections'][hour.PURPOSE]
    assert third['selections'][levels.PURPOSE] != first['selections'][levels.PURPOSE]


def test_levels_oldest_bar_expires_and_rehashed_corruption_cannot_poison_hour():
    first = store(); ref = first['selections'][levels.PURPOSE]
    frame = first['frames'].pop(ref); frame['raw_source_payload']['data'][0][4] = 999
    frame['raw_source_digest'] = dated.digest(frame['raw_source_payload'])
    from moex_data.rub_dated_market_source import _revision
    frame['revision_id'] = _revision(frame); ref = dated.digest(frame)
    first['frames'][ref] = frame; first['selections'][levels.PURPOSE] = ref
    accepted, rejected = dated.validated(first, NOW)
    assert hour.PURPOSE in accepted and levels.PURPOSE in rejected
    assert not dated.validated(store(), NOW + timedelta(hours=96))[0]


def test_public_projection_exact_dimensions_and_live_separation():
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    from test_rub_fast_market import snapshot
    from test_rub_dated_market_source import acquisitions
    from moex_data.rub_dated_market_source import capture as market_capture
    view = snapshot(); view['identity']['generated_at_utc'] = NOW.isoformat()
    view['accepted_dated_slow'] = store()
    view['accepted_dated_market'] = market_capture(None, acquisitions(), now=NOW)
    value = release.build(view, now=NOW, code_revision='a' * 40)
    projection_completeness(view, value, now=NOW)
    package = release.compact(view, now=NOW, code_revision='a' * 40)
    assert package['observed_range_levels']['status'] == 'AVAILABLE'
    dimensions = package['readiness_dimensions']
    assert all(dimensions['preparation']['markets'].values())
    assert dimensions['preparation']['basis']['complete_for_supported_sources'] is True
    assert dimensions['preparation']['timeframes'] == {'1H': True, '1D': False, '1W': False}
    assert dimensions['current_live']['status'] == 'UNAVAILABLE' and package['status'] == 'PARTIAL'
    assert 'minfin_fx_operations_plan' in dimensions['external_required']['blockers']
    assert dimensions['delivery']['operational_acceptance_claimed'] is False


def test_timeframe_readiness_matches_exact_timeframe():
    from moex_data.rub_factual_package import readiness_dimensions
    release = {'facts': [], 'dated_context': {'observations': {}}, 'market_structure': {'status': 'UNAVAILABLE'},
               'macro_evidence_inventory': {'requirements_coverage': []}, 'timeframe_context': [{'values': {'timeframe': '1D'}}]}
    dimension = readiness_dimensions(release, {'requirements': [], 'external_blockers': []})
    assert dimension['preparation']['timeframes'] == {'1H': False, '1D': True, '1W': False}


def test_original_live_basis_projection_supported_by_dimensions():
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_package import coverage, readiness_dimensions
    from test_rub_fast_market import snapshot, market, NOW as LIVE_NOW
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
    view = snapshot(); view['identity']['generated_at_utc'] = LIVE_NOW.isoformat()
    raw = market()
    live.attach_live_market_oi_context(view, raw, attempted_at_utc=LIVE_NOW.isoformat())
    live.attach_live_basis_carry_context(view, raw, attempted_at_utc=LIVE_NOW.isoformat())
    result = release.build(view, now=LIVE_NOW, code_revision='a' * 40)
    dimensions = readiness_dimensions(result, coverage(result))
    assert dimensions['current_live']['basis_available'] is True
    assert all(dimensions['current_live']['markets'].values())
