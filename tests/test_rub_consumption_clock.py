"""Compact-derived arithmetic cases and labelled synthetic clock acceptance."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import runpy

import pytest
from moex_data import rub_factual_release as release, rub_dated_context as dated
from moex_data.rub_snapshot_read_freshness import apply_read_freshness
from moex_data.rub_factual_release_acceptance import projection_completeness

HELPERS = runpy.run_path(str(Path(__file__).parent / 'unit' / 'test_rub_factual_projection.py'))
AS_OF = datetime.fromisoformat('2026-09-09T03:13:53.454649+00:00')


def morning():
    """Only supplied compact arithmetic, not an exact archived raw replay."""
    source = HELPERS['core_snapshot']()
    source['identity']['generated_at_utc'] = '2026-09-09T03:05:23.954546+00:00'
    for name, timeframe, selected, old in (
        ('stage9_daily', '1D', '2026-09-09T03:00:00+00:00', 323),
        ('stage9_weekly', '1W', '2026-09-07T03:00:00+00:00', 173123)):
        source['components'][name] = {'status': 'READY', 'data': {
            'identity': {'as_of': '2026-09-09T03:05:23.954546+00:00'}, 'server_core': {'blocks': [{
                'block_id': timeframe, 'stage': 7, 'status': 'ready', 'timeframe': timeframe,
                'causal_field': 'availability_ts_utc', 'selected_causal_ts_utc': selected,
                'age_seconds_at_as_of': old, 'selected_observation': {'close': 80,
                    'availability_ts_utc': selected, 'ingest_ts_utc': selected}}]}}}
    return source


def test_original_compact_arithmetic_uses_one_exact_consumption_time():
    source = morning(); before = deepcopy(source)
    package = release.compact(source, now=AS_OF, code_revision='a' * 40)
    blocks = {item['values']['timeframe']: item['values'] for item in package['timeframe_context']}
    assert blocks['1D']['age_seconds_at_as_of'] == 833.454649
    assert blocks['1W']['age_seconds_at_as_of'] == 173633.454649
    for value in blocks.values():
        assert value['age_reference_utc'] == package['as_of_utc']
        assert value['source_time_ages']['availability']['age_seconds_at_as_of'] == value['age_seconds_at_as_of']
        assert value['source_time_ages']['source_event']['age_seconds_at_as_of'] is None
        assert value['selected_observation']['close'] == 80
    assert source == before
    frozen = json.loads(json.dumps(source))
    assert release.compact(frozen, now=AS_OF, code_revision='a' * 40) == package


def test_all_stage9_stages_reclock_without_touching_source_values():
    source = morning(); blocks = source['components']['stage9_daily']['data']['server_core']['blocks']
    blocks.append({'block_id': 'stage3', 'stage': 3, 'status': 'ready', 'causal_field': 'ts',
        'selected_causal_ts_utc': '2026-09-09T03:10:00+00:00', 'age_seconds_at_as_of': 1,
        'selected_observation': {'ts': '2026-09-09T03:10:00+00:00', 'close': 82,
            'source_publication_time': '2026-09-09T03:10:02+00:00',
            'availability_ts_utc': '2026-09-09T03:10:03+00:00', 'ingest_ts_utc': '2026-09-09T03:10:04+00:00'}})
    original = deepcopy(blocks[-1]['selected_observation'])
    view = apply_read_freshness(source, now=AS_OF)
    stage3 = view['components']['stage9_daily']['data']['server_core']['blocks'][-1]
    assert stage3['selected_observation'] == original
    assert stage3['age_seconds_at_as_of'] == 233.454649
    ages = stage3['source_time_ages']
    assert ages['source_update']['age_seconds_at_as_of'] == 231.454649
    assert ages['availability']['age_seconds_at_as_of'] == 230.454649
    assert ages['receipt']['age_seconds_at_as_of'] == 229.454649
    assert apply_read_freshness(view, now=AS_OF) == view


@pytest.mark.parametrize('selected', [None, 'not-a-time', '2026-09-09T03:00:00', '2026-09-09T04:00:00+00:00'])
def test_invalid_or_future_time_never_keeps_old_age_or_grants_projection(selected):
    source = morning(); block = source['components']['stage9_daily']['data']['server_core']['blocks'][0]
    block['selected_causal_ts_utc'] = selected
    view = apply_read_freshness(source, now=AS_OF)
    actual = view['components']['stage9_daily']['data']['server_core']['blocks'][0]
    assert actual['age_seconds_at_as_of'] is None if selected != '2026-09-09T04:00:00+00:00' else actual['age_seconds_at_as_of'] < 0
    value = release.build(source, now=AS_OF, code_revision='a' * 40)
    assert not any(item['values'].get('timeframe') == '1D' for item in value['timeframe_context'])


def test_dated_market_basis_and_hour_projection_clocks_leave_witness_hashes_unchanged(tmp_path):
    from test_rub_fast_market import publish, read, NOW
    from test_rub_hourly_observation import component, NOW as HOUR_NOW
    publish(tmp_path); original = read(tmp_path, NOW)
    witness = deepcopy(original['accepted_dated_market'])
    later = NOW + timedelta(hours=12, microseconds=123456)
    value = release.build(original, now=later, code_revision='a' * 40)
    projection_completeness(original, value, now=later)
    for key, item in value['dated_context']['observations'].items():
        if key.startswith('market:'):
            assert item['source_times']['source_event_at_utc'] is None
            assert item['ages']['source_observation_age_seconds_at_as_of'] == (later - dated.stamp(item['source_identity']['timestamp'])).total_seconds()
        elif key.startswith('basis:'):
            for leg in item['original_legs'].values():
                assert leg['freshness_reference_utc'] == later.isoformat()
                assert leg['age_seconds'] == (later - dated.stamp(leg['timestamp'])).total_seconds()
    assert original['accepted_dated_market'] == witness
    assert all(dated.digest(frame) == ref for ref, frame in witness['frames'].items())
    hourly = {'identity': {'generated_at_utc': HOUR_NOW.isoformat()}, 'components': {'live_market_structure': component()}}
    dated.capture_slow(hourly, None, now=HOUR_NOW)
    before = deepcopy(hourly); consumed = HOUR_NOW + timedelta(hours=12, microseconds=123456)
    package = release.compact(hourly, now=consumed, code_revision='a' * 40)
    hour = next(item['values'] for item in package['timeframe_context'] if item['values']['timeframe'] == '1H')
    assert hour['age_seconds_at_as_of'] == (consumed - dated.stamp(hour['selected_causal_ts_utc'])).total_seconds()
    assert hour['source_time_ages']['receipt']['age_seconds_at_as_of'] == 43200.123456
    prior = package['dated_context']['observations']['timeframe:observed_1H.USDRUBF']['values']['values']
    assert prior['age_seconds_at_as_of'] == hour['age_seconds_at_as_of']
    assert hourly == before


def test_fractional_snapshot_and_structure_freshness_boundaries(tmp_path, monkeypatch):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current as current
    from test_rub_fast_market import snapshot, NOW
    source = snapshot(); generated = dated.stamp(source['identity']['generated_at_utc'])
    base._atomic_write(base.current_snapshot_path(tmp_path), source)
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    edge = generated + timedelta(seconds=base.STALE_AFTER_SECONDS)
    assert base.read_current_snapshot(now_fn=lambda: edge)[0]['read_freshness']['status'] == 'FRESH'
    beyond = base.read_current_snapshot(now_fn=lambda: edge + timedelta(microseconds=1))[0]['read_freshness']
    assert beyond['status'] == 'STALE' and beyond['snapshot_age_seconds'] == base.STALE_AFTER_SECONDS + .000001
    assert current._source_freshness(now_utc=edge, source_timestamp=generated)['status'] == 'FRESH'
    assert current._source_freshness(now_utc=edge + timedelta(microseconds=1), source_timestamp=generated)['status'] == 'STALE'


def test_actual_handler_current_export_and_frozen_builder_share_clock(tmp_path, monkeypatch):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
    from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import load_factual_release
    helpers = runpy.run_path(str(Path(__file__).parent / 'unit' / 'test_rub_factual_snapshot_http_server.py'))
    source = morning(); metadata = helpers['_snapshot']()
    for key in ('schema_version', 'refresh_policy', 'readiness', 'authority'): source[key] = metadata[key]
    source['identity']['project'] = 'MOEX_Bot'
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    base._atomic_write(base.current_snapshot_path(tmp_path), source)
    original_bytes = base.current_snapshot_path(tmp_path).read_bytes()
    calls = []
    def clock(): calls.append(1); return AS_OF
    def compact_loader(): return load_factual_release(now_fn=clock, code_revision='a' * 40)
    with helpers['_running_server'](lambda: base.read_current_snapshot(now_fn=lambda: AS_OF)[0], release_loader=compact_loader) as port:
        status, _, compact = helpers['_request'](port, helpers['api'].RELEASE_PATH)
        heavy_status, _, heavy = helpers['_request'](port, helpers['api'].SNAPSHOT_PATH)
    assert status == heavy_status == 200 and len(calls) == 1
    exported = release.export_current(output=tmp_path / 'exports', now_fn=clock, code_revision='a' * 40)
    assert len(calls) == 2 and json.loads(exported.read_bytes()) == compact
    assert release.compact(source, now=AS_OF, code_revision='a' * 40) == compact
    assert heavy['components']['stage9_daily']['data']['server_core']['blocks'][0]['age_seconds_at_as_of'] == 833.454649
    assert base.current_snapshot_path(tmp_path).read_bytes() == original_bytes
