"""Explicit synthetic complete-hour and witness refusal cases."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from moex_data import rub_hourly_observation as hourly, rub_dated_context as dated, rub_factual_release as release
from moex_data.rub_factual_release_acceptance import projection_completeness

NOW = datetime(2026, 9, 10, 13, 2, tzinfo=timezone.utc)


def bars():
    start = NOW.replace(hour=12, minute=0)
    return [{'end': (start + timedelta(minutes=5 * index)).isoformat(),
             'open': 80., 'high': 82., 'low': 79., 'close': 81., 'volume': index} for index in range(1, 13)]


def component():
    return {'status': 'READY', 'data': {'instrument': 'USDRUBF', 'requested_secid': 'USDRUBF',
        'source_id': hourly.SOURCE, 'source_contract_ref': hourly.CONTRACT, 'trade_date': '2026-09-10',
        'hourly_observation': hourly.build(bars(), now=NOW, receipt=NOW)}}


def test_complete_hour_is_observation_and_exact_rederived_ohlc():
    source = component(); result = hourly.admitted(source, now=NOW)
    assert result['hour_start_utc'] == '2026-09-10T12:00:00+00:00'
    assert result['hour_end_utc'] == '2026-09-10T13:00:00+00:00'
    assert result['values'] == dict(open=80, high=82, low=79, close=81, volume=78)
    assert result['session_completion_proven'] is False


@pytest.mark.parametrize('defect', ['gap', 'duplicate', 'partial_hour', 'unclosed', 'mixed_identity', 'outer_identity', 'outer_missing', 'wrong_date', 'receipt_semantics', 'future_receipt', 'missing_receipt', 'ohlc', 'changed_output', 'reordered'])
def test_hour_witness_refuses_defects(defect):
    source = component(); evidence = source['data']['hourly_observation']; rows = evidence['source_bars']
    if defect == 'gap': rows.pop(3)
    elif defect == 'duplicate': rows[4] = deepcopy(rows[3])
    elif defect == 'partial_hour':
        for row in rows: row['end'] = (dated.stamp(row['end']) + timedelta(minutes=5)).isoformat()
    elif defect == 'unclosed':
        for row in rows: row['end'] = (dated.stamp(row['end']) + timedelta(hours=1)).isoformat()
    elif defect == 'mixed_identity': evidence['requested_secid'] = 'SiU6'
    elif defect == 'outer_identity': source['data']['instrument'] = 'ALIEN'
    elif defect == 'outer_missing': source['data'].pop('source_id')
    elif defect == 'wrong_date': source['data']['trade_date'] = '1999-01-01'
    elif defect == 'receipt_semantics': evidence['receipt_semantics'] = 'fabricated'
    elif defect == 'future_receipt': evidence['receipt_upper_bound_utc'] = (NOW + timedelta(seconds=1)).isoformat()
    elif defect == 'missing_receipt': evidence.pop('receipt_upper_bound_utc')
    elif defect == 'ohlc': rows[0]['high'] = 1
    elif defect == 'changed_output': evidence['observation']['values']['close'] = 99
    else: rows.reverse()
    assert hourly.admitted(source, now=NOW) is None


def test_slow_capture_at_actual_completion_preserves_hour_on_next_failure():
    source = component(); completed = NOW + timedelta(seconds=20)
    snapshot = {'identity': {'generated_at_utc': completed.isoformat()}, 'components': {'live_market_structure': source}}
    dated.capture_slow(snapshot, None, now=completed)
    assert 'timeframe:observed_1H.USDRUBF' in snapshot['accepted_dated_slow']['selections']
    later = completed + timedelta(hours=12)
    failed = {'identity': {'generated_at_utc': later.isoformat()}, 'components': {'live_market_structure': {'status': 'RETAINED_PREVIOUS', 'data': source['data'], 'refresh_error': 'no current bars'}}}
    dated.capture_slow(failed, snapshot, now=later)
    value = release.build(failed, now=later, code_revision='a' * 40)
    assert not value['timeframe_context']
    prior = value['dated_context']['observations']['timeframe:observed_1H.USDRUBF']
    assert prior['accepted_at_utc'] == completed.isoformat()
    assert prior['values']['values']['hour_end_utc'] == '2026-09-10T13:00:00+00:00'
    projection_completeness(failed, value, now=later)
    for defect in ('omit', 'alter', 'inject'):
        corrupt = deepcopy(value)
        observations = corrupt['dated_context']['observations']
        if defect == 'omit': observations.clear()
        elif defect == 'alter': observations['timeframe:observed_1H.USDRUBF']['values']['values']['values']['close'] = 999
        else: observations['market:alien'] = {}
        with pytest.raises(AssertionError): projection_completeness(failed, corrupt, now=later)


def test_current_hour_discoverable_and_corrupt_rehashed_hour_is_not_retained():
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {'live_market_structure': component()}}
    dated.capture_slow(snapshot, None, now=NOW)
    value = release.compact(snapshot, now=NOW, code_revision='a' * 40)
    assert any(item['values']['timeframe'] == '1H' for item in value['timeframe_context'])
    assert next(row for row in value['factual_coverage']['requirements'] if row['requirement_id'] == 'timeframe_1H')['usable']
    store = snapshot['accepted_dated_slow']; old = next(iter(store['frames'])); frame = store['frames'].pop(old)
    frame['components']['live_market_structure']['data']['hourly_observation']['requested_secid'] = 'ALIEN'
    ref = dated.digest(frame); store['frames'][ref] = frame; store['selections'] = {key: ref for key in store['selections']}
    assert not dated.describe(snapshot, now=NOW)['observations']


def test_actual_producer_path_captures_receipt_before_final_acceptance(monkeypatch):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current as current
    receipt = NOW + timedelta(seconds=1); completed = NOW + timedelta(seconds=2)
    class ClockType(type):
        def __instancecheck__(cls, value): return isinstance(value, datetime)
    class Clock(datetime, metaclass=ClockType):
        @classmethod
        def now(cls, tz=None): return receipt.astimezone(tz)
    monkeypatch.setattr(current, 'datetime', Clock)
    raw = [{**row, 'end': dated.stamp(row['end'])} for row in bars()]
    def loader(secid, day):
        assert secid == 'USDRUBF'
        if day == NOW.date(): return raw
        if day == NOW.date() - timedelta(days=1):
            return [{**row, 'end': row['end'] - timedelta(days=1)} for row in raw]
        return []
    monkeypatch.setattr(current.live, '_load_bars', loader)
    produced = current._usdrubf_live_market_structure_component(NOW)
    source = {'status': 'READY', 'data': current.base._jsonable(produced.data)}
    snapshot = {'identity': {'generated_at_utc': completed.isoformat()}, 'components': {'live_market_structure': source}}
    dated.capture_slow(snapshot, None, now=completed)
    selectors = snapshot['accepted_dated_slow']['selections']
    assert 'structure' in selectors and 'timeframe:observed_1H.USDRUBF' in selectors
    observed = hourly.admitted(source, now=completed)
    assert observed['receipt_upper_bound_utc'] == receipt.isoformat()
    assert dated.stamp(observed['hour_end_utc']) <= receipt < completed
    value = release.compact(snapshot, now=completed, code_revision='a' * 40)
    assert value['dated_context']['observations']['structure']['source_times']['last_interaction_assessment_at_utc'] is not None
    # Corruption with a recomputed digest still fails original structure causality.
    store = snapshot['accepted_dated_slow']; old = selectors['structure']; frame = store['frames'].pop(old)
    frame['components']['live_market_structure']['data']['structural_levels']['level_interactions'][0]['event_timestamp'] = (completed + timedelta(hours=1)).isoformat()
    ref = dated.digest(frame); store['frames'][ref] = frame; store['selections'] = {key: ref for key in selectors}
    assert 'structure' not in dated.describe(snapshot, now=completed)['observations']
