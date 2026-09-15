"""Synthetic native TradeStats evidence; independent expected observed-hour values."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from moex_data import rub_dated_hour_source as source, rub_dated_context as dated

NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)
DATE = '2026-09-11'


def page(*, partial=10, now=NOW, empty=False):
    rows = []
    for i in range(12 + partial):
        end = datetime(2026, 9, 11, 22, 5) + timedelta(minutes=5 * i)
        rows.append(['USDRUBF', DATE, end.strftime('%H:%M:%S'), 80 + i / 100, 82, 79, 81, 10])
    if empty: rows = []
    return {'source_url': 'https://apim.moex.com/iss/datashop/algopack/fo/tradestats/USDRUBF.json',
            'request_params': {'from': DATE, 'till': DATE, 'start': 0, 'iss.meta': 'off'},
            'request_started_at_utc': (now - timedelta(seconds=2)).isoformat(), 'received_at_utc': (now - timedelta(seconds=1)).isoformat(),
            'payload': {'data': {'columns': ['secid', 'tradedate', 'tradetime', 'pr_open', 'pr_high', 'pr_low', 'pr_close', 'vol'], 'data': rows},
                        'data.cursor': {'columns': ['INDEX', 'TOTAL', 'PAGESIZE'], 'data': [[0, len(rows), 1000]]}}}


def acquisition(*, partial=10, now=NOW):
    p = page(partial=partial, now=now)
    _, hour, skipped = source.select([p], DATE, now=now)
    return {'source_date': DATE, 'pages': [p], 'latest_attempts': {DATE: {'status': 'SELECTED', 'skipped_hours': skipped}}}


def test_real_shape_latest_complete_hour_and_explicit_newer_10_of_12():
    raw = acquisition()
    store = source.capture(None, raw, now=NOW)
    accepted, rejected = dated.validated(store, NOW)
    assert not rejected
    hour = accepted[source.PURPOSE][2]['values']
    assert hour['hour_start_utc'] == '2026-09-11T19:00:00+00:00'
    assert hour['hour_end_utc'] == '2026-09-11T20:00:00+00:00'
    assert hour['values'] == {'open': 80., 'high': 82, 'low': 79, 'close': 81, 'volume': 120}
    view = dated.describe({'accepted_dated_slow': store, 'accepted_dated_market': {'schema_version': dated.SCHEMA, 'frames': {}, 'selections': {}}}, now=NOW)
    skipped = view['last_hour_source_attempts'][DATE]['skipped_hours']['2026-09-11T20:00:00+00:00']
    assert skipped['observed_bar_count'] == 10 and skipped['required_bar_count'] == 12
    assert hour['current_usable'] is hour['historical_pit_usable'] is hour['model_usable'] is False


def test_latest_attempt_changes_without_renewing_accepted_hour_then_real_revision():
    first = source.capture(None, acquisition(), now=NOW)
    later = NOW + timedelta(minutes=1)
    second = source.capture(first, acquisition(partial=11, now=later), now=later)
    assert second['frames'] == first['frames'] and second['selections'] == first['selections']
    assert second['last_hour_source_attempts'] != first['last_hour_source_attempts']
    corrected = acquisition(now=later)
    corrected['pages'][0]['payload']['data']['data'][0][3] = 80.5
    third = source.capture(second, corrected, now=later)
    ref = third['selections'][source.PURPOSE]
    assert ref != second['selections'][source.PURPOSE]
    assert third['frames'][ref]['accepted_at_utc'] == later.isoformat()


@pytest.mark.parametrize('field,value', [(0, 'ALIEN'), (1, '2026-09-10'), (2, '22:06:00'),
    (3, True), (3, '80'), (3, -1), (3, float('nan')), (4, 70), (7, -1)])
def test_native_identity_numeric_time_corruption_never_admitted(field, value):
    raw = acquisition(partial=0)
    raw['pages'][0]['payload']['data']['data'][0][field] = value
    assert source.capture(None, raw, now=NOW)['selections'] == {}


@pytest.mark.parametrize('defect', ['gap', 'duplicate', 'cursor', 'pages_digest', 'raw_digest', 'future_receipt', 'future_bar', 'units'])
def test_rehashed_defects_fail_closed(defect):
    raw = acquisition(partial=0)
    frame = source.make_frame(raw, now=NOW)
    rows = frame['source_pages'][0]['payload']['data']['data']
    if defect == 'gap': rows.pop(2)
    if defect == 'duplicate': rows[2] = rows[1]
    if defect == 'cursor': frame['source_pages'][0]['payload']['data.cursor']['data'][0][1] = 1001
    if defect == 'future_receipt': frame['source_pages'][0]['received_at_utc'] = (NOW + timedelta(seconds=1)).isoformat()
    if defect == 'future_bar':
        frame['source_pages'][0]['received_at_utc'] = '2026-09-11T19:30:00+00:00'
        frame['source_pages'][0]['request_started_at_utc'] = '2026-09-11T19:29:00+00:00'
    if defect == 'units': frame['units'] = {'price': 'USD_per_RUB', 'volume': 'contracts'}
    frame['source_pages_digest'] = dated.digest(frame['source_pages'])
    if defect == 'pages_digest': frame['source_pages_digest'] = '0' * 64
    if defect == 'raw_digest': frame['raw_source_digest'] = '0' * 64
    from moex_data.rub_dated_market_source import _revision
    frame['revision_id'] = _revision(frame)
    ref = dated.digest(frame)
    accepted, rejected = dated.validated({'schema_version': dated.SCHEMA, 'frames': {ref: frame}, 'selections': {source.PURPOSE: ref}}, NOW)
    assert not accepted and rejected


def test_96h_every_selected_bar_at_capture_and_read():
    first_end = datetime(2026, 9, 11, 19, 5, tzinfo=timezone.utc)
    edge = first_end + timedelta(hours=96)
    stored = source.capture(None, acquisition(partial=0, now=edge), now=edge)
    assert stored['selections']
    assert not dated.validated(stored, edge + timedelta(microseconds=1))[0]
    assert not source.capture(None, acquisition(partial=0, now=edge + timedelta(microseconds=1)), now=edge + timedelta(microseconds=1))['selections']


def test_finite_native_volumes_cannot_admit_nonfinite_aggregate():
    raw = acquisition(partial=0)
    for row in raw['pages'][0]['payload']['data']['data']: row[7] = 1e308
    selected, hour, skipped = source.select(raw['pages'], DATE, now=NOW)
    assert selected is None and hour is None
    assert next(iter(skipped.values()))['reason'] == 'invalid_aggregated_hour_numeric_values'
    assert source.capture(None, raw, now=NOW)['selections'] == {}


def test_acquire_empty_and_error_dates_are_distinct_and_previous_observed_selected():
    class Response:
        status_code = 200
        def __init__(self, url, payload): self.url = url; self.payload = payload
        def raise_for_status(self): pass
        def json(self): return deepcopy(self.payload)
    seen = []
    def get(url, **kwargs):
        day = kwargs['params']['from']; seen.append(day)
        assert kwargs['params']['till'] == day and kwargs['allow_redirects'] is False
        assert kwargs['headers']['Authorization'] == 'Bearer synthetic'
        if day == '2026-09-12': raise TimeoutError('synthetic')
        return Response(url, page(empty=day != DATE)['payload'])
    raw = source.acquire(now_fn=lambda: NOW, http_get=get, env={'MOEX_API_KEY': 'synthetic'})
    assert seen == ['2026-09-13', '2026-09-12', DATE]
    assert raw['latest_attempts']['2026-09-13']['status'] == 'EMPTY'
    assert raw['latest_attempts']['2026-09-12']['status'] == 'ERROR'
    assert raw['latest_attempts'][DATE]['status'] == 'SELECTED'


def test_h1_compact_full_release_and_reverse_projection_no_live_structure():
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    from test_rub_fast_market import snapshot
    view = snapshot(); view['identity']['generated_at_utc'] = NOW.isoformat()
    view['accepted_dated_slow'] = source.capture(None, acquisition(), now=NOW)
    value = release.build(view, now=NOW, code_revision='a' * 40)
    projection_completeness(view, value, now=NOW)
    compact = release.compact(view, now=NOW, code_revision='a' * 40)
    block = next(e for e in compact['timeframe_context'] if e['values']['block_id'] == 'observed_1H.USDRUBF')
    assert block['origin'] == 'source_observation_acquired_now' and block['acceptance_evidence_id']
    assert block['values']['current_usable'] is False
    assert all(f['factor'] != 'live_market_structure' for f in compact['facts'])


def test_slow_canonical_capture_finishes_after_hour_network_receipt(monkeypatch, tmp_path):
    from contextlib import nullcontext
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as overlay
    base = overlay.base
    for name in ('load_dotenv', 'install_timestamp_policy'):
        monkeypatch.setattr(base, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(base, '_data_root', lambda: tmp_path)
    monkeypatch.setattr(base, 'snapshot_state_dir', lambda root: tmp_path)
    monkeypatch.setattr(base, '_single_refresh_lock', lambda folder: nullcontext())
    monkeypatch.setattr(base, '_load_previous', lambda path: None)
    monkeypatch.setattr(overlay.current_context.current, 'current_producers', lambda: {})
    monkeypatch.setattr(overlay.current_context.context, 'run_refresh_all', lambda **kwargs: {})
    monkeypatch.setattr(overlay.current_context.delta_context, 'build_all', lambda **kwargs: {})
    monkeypatch.setattr(overlay.current_context, '_attach_futoi_context', lambda *args: None)
    monkeypatch.setattr(overlay.user_position, 'attach_user_position_context', lambda *args, **kwargs: None)
    monkeypatch.setattr(overlay.futoi, 'build_snapshot', lambda **kwargs: {
        'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {}, 'authority': {}, 'analysis_views': {}, 'analysis_workflow': {}})
    written = {}
    monkeypatch.setattr(base, '_atomic_write', lambda path, value: written.update(snapshot=value))
    # This fixture owns the hour network and acceptance clocks; C1 is tested separately.
    monkeypatch.setattr('moex_data.rub_historical_basis_carry_context.capture_snapshot', lambda *args, **kwargs: None)
    ticks = iter(NOW + timedelta(seconds=n) for n in range(6))
    def acquire(**kwargs):
        requested = kwargs['now_fn'](); received = kwargs['now_fn']()
        value = acquisition(now=received)
        if kwargs.get('secid') == 'CNYRUBF':
            value = cny_acquisition(value)
        value['pages'][0].update(request_started_at_utc=requested.isoformat(), received_at_utc=received.isoformat())
        return value
    monkeypatch.setattr(source, 'acquire', acquire)
    value, _ = overlay.refresh_snapshot(now_fn=lambda: next(ticks), live_loader=lambda: {'status': 'UNAVAILABLE'})
    frame = value['accepted_dated_slow']['frames'][value['accepted_dated_slow']['selections'][source.PURPOSE]]
    assert frame['received_at_utc'] == (NOW + timedelta(seconds=2)).isoformat()
    assert frame['accepted_at_utc'] == value['identity']['generated_at_utc'] == (NOW + timedelta(seconds=5)).isoformat()
    cny = value['accepted_dated_slow']['frames'][value['accepted_dated_slow']['selections']['timeframe:observed_1H.CNYRUBF']]
    assert cny['received_at_utc'] == (NOW + timedelta(seconds=4)).isoformat()
    assert cny['accepted_at_utc'] == frame['accepted_at_utc']
    assert written['snapshot'] is value


def cny_acquisition(value=None):
    raw = deepcopy(value if value is not None else acquisition(partial=0))
    raw['secid'] = 'CNYRUBF'
    for item in raw['pages']:
        item['source_url'] = item['source_url'].replace('USDRUBF', 'CNYRUBF')
        for row in item['payload']['data']['data']:
            row[0] = 'CNYRUBF'
            for index in (3, 4, 5, 6): row[index] /= 6
    return raw


def test_cny_hour_is_separate_identity_and_first_acceptance_survives_reingestion():
    usd = source.capture(None, acquisition(), now=NOW)
    both = source.capture(usd, cny_acquisition(), now=NOW)
    accepted, rejected = dated.validated(both, NOW)
    assert not rejected and set(accepted) == {source.PURPOSE, 'timeframe:observed_1H.CNYRUBF'}
    cny = accepted['timeframe:observed_1H.CNYRUBF']
    assert cny[2]['values']['instrument'] == cny[2]['values']['requested_secid'] == 'CNYRUBF'
    assert cny[1]['units'] == {'price': 'RUB_per_CNY', 'volume': 'contracts'}
    assert cny[2]['values']['values']['close'] == 13.5
    later = NOW + timedelta(minutes=1)
    again = source.capture(both, cny_acquisition(acquisition(partial=0, now=later)), now=later)
    assert again['frames'] == both['frames'] and again['selections'] == both['selections']
    assert both['frames'][usd['selections'][source.PURPOSE]] == usd['frames'][usd['selections'][source.PURPOSE]]


@pytest.mark.parametrize('defect', ['endpoint', 'raw_identity', 'units', 'purpose', 'gap', 'duplicate', 'future_receipt'])
def test_cny_rehashed_source_mismatch_and_incomplete_hour_refused(defect):
    frame = source.make_frame(cny_acquisition(), now=NOW)
    page = frame['source_pages'][0]
    if defect == 'endpoint': page['source_url'] = page['source_url'].replace('CNYRUBF', 'USDRUBF')
    elif defect == 'raw_identity': page['payload']['data']['data'][0][0] = 'USDRUBF'
    elif defect == 'units': frame['units']['price'] = 'RUB_per_USD'
    elif defect == 'purpose': frame['purpose'] = source.PURPOSE
    elif defect == 'gap': page['payload']['data']['data'].pop(2)
    elif defect == 'duplicate': page['payload']['data']['data'][2] = deepcopy(page['payload']['data']['data'][1])
    else: page['received_at_utc'] = (NOW+timedelta(seconds=1)).isoformat()
    frame['source_pages_digest'] = dated.digest(frame['source_pages'])
    from moex_data.rub_dated_market_source import _revision
    frame['revision_id'] = _revision(frame)
    ref = dated.digest(frame)
    valid, rejected = dated.validated({'schema_version': dated.SCHEMA, 'frames': {ref: frame},
        'selections': {frame['purpose']: ref}}, NOW)
    assert not valid and rejected


def test_both_hour_release_frozen_export_and_completeness(tmp_path):
    import json
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    store = source.capture(source.capture(None, acquisition(), now=NOW), cny_acquisition(), now=NOW)
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {}, 'accepted_dated_slow': store}
    original = deepcopy(snapshot)
    value = release.build(snapshot, now=NOW, code_revision='a'*40)
    projection_completeness(snapshot, value, now=NOW)
    assert {row['values']['block_id'] for row in value['timeframe_context']} == {'observed_1H.USDRUBF', 'observed_1H.CNYRUBF'}
    directory = release.export(snapshot, now=NOW, code_revision='a'*40, output=tmp_path)
    replay = release.build(json.loads((directory/'input_snapshot.json').read_text()), now=NOW, code_revision='a'*40)
    assert replay == value and snapshot == original
    bad = deepcopy(value)
    bad['timeframe_context'] = [row for row in bad['timeframe_context'] if row['values']['block_id'] != 'observed_1H.CNYRUBF']
    with pytest.raises(AssertionError): projection_completeness(snapshot, bad, now=NOW)
