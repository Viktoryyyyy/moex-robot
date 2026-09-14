from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from moex_data import rub_fx_observed_context as fx

NOW = datetime(2026, 9, 13, 13, tzinfo=timezone.utc)


def evidence(count=30, timeframe='1D'):
    rows = []
    for index in range(count):
        end = datetime(2026, 9, 11, tzinfo=timezone.utc) - timedelta(days=count-1-index)
        start = end
        if timeframe == '1W':
            end = datetime(2026, 9, 6, tzinfo=timezone.utc) - timedelta(weeks=count-1-index)
            start = end - timedelta(days=6)
        rows.append({'instrument_id': 'usdrubf_futures_family', 'secid': 'USDRUBF',
            'timeframe': timeframe, 'trade_date': end.date().isoformat(),
            'period_start_date': start.date().isoformat(), 'period_end_date': end.date().isoformat(),
            'availability_ts_utc': (end+timedelta(days=1, hours=3)).isoformat(),
            'build_ts_utc': '2026-09-12T21:34:30+00:00',
            'open': 80.+index, 'high': 82.+index, 'low': 79.+index, 'close': 81.+index,
            'volume': 100., 'value': 8000., 'num_trades': 10., 'source_lineage_sha256': 'a'*64})
    return {'schema_version': fx.SCHEMA, 'instrument_id': 'usdrubf_futures_family',
        'timeframe': timeframe, 'rows': rows, 'source_provenance': {'acceptance_run_id': 'actual_run'}}


def block(source=None):
    source = source or evidence()
    return {'block_id': 'stage7.ohlcv.1D.usdrubf_futures_family', 'stage': 7,
        'dataset_id': 'rub_native_ohlcv_htf', 'instrument_id': source['instrument_id'],
        'timeframe': source['timeframe'], 'status': 'ready',
        'selected_observation': deepcopy(source['rows'][-1]),
        'selected_causal_ts_utc': source['rows'][-1]['availability_ts_utc'],
        'provenance': deepcopy(source['source_provenance']), 'observed_context_evidence': source}


def test_exact_lags_and_partial_week_are_observed_dates_with_weekends():
    source = evidence()
    result = fx.describe(source, now=NOW)
    assert result['status'] == 'AVAILABLE'
    assert result['observation_count'] == 30
    for lag in (1, 5, 20):
        item = result['comparisons'][str(lag)]
        assert item['target_date'] == source['rows'][-1-lag]['trade_date']
        assert item['absolute_change'] == lag
        assert len(item['observed_date_witness']) == lag+1
    assert result['comparisons']['5']['target_date'] == '2026-09-06'  # observed Sunday
    wtd = result['week_to_date']
    assert wtd['source_dates'] == [f'2026-09-{day:02d}' for day in range(7, 12)]
    assert wtd['calendar_dates_without_observations'] == ['2026-09-12', '2026-09-13']
    assert wtd['volume'] == 500
    assert wtd['week_completion_proven'] is False
    assert result['first_accepted_at_utc'] is None
    assert result['source_provenance'] == source['source_provenance']


@pytest.mark.parametrize('defect', ['nan', 'infinity', 'zero', 'ohlc', 'negative_volume', 'bool',
    'duplicate', 'unordered', 'identity', 'secid', 'timeframe', 'date', 'timestamp', 'naive', 'build', 'bound'])
def test_bad_retained_rows_refuse_without_shifting_lag(defect):
    source = evidence()
    row = source['rows'][-6]
    if defect == 'nan': row['close'] = float('nan')
    elif defect == 'infinity': row['high'] = float('inf')
    elif defect == 'zero': row['close'] = 0
    elif defect == 'ohlc': row['low'] = row['high'] + 1
    elif defect == 'negative_volume': row['volume'] = -1
    elif defect == 'bool': row['close'] = True
    elif defect == 'duplicate': source['rows'][-6] = deepcopy(source['rows'][-7])
    elif defect == 'unordered': source['rows'].reverse()
    elif defect == 'identity': row['instrument_id'] = 'cnyrubf_futures_family'
    elif defect == 'secid': row['secid'] = 'SiZ6'
    elif defect == 'timeframe': row['timeframe'] = '1W'
    elif defect == 'date': row['trade_date'] = '2026-09-30'
    elif defect == 'timestamp': row['availability_ts_utc'] = 'invalid'
    elif defect == 'naive': row['availability_ts_utc'] = '2026-09-12T03:00:00'
    elif defect == 'build': row['build_ts_utc'] = 'invalid'
    elif defect == 'bound': source['rows'].insert(0, deepcopy(source['rows'][0]))
    result = fx.describe(source, now=NOW)
    assert result['status'] == 'UNAVAILABLE'
    assert result['comparisons'] == {}


def test_future_build_cannot_masquerade_as_historical_pit_or_shift_lag():
    source = evidence()
    assert fx.describe(source, now=NOW-timedelta(days=2))['status'] == 'UNAVAILABLE'
    source['rows'][-6]['build_ts_utc'] = (NOW+timedelta(days=1)).isoformat()
    assert fx.describe(source, now=NOW)['reason'] == 'noncausal_hole_no_lag_shift'


def test_forward_week_boundary_recomputes_without_mutating_accepted_weekly():
    daily, weekly = block(), block(evidence(8, '1W'))
    original = deepcopy((daily, weekly))
    fx.apply(daily, NOW)
    fx.apply(weekly, NOW)
    assert daily['observed_context']['week_to_date']['status'] == 'AVAILABLE'
    assert weekly['observed_context']['observation_count'] == 8
    fx.apply(daily, datetime(2026, 9, 13, 21, tzinfo=timezone.utc))  # Monday in Moscow
    assert daily['observed_context']['week_to_date']['status'] == 'UNAVAILABLE'
    assert daily['observed_context']['week_to_date']['week_start_date'] == '2026-09-14'
    fx.apply(daily, NOW)
    assert daily['observed_context']['week_to_date']['status'] == 'AVAILABLE'
    assert daily['observed_context_evidence'] == original[0]['observed_context_evidence']
    assert weekly['selected_observation'] == original[1]['selected_observation']
    assert 'week_to_date' not in weekly['observed_context']


def test_short_history_and_null_volume_are_explicit():
    source = evidence(5)
    source['rows'][0]['volume'] = None
    result = fx.describe(source, now=NOW)
    assert result['comparisons']['1']['status'] == 'AVAILABLE'
    assert result['comparisons']['5']['status'] == 'UNAVAILABLE'
    assert result['comparisons']['20']['status'] == 'UNAVAILABLE'
    assert result['week_to_date']['volume'] is None


def test_finite_rows_with_overflowing_aggregate_refused():
    source = evidence()
    for row in source['rows']: row['volume'] = 1e308
    assert fx.describe(source, now=NOW)['reason'] == 'nonfinite_derived_values'


@pytest.mark.parametrize('field', ['instrument_id', 'timeframe', 'provenance', 'stage', 'dataset_id'])
def test_block_binding_refuses_cross_scope(field):
    value = block()
    value[field] = 'mismatch'
    fx.apply(value, NOW)
    assert value['observed_context']['status'] == 'UNAVAILABLE'


def test_capture_bounds_from_full_partition_and_boundary_duplicate():
    import pandas as pd
    from moex_data.step9_rub_analysis_bundle import _stage7_specs
    spec = _stage7_specs('daily')[0]
    rows = evidence(60)['rows']
    source = fx.capture(pd.DataFrame(rows), spec, {'acceptance_run_id': 'actual'}, now=NOW)
    assert len(source['rows']) == 30
    assert source['rows'][0] == rows[-30]
    assert fx.describe(source, now=NOW)['status'] == 'AVAILABLE'
    rows.insert(30, deepcopy(rows[30]))
    source = fx.capture(pd.DataFrame(rows), spec, {}, now=NOW)
    assert fx.describe(source, now=NOW)['status'] == 'UNAVAILABLE'


def test_same_input_release_projection_oracle_and_export_parity(tmp_path):
    import json
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    source = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {
        'stage9_daily': {'status': 'READY', 'data': {'server_core': {'blocks': [block()]}}},
        'stage9_weekly': {'status': 'READY', 'data': {'server_core': {'blocks': [block(evidence(8, '1W'))]}}}}}
    before = deepcopy(source)
    built = release.build(source, now=NOW, code_revision='a'*40)
    projection_completeness(source, built, now=NOW)
    directory = release.export(source, now=NOW, code_revision='a'*40, output=tmp_path)
    replay = release.build(json.loads((directory/'input_snapshot.json').read_text()), now=NOW, code_revision='a'*40)
    assert replay == built
    assert source == before
    assert 'observed_context_evidence' not in built['timeframe_context'][0]['values']
    later = release.build(source, now=NOW+timedelta(days=1), code_revision='a'*40)
    projection_completeness(source, later, now=NOW+timedelta(days=1))
    assert later['timeframe_context'][0]['values']['observed_context']['week_to_date']['status'] == 'UNAVAILABLE'
    for defect in ('omit', 'alter', 'inject'):
        bad = deepcopy(built)
        context = bad['timeframe_context'][0]['values']['observed_context']
        if defect == 'omit': context['observations'].pop()
        elif defect == 'alter': context['comparisons']['5']['percent_change'] += 1
        else: context['week_to_date']['source_dates'].append('2026-09-12')
        with pytest.raises(AssertionError, match='timeframe completeness'):
            projection_completeness(source, bad, now=NOW)


@pytest.mark.parametrize('timeframe,count', [('1D', 30), ('1W', 8)])
@pytest.mark.parametrize('defect', ['missing', 'null', 'naive', 'empty', 'invalid', 'number', 'bool'])
def test_required_build_timestamp_refuses_entire_retained_window(timeframe, count, defect):
    source = evidence(count, timeframe)
    row = source['rows'][-2]
    if defect == 'missing':
        row.pop('build_ts_utc')
    else:
        row['build_ts_utc'] = {
            'null': None, 'naive': '2026-09-12T21:34:30',
            'empty': '', 'invalid': 'invalid', 'number': 0, 'bool': True,
        }[defect]
    before = deepcopy(source)
    result = fx.describe(source, now=NOW)
    assert result['status'] == 'UNAVAILABLE'
    assert result['reason'] == 'invalid_retained_row_no_lag_shift'
    assert result['observations'] == []
    assert result['comparisons'] == {}
    assert source == before


@pytest.mark.parametrize('timeframe', ['1D', '1W'])
def test_removing_build_clock_does_not_admit_prebuild_frozen_history(timeframe):
    source = evidence(2, timeframe)
    before_build = datetime(2026, 9, 12, 12, tzinfo=timezone.utc)
    assert fx.describe(source, now=before_build)['status'] == 'UNAVAILABLE'
    for row in source['rows']:
        row.pop('build_ts_utc')
    before = deepcopy(source)
    result = fx.describe(source, now=before_build)
    assert result['status'] == 'UNAVAILABLE'
    assert result['observations'] == []
    assert result['comparisons'] == {}
    assert source == before


@pytest.mark.parametrize('timeframe', ['1D', '1W'])
@pytest.mark.parametrize('offset_hours', [-7, 0, 3, 14])
def test_build_clock_boundary_and_offset_preserve_original_rows(timeframe, offset_hours):
    source = evidence(2, timeframe)
    built = datetime(2026, 9, 12, 21, 34, 30, 123456, tzinfo=timezone.utc)
    for row in source['rows']:
        row['build_ts_utc'] = built.astimezone(timezone(timedelta(hours=offset_hours))).isoformat()
    before = deepcopy(source)
    assert fx.describe(source, now=built-timedelta(microseconds=1))['status'] == 'UNAVAILABLE'
    accepted = fx.describe(source, now=built)
    assert accepted['status'] == 'AVAILABLE'
    assert accepted['observations'] == source['rows']
    assert fx.describe(source, now=built+timedelta(microseconds=1))['observations'] == source['rows']
    assert source == before


@pytest.mark.parametrize('timeframe', ['1D', '1W'])
@pytest.mark.parametrize('defect', ['missing', 'null', 'naive', 'empty', 'invalid', 'number', 'bool'])
def test_independent_fx_oracle_refuses_corrupt_build_clock(timeframe, defect):
    from moex_data.rub_factual_release_acceptance import _fx_arithmetic_completeness
    source = evidence(2, timeframe)
    value = block(source)
    value['observed_context'] = fx.describe(source, now=NOW)
    assert value['observed_context']['status'] == 'AVAILABLE'
    # Inject the same fault into evidence and claimed output. The independent
    # oracle must refuse even if a faulty shared descriptor emits matching rows.
    for row in (source['rows'][0], value['observed_context']['observations'][0]):
        if defect == 'missing':
            row.pop('build_ts_utc')
        else:
            row['build_ts_utc'] = {
                'null': None, 'naive': '2026-09-12T21:34:30',
                'empty': '', 'invalid': 'invalid', 'number': 0, 'bool': True,
            }[defect]
    before = deepcopy(value)
    with pytest.raises(AssertionError, match='FX required build timestamp refusal'):
        _fx_arithmetic_completeness(value, now=NOW)
    assert value == before
    # A truthful refusal remains a valid partial package, not an acceptance crash.
    fx.apply(value, NOW)
    assert value['observed_context']['status'] == 'UNAVAILABLE'
    _fx_arithmetic_completeness(value, now=NOW)


@pytest.mark.parametrize('timeframe', ['1D', '1W'])
@pytest.mark.parametrize('defect', ['missing', 'null'])
def test_required_build_clock_full_release_and_frozen_export(timeframe, defect, tmp_path):
    import json
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    retained = evidence(2, timeframe)
    value = block(retained)
    # Keep the selected latest observation intact: only the retained historical
    # row is damaged, so its removal cannot silently shift historical lags.
    if defect == 'missing':
        retained['rows'][0].pop('build_ts_utc')
    else:
        retained['rows'][0]['build_ts_utc'] = None
    component = 'stage9_daily' if timeframe == '1D' else 'stage9_weekly'
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {
        component: {'status': 'READY', 'data': {'server_core': {'blocks': [value]}}}}}
    before = deepcopy(snapshot)
    built = release.build(snapshot, now=NOW, code_revision='a'*40)
    context = built['timeframe_context'][0]['values']['observed_context']
    assert context['status'] == 'UNAVAILABLE'
    assert context['reason'] == 'invalid_retained_row_no_lag_shift'
    assert context['observations'] == [] and context['comparisons'] == {}
    projection_completeness(snapshot, built, now=NOW)
    directory = release.export(snapshot, now=NOW, code_revision='a'*40, output=tmp_path)
    frozen = json.loads((directory / 'input_snapshot.json').read_text())
    assert release.build(frozen, now=NOW, code_revision='a'*40) == built
    assert snapshot == before


@pytest.mark.parametrize('timeframe', ['1D', '1W'])
def test_required_build_clock_also_applies_to_cny(timeframe):
    source = evidence(2, timeframe)
    source['instrument_id'] = 'cnyrubf_futures_family'
    for row in source['rows']:
        row.update(instrument_id='cnyrubf_futures_family', secid='CNYRUBF')
    assert fx.describe(source, now=NOW)['status'] == 'AVAILABLE'
    source['rows'][0].pop('build_ts_utc')
    before = deepcopy(source)
    refused = fx.describe(source, now=NOW)
    assert refused['status'] == 'UNAVAILABLE'
    assert refused['observations'] == [] and refused['comparisons'] == {}
    assert source == before
