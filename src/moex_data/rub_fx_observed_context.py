"""Bounded accepted perpetual OHLCV; observed lags never imply trading sessions."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from math import isfinite

SCHEMA = 'rub_fx_observed_context.v1'
INSTRUMENTS = {'usdrubf_futures_family': 'USDRUBF', 'cnyrubf_futures_family': 'CNYRUBF'}
MOSCOW = timezone(timedelta(hours=3))


def _time(value):
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError('timezone required')
    return parsed


def _number(value, *, positive=False):
    return (isinstance(value, (float, int)) and not isinstance(value, bool)
            and isfinite(value) and (value > 0 if positive else value >= 0))


def _finite_tree(value):
    if isinstance(value, dict): return all(_finite_tree(item) for item in value.values())
    if isinstance(value, list): return all(_finite_tree(item) for item in value)
    return not isinstance(value, float) or isfinite(value)


def _row_valid(row, instrument, timeframe):
    try:
        start, end = date.fromisoformat(row['period_start_date']), date.fromisoformat(row['period_end_date'])
        available = _time(row['availability_ts_utc'])
        # Stage7 rows require their original build clock, including on replay.
        _time(row['build_ts_utc'])
        if (row['instrument_id'] != instrument or row['secid'] != INSTRUMENTS[instrument]
                or row['timeframe'] != timeframe or end < start
                or available.astimezone(MOSCOW).date() <= end):
            return False
        if timeframe == '1D' and (start != end or row.get('trade_date') != start.isoformat()):
            return False
        if timeframe == '1W' and (start.weekday() != 0 or (end-start).days != 6):
            return False
        if any(not _number(row.get(key), positive=True) for key in ('open', 'high', 'low', 'close')):
            return False
        if not row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high']:
            return False
        if any(row.get(key) is not None and not _number(row[key]) for key in ('volume', 'value', 'num_trades')):
            return False
        return True
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def describe(evidence, *, now):
    """Recompute from immutable bounded source rows, including on frozen replay."""
    result = {'schema_version': SCHEMA, 'status': 'UNAVAILABLE',
        'scope': 'accepted_observed_periods_not_session_completion',
        'historical_pit_usable': False, 'model_usable': False,
        'session_completion_proven': False, 'first_accepted_at_utc': None,
        'acceptance_time_limitation': 'accepted_pointer_has_run_identity_not_first_acceptance_timestamp',
        'as_of_utc': now.isoformat(), 'observations': [], 'comparisons': {}}
    if not isinstance(evidence, dict) or evidence.get('schema_version') != SCHEMA:
        return {**result, 'reason': 'missing_or_unknown_evidence'}
    if evidence.get('duplicate_retained_period') is True:
        return {**result, 'reason': 'duplicate_or_unordered_period_no_lag_shift'}
    instrument, timeframe = evidence.get('instrument_id'), evidence.get('timeframe')
    rows = evidence.get('rows')
    bound = 30 if timeframe == '1D' else 8
    if (instrument not in INSTRUMENTS or timeframe not in ('1D', '1W')
            or not isinstance(rows, list) or not rows or len(rows) > bound):
        return {**result, 'reason': 'invalid_identity_or_bound'}
    # Refuse the whole retained window instead of dropping a bad row and shifting a lag.
    if any(not isinstance(row, dict) or not _row_valid(row, instrument, timeframe) for row in rows):
        return {**result, 'reason': 'invalid_retained_row_no_lag_shift'}
    dates = [row['period_end_date'] for row in rows]
    if dates != sorted(set(dates)):
        return {**result, 'reason': 'duplicate_or_unordered_period_no_lag_shift'}
    eligible = [deepcopy(row) for row in rows if _time(row['availability_ts_utc']) <= now
                and _time(row['build_ts_utc']) <= now]
    # Build availability may differ by row. A hole is not a shorter observed lag.
    if eligible and eligible != rows[:len(eligible)]:
        return {**result, 'reason': 'noncausal_hole_no_lag_shift'}
    result.update(instrument_id=instrument, secid=INSTRUMENTS[instrument], timeframe=timeframe,
        units={'price': 'RUB_per_USD' if instrument == 'usdrubf_futures_family' else 'RUB_per_CNY',
               'volume': 'contracts', 'value': 'RUB', 'num_trades': 'trades'},
        requested_observation_count=bound, observations=eligible,
        observation_count=len(eligible), retained_observation_count=len(rows),
        history_limitation='bounded_retained_current_accepted_partition_not_historical_vintage',
        source_provenance=deepcopy(evidence.get('source_provenance', {})),
        accepted_partition_coverage=deepcopy(evidence.get('accepted_partition_coverage', {})))
    if not eligible:
        return {**result, 'reason': 'no_causal_retained_observations'}
    result.update(status='AVAILABLE', source_start_date=eligible[0]['period_start_date'],
                  source_end_date=eligible[-1]['period_end_date'])
    latest = eligible[-1]
    for lag in ((1, 5, 20) if timeframe == '1D' else (1,)):
        item = {'observed_period_lag': lag, 'status': 'UNAVAILABLE',
                'absolute_change_unit': result['units']['price'], 'percent_change_unit': 'percent',
                'semantics': 'exact_observed_period_close_change_not_calendar_or_session_return'}
        if len(eligible) > lag:
            previous = eligible[-1-lag]
            item.update(status='AVAILABLE', source_date=latest['period_end_date'],
                target_date=previous['period_end_date'], source_close=latest['close'], target_close=previous['close'],
                absolute_change=latest['close']-previous['close'],
                percent_change=(latest['close']/previous['close']-1)*100,
                observed_date_witness=[row['period_end_date'] for row in eligible[-1-lag:]])
        else:
            item['reason'] = 'insufficient_retained_observed_periods'
        result['comparisons'][str(lag)] = item
    if timeframe == '1D':
        today = now.astimezone(MOSCOW).date()
        monday = today - timedelta(days=today.weekday())
        part = [row for row in eligible if monday.isoformat() <= row['trade_date'] <= today.isoformat()]
        observed = [row['trade_date'] for row in part]
        wtd = {'status': 'AVAILABLE' if part else 'UNAVAILABLE',
            'scope': 'dynamic_moscow_week_observed_D1_not_accepted_W1',
            'week_start_date': monday.isoformat(), 'as_of_moscow_date': today.isoformat(),
            'week_end_date': (monday+timedelta(days=6)).isoformat(),
            'source_dates': observed, 'source_period_count': len(part),
            'session_completion_proven': False, 'week_completion_proven': False,
            'gap_semantics': 'calendar_dates_without_observations_not_proven_missing_sessions',
            'calendar_dates_without_observations': [(monday+timedelta(days=i)).isoformat()
                for i in range((today-monday).days+1) if (monday+timedelta(days=i)).isoformat() not in observed]}
        if part:
            wtd.update(source_start_date=observed[0], source_end_date=observed[-1],
                open=part[0]['open'], high=max(row['high'] for row in part),
                low=min(row['low'] for row in part), close=part[-1]['close'],
                open_to_close_percent=(part[-1]['close']/part[0]['open']-1)*100,
                availability_upper_bound_utc=max(_time(row['availability_ts_utc']) for row in part).isoformat(),
                build_upper_bound_utc=max(_time(row['build_ts_utc']) for row in part).isoformat())
            for field in ('volume', 'value', 'num_trades'):
                wtd[field] = sum(row[field] for row in part) if all(row.get(field) is not None for row in part) else None
            prior = [row for row in eligible if row['trade_date'] < monday.isoformat()]
            wtd['prior_observed_close_comparison'] = ({
                'target_date': prior[-1]['trade_date'], 'target_close': prior[-1]['close'],
                'percent_change': (part[-1]['close']/prior[-1]['close']-1)*100,
                'semantics': 'last_observed_close_before_week_not_proven_previous_session'} if prior else None)
        else:
            wtd['reason'] = 'no_retained_causal_D1_in_consumption_week'
        result['week_to_date'] = wtd
    if not _finite_tree(result):
        return {'schema_version': SCHEMA, 'status': 'UNAVAILABLE', 'reason': 'nonfinite_derived_values',
                'observations': [], 'comparisons': {}, 'historical_pit_usable': False, 'model_usable': False,
                'session_completion_proven': False, 'as_of_utc': now.isoformat()}
    return result


def apply(block, now):
    if 'observed_context_evidence' in block or 'observed_context' in block:
        evidence = block.get('observed_context_evidence')
        if (not isinstance(evidence, dict) or block.get('stage') != 7
                or block.get('dataset_id') != 'rub_native_ohlcv_htf'
                or evidence.get('instrument_id') != block.get('instrument_id')
                or evidence.get('timeframe') != block.get('timeframe')
                or evidence.get('source_provenance') != block.get('provenance')):
            evidence = None
        block['observed_context'] = describe(evidence, now=now)


def capture(frame, spec, provenance, *, now):
    """Called only after Stage9 pointer/hash/support and identity/causal validation."""
    from moex_data.step9_rub_analysis_bundle import _json_value, _to_utc_series
    eligible = frame.loc[_to_utc_series(frame, spec) <= now]
    if 'period_end_date' not in eligible:
        rows = []
        duplicate = False
    else:
        tail = eligible.sort_values('period_end_date', kind='mergesort').tail(30 if spec.timeframe == '1D' else 8)
        duplicate = bool(eligible.loc[eligible['period_end_date'].isin(tail['period_end_date']), 'period_end_date'].duplicated().any())
        rows = [{str(key): _json_value(value, str(key)) for key, value in row.items()} for row in tail.to_dict('records')]
    return {'schema_version': SCHEMA, 'instrument_id': spec.instrument_id, 'timeframe': spec.timeframe,
            'rows': rows, 'duplicate_retained_period': duplicate, 'source_provenance': deepcopy(provenance),
            'accepted_partition_coverage': {'row_count': len(frame), 'causal_row_count_at_capture': len(eligible),
                'capture_as_of_utc': now.isoformat(),
                'source_start_date': str(frame['period_start_date'].min()) if 'period_start_date' in frame else None,
                'source_end_date': str(frame['period_end_date'].max()) if 'period_end_date' in frame else None,
                'scope': 'accepted_partition_inventory_not_historical_PIT_or_session_coverage'}}
