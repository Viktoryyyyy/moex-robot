"""Complete observed clock hours from the existing live closed-5m source only."""
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from moex_data.rub_dated_context import stamp, _number, MAX_AGE_SECONDS

SCHEMA = 'rub_observed_clock_hour.v1'
SOURCE = 'moex_algopack_fo_tradestats_5m'
CONTRACT = 'contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml'


def aggregate(rows):
    """Reuse numeric OHLCV aggregation after strict complete-hour validation."""
    if not isinstance(rows, list) or len(rows) != 12: raise ValueError('hour_requires_12_observed_bars')
    parsed = []
    for row in rows:
        if set(row) != {'end', 'open', 'high', 'low', 'close', 'volume'}: raise ValueError('invalid_hour_source_fields')
        end = stamp(row['end'])
        if end.second or end.microsecond or end.minute % 5: raise ValueError('unaligned_5m_end')
        if not all(_number(row.get(key), key != 'volume') for key in ('open', 'high', 'low', 'close', 'volume')): raise ValueError('invalid_hour_numeric_values')
        if not row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high']: raise ValueError('invalid_hour_ohlc')
        parsed.append({**row, 'end': end})
    ends = [row['end'] for row in parsed]
    finish = ends[-1]
    start = finish - timedelta(hours=1)
    if finish.minute or ends != [start + timedelta(minutes=5 * index) for index in range(1, 13)]: raise ValueError('incomplete_duplicate_or_unordered_clock_hour')
    dates = {end.astimezone(ZoneInfo('Europe/Moscow')).date() for end in ends}
    if len(dates) != 1: raise ValueError('mixed_observed_source_dates')
    from moex_data.futures.resampling import _aggregate_group
    # Only its existing OHLCV arithmetic is reused. No manifest/calendar contract is granted.
    identity = SimpleNamespace(family=None, secid=None, board=None, market=None, series_type=None)
    request = SimpleNamespace(timeframe='1h', identity=identity)
    inputs = tuple({**row, 'ts': row['end'] - timedelta(minutes=5),
                    'trade_date': next(iter(dates)), 'session_date': next(iter(dates))} for row in parsed)
    result = _aggregate_group(inputs, request=request)
    return {'instrument': 'USDRUBF', 'timeframe': '1H', 'timezone': 'UTC',
            'hour_start_utc': start.isoformat(), 'hour_end_utc': finish.isoformat(),
            'source_observed_moscow_date': next(iter(dates)).isoformat(),
            'source_bar_count': 12, 'values': {key: result[key] for key in ('open', 'high', 'low', 'close', 'volume')},
            'scope': 'complete_observed_clock_hour_not_accepted_HTF_dataset_or_session_completion',
            'session_completion_proven': False}


def build(rows, *, now, receipt):
    """Called only with the existing producer's validated actual closed source bars."""
    now, receipt = stamp(now), stamp(receipt)
    if receipt > now: raise ValueError('receipt_after_hour_check')
    groups = {}
    for row in rows:
        end = stamp(row['end'])
        start = (end - timedelta(microseconds=1)).replace(minute=0, second=0, microsecond=0)
        groups.setdefault(start, []).append({key: end.isoformat() if key == 'end' else row[key]
                                            for key in ('end', 'open', 'high', 'low', 'close', 'volume')})
    for start in sorted(groups, reverse=True):
        try:
            source = groups[start]
            hour = aggregate(source)
            if stamp(hour['hour_end_utc']) > receipt or (now - stamp(hour['hour_end_utc'])).total_seconds() > MAX_AGE_SECONDS: continue
            return {'schema_version': SCHEMA, 'status': 'AVAILABLE', 'requested_secid': 'USDRUBF',
                    'source_id': SOURCE, 'source_contract_ref': CONTRACT,
                    'receipt_upper_bound_utc': receipt.isoformat(), 'receipt_semantics': 'current_loader_return_upper_bound',
                    'source_bars': source, 'observation': hour}
        except (KeyError, TypeError, ValueError): continue
    return {'schema_version': SCHEMA, 'status': 'UNAVAILABLE', 'reason': 'no_complete_12_bar_observed_clock_hour'}


def admitted(component, *, now):
    try:
        if component.get('status') != 'READY': return None
        data = component['data']; evidence = data['hourly_observation']
        if data.get('instrument') != 'USDRUBF' or data.get('requested_secid') != 'USDRUBF' or data.get('source_id') != SOURCE or data.get('source_contract_ref') != CONTRACT: return None
        if evidence.get('schema_version') != SCHEMA or evidence.get('status') != 'AVAILABLE': return None
        if evidence.get('requested_secid') != 'USDRUBF' or evidence.get('source_id') != SOURCE or evidence.get('source_contract_ref') != CONTRACT: return None
        receipt = stamp(evidence['receipt_upper_bound_utc']); now = stamp(now)
        hour = aggregate(evidence['source_bars'])
        if data.get('trade_date') != hour['source_observed_moscow_date'] or evidence.get('receipt_semantics') != 'current_loader_return_upper_bound': return None
        if hour != evidence['observation'] or not stamp(hour['hour_end_utc']) <= receipt <= now: return None
        if not 0 <= (now - stamp(hour['hour_end_utc'])).total_seconds() <= MAX_AGE_SECONDS: return None
        return {**deepcopy(hour), 'source_id': SOURCE, 'requested_secid': 'USDRUBF',
                'receipt_upper_bound_utc': receipt.isoformat(), 'receipt_semantics': evidence['receipt_semantics']}
    except (KeyError, TypeError, ValueError, AttributeError): return None
