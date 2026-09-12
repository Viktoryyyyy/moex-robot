"""Explicit read-view age boundaries; original evidence and values are untouched."""
from datetime import datetime, timezone


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _items(value):
    return value if isinstance(value, list) else []


def age(value, now):
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(parsed, datetime) or parsed.utcoffset() is None: return None
        return (now - parsed).total_seconds()
    except (TypeError, ValueError, OverflowError): return None


def times(values, now):
    return {'as_of_utc': now.isoformat(), **{name: {
        'timestamp_utc': value, 'age_seconds_at_as_of': age(value, now),
        'status': 'MISSING' if value is None else 'INVALID' if age(value, now) is None else
                  'FUTURE' if age(value, now) < 0 else 'DATED'} for name, value in values.items()}}


def market(row, now):
    """timestamp is an observation/update stamp, never proof of last trade time."""
    observation = row.get('timestamp')
    update = row.get('source_update_timestamp_utc')
    if update is None and row.get('timestamp_semantics') == 'source_row_update_time_not_last_trade_time': update = observation
    row['age_seconds'] = age(observation, now)
    row['freshness_reference_utc'] = now.isoformat()
    row['source_time_ages'] = times({'source_observation': observation, 'source_event': None,
        'source_update': update, 'availability': None, 'receipt': row.get('received_at_utc')}, now)


def metric(value, instruments, now):
    freshness = value.get('freshness')
    if isinstance(freshness, dict):
        freshness['age_seconds_by_leg'] = {key: age(_mapping(instruments.get(key)).get('timestamp'), now)
                                          for key in _items(value.get('legs')) if isinstance(key, str)}
        freshness['age_reference_utc'] = now.isoformat()
    value['age_reference_utc'] = now.isoformat()


def structure(levels, now):
    if not isinstance(levels, dict): return
    price = levels.get('price_context')
    if isinstance(price, dict):
        source = price.get('source_timestamp')
        price['source_time_ages'] = times({'source_event': source, 'source_update': None,
                                          'availability': None, 'receipt': None}, now)
        freshness = price.get('freshness')
        if isinstance(freshness, dict):
            freshness['age_seconds'] = age(source, now)
            freshness['age_reference_utc'] = now.isoformat()
            seconds = freshness['age_seconds']; limit = freshness.get('stale_after_seconds')
            if seconds is None or seconds < 0 or (isinstance(limit, (int, float)) and seconds > limit):
                freshness['status'] = 'STALE'
    for level in _items(levels.get('active_levels')):
        if isinstance(level, dict):
            level['age_seconds'] = age(level.get('created_at'), now)
            level['age_reference_utc'] = now.isoformat()


def block(value, now):
    selected = value.get('selected_causal_ts_utc')
    value['age_seconds_at_as_of'] = age(selected, now)
    value['age_reference_utc'] = now.isoformat()
    value['age_basis'] = 'selected_causal_ts_utc'
    observation = value.get('selected_observation')
    observation = observation if isinstance(observation, dict) else {}
    causal = value.get('causal_field')
    event = observation.get('snapshot_ts') or observation.get('snapshot_ts_utc')
    if event is None and causal == 'ts': event = observation.get('ts') or selected
    available = observation.get('availability_ts_utc')
    if available is None and causal == 'availability_ts_utc': available = selected
    value['source_time_ages'] = times({'source_event': event,
        'source_update': observation.get('source_publication_time') or observation.get('source_update_timestamp_utc'),
        'availability': available, 'receipt': observation.get('ingest_ts_utc') or observation.get('received_at_utc')}, now)


def hour(value, now):
    value['age_seconds_at_as_of'] = age(value.get('hour_end_utc'), now)
    value['age_reference_utc'] = now.isoformat()
    value['age_basis'] = 'hour_end_utc'
    value['source_time_ages'] = times({'source_event': value.get('hour_end_utc'), 'source_update': None,
        'availability': None, 'receipt': value.get('receipt_upper_bound_utc')}, now)


def apply(snapshot, *, now):
    """Mutate the already-independent canonical read view, not accepted frames."""
    now = now.astimezone(timezone.utc)
    components = _mapping(snapshot.get('components'))
    for key in ('stage9_daily', 'stage9_weekly'):
        data = _mapping(_mapping(components.get(key)).get('data'))
        for value in _items(_mapping(data.get('server_core')).get('blocks')):
            if isinstance(value, dict): block(value, now)
    # JSON persistence separates these assembly aliases from their component blocks.
    views = _mapping(snapshot.get('analysis_views'))
    for key in ('carry', 'cny_accepted_context'):
        for value in _items(views.get(key)):
            if isinstance(value, dict): block(value, now)
    instruments = _mapping(_mapping(_mapping(components.get('synchronized_live_market_oi')).get('data')).get('instruments'))
    for row in instruments.values():
        if isinstance(row, dict): market(row, now)
    pairs = _mapping(_mapping(_mapping(components.get('live_basis_carry')).get('data')).get('pairs'))
    for pair in pairs.values():
        if not isinstance(pair, dict): continue
        for leg in _mapping(pair.get('legs')).values():
            if isinstance(leg, dict): market(leg, now)
        for value in _items(pair.get('metrics')):
            if isinstance(value, dict): metric(value, instruments, now)
    data = _mapping(_mapping(components.get('live_market_structure')).get('data'))
    structure(data.get('structural_levels'), now)
    # hourly_observation is an immutable derivation witness. Normalize only its projection.
