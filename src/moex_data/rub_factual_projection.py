"""Read-time admission shared by the matrix and lossless factual projection."""
from datetime import datetime
from copy import deepcopy
from math import isfinite

from moex_data.rub_snapshot_read_freshness import MAX_LIVE_AGE_SECONDS, MAX_FUTURE_SKEW_SECONDS


def _dict(value):
    return value if isinstance(value, dict) else {}


def market_data(snapshot):
    return _dict(_dict(_dict(snapshot.get('components')).get('synchronized_live_market_oi')).get('data'))


def reference(snapshot):
    try:
        value = (snapshot['live_read_freshness']['read_at_utc'] if 'live_read_freshness' in snapshot
                 else snapshot['identity']['generated_at_utc'])
        now = datetime.fromisoformat(value)
        return now if now.utcoffset() is not None else None
    except (KeyError, TypeError, ValueError):
        return None


def fresh(item, now):
    try:
        age = (now - datetime.fromisoformat(item['timestamp'])).total_seconds()
        return item.get('stale') is False and -MAX_FUTURE_SKEW_SECONDS <= age <= MAX_LIVE_AGE_SECONDS
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def spot_usable(snapshot):
    data = market_data(snapshot)
    item = _dict(_dict(data.get('instruments')).get('cnyrub_tom'))
    quality = _dict(data.get('quality'))
    value = item.get('last')
    return (isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and value > 0
            and fresh(item, reference(snapshot)) and item.get('spot_price_usable') is not False
            and quality.get('spot_price_usable') is True)


def basis_metrics(snapshot):
    """One deterministic occurrence per admitted ID; conflicting duplicates revoke it."""
    components = _dict(snapshot.get('components'))
    component = components.get('live_basis_carry', {})
    if not isinstance(component, dict) or component.get('status') not in {'READY', 'PARTIAL'}:
        return []
    data = component.get('data')
    pairs = data.get('pairs') if isinstance(data, dict) else None
    if not isinstance(pairs, dict): return []
    instruments = _dict(market_data(snapshot).get('instruments'))
    now = reference(snapshot)
    grouped = {}
    for key in sorted(k for k in pairs if isinstance(k, str)):
        pair = pairs[key]
        metrics = pair.get('metrics') if isinstance(pair, dict) else None
        if not isinstance(metrics, list): continue
        for index, metric in enumerate(metrics):
            if not isinstance(metric, dict): continue
            identity = metric.get('metric_id')
            if not isinstance(identity, str) or not identity: continue
            path = f'components.live_basis_carry.data.pairs.{key}.metrics.{index}'
            grouped.setdefault(identity, []).append((path, metric))
    result = []
    for identity in sorted(grouped):
        copies = grouped[identity]
        path, metric = copies[0]
        if any(other != metric for _, other in copies): continue
        legs = metric.get('legs')
        value = metric.get('value')
        if (metric.get('status') == 'READY' and isinstance(legs, list) and legs
                and isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)
                and all(isinstance(leg, str) and fresh(instruments.get(leg, {}), now) for leg in legs)):
            result.append((path, metric))
    return result


MARKET_FIELDS = ('last', 'oi', 'open', 'high', 'low', 'close', 'volume', 'trades',
    'units', 'price_unit', 'quote_unit', 'oi_unit', 'expiry_date', 'expiry_metadata',
    'contract_size', 'price_scale', 'normalization', 'wap', 'wap_method')
QUOTE_FIELDS = ('bid', 'ask', 'spread')
IDENTITY_FIELDS = ('secid', 'logical_id', 'asset_type', 'timestamp', 'source_trade_date',
    'timestamp_semantics', 'source_update_timestamp_utc', 'received_at_utc', 'source_id',
    'last_trade_time_moscow', 'source_trading_status')
LEG_METADATA_FIELDS = ('raw_unit', 'normalization_divisor', 'normalized_unit', 'expiry_date', 'expiry_metadata')


def contract_metadata(snapshot, key, item):
    component = _dict(_dict(snapshot.get('components')).get('live_basis_carry'))
    from moex_data.synchronized_live_market_oi_context import FORTS_SOURCE_ID, CETS_SOURCE_ID, LOGICAL_ORDER
    expected_source = CETS_SOURCE_ID if key == 'cnyrub_tom' else FORTS_SOURCE_ID
    if key not in LOGICAL_ORDER or item.get('source_id') != expected_source:
        return None
    now = reference(snapshot)
    from moex_data.rub_dated_context import MAX_AGE_SECONDS
    if not _causal(item.get('received_at_utc'), now, MAX_AGE_SECONDS) or not _causal(item.get('timestamp'), now, MAX_AGE_SECONDS): return None
    matches = []
    for pair_id, pair in _dict(_dict(component.get('data')).get('pairs')).items():
        leg = _dict(_dict(_dict(pair).get('legs')).get(key))
        metadata_status = leg.get('status') == 'READY' or (leg.get('status') == 'UNAVAILABLE' and
            leg.get('unavailable_reason') in ('source_leg_stale', 'source_leg_freshness_exceeds_threshold', 'source_not_fresh_at_read'))
        if (not metadata_status or not all(item.get(field) is not None and leg.get(field) == item[field]
                for field in ('secid', 'timestamp', 'received_at_utc', 'source_id')) or leg.get('raw_value') != item.get('last')):
            continue
        metadata = {field: deepcopy(leg[field]) for field in LEG_METADATA_FIELDS if field in leg}
        matches.append((f'components.live_basis_carry.data.pairs.{pair_id}.legs.{key}', metadata))
    if not matches or any(value != matches[0][1] for _, value in matches): return None
    return {'snapshot_path': matches[0][0], 'values': matches[0][1], 'secid': item['secid'],
        'applicable_source_timestamp_utc': item['timestamp'], 'received_at_utc': item['received_at_utc'],
        'checked_at_utc': now.isoformat(), 'scope': 'exact_source_contract_metadata_independent_of_live_price'}


def market_values(item, *, spot=False):
    fields = MARKET_FIELDS + (QUOTE_FIELDS if item.get('quote_usable') is True else ())
    return {key: deepcopy(item[key]) for key in fields if key in item and not (spot and key == 'oi')}


def _causal(value, now, maximum_age=None):
    try:
        age = (now - datetime.fromisoformat(value)).total_seconds()
        return age >= 0 and (maximum_age is None or age <= maximum_age)
    except (TypeError, ValueError, AttributeError):
        return False


def _same_pair(pair, current):
    """Compare the engine's normalized payload to the complete admitted pair."""
    keys = ('trade_date', 'snapshot_ts', 'source_publication_time', 'availability_ts_utc', 'ingest_ts_utc', 'total_open_interest')
    if not all(pair.get(key) is not None and pair.get(key) == current.get(key) for key in keys): return False
    for side in ('fiz', 'yur'):
        left = _dict(pair.get(side)); right = _dict(current.get(side))
        fields = ('long', 'short', 'net', 'long_participants', 'short_participants')
        if not all(left.get(key) is not None and left.get(key) == right.get(key) for key in fields): return False
        try:
            if left.get('net_share_of_oi') != right['net'] / current['total_open_interest']: return False
        except (KeyError, TypeError, ZeroDivisionError): return False
    return True


def consumer_context(snapshot):
    """Compact, explicitly scoped dated context; does not grant model authority."""
    components = _dict(snapshot.get('components')); now = reference(snapshot)
    market = market_data(snapshot); instruments = _dict(market.get('instruments'))
    market_context = {}
    for key in ('usdrubf', 'si_front', 'si_next', 'cnyrubf', 'cr_front', 'cr_next', 'cnyrub_tom'):
        item = _dict(instruments.get(key))
        usable = spot_usable(snapshot) if key == 'cnyrub_tom' else item.get('price_oi_usable') is True and fresh(item, now)
        metadata = contract_metadata(snapshot, key, item)
        metadata_values = _dict(_dict(metadata).get('values'))
        quote_allowed = item.get('quote_usable') is True and fresh(item, now)
        market_context[key] = {'price_oi_usable': usable,
            'source_time_ages': deepcopy(item.get('source_time_ages')),
            'quote_usable': quote_allowed,
            'quote': {'values': {field: item.get(field) for field in QUOTE_FIELDS},
                'source_identity': {field: item[field] for field in IDENTITY_FIELDS if field in item}} if quote_allowed else None,
            'contract_metadata': metadata,
            'cross_market_comparison_usable': usable and _dict(market.get('synchronization')).get('synchronized') is True,
            'quote_status': item.get('quote_status', 'UNAVAILABLE'),
            'quote_reason': item.get('quote_reason'),
            'missing_metadata': (['units'] if not any(item.get(field) for field in ('units', 'price_unit', 'quote_unit')) and not metadata_values.get('raw_unit') else [])
                + (['expiry_date'] if key in ('si_front', 'si_next', 'cr_front', 'cr_next') and not (item.get('expiry_date') or metadata_values.get('expiry_date')) else []),
            'missing_reason': None if usable else item.get('read_freshness_reason') or
                _dict(components.get('synchronized_live_market_oi')).get('refresh_error') or 'source_missing_stale_or_not_admitted'}

    component = _dict(components.get('live_market_structure'))
    levels = _dict(_dict(component.get('data')).get('structural_levels'))
    allowed = (component.get('status') == 'READY' and levels.get('status') == 'FRESH'
        and _causal(levels.get('data_as_of'), now, 1200))
    structure = {'status': 'AVAILABLE' if allowed else 'UNAVAILABLE',
        'reason': None if allowed else 'missing_stale_or_unaccepted_structure',
        'session_completion_proven': False}
    if allowed:
        structure['values'] = deepcopy(levels)
        source_data = _dict(component.get('data'))
        structure['deterministic_context'] = {key: deepcopy(source_data[key]) for key in
            ('market_regime',) if key in source_data}
        ema = _dict(source_data.get('ema_3_19')); details = _dict(ema.get('details'))
        ema_allowed = (ema.get('quality_status') == 'OK' and _causal(ema.get('available_at'), now)
            and all(isinstance(details.get(key), (int, float)) and not isinstance(details.get(key), bool)
                and isfinite(details[key]) for key in ('ema_fast', 'ema_slow')))
        structure['deterministic_context']['ema_3_19'] = ({'status': 'AVAILABLE',
            'available_at': ema['available_at'], 'quality_status': 'OK',
            'values': {key: deepcopy(details[key]) for key in ('ema_fast', 'ema_slow', 'bar_count', 'source') if key in details},
            'relation': ema.get('direction'), 'relation_semantics': 'sign_of_ema_fast_minus_ema_slow_not_forecast',
            'standalone_directional_authority': False, 's7_2_verdict': 'REJECT_AS_STANDALONE_DIRECTIONAL_SIGNAL'}
            if ema_allowed else {'status': 'UNAVAILABLE', 'reason': 'ema_quality_values_or_causal_time_not_admitted'})
        if ema_allowed and 'trend' in source_data:
            structure['deterministic_context']['trend'] = deepcopy(source_data['trend'])
        structure['standalone_directional_authority'] = False
        extrema = structure['values'].get('observed_extrema', {})
        if 'prior_completed_session' in extrema:
            prior = extrema.pop('prior_completed_session')
            prior.pop('partial_session', None)
            prior.update(session_completion_proven=False, session_completion_state='UNKNOWN')
            extrema['prior_observed_date'] = prior

    timeframes = []
    seen = set()
    for name in ('stage9_daily', 'stage9_weekly'):
        component = _dict(components.get(name)); data = _dict(component.get('data'))
        if component.get('status') != 'READY': continue
        for block in _dict(data.get('server_core')).get('blocks', []):
            if not isinstance(block, dict): continue
            if (block.get('status') != 'ready' or block.get('stage') != 7
                    or block.get('timeframe') not in ('1H', '1D', '1W')
                    or not _causal(block.get('selected_causal_ts_utc'), now)):
                continue
            identity = (block.get('block_id'), block.get('selected_causal_ts_utc'))
            if identity in seen: continue
            seen.add(identity)
            timeframes.append({'snapshot_path': f'components.{name}.data.server_core.blocks',
                'scope': 'accepted_dated_observation_not_session_completion', 'values': deepcopy(block)})

    from moex_data.rub_hourly_observation import admitted as admitted_hour
    hour = admitted_hour(_dict(components.get('live_market_structure')), now=now)
    if hour is not None:
        from moex_data.rub_consumption_clock import hour as hour_clock
        hour_clock(hour, now)
        timeframes.append({'snapshot_path': 'components.live_market_structure.data.hourly_observation',
            'scope': hour['scope'], 'values': {'block_id': 'observed_1H.USDRUBF',
                'selected_causal_ts_utc': hour['hour_end_utc'], 'selected_causal_time_semantics': 'observed_hour_end_not_availability', **hour}})

    futoi = {}
    temporal = _dict(_dict(snapshot.get('temporal_applicability')).get('components'))
    for name in ('futoi_live', 'futoi_live_cr'):
        component = _dict(components.get(name)); data = _dict(component.get('data'))
        current = _dict(data.get('current_intraday'))
        admitted = (component.get('status') == 'READY' and data.get('consumer_factual_use_allowed') is True
            and data.get('factual_authority') is True)
        context = {'scope': 'current_pair_only' if name.endswith('_cr') else 'si_admitted_dated_context',
            'current_usable': admitted, 'previous_observation': None, 'comparisons': None,
            'reason': None if admitted else (current.get('refresh_error') or component.get('refresh_error')
                or _dict(_dict(temporal.get(name)).get('current')).get('reason') or 'latest_current_pair_not_admitted'),
            'session_completion_proven': False}
        if name == 'futoi_live' and _dict(data.get('governance')).get('factual_use_allowed') is True:
            previous = _dict(data.get('previous_completed_session'))
            prior_allowed = (_dict(_dict(temporal.get(name)).get('previous')).get('dated_observation_available') is True
                and previous.get('consumer_factual_use_allowed') is not False)
            if prior_allowed:
                context['previous_observation'] = deepcopy(previous.get('factual'))
            delta = _dict(data.get('delta_statistics')); pair = _dict(delta.get('current'))
            pair_fact = _dict(pair.get('factual')); current_fact = _dict(current.get('factual'))
            match = delta.get('instrument_id') == data.get('instrument_id') == 'si_futures_family' and _same_pair(pair_fact, current_fact)
            witness = _dict(delta.get('observed_date_witness'))
            witness_match = (witness.get('status') == 'PASS' and witness.get('current_observed_trade_date') == current_fact.get('trade_date'))
            if (admitted and pair.get('status') == 'AVAILABLE' and match and delta.get('consumer_factual_use_allowed') is not False and witness_match):
                context['comparisons'] = {key: deepcopy(delta[key]) for key in ('deltas', 'statistics',
                    'lag_targets', 'observed_date_witness', 'historical_context') if key in delta}
                for delta_name, item in context['comparisons'].get('deltas', {}).items():
                    if delta_name == 'delta_1d' and (not prior_allowed or not _same_pair(
                            _dict(_dict(delta.get('previous_observed_session')).get('factual')), _dict(previous.get('factual')))):
                        item.update(status='UNAVAILABLE', reason='previous_observation_not_admitted_or_baseline_mismatch')
                    if item.get('status') != 'AVAILABLE': item['values'] = None
                stats = context['comparisons'].get('statistics', {})
                if stats.get('status') != 'AVAILABLE': stats['variables'] = None
                for variable in (stats.get('variables') or {}).values():
                    for window in variable.get('windows', {}).values():
                        if window.get('status') != 'AVAILABLE':
                            for field in ('percentile', 'zscore', 'population_mean', 'population_std_ddof_0'): window.pop(field, None)
            context['comparison_limitation'] = None if context['comparisons'] else 'delta_current_identity_witness_or_admission_unavailable'
        futoi[name] = context

    component = _dict(components.get('official_news')); data = _dict(component.get('data'))
    from moex_data.rub_news_read_view import project as project_news
    news_selection = project_news(component, now=now)
    events = []
    for event in news_selection['events']:
        item = {key: deepcopy(event[key]) for key in ('event_id', 'source_id', 'source_reference',
            'source_tier', 'published_at', 'available_at', 'ingested_at', 'headline', 'event_type',
            'entities', 'content_hash', 'primary_provenance', 'publication_identity_policy', 'audit_ref',
            'publication_age_seconds_at_as_of', 'age_reference_utc', 'selection_band', 'retention_reason') if key in event}
        item.update(direction='UNKNOWN', classification_status='NOT_ANALYZED',
            content_status='AVAILABLE' if item.get('headline') else 'HEADLINE_NOT_PRESERVED_BY_SOURCE',
            event_semantics='source_publication_not_verified_economic_actual_or_consensus')
        events.append(item)
    news = {'events': events, 'summary': deepcopy(data.get('summary', {})),
        'source_status': component.get('status', 'UNAVAILABLE'),
        'source_acquisition_status': data.get('source_acquisition_status', 'LEGACY_COMPLETENESS_FROM_SUMMARY'),
        'source_refresh_attempted_at': component.get('refresh_attempted_at'),
        'source_refresh_error_class': component.get('refresh_error_class'),
        'source_refresh_error': component.get('refresh_error'),
        'acquisition_fresh': component.get('status') in {'READY', 'PARTIAL'} and _causal(
            component.get('refresh_attempted_at') or _dict(snapshot.get('identity')).get('generated_at_utc'), now, 1200),
        'classification_status': 'NOT_ANALYZED', 'direction': 'UNKNOWN',
        'selection_scope': news_selection['selection_scope'],
        'selection_at_read': news_selection['selection_at_read'],
        'excluded_event_count': news_selection['invalid_causal_or_identity_count'], 'source_as_of': component.get('data_as_of'),
        'status': 'AVAILABLE' if events else 'UNAVAILABLE'}

    position = _dict(snapshot.get('user_position_context'))
    valid = (position.get('status') == 'AVAILABLE' and position.get('explicit_user_input') is True
        and position.get('instrument') == 'USDRUBF' and _causal(position.get('user_input_updated_at'), now))
    price = position.get('average_entry_price'); direction = position.get('direction')
    valid = valid and ((direction == 'FLAT' and price is None) or (direction in ('LONG', 'SHORT')
        and isinstance(price, (int, float)) and not isinstance(price, bool) and isfinite(price) and price > 0))
    if valid:
        position = {key: position[key] for key in ('instrument', 'direction', 'average_entry_price', 'user_input_updated_at')}
        position.update(status='AVAILABLE', explicit_user_input=True)
    else:
        position = {'status': 'UNAVAILABLE', 'availability': 'INVALID_EXPLICIT_USER_INPUT' if position.get('explicit_user_input') is True
            or position.get('availability') == 'INVALID_EXPLICIT_USER_INPUT' else 'NO_EXPLICIT_USER_INPUT',
            'direction': None, 'average_entry_price': None, 'explicit_user_input': False}
    return {'market_usability': market_context, 'market_structure': structure, 'timeframe_context': timeframes,
        'futoi_context': futoi, 'news_context': news, 'user_position_context': position}
