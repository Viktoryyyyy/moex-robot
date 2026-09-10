"""Bounded acceptance witnesses in the canonical snapshot, never live fallback."""
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from math import isfinite

SCHEMA = 'rub_dated_context.v1'
MAX_AGE_SECONDS = 96 * 3600
MARKETS = ('usdrubf', 'si_front', 'si_next', 'cnyrubf', 'cr_front', 'cr_next', 'cnyrub_tom')


def stamp(value):
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.utcoffset() is None:
        raise ValueError('aware evidence timestamp required')
    return result.astimezone(timezone.utc)


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _number(value, positive=False):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and (value > 0 if positive else value >= 0)


def _age(value, now):
    age = (now - stamp(value)).total_seconds()
    if not 0 <= age <= MAX_AGE_SECONDS:
        raise ValueError('dated_source_age_outside_96_hours')
    return age


def _view(frame):
    return {'identity': {'generated_at_utc': frame['accepted_at_utc']},
            'components': deepcopy(frame['components'])}


def _structure_valid(component, accepted):
    from dataclasses import fields
    from moex_research.intelligence.usdrubf_level_structure import LevelZone, InteractionSnapshot
    try:
        data = component['data']; levels = data['structural_levels']; price = levels['price_context']
        if data.get('instrument') != 'USDRUBF' or levels.get('instrument') != 'USDRUBF' or levels.get('schema_version') != 'usdrubf_structural_levels_snapshot.v1': return False
        if price.get('requested_secid') != 'USDRUBF' or price.get('source_id') != 'moex_algopack_fo_tradestats_5m' or price.get('closed_bar_only') is not True or not _number(price.get('price'), True): return False
        if stamp(price['source_timestamp']) != stamp(levels['data_as_of']): return False
        zones = levels['active_levels']; interactions = levels['level_interactions']
        if not isinstance(zones, list) or not zones or not isinstance(interactions, list): return False
        ids = set()
        for row in zones:
            if not all(_number(row.get(key), True) for key in ('center_price', 'lower_bound', 'upper_bound')): return False
            zone = LevelZone(**{field.name: row[field.name] for field in fields(LevelZone)})
            if zone.level_id in ids or stamp(zone.created_at) > accepted: return False
            if row['provenance']['requested_secid'] != 'USDRUBF': return False
            ids.add(zone.level_id)
        interaction_ids = []
        for row in interactions:
            interaction = InteractionSnapshot(**{field.name: row[field.name] for field in fields(InteractionSnapshot)})
            if row['provenance']['requested_secid'] != 'USDRUBF' or stamp(row['provenance']['source_data_as_of']) > accepted: return False
            for key in ('event_timestamp', 'as_of_timestamp'):
                if row.get(key) is not None and stamp(row[key]) > stamp(levels['data_as_of']): return False
            if type(row.get('touch_count')) is not int or not _number(row.get('structural_quality')): return False
            interaction_ids.append(interaction.level_id)
        if len(interaction_ids) != len(ids) or set(interaction_ids) != ids: return False
        return levels['methodology'].get('lookahead_forbidden') is True
    except (KeyError, TypeError, ValueError, AttributeError): return False


def eligible(frame):
    """Re-run original admission, not a stored accepted=true label."""
    from moex_data import rub_factual_projection as projection
    accepted = stamp(frame['accepted_at_utc'])
    if stamp(frame['generation_at_utc']) > accepted:
        raise ValueError('generation_after_acceptance')
    view = _view(frame)
    if frame['kind'] == 'market':
        from moex_data.rub_snapshot_read_freshness import apply_read_freshness
        component = view['components']['synchronized_live_market_oi']
        raw = component['data']
        quality = raw.get('quality', {})
        if (component.get('status') not in ('READY', 'PARTIAL') or raw.get('status') not in ('READY', 'PARTIAL')
                or not isinstance(quality, dict) or quality.get('status') not in ('PASS', 'PARTIAL')
                or quality.get('factual_context_usable') is not True):
            raise ValueError('original_market_component_or_quality_not_admitted')
        original_usable = quality.get('price_oi_usable_by_instrument', {})
        if not isinstance(original_usable, dict): original_usable = {}
        instruments = raw['instruments']
        if not isinstance(instruments, dict) or set(instruments) - set(MARKETS): raise ValueError('market_scope_mismatch')
        invalid = set()
        from moex_data.synchronized_live_market_oi_context import FORTS_SOURCE_ID, CETS_SOURCE_ID
        for key, item in instruments.items():
            try:
                if item.get('logical_id') != key or not item.get('secid') or raw['bindings'].get(key) != item['secid']:
                    raise ValueError('market_identity_mismatch')
                expected_source = CETS_SOURCE_ID if key == 'cnyrub_tom' else FORTS_SOURCE_ID
                if item.get('source_id') != expected_source or stamp(item['timestamp']) > accepted or stamp(item['received_at_utc']) > accepted:
                    raise ValueError('noncausal_market_evidence')
            except (KeyError, TypeError, ValueError, AttributeError): invalid.add(key)
        from src.moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot_live_market_oi import _load_basis_carry_or_unavailable
        if view['components']['live_basis_carry']['data'] != _load_basis_carry_or_unavailable(raw):
            raise ValueError('basis_differs_from_original_frame_derivation')
        view = apply_read_freshness(view, now=accepted)
        from moex_data.synchronized_live_market_oi_context_partial import _future_price_oi_usable
        keys = {}
        for key, item in projection.market_data(view)['instruments'].items():
            usable = projection.spot_usable(view) if key == 'cnyrub_tom' else original_usable.get(key) is True and item.get('price_oi_usable') is True and projection.fresh(item, accepted) and _future_price_oi_usable(item)
            if key not in invalid and usable and _number(item.get('last'), True) and (key == 'cnyrub_tom' or _number(item.get('oi'))):
                keys['market:' + key] = item
        for path, metric in projection.basis_metrics(view):
            legs = metric['legs']
            pair_id = path.split('.')[4]
            pair = view['components']['live_basis_carry']['data']['pairs'][pair_id]
            if not all('market:' + key in keys and projection.contract_metadata(view, key, instruments[key]) is not None
                       and pair['legs'][key].get('secid') == instruments[key]['secid'] for key in legs):
                continue
            keys['basis:' + metric['metric_id']] = metric
        return keys
    if frame['kind'] != 'slow': raise ValueError('invalid_frame_kind')
    context = projection.consumer_context(view)
    result = {}
    if context['market_structure']['status'] == 'AVAILABLE' and _structure_valid(view['components']['live_market_structure'], accepted):
        result['structure'] = context['market_structure']
    from moex_data.rub_hourly_observation import admitted as admitted_hour
    hour = admitted_hour(view['components'].get('live_market_structure', {}), now=accepted)
    if hour is not None:
        result['timeframe:observed_1H.USDRUBF'] = {'values': hour}
    return result


def _source_times(frame, key, value):
    if key.startswith('market:'):
        return {'source_event_at_utc': value['timestamp'], 'source_update_at_utc': value.get('source_update_timestamp_utc'),
                'received_at_utc': value['received_at_utc'], 'available_at_utc': None}
    if key.startswith('basis:'):
        instruments = frame['components']['synchronized_live_market_oi']['data']['instruments']
        return {leg: _source_times(frame, 'market:' + leg, instruments[leg]) for leg in value['legs']}
    if key == 'structure':
        assessments = [row['as_of_timestamp'] for row in value['values']['level_interactions'] if row.get('as_of_timestamp') is not None]
        return {'source_event_at_utc': value['values']['data_as_of'], 'source_update_at_utc': None,
                'available_at_utc': None, 'received_at_utc': frame['accepted_at_utc'],
                'receipt_semantics': 'collection_completion_upper_bound',
                'last_interaction_assessment_at_utc': max(assessments, key=stamp) if assessments else None}
    block = value['values']
    return {'source_event_at_utc': block['hour_end_utc'], 'source_update_at_utc': None,
            'available_at_utc': None, 'received_at_utc': block['receipt_upper_bound_utc'],
            'receipt_semantics': block['receipt_semantics']}


def _ages(times, now):
    result = {}
    for key, value in times.items():
        if isinstance(value, dict): result[key] = _ages(value, now)
        elif key.endswith('_at_utc'): result[key.replace('_at_utc', '_age_seconds_at_as_of')] = None if value is None else _age(value, now)
    return result


def validated(store, now):
    """Return only bounded, digest-verified, independently re-admitted witnesses."""
    if not isinstance(store, dict) or store.get('schema_version') != SCHEMA:
        return {}, {'evidence': 'missing_or_invalid_acceptance_witness'}
    frames = store.get('frames'); selections = store.get('selections')
    if not isinstance(frames, dict) or not isinstance(selections, dict) or len(frames) > 64 or len(selections) > 64:
        return {}, {'evidence': 'malformed_or_unbounded_acceptance_witness'}
    result, rejected = {}, {}
    cache = {}
    for key, ref in selections.items():
        try:
            frame = frames[ref]
            if digest(frame) != ref: raise ValueError('acceptance_digest_mismatch')
            _age(frame['accepted_at_utc'], now)
            if ref not in cache: cache[ref] = eligible(frame)
            value = cache[ref][key]
            times = _source_times(frame, key, value)
            ages = _ages(times, now)
            result[key] = (ref, frame, value, times, ages)
        except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
            rejected[key] = str(exc) if isinstance(exc, ValueError) else 'malformed_or_missing_acceptance_evidence'
    return result, rejected


def capture(previous, *, components, now, kind):
    """Producer-only admission. Existing payloads/times are never renewed by retention."""
    now = stamp(now)
    admitted, _ = validated(previous, now)
    frames = {ref: deepcopy(frame) for ref, frame, *_ in admitted.values()}
    selections = {key: entry[0] for key, entry in admitted.items()}
    frame = {'kind': kind, 'accepted_at_utc': now.isoformat(), 'generation_at_utc': now.isoformat(),
             'components': deepcopy(components)}
    try:
        keys = eligible(frame)
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError): keys = {}
    if keys:
        ref = digest(frame); frames[ref] = frame
        def stable(value):
            if isinstance(value, dict):
                return {key: stable(item) for key, item in value.items() if 'age_seconds' not in key
                        and 'receipt' not in key and 'received_at' not in key
                        and key not in ('freshness_reference_utc', 'checked_at_utc')}
            if isinstance(value, list): return [stable(item) for item in value]
            return value
        for key, value in keys.items():
            prior = admitted.get(key)
            def observation(source_frame, source_value):
                result = {'value': stable(source_value)}
                if key.startswith('basis:'):
                    instruments = source_frame['components']['synchronized_live_market_oi']['data']['instruments']
                    result['legs'] = {leg: stable(instruments[leg]) for leg in source_value['legs']}
                if key.startswith('timeframe:'):
                    result['source_bars'] = source_frame['components']['live_market_structure']['data']['hourly_observation']['source_bars']
                return result
            if prior and observation(prior[1], prior[2]) == observation(frame, value):
                continue
            selections[key] = ref
    referenced = set(selections.values())
    return {'schema_version': SCHEMA, 'frames': {key: value for key, value in frames.items() if key in referenced},
            'selections': selections}


def capture_slow(snapshot, previous, *, now):
    names = ('live_market_structure',)
    components = {key: snapshot['components'][key] for key in names if key in snapshot['components']}
    snapshot['accepted_dated_slow'] = capture((previous or {}).get('accepted_dated_slow'), components=components, now=now, kind='slow')


def describe(snapshot, *, now):
    """Dated preparation context never contributes values to current facts."""
    now = stamp(now)
    observations = {}; rejected = {}
    for field in ('accepted_dated_slow', 'accepted_dated_market'):
        accepted, errors = validated(snapshot.get(field), now)
        rejected.update({field + ':' + key: value for key, value in errors.items()})
        for key, (ref, frame, value, times, ages) in accepted.items():
            components = snapshot.get('components', {})
            if key.startswith('market:'):
                current = components.get('synchronized_live_market_oi', {})
                row = (current.get('data') or {}).get('instruments', {}).get(key.split(':', 1)[1], {})
                current_reason = row.get('read_freshness_reason') or current.get('refresh_error')
                current_status = current.get('status', 'UNAVAILABLE')
            else:
                component_key = 'live_basis_carry' if key.startswith('basis:') else 'live_market_structure'
                current = components.get(component_key, {})
                current_status = current.get('status', 'UNAVAILABLE')
                current_reason = current.get('refresh_error')
                if key.startswith('basis:'):
                    metrics = [metric for pair in (current.get('data') or {}).get('pairs', {}).values() for metric in pair.get('metrics', []) if metric.get('metric_id') == key.split(':', 1)[1]]
                    current_reason = next((metric.get('unavailable_reason') for metric in metrics if metric.get('status') != 'READY'), current_reason)
                    if not metrics: current_reason = current_reason or 'current_metric_missing'
                elif key.startswith('timeframe:'):
                    from moex_data.rub_hourly_observation import admitted
                    if admitted(current, now=now) is None:
                        current_reason = current_reason or (current.get('data') or {}).get('hourly_observation', {}).get('reason') or 'current_hour_missing_expired_or_evidence_not_admitted'
                elif current_status != 'READY': current_reason = current_reason or 'current_structure_component_not_ready'
            item = {'scope': 'LAST_ACCEPTED_DATED_PREPARATION_ONLY', 'current_usable': False,
                    'latest_component_status': current_status, 'current_refusal_or_admission_detail': current_reason,
                    'source_generation_at_utc': frame['generation_at_utc'], 'accepted_at_utc': frame['accepted_at_utc'],
                    'checked_at_utc': now.isoformat(), 'acceptance_evidence_id': ref,
                    'source_times': times, 'ages': ages, 'values': deepcopy(value)}
            if key.startswith('market:'):
                from moex_data.rub_factual_projection import market_values, IDENTITY_FIELDS, contract_metadata
                item['source_identity'] = {name: value[name] for name in IDENTITY_FIELDS if name in value}
                item['values'] = market_values(value, spot=key == 'market:cnyrub_tom')
                item['contract_metadata'] = contract_metadata(_view(frame), key.split(':', 1)[1], value)
            elif key.startswith('basis:'):
                market = frame['components']['synchronized_live_market_oi']['data']
                item['original_bindings'] = {leg: market['bindings'][leg] for leg in value['legs']}
                item['original_legs'] = {leg: deepcopy(market['instruments'][leg]) for leg in value['legs']}
            observations[key] = item
    return {'status': 'AVAILABLE' if observations else 'UNAVAILABLE', 'maximum_source_age_seconds': MAX_AGE_SECONDS,
            'scope': 'preparation_only_not_current_or_completed_session', 'as_of_utc': now.isoformat(),
            'observations': observations, 'refusals': rejected, 'current_failures_remain_in_current_coverage': True}
