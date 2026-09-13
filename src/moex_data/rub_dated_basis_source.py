"""Same-acquisition dated basis: independent admission, existing arithmetic."""
from copy import deepcopy

from moex_data import rub_dated_context as dated
from moex_data import rub_dated_market_source as market
from moex_data import rub_dated_source_admission as envelope
from moex_data import live_basis_carry_context as basis

SOURCE = 'dated_rfud_cets_same_acquisition_basis'


def refusals(legs, derived):
    result = {}
    for spec in basis.PAIR_SPECS:
        required = {}
        for prefix, a, b in (('perpetual_spot_basis', spec.perpetual, spec.spot), ('front_spot_basis', spec.front, spec.spot),
                            ('next_spot_basis', spec.next, spec.spot), ('front_perpetual_basis', spec.front, spec.perpetual),
                            ('next_perpetual_basis', spec.next, spec.perpetual), ('front_next_spread', spec.next, spec.front)):
            for suffix in ('abs', 'bps'): required[prefix + '_' + suffix] = (a, b)
        for name, keys in (('front_spot_implied_carry_annualized', (spec.front, spec.spot, spec.next)),
                           ('next_spot_implied_carry_annualized', (spec.next, spec.spot, spec.front)),
                           ('front_next_term_carry_annualized', (spec.next, spec.front))): required[name] = keys
        for name, keys in required.items():
            key = 'basis:' + spec.key + '.' + name
            if key in derived: continue
            missing = [leg for leg in keys if leg not in legs]
            result[key] = ('usd_spot_source_not_supported' if 'usd_tom' in missing else
                           'dated_leg_not_admitted:' + ','.join(missing) if missing else
                           'dated_pair_source_date_skew_or_expiry_not_admitted')
    return result


def same_observation(previous, frame, metric):
    if previous.get('source_id') != SOURCE: return False
    keys = set(metric['legs'])
    # Carry also depends on both expiry metadata, even for front/spot.
    if 'carry_semantics' in metric:
        spec = next(s for s in basis.PAIR_SPECS if s.pair_id == metric['pair_id'])
        keys.update((spec.front, spec.next))
    return all(previous.get('leg_evidence', {}).get(k, {}).get('revision_id') == frame['leg_evidence'][k]['revision_id'] for k in keys)


def _raw(legs):
    return {key: {field: frame[field] for field in ('raw_source_payload', 'identity', 'units',
                 'source_id', 'purpose', 'source_observation_at_utc')} for key, frame in legs.items()}


def make_frame(legs, *, now):
    frame = deepcopy(next(iter(legs.values())))
    frame.update(source_id=SOURCE, purpose='basis', identity={'derivation': 'same_acquisition_native_legs'},
                 units={'policy': 'live_basis_carry_context.PAIR_SPECS'},
                 raw_source_payload=_raw(legs), leg_evidence=deepcopy(legs),
                 request_started_at_utc=min((f['request_started_at_utc'] for f in legs.values()), key=dated.stamp),
                 received_at_utc=max((f['received_at_utc'] for f in legs.values()), key=dated.stamp),
                 source_observation_at_utc=min((f['source_observation_at_utc'] for f in legs.values()), key=dated.stamp))
    frame['raw_source_digest'] = dated.digest(frame['raw_source_payload'])
    frame['revision_id'] = market._revision(frame)
    return frame


def replay(frame):
    envelope.validate_envelope(frame, now=frame['accepted_at_utc'])
    legs = frame['leg_evidence']
    if (frame['purpose'] != 'basis' or frame['source_id'] != SOURCE or not isinstance(legs, dict)
            or not 2 <= len(legs) <= 7 or frame['raw_source_payload'] != _raw(legs)
            or frame['identity'] != {'derivation': 'same_acquisition_native_legs'}
            or frame['units'] != {'policy': 'live_basis_carry_context.PAIR_SPECS'}):
        raise ValueError('invalid_dated_basis_bundle')
    if (frame['received_at_utc'] != max((e['received_at_utc'] for e in legs.values()), key=dated.stamp)
            or frame['source_observation_at_utc'] != min((e['source_observation_at_utc'] for e in legs.values()), key=dated.stamp)):
        raise ValueError('dated_basis_custody_mismatch')
    instruments = {}
    requests = {evidence['request_started_at_utc'] for evidence in legs.values()}
    if len(requests) != 1 or next(iter(requests)) != frame['request_started_at_utc']:
        raise ValueError('dated_basis_mixed_acquisitions')
    for key, evidence in legs.items():
        if (evidence['accepted_at_utc'] != frame['accepted_at_utc'] or
                evidence['generation_at_utc'] != frame['generation_at_utc'] or
                evidence['identity']['logical_id'] != key):
            raise ValueError('dated_basis_mixed_acquisitions')
        instruments[key] = market.replay(evidence)
    result = {}
    for spec in basis.PAIR_SPECS:
        observations = {}
        for key, divisor, raw_unit in ((spec.spot, 1., spec.spot_raw_unit), (spec.perpetual, 1., spec.perpetual_raw_unit),
                                      (spec.front, spec.front_divisor, spec.front_raw_unit), (spec.next, spec.next_divisor, spec.next_raw_unit)):
            if key in instruments:
                observations[key] = {**instruments[key], 'normalized_rate': instruments[key]['last'] / divisor,
                                     'normalization_divisor': divisor, 'raw_unit': raw_unit, 'normalized_unit': spec.unit}
        def sync(comparison, reference):
            pair = (comparison, reference)
            if not all(key in observations for key in pair): return None
            times = {key: dated.stamp(observations[key]['timestamp']) for key in pair}
            skew = (max(times.values()) - min(times.values())).total_seconds()
            if skew > basis.MAX_SKEW_SECONDS or len({t.astimezone(basis.MOSCOW).date() for t in times.values()}) != 1:
                return None
            return {'synchronized': True, 'status': 'DATED_PREPARATION_ONLY', 'unavailable_reason': None,
                    'data_as_of': max(times.values()).isoformat(), 'source_timestamps': {k: t.isoformat() for k, t in times.items()},
                    'source_refs': {k: {f: observations[k].get(f) for f in ('secid', 'source_id', 'received_at_utc', 'expiry_date')} for k in pair},
                    'freshness': {'status': 'DATED_96H', 'threshold_seconds': dated.MAX_AGE_SECONDS,
                                  'age_seconds_by_leg': {k: observations[k]['age_seconds'] for k in pair}},
                    'max_leg_skew_seconds': skew, 'max_accepted_skew_seconds': basis.MAX_SKEW_SECONDS,
                    'source_trade_date': next(iter(times.values())).astimezone(basis.MOSCOW).date().isoformat()}
        def add(metric):
            if metric['status'] == 'READY':
                if 'expiry_metadata' in metric:
                    metric['expiry_metadata']['source_trade_date_semantics'] = 'source_row_observation_update_date_Europe/Moscow_not_last_trade_event'
                metric.update(status='DATED_PREPARATION_ONLY', current_usable=False, model_usable=False, historical_pit_usable=False,
                              status_semantics='source_replayed_dated_derivation_not_live', rate_field='last')
                metric.pop('live_rate_field', None)
                result['basis:' + metric['metric_id']] = metric
        for prefix, comp, ref in (('perpetual_spot_basis', spec.perpetual, spec.spot), ('front_spot_basis', spec.front, spec.spot),
                                 ('next_spot_basis', spec.next, spec.spot), ('front_perpetual_basis', spec.front, spec.perpetual),
                                 ('next_perpetual_basis', spec.next, spec.perpetual), ('front_next_spread', spec.next, spec.front)):
            checked = sync(comp, ref)
            if checked:
                for kind in ('abs', 'bps'):
                    add(basis._basis_metric(spec, observations, prefix + '_' + kind, comp, ref, kind, sync_override=checked))
        if spec.front in observations and spec.next in observations:
            for metric_id, comp, ref, horizon in (('front_spot_implied_carry_annualized', spec.front, spec.spot, 'front_spot'),
                    ('next_spot_implied_carry_annualized', spec.next, spec.spot, 'next_spot'),
                    ('front_next_term_carry_annualized', spec.next, spec.front, 'front_next')):
                checked = sync(comp, ref)
                if checked: add(basis._carry_metric(spec, observations, metric_id, comp, ref, horizon, sync_override=checked))
    return result
