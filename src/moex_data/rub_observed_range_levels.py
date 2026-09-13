"""Neutral observed-range boundaries from already acquired native source pages."""
from copy import deepcopy
from datetime import timedelta

from moex_data import rub_dated_context as dated, rub_dated_hour_source as hour
from moex_data import rub_dated_source_admission as envelope

PURPOSE = 'structure:observed_range_levels.USDRUBF'


def derive(pages, source_date, *, now):
    rows = hour.native_rows(pages, source_date)
    if not rows: raise ValueError('observed_range_has_no_bars')
    ends = [end for end, _, _ in rows]
    if ends != sorted(set(ends)): raise ValueError('observed_range_duplicate_or_unordered_bars')
    for end, raw, receipt in rows:
        if end > receipt: raise ValueError('observed_range_bar_after_receipt')
        dated._age(end, now)
        if not all(dated._number(raw[key], key != 'vol') for key in ('pr_open', 'pr_high', 'pr_low', 'pr_close', 'vol')):
            raise ValueError('observed_range_invalid_numeric')
        if not raw['pr_low'] <= min(raw['pr_open'], raw['pr_close']) <= max(raw['pr_open'], raw['pr_close']) <= raw['pr_high']:
            raise ValueError('observed_range_invalid_ohlc')
    missing = []
    for left, right in zip(ends, ends[1:]):
        candidate = left + timedelta(minutes=5)
        while candidate < right:
            missing.append(candidate.isoformat()); candidate += timedelta(minutes=5)
    low = min(raw['pr_low'] for _, raw, _ in rows); high = max(raw['pr_high'] for _, raw, _ in rows)
    value = {'instrument': 'USDRUBF', 'source_id': hour.hourly.SOURCE, 'source_observed_moscow_date': source_date,
             'source_first_bar_end_utc': ends[0].isoformat(), 'source_last_bar_end_utc': ends[-1].isoformat(),
             'source_bar_count': len(rows), 'price_unit': 'RUB_per_USD',
             'source_page_count': len(pages),
             'levels': [{'level_id': 'observed_range_low', 'level_type': 'RANGE_BOUNDARY', 'side': 'LOW', 'price': low},
                        {'level_id': 'observed_range_high', 'level_type': 'RANGE_BOUNDARY', 'side': 'HIGH', 'price': high}],
             'missing_5m_endpoints': missing, 'gap_count': len(missing), 'gaps_filled': False,
             'scope': 'observed_date_range_boundaries_not_session_levels_or_interaction_history',
             'session_completion_proven': False, 'current_usable': False, 'historical_pit_usable': False, 'model_usable': False}
    raw = {'columns': list(hour.COLUMNS), 'data': [[row[c] for c in hour.COLUMNS] for _, row, _ in rows]}
    return raw, value


def make_frame(acquisition, *, now):
    frame = hour.make_frame(acquisition, now=now)
    raw, value = derive(frame['source_pages'], frame['identity']['source_date'], now=now)
    frame.update(purpose=PURPOSE, raw_source_payload=raw, raw_source_digest=dated.digest(raw),
                 source_observation_at_utc=value['source_last_bar_end_utc'])
    from moex_data.rub_dated_market_source import _revision
    frame['revision_id'] = _revision(frame)
    return frame


def replay(frame):
    accepted = dated.stamp(frame['accepted_at_utc']); envelope.validate_envelope(frame, now=accepted)
    if frame['purpose'] != PURPOSE: raise ValueError('observed_range_purpose_mismatch')
    # Replay the underlying date's H1 custody contract without mutating the frame.
    copy = deepcopy(frame); copy['purpose'] = hour.PURPOSE
    raw_hour, selected_hour, _ = hour.select(frame['source_pages'], frame['identity']['source_date'], now=accepted)
    if selected_hour is None: raise ValueError('observed_range_source_hour_missing')
    copy.update(raw_source_payload=raw_hour, raw_source_digest=dated.digest(raw_hour), source_observation_at_utc=selected_hour['hour_end_utc'])
    from moex_data.rub_dated_market_source import _revision
    copy['revision_id'] = _revision(copy)
    hour.replay(copy)
    raw, value = derive(frame['source_pages'], frame['identity']['source_date'], now=accepted)
    if raw != frame['raw_source_payload'] or value['source_last_bar_end_utc'] != frame['source_observation_at_utc']:
        raise ValueError('observed_range_replay_mismatch')
    return {'values': value}


def capture(previous, acquisition, *, now):
    admitted, _ = dated.validated(previous, now)
    frames = {ref: deepcopy(frame) for ref, frame, *_ in admitted.values()}; selections = {key: value[0] for key, value in admitted.items()}
    refusal = None
    try:
        frame = make_frame(acquisition, now=now); value = replay(frame); prior = admitted.get(PURPOSE)
        if not prior or (prior[1]['revision_id'] != frame['revision_id'] and
                dated.stamp(prior[2]['values']['source_last_bar_end_utc']) <= dated.stamp(value['values']['source_last_bar_end_utc'])):
            ref = dated.digest(frame); frames[ref] = frame; selections[PURPOSE] = ref
    except (ValueError, KeyError, TypeError, AttributeError, IndexError) as exc:
        refusal = str(exc) if isinstance(exc, ValueError) else 'observed_range_source_evidence_missing_or_malformed'
    return {**(previous if isinstance(previous, dict) else {}), 'schema_version': dated.SCHEMA,
            'frames': {ref: frame for ref, frame in frames.items() if ref in selections.values()},
            'selections': selections, 'last_observed_range_refusal': refusal}
