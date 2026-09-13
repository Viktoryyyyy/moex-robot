"""Preparation-only replay of bounded native RFUD/CETS observations."""
from copy import deepcopy
from datetime import date
import re

from moex_data import rub_dated_context as dated
from moex_data import rub_dated_source_admission as envelope
from moex_data import synchronized_live_market_oi_context as core
from moex_data import synchronized_live_market_oi_context_apim as apim


def units(key):
    price = 'RUB_per_1000_USD' if key.startswith('si_') else 'RUB_per_USD' if key == 'usdrubf' else 'RUB_per_CNY'
    return {'last': price, **({'oi': 'contracts'} if key != 'cnyrub_tom' else {})}


def _rows(payload, block):
    columns, rows = core._table_parts(payload, block, allow_empty=True)
    if len(columns) != len(set(columns)) or any(len(row) != len(columns) for row in rows):
        raise ValueError('ambiguous_native_table')
    return [dict(zip(columns, row)) for row in rows]


def _bindings(reference, at):
    result = dict(core.STATIC_FUTURES_SECIDS)
    for root, prefix in (('Si', 'si'), ('CR', 'cr')):
        try:
            selected = core.front_next_binding.bind_front_next(core._table_frame(reference, 'securities'), root=root,
                as_of_date=at.astimezone(core.MOSCOW).date().isoformat(), availability_ts_utc=at.isoformat(), minimum_days_to_expiry=1)
            result.update({prefix + '_' + row['role']: row['secid'] for row in selected})
        except ValueError:
            continue
    return result


def collect(target, *, payload, source_url, requested, received, future):
    """Bound evidence before it leaves the authenticated adapter; no secrets."""
    try:
        securities = _rows(payload, 'securities') if future else []
        # Preserve root selection evidence, not the entire RFUD market universe.
        securities = [row for row in securities if row.get('SECID') in core.STATIC_FUTURES_SECIDS.values()
                      or re.fullmatch(r'(Si|CR)[HMUZ][0-9]', str(row.get('SECID', '')), re.I)]
        if len(securities) > 64: return
        reference = {'securities': {'columns': list(core.FUTURES_SECURITY_COLUMNS),
                     'data': [[row.get(field) for field in core.FUTURES_SECURITY_COLUMNS] for row in securities]}}
        bindings = _bindings(reference, received) if future else {'cnyrub_tom': 'CNYRUB_TOM'}
        native = _rows(payload, 'marketdata')
        for key, secid in bindings.items():
            matching = [row for row in native if row.get('SECID') == secid]
            if len(matching) != 1: continue
            receipt = payload.get(core.FORTS_ROW_RECEIPTS_KEY, {}).get(secid, received.isoformat()) if future else received.isoformat()
            target[key] = {'source_url': source_url, 'request_started_at_utc': requested.isoformat(),
                           'received_at_utc': receipt, 'binding_at_utc': received.isoformat(),
                           'binding_reference': deepcopy(reference),
                           'raw_source_payload': {'marketdata_row': matching[0], 'securities': {
                               'columns': reference['securities']['columns'],
                               'data': [r for r in reference['securities']['data'] if r[0] == secid]}}}
    except (ValueError, KeyError, TypeError, AttributeError, core.SynchronizedLiveMarketOIError):
        return


def _revision(frame):
    revision = {key: deepcopy(frame[key]) for key in
                ('source_id', 'purpose', 'identity', 'units', 'scope', 'revision_semantics')}
    revision.update(raw_source_digest=frame['raw_source_digest'],
                    source_observation_at_utc=dated.stamp(frame['source_observation_at_utc']).isoformat())
    return dated.digest(revision)


def make_frame(key, acquisition, *, now):
    raw = deepcopy(acquisition['raw_source_payload'])
    secid = raw['marketdata_row']['SECID']
    future = key != 'cnyrub_tom'
    frame = {'schema_version': envelope.SCHEMA, 'origin': envelope.ORIGIN, 'kind': 'market',
             'scope': 'preparation_only', 'revision_semantics': 'observed_now_not_historical_pit',
             'current_usable': False, 'historical_pit_usable': False, 'model_usable': False,
             'source_id': core.FORTS_SOURCE_ID if future else core.CETS_SOURCE_ID,
             'purpose': 'market:' + key, 'units': units(key),
             'identity': {'logical_id': key, 'secid': secid, 'boardid': 'RFUD' if future else 'CETS',
                          'source_url': acquisition['source_url'].split('?', 1)[0]},
             'source_response_url': acquisition['source_url'], 'binding_reference': deepcopy(acquisition['binding_reference']),
             'raw_source_payload': raw, 'raw_source_digest': dated.digest(raw),
             'source_observation_at_utc': core._source_event_time(raw['marketdata_row']['SYSTIME'], 'SYSTIME').isoformat(),
             'request_started_at_utc': acquisition['request_started_at_utc'],
             'received_at_utc': acquisition['received_at_utc'], 'binding_at_utc': acquisition['binding_at_utc'],
             'accepted_at_utc': now.isoformat(), 'generation_at_utc': now.isoformat()}
    frame['revision_id'] = _revision(frame)
    return frame


def replay(frame):
    """Derive values from native evidence; never renew freshness or trust labels."""
    accepted = dated.stamp(frame['accepted_at_utc'])
    envelope.validate_envelope(frame, now=accepted)
    if dated.stamp(frame['generation_at_utc']) != accepted:
        raise ValueError('source_generation_acceptance_mismatch')
    key = frame['identity']['logical_id']
    if key not in core.LOGICAL_ORDER or frame['purpose'] != 'market:' + key:
        raise ValueError('unsupported_source_replay')
    future = key != 'cnyrub_tom'
    expected_source = core.FORTS_SOURCE_ID if future else core.CETS_SOURCE_ID
    identity = frame['identity']; secid = identity['secid']
    if frame['source_id'] != expected_source or frame['units'] != units(key):
        raise ValueError('source_or_units_mismatch')
    endpoint = core.FORTS_ENDPOINT if future else core.CETS_ENDPOINT
    from urllib.parse import urlsplit
    url = urlsplit(identity['source_url'])
    if (url.scheme, url.netloc, url.path) != ('https', 'apim.moex.com', endpoint):
        raise ValueError('source_endpoint_mismatch')
    response_url = urlsplit(frame['source_response_url'])
    if (response_url.scheme, response_url.netloc, response_url.path) != (url.scheme, url.netloc, url.path):
        raise ValueError('source_response_endpoint_mismatch')
    # Query parameters are request transport, never historical/date evidence.
    from urllib.parse import parse_qs
    if set(parse_qs(response_url.query)) - {'iss.meta', 'iss.only', 'securities.columns', 'marketdata.columns', 'start'}:
        raise ValueError('unsupported_source_query')
    if identity['boardid'] != ('RFUD' if future else 'CETS'):
        raise ValueError('source_board_mismatch')
    receipt = dated.stamp(frame['received_at_utc']); binding_at = dated.stamp(frame['binding_at_utc'])
    if not receipt <= binding_at <= accepted:
        raise ValueError('binding_acquisition_mismatch')
    raw = frame['raw_source_payload']; row = raw['marketdata_row']
    if row['SECID'] != secid: raise ValueError('native_secid_mismatch')
    numeric_fields = set(core.FUTURES_MARKETDATA_COLUMNS if future else core.CETS_MARKETDATA_COLUMNS) - {'SECID', 'SYSTIME'}
    for field in numeric_fields:
        value = row.get(field)
        if value is not None and not dated._number(value):
            raise ValueError('native_numeric_value_invalid')
    if not dated._number(row.get('LAST'), True) or (future and
            (not dated._number(row.get('OPENPOSITION')) or not float(row['OPENPOSITION']).is_integer())):
        raise ValueError('native_price_or_oi_invalid')
    security = None
    if future:
        securities = _rows(raw, 'securities')
        if not 1 <= len(securities) <= 64: raise ValueError('unbounded_binding_evidence')
        reference = frame['binding_reference']
        if len(_rows(reference, 'securities')) > 64: raise ValueError('unbounded_binding_evidence')
        binding = _bindings(reference, binding_at)
        if binding.get(key) != secid: raise ValueError('native_binding_mismatch')
        matches = [r for r in securities if r.get('SECID') == secid]
        if len(matches) != 1: raise ValueError('native_security_ambiguous')
        security = matches[0]
        if [r for r in _rows(reference, 'securities') if r.get('SECID') == secid] != [security]:
            raise ValueError('native_binding_metadata_mismatch')
        if security.get('BOARDID') != 'RFUD' or any(not dated._number(security.get(f), True) for f in ('MINSTEP', 'STEPPRICE')):
            raise ValueError('native_contract_metadata_invalid')
    elif secid != 'CNYRUB_TOM': raise ValueError('native_spot_identity_mismatch')
    normalized_row = deepcopy(row)
    if future: normalized_row['VALTODAY'] = None  # existing APIM WAP semantics remain unproven
    item = core._normalize_row(logical_id=key, secid=secid, row=normalized_row, source_id=expected_source,
                              received_at_utc=receipt, freshness_reference_utc=accepted,
                              is_future=future, security_row=security)
    if dated.stamp(item['timestamp']) != dated.stamp(frame['source_observation_at_utc']):
        raise ValueError('native_observation_mismatch')
    if future:
        item.update(wap=None, wap_method=None, wap_status=apim.FORTS_WAP_STATUS)
    if key in apim.EXPIRING_LOGICAL_IDS:
        item['expiry_date'] = date.fromisoformat(security['LASTTRADEDATE']).isoformat()
    item.update(current_usable=False, model_usable=False, historical_pit_usable=False)
    item['dated_contract_metadata'] = {'secid': secid, 'boardid': identity['boardid'], 'units': units(key),
        'expiry_date': item.get('expiry_date'), 'native_security_row': deepcopy(security),
        'binding_at_utc': binding_at.isoformat(), 'binding_semantics': 'concrete_contract_selected_at_receipt_not_historical_front'}
    return item


def capture(previous, acquisitions, *, now):
    """Independent source candidates, stable first acceptance per revision."""
    admitted, _ = dated.validated(previous, now)
    frames = {ref: deepcopy(frame) for ref, frame, *_ in admitted.values()}
    selections = {key: value[0] for key, value in admitted.items()}
    candidate_legs = {}
    refusals = {'market:' + key: 'source_observation_not_acquired_or_binding_unavailable'
                for key in core.LOGICAL_ORDER if key not in acquisitions}
    for key, acquisition in acquisitions.items():
        try:
            frame = make_frame(key, acquisition, now=now)
            item = replay(frame)
            candidate_legs[key] = frame
            selection = 'market:' + key
            prior = admitted.get(selection)
            if prior:
                old_frame, old_value = prior[1:3]
                if (old_frame.get('revision_id') == frame['revision_id'] or
                        dated.stamp(old_value['timestamp']) > dated.stamp(item['timestamp'])): continue
                if old_frame.get('origin', 'previously_accepted_live') == 'previously_accepted_live' and old_value['timestamp'] == item['timestamp']:
                    continue
            ref = dated.digest(frame); frames[ref] = frame; selections[selection] = ref
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError, core.SynchronizedLiveMarketOIError) as exc:
            refusals['market:' + key] = str(exc) if isinstance(exc, ValueError) else 'native_source_evidence_malformed'
            continue
    from moex_data import rub_dated_basis_source as basis
    derived = {}
    if len(candidate_legs) >= 2:
        frame = basis.make_frame(candidate_legs, now=now)
        try:
            derived = basis.replay(frame)
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError, core.SynchronizedLiveMarketOIError):
            derived = {}
        for key, value in derived.items():
            prior = admitted.get(key)
            if prior and (basis.same_observation(prior[1], frame, value) or
                          dated.stamp(prior[2]['data_as_of']) > dated.stamp(value['data_as_of'])):
                continue
            if prior and prior[1].get('origin', 'previously_accepted_live') == 'previously_accepted_live' and prior[2]['data_as_of'] == value['data_as_of']:
                continue
            ref = dated.digest(frame); frames[ref] = frame; selections[key] = ref
    refusals.update(basis.refusals(candidate_legs, derived))
    return {'schema_version': dated.SCHEMA, 'frames': {ref: frame for ref, frame in frames.items() if ref in selections.values()},
            'selections': selections, 'last_source_admission_refusals': refusals}
