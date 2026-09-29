"""Supplement missing exact Price/OI and dated own-leg bases, without promotion."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import base64

from moex_data import rub_contract_price_market_oi_observed as price
from moex_data import rub_historical_basis_carry_context as basis
from moex_data import rub_exact_comparison_sources as source
from moex_data import rub_cny_basis_calendar as calendar

STORE_KEY = 'accepted_exact_comparison_sources'
SCHEMA = 'exact_comparison_evidence.v1'
SCHEMA_V2 = 'exact_comparison_evidence.v2'
require = source.require
stamp = source.stamp
digest = source.digest


def targets(dates, current_day):
    dated = {dates[-1-lag] for lag in (0, 1, 5, 20) if len(dates) > lag}
    current = dates if current_day == dates[-1] else (dates+[current_day])[-22:]
    if date.fromisoformat(current_day)-date.fromisoformat(dates[-1]) > timedelta(days=1): current = []
    return sorted(dated | {current[-1-lag] for lag in (1, 5, 20) if len(current) > lag}), [dates[-1-lag] for lag in (0, 1, 5) if len(dates) > lag]


def _binding(e, buffers, now):
    price._portable_witness(e, buffers, now)
    price._portable_native_body(e['binding_proof'], buffers, e['bindings'], now)
    result = {}
    for item in e['binding_proof']['retained_http_inventory']:
        if item['role'] != 'selected_values': continue
        payload = price._source_object(price._buffer_bytes(buffers, item['response']))
        for row in price._table(payload, 'securities'):
            if row['SECID'] in e['bindings'].values(): result[row['SECID']] = row['LASTTRADEDATE']
    require(set(result) == set(e['bindings'].values()), 'exact_binding_expiry_inventory')
    for expiry in result.values(): price._day(expiry)
    return result


def _core(e):
    value = {k: deepcopy(e[k]) for k in ('bindings', 'binding_proof', 'observed_dates', 'witness_proof')}
    needed = {d for _,d in price._proof_references(value)}
    value['original_byte_buffers'] = {k:v for k,v in e['original_byte_buffers'].items() if k in needed}
    return value


def _basis_missing(e, day, bindings):
    # A selected rejected archive is decisive. An absent archive is an acquisition gap.
    if day not in e['history']:
        return e['source_errors'].get(day) == 'accepted_Stage4_run_for_exact_observed_date_unavailable'
    for pair, prefix in (('usd_rub', 'si'), ('cny_rub', 'cr')):
        for metric in e['history'][day].get(pair, {}).get('metrics', {}).values():
            for key in ('comparison_leg', 'reference_leg'):
                leg = metric.get(key) or {}; role = leg.get('instrument_id', '').removeprefix(prefix+'_').removesuffix('_contract')
                if role in ('front', 'next') and leg.get('secid') != bindings[prefix+'_'+role]: return True
    return False


def capture_snapshot(snapshot, previous, *, root, now_fn, refresh_started_at, http_get=None, env=None):
    """Called once by the existing heavy builder after legacy evidence capture."""
    started = stamp(now_fn()); require(started >= stamp(refresh_started_at), 'exact_capture_clock')
    try:
        p = price._admit(snapshot, started); e = _core(p)
        buffers = price._decode_buffers(e['original_byte_buffers']); expiries = _binding(e, buffers, started)
        budget = source.Budget(buffers.values())
        # The binding witness is independent of a usable current price/OI pair.
        current_day = started.astimezone(price.archive.MOSCOW).date().isoformat()
        # Acquisition only anticipates one observed current day. The consumer
        # separately proves that day from its own native current rows.
        price_days, basis_days = targets(e['observed_dates'], current_day)
        require(len(price_days) <= 7, 'exact_target_date_limit')
        selection = calendar.select(e['observed_dates'])
        e.update(schema_version=SCHEMA_V2, contract=source.contract('v2'), cny_spot_selection=selection, current_candidate_date=current_day,
                 expiry_dates=expiries, price_target_dates=price_days, basis_target_dates=basis_days,
                 capture_started_at_utc=started.isoformat(), entries={})
        try: b = basis._admit(snapshot, started)
        except (ValueError, KeyError, TypeError): b = None
        wanted = set()
        for day in price_days:
            if day >= current_day or day in p['source_errors']: continue
            for secid in e['bindings'].values():
                if secid not in p['history'].get(day, {}): wanted.add((day, secid))
        if b is not None and b['observed_dates'] == e['observed_dates']:
            for day in basis_days:
                if day < current_day and _basis_missing(b, day, e['bindings']):
                    wanted.update((day, secid) for secid in (*e['bindings'].values(), 'USDRUBF', 'CNYRUBF'))
            for day in selection['target_dates']:
                if day < current_day and _basis_missing(b, day, e['bindings']):
                    wanted.update((day, secid) for secid in (e['bindings']['cr_front'], e['bindings']['cr_next'], 'CNYRUBF', 'CNYRUB_TOM'))
        require(len(wanted) <= e['contract']['max_sources'], 'exact_target_source_limit')
        for day, secid in sorted(wanted):
            key = day+'/'+secid
            try:
                item = source.retained(root, day, secid, now_fn=now_fn, http_get=http_get, env=env, budget=budget)
                e['entries'][key] = {'source': item, 'source_sha256': digest(item), 'refusal': None}
            except source.BudgetExceeded: raise
            except (OSError, ValueError, TypeError, KeyError) as exc:
                e['entries'][key] = {'source': None, 'source_sha256': None, 'refusal': type(exc).__name__+': '+str(exc)[:180]}
        completed = stamp(now_fn()); require(completed >= started, 'exact_capture_completion_clock')
        e['accepted_at_utc'] = completed.isoformat()
        old = (previous or {}).get(STORE_KEY)
        if old and old.get('evidence') is not None:
            try:
                prior = _validate({**old,'error':None}, completed, enforce_lifetime=False)[0]
                keys = ('bindings', 'observed_dates', 'expiry_dates', 'price_target_dates', 'basis_target_dates', 'entries', 'contract', 'cny_spot_selection')
                if all(prior.get(k) == e[k] for k in keys): e = deepcopy(prior)
            except (ValueError, KeyError, TypeError): pass
        store = {'evidence': e, 'evidence_sha256': digest(e), 'checked_at_utc': completed.isoformat(), 'error': None}
        _validate(store, completed, enforce_lifetime=False)
        snapshot[STORE_KEY] = store
    except (ValueError, KeyError, TypeError, OSError) as exc:
        old = (previous or {}).get(STORE_KEY) or {}
        if (old.get('evidence') or {}).get('schema_version') != SCHEMA_V2:
            old = {}  # A new v2 capture failure never opts back into historical v1.
        snapshot[STORE_KEY] = {'evidence': deepcopy(old.get('evidence')), 'evidence_sha256':old.get('evidence_sha256'),
            'checked_at_utc': stamp(now_fn()).isoformat(), 'error': type(exc).__name__+': '+str(exc)[:180]}
    return stamp(snapshot[STORE_KEY]['checked_at_utc'])


def _validate(store, now, *, enforce_lifetime=True):
    require(set(store) == {'evidence', 'evidence_sha256', 'checked_at_utc', 'error'}, 'exact_store_shape')
    require(stamp(store['checked_at_utc']) <= now, 'exact_store_check_future')
    require(store['error'] is None or isinstance(store['error'],str), 'exact_capture_error_shape')
    e = store['evidence']; require(e is not None, store['error'] or 'exact_evidence_missing')
    require(digest(e) == store['evidence_sha256'], 'exact_evidence_hash')
    v2 = e.get('schema_version') == SCHEMA_V2
    expected_fields = {'schema_version', 'contract', 'bindings', 'binding_proof', 'observed_dates', 'witness_proof',
            'original_byte_buffers', 'expiry_dates', 'current_candidate_date', 'price_target_dates', 'basis_target_dates',
            'capture_started_at_utc', 'accepted_at_utc', 'entries'} | ({'cny_spot_selection'} if v2 else set())
    require(set(e) == expected_fields and e['schema_version'] in (SCHEMA,SCHEMA_V2)
            and e['contract'] == source.contract('v2' if v2 else 'v1'), 'exact_evidence_identity_or_contract')
    start, accepted = stamp(e['capture_started_at_utc']), stamp(e['accepted_at_utc'])
    require(start <= accepted <= stamp(store['checked_at_utc']) <= now, 'exact_evidence_noncausal')
    require(not enforce_lifetime or (now-accepted).total_seconds() <= 345600, 'exact_source_admission_expired')
    buffers = price._decode_buffers(e['original_byte_buffers'])
    require(set(buffers) == {d for _,d in price._proof_references({k:v for k,v in e.items() if k != 'entries'})}, 'exact_binding_buffer_inventory')
    require(e['expiry_dates'] == _binding(e, buffers, start), 'exact_expiry_original_mismatch')
    dates = e['observed_dates']; require(1 <= len(dates) <= 22 and dates == sorted(set(dates)), 'exact_witness_inventory')
    require(0 <= (start.astimezone(price.archive.MOSCOW).date()-date.fromisoformat(dates[0])).days <= 45,
            'exact_witness_lookback')
    current = e['current_candidate_date']; require(current == start.astimezone(price.archive.MOSCOW).date().isoformat()
            and current >= dates[-1], 'exact_current_candidate_date')
    p, b = targets(dates, current)
    require(e['price_target_dates'] == p and e['basis_target_dates'] == b, 'exact_target_indices')
    cny = []
    if v2:
        require(e['cny_spot_selection'] == calendar.select(dates), 'exact_CNY_calendar_selection_mismatch')
        cny = e['cny_spot_selection']['target_dates']
    require(len(set(p+b+cny)) <= e['contract']['max_target_dates'], 'exact_target_date_limit')
    entries = e['entries']; require(isinstance(entries, dict) and len(entries) <= e['contract']['max_sources'], 'exact_source_inventory_limit')
    budget = source.Budget(buffers.values()); rows = {}; errors = {}
    for key, item in entries.items():
        day, secid = key.split('/')
        require((day in p and secid in e['bindings'].values())
                or (day in b and secid in (('USDRUBF','CNYRUBF') if v2 else ('USDRUBF','CNYRUBF','CNYRUB_TOM')))
                or (day in cny and secid in (e['bindings']['cr_front'],e['bindings']['cr_next'],'CNYRUBF','CNYRUB_TOM')), 'exact_source_outside_selected_targets')
        require(set(item) == {'source', 'source_sha256', 'refusal'}, 'exact_source_entry_shape')
        if item['source'] is None:
            require(item['source_sha256'] is None and isinstance(item['refusal'], str) and bool(item['refusal']), 'exact_source_failure_shape')
            errors[key] = item['refusal']; continue
        require(item['refusal'] is None and digest(item['source']) == item['source_sha256'], 'exact_source_entry_hash')
        budget.source(item['source'])
        try: rows[key] = source.replay(item['source'], day=day, secid=secid, now=accepted)
        except (ValueError, KeyError, TypeError) as exc: errors[key] = type(exc).__name__+': '+str(exc)
    return e, rows, errors


def _supplement(snapshot, now):
    if STORE_KEY not in snapshot: return None, {}, {}
    try: return _validate(snapshot[STORE_KEY], stamp(now))
    except (ValueError, KeyError, TypeError) as exc: return None, {}, {'*': str(exc)}


def _metadata(snapshot, extra, errors):
    return {'errors':errors,'evidence_sha256':snapshot[STORE_KEY].get('evidence_sha256'),
        'schema_version':(snapshot[STORE_KEY].get('evidence') or {}).get('schema_version',SCHEMA),
        'last_capture_error':snapshot[STORE_KEY].get('error'),
        'admission_contract':source.CONTRACT.replace('_v1.', '_v2.') if extra and extra['schema_version']==SCHEMA_V2 else source.CONTRACT,
        'accepted_at_utc':extra['accepted_at_utc'] if extra else None,
        'valid_until_utc':(stamp(extra['accepted_at_utc'])+timedelta(seconds=345600)).isoformat() if extra else None,
        'source_scope':'exact_official_TradeStats_and_existing_CETS_5m_normalization',
        'role_scope':'current_binding_selects_exact_SECIDs_not_historical_roles',
        'source_revision_scope':'captured_now_not_historical_PIT'}


def enrich_price(e, snapshot, now):
    if STORE_KEY not in snapshot: return e
    result = deepcopy(e); extra, rows, errors = _supplement(snapshot, now)
    result['_exact'] = _metadata(snapshot, extra, errors)
    if extra is None: return result
    if extra['bindings'] != e['bindings'] or extra['observed_dates'] != e['observed_dates']:
        result['_exact']['errors'] = {'*': 'exact_source_binding_or_witness_changed'}; return result
    for day in extra['price_target_dates']:
        for secid in e['bindings'].values():
            key = day+'/'+secid
            if secid in e['history'].get(day, {}) or day in e['source_errors']: continue
            if key not in rows: continue
            try:
                fact = source.price_pair(rows[key], day=day, secid=secid, source=extra['entries'][key]['source'])
                fact['evidence_reference'] = {'source_envelope_digest': extra['entries'][key]['source_sha256'],
                    'response_digest': fact['proof']['response_sha256'], 'audit_reference': 'input_snapshot.json#/'+STORE_KEY+'/evidence/entries/'+key}
                result['history'].setdefault(day, {})[secid] = fact
            except (ValueError, KeyError, TypeError) as exc: result['_exact']['errors'][key] = str(exc)
    return result


def _legs(rows, day, secid, role, prefix, expiry):
    """Keep invalid observations at their timestamps so selection cannot skip them."""
    spot = role == 'spot'; result = {}
    if spot:
        import pandas as pd
        from moex_data.currency.materialize_cets_tom_raw_5m import normalize_to_5m
        for row in rows: source.quote(row, spot=True)
        frame = pd.DataFrame([{k.lower():v for k,v in r['raw'].items()} for r in rows])
        frame = normalize_to_5m(frame, trade_date=day, instrument_id='cny_tom', secid=secid, source_url=source.endpoint(secid))
        # The existing normalizer owns the grid. Its fresh ingest clock is not evidence.
        normalized = []
        for bar in frame.to_dict('records'):
            event = basis.resolver.utc(bar['ts']); own = [r for r in rows if event-timedelta(minutes=5) < r['event'] <= event]
            normalized.append({'event':event,'raw':{'OPEN':bar['open'],'HIGH':bar['high'],'LOW':bar['low'],'CLOSE':bar['close']},
                'receipt':max(r['receipt'] for r in own), 'response_sha256':sorted({r['response_sha256'] for r in own})})
        rows = normalized
    for row in rows:
        try:
            value, publication = source.quote(row, spot=spot)
            divisor = 1000 if prefix == 'si' and role in ('front','next') else 1
            result[row['event']] = {'secid':secid, 'instrument_id':prefix+'_'+role+'_contract' if role in ('front','next') else 'cny_tom' if spot else secid.lower()+'_futures_family',
                'trade_date':day,'source_timestamp_utc':row['event'].isoformat(),'received_at_utc':row['receipt'].isoformat(),
                'price':value,'normalization_divisor':divisor,'normalized_rate':float(Decimal(str(value))/divisor),
                'expiry_date':expiry,'price_source_field':'close' if spot else 'PR_CLOSE',
                'timestamp_semantics':'five_minute_bar_endpoint_not_official_session_close',
                'price_publication_at_utc':publication.isoformat() if publication else None,
                'evidence_reference':{'response_digest':row['response_sha256']}}
        except (ValueError, KeyError, TypeError) as exc: result[row['event']] = {'error': str(exc)}
    return result


def _basis_day(extra, rows, errors, day):
    result = {}
    for pair, prefix, root, perpetual in (('usd_rub','si','Si','USDRUBF'),('cny_rub','cr','CR','CNYRUBF')):
        secids = {role: extra['bindings'][prefix+'_'+role] for role in ('front','next')}
        secids.update(perpetual=perpetual, spot='USD000UTSTOM' if prefix == 'si' else 'CNYRUB_TOM')
        legs = {}; failures = {}; metrics = {}
        expiry = extra['expiry_dates']; days = {r:(date.fromisoformat(expiry[secids[r]])-date.fromisoformat(day)).days for r in ('front','next')}
        days['term'] = days['next']-days['front']
        require(all(v > 0 for v in days.values()), 'exact_basis_expiry_tenor')
        for role, secid in secids.items():
            key = day+'/'+secid; legs[role] = {}
            if role == 'spot' and prefix == 'si': failures[role] = 'USD_spot_production_scope_not_admitted'; continue
            if key not in rows: failures[role] = errors.get(key, 'exact_target_source_not_captured'); continue
            try: legs[role] = _legs(rows[key], day, secid, role, prefix, expiry.get(secid))
            except (ValueError, KeyError, TypeError) as exc: failures[role] = str(exc)
        for name, (c,r,unit,horizon) in basis.METRICS.items():
            reason = failures.get(c) or failures.get(r)
            shared = set(legs[c]) & set(legs[r])
            if reason is None and shared:
                selected = max(shared)
                reason = legs[c][selected].get('error') or legs[r][selected].get('error')
            if reason:
                item = {'status':'UNAVAILABLE','reason':reason,'value':None}
            else: item = basis.resolver.metric_endpoint(name,c,r,legs,days,unit,horizon,root)
            metrics[name] = item
        result[pair] = {'trade_date':day,'run_id':None,'source_kind':'EXACT_TARGET_OWN_LEGS_V1',
            'role_binding_as_of_utc':extra['binding_proof']['binding_as_of_utc'],
            'role_binding_scope':'same_SECIDs_selected_now_not_historical_role_proof','metrics':metrics}
    return result


def enrich_basis(e, snapshot, now):
    if STORE_KEY not in snapshot: return e
    result = deepcopy(e); extra, rows, errors = _supplement(snapshot, now)
    result['_exact'] = _metadata(snapshot, extra, errors)
    selected = snapshot[STORE_KEY].get('evidence') or {}
    if selected.get('schema_version') != SCHEMA or 'cny_spot_selection' in selected:
        # A damaged/expired selected v2 cannot silently restore the v1 calendar.
        result['_cny_spot_selection'] = {'eligible_observed_dates': [], 'target_dates': [],
            'reason': errors.get('*','exact_CNY_selection_unavailable'), 'schema_version':calendar.SCHEMA}
    if extra is None: return result
    if extra['observed_dates'] != e['observed_dates']:
        result['_exact']['errors'] = {'*':'exact_source_witness_changed'}; return result
    cny_days = []
    if extra['schema_version'] == SCHEMA_V2:
        result['_cny_spot_selection'] = deepcopy(extra['cny_spot_selection'])
        cny_days = extra['cny_spot_selection']['target_dates']
    for day in sorted(set(extra['basis_target_dates']+cny_days)):
        if not _basis_missing(e, day, extra['bindings']): continue
        try:
            additions = _basis_day(extra, rows, errors, day)
            if extra['schema_version'] == SCHEMA_V2:
                for pair in list(additions):
                    additions[pair]['metrics'] = {name:item for name,item in additions[pair]['metrics'].items()
                        if (pair=='cny_rub' and 'spot' in name and day in cny_days)
                        or (not (pair=='cny_rub' and 'spot' in name) and day in extra['basis_target_dates'])}
            # Preserve an already admitted metric with the same leg identities.
            for pair, pair_data in additions.items():
                old = e['history'].get(day,{}).get(pair,{}).get('metrics',{})
                for name, metric in pair_data['metrics'].items():
                    previous_metric = old.get(name) or {}
                    own_legs = [previous_metric.get(k) or {} for k in ('comparison_leg','reference_leg')]
                    matches = all(leg.get('instrument_id') not in price.INSTRUMENTS.values()
                        or leg.get('secid') == extra['bindings'].get(next(r for r,i in price.INSTRUMENTS.items() if i == leg['instrument_id'])) for leg in own_legs)
                    if previous_metric.get('status') == 'AVAILABLE' and matches:
                        pair_data['metrics'][name] = deepcopy(old[name])
            for pair, pair_data in additions.items():
                retained = result['history'].get(day,{}).get(pair,{}).get('metrics',{})
                pair_data['metrics'] = {**retained, **pair_data['metrics']}
            result['history'][day] = additions
            result['source_errors'].pop(day,None)
        except (ValueError, KeyError, TypeError) as exc: result['_exact']['errors'][day] = str(exc)
    return result


def price_coverage(view, e):
    if '_exact' not in e: return view
    counts = {str(lag):0 for lag in (1,5,20)}
    for item in view.get('contracts',{}).values():
        for lag, change in item['changes'].items():
            available = change['values'] is not None
            change['status'] = 'AVAILABLE' if available else 'UNAVAILABLE'; counts[lag] += available
            if not available:
                key = str(change['target_observed_trade_date'])+'/'+item['secid']; errors = e['_exact']['errors']
                change['reason'] = errors.get(key, errors.get('*', e['source_errors'].get(change['target_observed_trade_date'],change['reason'])))
    view['comparison_coverage'] = {k:{'available':v,'required':4,'status':'AVAILABLE' if v==4 else 'PARTIAL' if v else 'UNAVAILABLE'} for k,v in counts.items()}
    view['supplemental_evidence'] = {'schema_version':SCHEMA, 'evidence_digest':e['_exact']['evidence_sha256'],
        **{k:v for k,v in e['_exact'].items() if k!='evidence_sha256'}, 'audit_reference':'input_snapshot.json#/'+STORE_KEY}
    return view


def basis_coverage(view, e):
    if '_exact' not in e: return view
    counts = {'1':0,'5':0}
    for pair in view.get('pairs',{}).values():
        for item in pair['metrics'].values():
            for lag, change in item['changes'].items():
                available = change['change'] is not None
                change['status'] = 'AVAILABLE' if available else 'UNAVAILABLE'; counts[lag] += available
                if not available:
                    base = change.get('baseline') or {}; anchor = item.get('anchor') or {}
                    change['reason'] = base.get('reason') or anchor.get('reason') or e['_exact']['errors'].get('*') or change['reason']
    view['comparison_coverage'] = {k:{'available':v,'required':30,'status':'AVAILABLE' if v==30 else 'PARTIAL' if v else 'UNAVAILABLE'} for k,v in counts.items()}
    view['supplemental_evidence'] = {'schema_version':SCHEMA, 'evidence_digest':e['_exact']['evidence_sha256'],
        **{k:v for k,v in e['_exact'].items() if k!='evidence_sha256'}, 'audit_reference':'input_snapshot.json#/'+STORE_KEY}
    return view


def prepare_publication_expiry(snapshot, prepared, *, now):
    """Precompute a verified legacy-only projection before the final live clock.

    The supplemental deadline can precede the legacy deadline. No evidence I/O
    or new acquisition is deferred to final publication; valid legacy metrics
    remain available if just the supplemental admission expires in that gap.
    """
    if prepared is None or STORE_KEY not in snapshot: return
    extra, _, _ = _supplement(snapshot, now)
    if extra is None: return
    masked = dict(snapshot)
    masked[STORE_KEY] = {**snapshot[STORE_KEY], 'evidence':None, 'evidence_sha256':None,
                        'error':'exact_source_admission_expired'}
    prepared['exact_comparison_expiry'] = {
        'valid_until_utc':(stamp(extra['accepted_at_utc'])+timedelta(seconds=345600)).isoformat(),
        'contexts':{'contract_price_market_oi_context':price.describe(masked,now=now),
                    basis.OUTPUT_KEY:basis.describe(masked,now=now)}}
    # The forced projection refusal is not a new source capture failure.
    for context in prepared['exact_comparison_expiry']['contexts'].values():
        for view in ('dated','current'):
            metadata = context.get(view,{}).get('supplemental_evidence')
            if metadata is not None:
                metadata['evidence_digest'] = snapshot[STORE_KEY]['evidence_sha256']
                metadata['last_capture_error'] = snapshot[STORE_KEY]['error']


def apply_publication_expiry(prepared, *, now):
    """Pure downgrade at the publication clock; never renew source or admission."""
    fallback = (prepared or {}).get('exact_comparison_expiry')
    if fallback is None or stamp(now) <= stamp(fallback['valid_until_utc']): return
    for key, context in fallback['contexts'].items():
        prepared['release'][key] = deepcopy(context)
        prepared['release'][key]['checked_at_utc'] = stamp(now).isoformat()
