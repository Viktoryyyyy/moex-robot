"""C1: separately admitted Stage4 dated basis/carry; native current is C2 debt."""
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import base64
import json
from moex_data import rub_contract_price_market_oi_observed as bytesource
from moex_data import rub_accepted_stage4_resolver as resolver

SCHEMA='historical_basis_carry_observed.v1'
STORE_KEY='accepted_historical_basis_carry'
OUTPUT_KEY='historical_basis_carry_context'
CONTRACT='contracts/intelligence/historical_basis_carry_dated_v1.json'
SCOPE='DATED_STAGE4_OWN_LEG_BASIS_CARRY_EXACT_OBSERVED_COMPARISONS'
TTL=345600
FLAGS={'current_usable':False,'historical_pit_usable':False,'session_completion_proven':False,
       'model_input_allowed':False,'signal_eligible':False,'decision_eligible':False,'action_authority':False}
POLICY={'project':'MOEX_Bot','schema_version':'historical_basis_carry_dated_admission.v1',
    'task_id':'historical_basis_carry_observed_comparisons_v1','scope':SCOPE,'dated_lifetime_seconds':TTL,
    'lags':[1,5],'witness_slots':22,'archive_directory_limit':256,'original_buffer_limit':1024,
    'original_bytes_limit':64000000,'original_single_buffer_limit':8000000,
    'source_kind':'CURRENT_REVALIDATED_STAGE4_OWN_INPUT_LEGS',
    'source_hash_scope':'current_revalidation_not_original_historical_digest_attestation',
    'alignment':'each_metric_exact_two_leg_timestamp_intersection_no_fill',
    'latest_invalid_run_policy':'refuse_date_without_older_run_fallback',
    'USD_spot_policy':'production_spot_dependent_metrics_refused',
    'current_requires_separate_native_contract_and_proof':True,'current_authority':False,
    'preserve_original_source_clocks':True,'no_original_full_reference_response_claim':True,**FLAGS}
require=bytesource._require
stamp=bytesource._stamp
digest=bytesource.common._digest
METRICS={name+suffix:(c,r,'normalized_rate_difference' if suffix=='_abs' else 'basis_points',None)
         for name,c,r in resolver.BASES for suffix in ('_abs','_bps')}
METRICS.update({name:(c,r,'annualized_fraction',h) for name,c,r,h in resolver.CARRIES})


def _contract():
    value=bytesource._source_object((Path(__file__).resolve().parents[2]/CONTRACT).read_bytes())
    require(digest(value)==digest(POLICY),'historical_basis_contract_invalid_or_revoked');return value


def _freeze(root,raw,budget):
    budget.charge(raw);key=bytesource.sha256(raw).hexdigest()
    path=root/'state/evidence/historical_basis_carry_observed_v1'/key[:2]/(key+'.bin')
    require(not path.is_symlink(),'historical_basis_immutable_path_invalid')
    if path.exists():require(bytesource._read_bytes(root,'${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix(),budget=budget)==raw,'historical_basis_immutable_bytes_changed')
    else:
        require(path.parent.resolve().is_relative_to(root.resolve()),'historical_basis_archive_escape')
        path.parent.mkdir(parents=True,exist_ok=True)
        with path.open('xb') as handle:handle.write(raw)


def _witness(root,now,budget):
    from moex_data import step9_rub_analysis_bundle as step9
    from moex_data.futures import futoi_delta_statistics_context as engine
    spec=engine._spec(stage=7,dataset_id=engine.OBSERVED_DATE_WITNESS_DATASET_ID,
        instrument_id=engine.OBSERVED_DATE_WITNESS_INSTRUMENT_ID,timeframe=engine.OBSERVED_DATE_WITNESS_TIMEFRAME)
    path=step9._pointer_path(root,spec);raw={'pointer':bytesource._read_bytes(root,'${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix(),budget=budget)}
    pointer=bytesource._source_object(raw['pointer'])
    for key in ('partition','manifest','quality_report'):raw[key]=bytesource._read_bytes(root,pointer[key+'_ref'],pointer[key+'_sha256'],budget=budget)
    from io import BytesIO
    import pandas as pd
    frame=pd.read_parquet(BytesIO(raw['partition']));dates=sorted({bytesource._day(str(x)) for x in frame.trade_date})[-22:]
    proof={key:bytesource._inline_ref(value) for key,value in raw.items()};buffers={bytesource.sha256(v).hexdigest():v for v in raw.values()}
    bytesource._portable_witness({'witness_proof':proof,'observed_dates':dates},buffers,now)
    return dates,proof,buffers


def _scan(root,source,dates,now,*,discovery=None):
    """Discovery budget precedes content reads; latest invalid attempt is decisive."""
    if discovery is None:
        parent=root/resolver.ARCHIVE;entries=[]
        require(not parent.is_symlink(),'historical_basis_archive_symlink')
        if parent.exists():
            for entry in parent.iterdir():
                entries.append(entry)
                require(len(entries)<=256,'historical_basis_archive_inventory_limit')
        discovery=sorted(entry.name[len('run_id='):] for entry in entries
            if entry.is_dir() and not entry.is_symlink() and entry.name.startswith('run_id='))
    require(isinstance(discovery,list) and len(discovery)<=256 and len(set(discovery))==len(discovery),'historical_basis_discovery_inventory')
    selected={};errors={};unordered=set()
    for run in discovery:
        require(isinstance(run,str) and resolver.re.fullmatch(r'[A-Za-z0-9_.-]+',run) is not None,'historical_basis_discovery_run')
        day=None
        try:
            pilot=source.json(resolver.ARCHIVE+'/run_id='+run+'/pilot_evidence.json');day=bytesource._day(pilot['trade_date'])
            if day not in dates:continue
            require(run.endswith('_stage4'),'stage4_successful_parent_missing')
            manifest=source.json('runs/step10_rub_daily_refresh/run_id='+run[:-7]+'/run_manifest.json')
            finished=stamp(manifest['finished_at_utc']);require(finished<=now,'stage4_future_parent_ordering_refused')
            if day not in selected or (finished,run)>selected[day]:selected[day]=(finished,run)
        except bytesource.CaptureBudgetExceeded:raise
        except OSError:
            if source.root is not None:raise
            if day in dates:unordered.add(day);errors[day]='stage4_parent_ordering_or_admission_unavailable'
        except (ValueError,KeyError,TypeError,OverflowError) as exc:
            if day in dates:unordered.add(day);errors[day]='stage4_parent_ordering_or_admission_unavailable'
            else:raise ValueError('stage4_archive_date_unreadable: '+str(exc)) from exc
    history={};runs={}
    for day,(_,run) in sorted(selected.items()):
        if day in unordered:continue
        try:history[day]=resolver.validate_run(source,run,now=now);runs[day]=run
        except bytesource.CaptureBudgetExceeded:raise
        except resolver.Stage4SourceReadError:raise
        except Exception as exc:
            source.budget.ensure_active()
            chain=exc;seen=set()
            while chain is not None and id(chain) not in seen:
                seen.add(id(chain))
                if isinstance(chain,resolver.Stage4SourceReadError):raise chain
                chain=chain.__cause__
            errors[day]='latest_Stage4_run_not_admitted'
    for day in dates:
        if day not in history:errors.setdefault(day,'accepted_Stage4_run_for_exact_observed_date_unavailable')
    return history,runs,errors,discovery


def _capture(root,now):
    budget=bytesource.CaptureBudget();dates,witness,buffers=_witness(root,now,budget)
    source=resolver.Source(root,budget=budget);history,runs,errors,discovery=_scan(root,source,dates,now)
    buffers.update(source.buffers)
    # All processing is budgeted before any C archive write.
    for raw in buffers.values():_freeze(root,raw,budget)
    return {'schema_version':SCHEMA,'contract':_contract(),'causal_cutoff_at_utc':now.isoformat(),
        'observed_dates':dates,'witness_proof':witness,'discovery_runs':discovery,'runs':runs,'history':history,'source_errors':errors,
        'original_artifacts':source.proof,'original_byte_buffers':{key:base64.b64encode(raw).decode('ascii') for key,raw in sorted(buffers.items())}}


def _validate_evidence(e,now,*,enforce_lifetime=True):
    require(isinstance(e,dict) and set(e)=={'schema_version','contract','causal_cutoff_at_utc','accepted_at_utc','observed_dates','witness_proof',
        'runs','history','source_errors','original_artifacts','original_byte_buffers','discovery_runs'},'historical_basis_evidence_shape')
    require(e['schema_version']==SCHEMA and digest(e['contract'])==digest(_contract()),'historical_basis_contract_revoked')
    cutoff,accepted=stamp(e['causal_cutoff_at_utc']),stamp(e['accepted_at_utc'])
    require(cutoff<=accepted<=now and (not enforce_lifetime or (now-accepted).total_seconds()<=TTL),'historical_basis_clock_or_expired')
    buffers=bytesource._decode_buffers(e['original_byte_buffers']);bytesource._portable_witness(e,buffers,cutoff)
    dates=e['observed_dates'];require(isinstance(dates,list) and 1<=len(dates)<=22,'historical_basis_witness_scope')
    require(isinstance(e['runs'],dict) and isinstance(e['history'],dict) and set(e['runs'])==set(e['history']) and set(e['runs'])<=set(dates),'historical_basis_run_date_inventory')
    errors=e['source_errors'];require(isinstance(errors,dict) and set(errors)==set(dates)-set(e['runs'])
        and all(isinstance(v,str) and v for v in errors.values()),'historical_basis_source_errors')
    artifacts=e['original_artifacts'];require(isinstance(artifacts,dict) and len(artifacts)<=1024,'historical_basis_original_inventory')
    discovered={key[len(resolver.ARCHIVE+'/run_id='):].split('/')[0] for key in artifacts
        if key.startswith(resolver.ARCHIVE+'/run_id=') and key.endswith('/pilot_evidence.json')}
    require(discovered==set(e['discovery_runs']),'historical_basis_discovery_original_inventory')
    source=resolver.Source(proof=artifacts,buffers=buffers)
    actual,runs,rejections,_=_scan(None,source,dates,cutoff,discovery=e['discovery_runs'])
    require(digest(actual)==digest(e['history']) and runs==e['runs'] and rejections==errors,'historical_basis_original_selection_or_fact_mismatch')
    require(set(artifacts)<=source.used,'historical_basis_unused_original_artifact')
    referenced={item['sha256'] for item in artifacts.values()}|{v['sha256'] for v in e['witness_proof'].values()}
    require(set(buffers)==referenced,'historical_basis_buffer_inventory')
    return e


def _admit(snapshot,now,*,enforce_lifetime=True):
    store=snapshot[STORE_KEY]
    require(isinstance(store,dict) and set(store)=={'evidence','evidence_sha256','last_capture_attempt_at_utc','last_capture_error'},'historical_basis_store_shape')
    require(digest(store['evidence'])==store['evidence_sha256'],'historical_basis_evidence_hash')
    e=_validate_evidence(store['evidence'],now,enforce_lifetime=enforce_lifetime)
    require(stamp(store['last_capture_attempt_at_utc'])>=stamp(e['accepted_at_utc']),'historical_basis_capture_chronology')
    require(store['last_capture_error'] is None or isinstance(store['last_capture_error'],str),'historical_basis_error_type')
    return e


def _semantic(e):
    # Frozen source bytes/clock/identity revisions are meaningful; capture clocks are not.
    return {key:deepcopy(e[key]) for key in ('schema_version','contract','observed_dates','witness_proof','discovery_runs','runs','history','original_artifacts','original_byte_buffers')}


def capture_snapshot(snapshot,previous,*,now_fn,refresh_started_at,previous_capture_completed=None):
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    start=stamp(now_fn());floor=stamp(refresh_started_at);old=deepcopy((previous or {}).get(STORE_KEY))
    if previous_capture_completed is not None:floor=max(floor,stamp(previous_capture_completed))
    if old is not None:floor=max(floor,stamp(old['last_capture_attempt_at_utc']))
    prior_failure=(previous or {}).get('historical_basis_capture_error')
    if prior_failure is not None:
        require(isinstance(prior_failure,dict) and set(prior_failure)=={'checked_at_utc','error'} and isinstance(prior_failure['error'],str),'historical_basis_prior_failure_shape')
        floor=max(floor,stamp(prior_failure['checked_at_utc']))
    require(start>=floor,'historical_basis_capture_clock_reversed_before_work')
    candidate=None;retained=None;error=None
    try:
        candidate=_capture(source._data_root(),start)
        candidate['accepted_at_utc']=start.isoformat()
        _validate_evidence(candidate,start)
        if old is not None:
            try:
                previous_e=_admit({STORE_KEY:old},start,enforce_lifetime=False)
                if digest(_semantic(previous_e))==digest(_semantic(candidate)):retained=deepcopy(previous_e)
            except (ValueError,TypeError,KeyError,AttributeError,OverflowError):pass
    except Exception as exc:error=type(exc).__name__+': '+str(exc)
    completed=stamp(now_fn());require(completed>=start,'historical_basis_validation_clock_reversed')
    if error is None:
        candidate['accepted_at_utc']=completed.isoformat()
        # Expiry changes readability, never the first acceptance of unchanged sources.
        if retained is not None:candidate=retained
        snapshot[STORE_KEY]={'evidence':candidate,'evidence_sha256':digest(candidate),
            'last_capture_attempt_at_utc':completed.isoformat(),'last_capture_error':None}
        snapshot.pop('historical_basis_capture_error',None)
    else:
        if old is not None:
            old.update(last_capture_attempt_at_utc=completed.isoformat(),last_capture_error=error);snapshot[STORE_KEY]=old
        snapshot['historical_basis_capture_error']={'checked_at_utc':completed.isoformat(),'error':error}
    return completed


def _metric_value(item,name):
    _,_,unit,h=METRICS[name];a,b=item['comparison_leg'],item['reference_leg']
    x=Decimal(str(a['price']))/Decimal(a['normalization_divisor']);y=Decimal(str(b['price']))/Decimal(b['normalization_divisor'])
    return float((x/y-1)*365/Decimal(item['calendar_tenor_days'])) if h else float(x-y) if unit=='normalized_rate_difference' else float((x/y-1)*10000)


def _compatible(a,b):
    return all(a[k]['secid']==b[k]['secid'] and a[k]['normalization_divisor']==b[k]['normalization_divisor']
               and a[k]['expiry_date']==b[k]['expiry_date'] for k in ('comparison_leg','reference_leg'))


def _view(e,*,oracle=False):
    dates=e['observed_dates'];anchor_date=dates[-1];result={}
    for pair in ('usd_rub','cny_rub'):
        metrics={};anchor_pair=e['history'].get(anchor_date,{}).get(pair,{})
        for name in METRICS:
            anchor=anchor_pair.get('metrics',{}).get(name)
            admitted=anchor is not None and anchor['status']=='AVAILABLE'
            if oracle and admitted:require(anchor['value']==_metric_value(anchor,name),'historical_basis_oracle_anchor_arithmetic')
            changes={}
            for lag in (1,5):
                target=dates[-lag-1] if len(dates)>lag else None
                baseline=e['history'].get(target,{}).get(pair,{}).get('metrics',{}).get(name)
                available=admitted and baseline is not None and baseline['status']=='AVAILABLE' and _compatible(anchor,baseline)
                if oracle and available:require(baseline['value']==_metric_value(baseline,name),'historical_basis_oracle_baseline_arithmetic')
                value=float(Decimal(str(_metric_value(anchor,name) if oracle else anchor['value']))-Decimal(str(_metric_value(baseline,name) if oracle else baseline['value']))) if available else None
                changes[str(lag)]={'target_observed_trade_date':target,'baseline':deepcopy(baseline),'change':value,
                    'change_unit':METRICS[name][2],
                    'reason':None if available else 'exact_anchor_or_baseline_unavailable_or_contract_identity_changed'}
            previous=None
            for distance,day in enumerate(reversed(dates[:-1]),1):
                item=e['history'].get(day,{}).get(pair,{}).get('metrics',{}).get(name)
                if admitted and item and item['status']=='AVAILABLE' and _compatible(anchor,item):
                    previous={'trade_date':day,'observed_distance':distance,'observation':deepcopy(item),'is_exact_previous_observation':distance==1};break
            metrics[name]={'status':'AVAILABLE' if admitted else 'UNAVAILABLE',
                'reason':None if admitted else anchor.get('reason') if anchor else e['source_errors'].get(anchor_date,'metric_not_admitted'),
                'anchor':deepcopy(anchor),'previous_comparable':previous,'changes':changes}
        result[pair]={'metrics':metrics,'role_binding_as_of_utc':anchor_pair.get('role_binding_as_of_utc'),
            'run_id':anchor_pair.get('run_id')}
    return {'status':'AVAILABLE' if any(v['status']=='AVAILABLE' for p in result.values() for v in p['metrics'].values()) else 'UNAVAILABLE',
        'anchor_trade_date':anchor_date,'pairs':result}


def _oracle_view(e):
    dates=e['observed_dates'];anchor_date=dates[-1];result={}
    for pair in ('usd_rub','cny_rub'):
        metrics={};anchor_pair=e['history'].get(anchor_date,{}).get(pair,{})
        for name in ('perpetual_spot_basis_abs','perpetual_spot_basis_bps','front_spot_basis_abs','front_spot_basis_bps','next_spot_basis_abs','next_spot_basis_bps','front_perpetual_basis_abs','front_perpetual_basis_bps','next_perpetual_basis_abs','next_perpetual_basis_bps','front_next_spread_abs','front_next_spread_bps','front_spot_implied_carry_annualized','next_spot_implied_carry_annualized','front_next_term_carry_annualized'):
            anchor=anchor_pair.get('metrics',{}).get(name)
            admitted=anchor is not None and anchor['status']=='AVAILABLE'
            if admitted:require(anchor['value']==_metric_value(anchor,name),'historical_basis_oracle_anchor_arithmetic')
            changes={}
            for lag in (1,5):
                target=dates[-lag-1] if len(dates)>lag else None
                baseline=e['history'].get(target,{}).get(pair,{}).get('metrics',{}).get(name)
                available=admitted and baseline is not None and baseline['status']=='AVAILABLE' and _compatible(anchor,baseline)
                if available:require(baseline['value']==_metric_value(baseline,name),'historical_basis_oracle_baseline_arithmetic')
                value=float(Decimal(str(_metric_value(anchor,name)))-Decimal(str(_metric_value(baseline,name)))) if available else None
                changes[str(lag)]={'target_observed_trade_date':target,'baseline':deepcopy(baseline),'change':value,
                    'change_unit':METRICS[name][2],
                    'reason':None if available else 'exact_anchor_or_baseline_unavailable_or_contract_identity_changed'}
            previous=None
            for distance,day in enumerate(reversed(dates[:-1]),1):
                item=e['history'].get(day,{}).get(pair,{}).get('metrics',{}).get(name)
                if admitted and item and item['status']=='AVAILABLE' and _compatible(anchor,item):
                    previous={'trade_date':day,'observed_distance':distance,'observation':deepcopy(item),'is_exact_previous_observation':distance==1};break
            metrics[name]={'status':'AVAILABLE' if admitted else 'UNAVAILABLE',
                'reason':None if admitted else anchor.get('reason') if anchor else e['source_errors'].get(anchor_date,'metric_not_admitted'),
                'anchor':deepcopy(anchor),'previous_comparable':previous,'changes':changes}
        result[pair]={'metrics':metrics,'role_binding_as_of_utc':anchor_pair.get('role_binding_as_of_utc'),
            'run_id':anchor_pair.get('run_id')}
    return {'status':'AVAILABLE' if any(v['status']=='AVAILABLE' for p in result.values() for v in p['metrics'].values()) else 'UNAVAILABLE',
        'anchor_trade_date':anchor_date,'pairs':result}


def _describe(snapshot,now,*,oracle=False):
    try:
        now=stamp(now);e=_admit(snapshot,now);store=snapshot[STORE_KEY]
        result={'project':'MOEX_Bot','schema_version':SCHEMA,'scope':SCOPE,'status':'PARTIAL',
            'phase':'C1_DATED_ONLY_C2_NATIVE_CURRENT_NOT_ADMITTED','checked_at_utc':now.isoformat(),
            'dated':_view(e,oracle=oracle),'current':{'status':'UNAVAILABLE','reason':'native_current_proof_not_admitted'},
            'observed_trade_dates':e['observed_dates'],'horizon_basis':'exact_common_witness_indices_not_archive_success_positions',
            'previous_comparable_scope':'nearest_compatible_available_source_within_retained_witness_not_exact_lag_substitution',
            'accepted_at_utc':e['accepted_at_utc'],'valid_until_utc':(stamp(e['accepted_at_utc'])+timedelta(seconds=TTL)).isoformat(),
            'evidence_sha256':store['evidence_sha256'],'audit_reference':'input_snapshot.json#/'+STORE_KEY+'/evidence',
            'source_custody_scope':'original_run_bytes_revalidated_now_not_historical_digest_or_full_binding_response_proof',
            'price_semantics':'own_exact_two_leg_5m_CLOSE_endpoints_not_official_session_close; Si_expiring_price_divided_by_1000',
            'change_semantics':'anchor_minus_exact_baseline_in_normalized_rate_units_basis_points_or_annualized_fraction_not_percentage_return',
            'formula_conventions':{
                'normalized_rate':'own_close / normalization_divisor',
                'absolute_basis_or_spread':'comparison_normalized_rate - reference_normalized_rate',
                'basis_points':'(comparison_normalized_rate / reference_normalized_rate - 1) * 10000',
                'annualized_fraction':'(comparison_normalized_rate / reference_normalized_rate - 1) * 365 / calendar_tenor_days',
                'annualization':'simple_not_compounded; 365_calendar_days_per_year',
                'tenor_day_count':'calendar_days_not_business_days',
                'front_or_next_spot_tenor':'own_contract_expiry_date - source_trade_date',
                'front_next_term_tenor':'next_contract_expiry_date - front_contract_expiry_date'},
            'clock_conventions':{
                'source_timestamp_utc':'own_pair_source_bar_endpoint_not_publication_receipt_binding_or_first_acceptance',
                'price_publication_at_utc':'unknown_not_inferred_from_bar_endpoint',
                'received_at_utc':'original_leg_receipt_not_bar_endpoint_or_first_acceptance',
                'role_binding_as_of_utc':'original_binding_reference_observation_not_bar_endpoint',
                'accepted_at_utc':'first_C1_admission_after_validation_not_original_source_receipt'},
            'latest_capture_error':store['last_capture_error'] if stamp(store['last_capture_attempt_at_utc'])<=now else None,
            'source_errors':deepcopy(e['source_errors']),**FLAGS}
        if result['dated']['status']=='UNAVAILABLE':result['status']='UNAVAILABLE'
        return result
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as exc:
        failure=snapshot.get('historical_basis_capture_error') or {};message=str(exc)
        if isinstance(failure,dict) and isinstance(failure.get('error'),str):
            try:
                if stamp(failure['checked_at_utc'])<=stamp(now):message=failure['error']
            except (ValueError,TypeError,KeyError):pass
        return {'project':'MOEX_Bot','schema_version':SCHEMA,'scope':SCOPE,'status':'UNAVAILABLE','reason':message,
            'current':{'status':'UNAVAILABLE','reason':'native_current_proof_not_admitted'},**FLAGS}


def _oracle_description(snapshot,now):
    try:
        now=stamp(now);e=_admit(snapshot,now);store=snapshot[STORE_KEY]
        result={'project':'MOEX_Bot','schema_version':SCHEMA,'scope':SCOPE,'status':'PARTIAL',
            'phase':'C1_DATED_ONLY_C2_NATIVE_CURRENT_NOT_ADMITTED','checked_at_utc':now.isoformat(),
            'dated':_oracle_view(e),'current':{'status':'UNAVAILABLE','reason':'native_current_proof_not_admitted'},
            'observed_trade_dates':e['observed_dates'],'horizon_basis':'exact_common_witness_indices_not_archive_success_positions',
            'previous_comparable_scope':'nearest_compatible_available_source_within_retained_witness_not_exact_lag_substitution',
            'accepted_at_utc':e['accepted_at_utc'],'valid_until_utc':(stamp(e['accepted_at_utc'])+timedelta(seconds=TTL)).isoformat(),
            'evidence_sha256':store['evidence_sha256'],'audit_reference':'input_snapshot.json#/'+STORE_KEY+'/evidence',
            'source_custody_scope':'original_run_bytes_revalidated_now_not_historical_digest_or_full_binding_response_proof',
            'price_semantics':'own_exact_two_leg_5m_CLOSE_endpoints_not_official_session_close; Si_expiring_price_divided_by_1000',
            'change_semantics':'anchor_minus_exact_baseline_in_normalized_rate_units_basis_points_or_annualized_fraction_not_percentage_return',
            'formula_conventions':{
                'normalized_rate':'own_close / normalization_divisor',
                'absolute_basis_or_spread':'comparison_normalized_rate - reference_normalized_rate',
                'basis_points':'(comparison_normalized_rate / reference_normalized_rate - 1) * 10000',
                'annualized_fraction':'(comparison_normalized_rate / reference_normalized_rate - 1) * 365 / calendar_tenor_days',
                'annualization':'simple_not_compounded; 365_calendar_days_per_year',
                'tenor_day_count':'calendar_days_not_business_days',
                'front_or_next_spot_tenor':'own_contract_expiry_date - source_trade_date',
                'front_next_term_tenor':'next_contract_expiry_date - front_contract_expiry_date'},
            'clock_conventions':{
                'source_timestamp_utc':'own_pair_source_bar_endpoint_not_publication_receipt_binding_or_first_acceptance',
                'price_publication_at_utc':'unknown_not_inferred_from_bar_endpoint',
                'received_at_utc':'original_leg_receipt_not_bar_endpoint_or_first_acceptance',
                'role_binding_as_of_utc':'original_binding_reference_observation_not_bar_endpoint',
                'accepted_at_utc':'first_C1_admission_after_validation_not_original_source_receipt'},
            'latest_capture_error':store['last_capture_error'] if stamp(store['last_capture_attempt_at_utc'])<=now else None,
            'source_errors':deepcopy(e['source_errors']),**FLAGS}
        if result['dated']['status']=='UNAVAILABLE':result['status']='UNAVAILABLE'
        return result
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as exc:
        failure=snapshot.get('historical_basis_capture_error') or {};message=str(exc)
        if isinstance(failure,dict) and isinstance(failure.get('error'),str):
            try:
                if stamp(failure['checked_at_utc'])<=stamp(now):message=failure['error']
            except (ValueError,TypeError,KeyError):pass
        return {'project':'MOEX_Bot','schema_version':SCHEMA,'scope':SCOPE,'status':'UNAVAILABLE','reason':message,
            'current':{'status':'UNAVAILABLE','reason':'native_current_proof_not_admitted'},**FLAGS}


def describe(snapshot,*,now):return _describe(snapshot,now)


def attach_consumer(snapshot,release,*,now):release[OUTPUT_KEY]=describe(snapshot,now=now)


def verify_projection(snapshot,release,*,now):
    expected=_oracle_description(snapshot,now)
    require(digest(release.get(OUTPUT_KEY))==digest(expected),'historical basis independent projection mismatch')
