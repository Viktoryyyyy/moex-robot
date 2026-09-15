"""Exact-SECID price/market-OI comparisons; no continuous-contract or trading authority."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
from pathlib import Path, PurePosixPath, PureWindowsPath
import base64
import json

from moex_data import rub_si_futoi_dated_context as common
from moex_data import rub_contract_observed_context as archive

SCHEMA = 'contract_price_market_oi_observed.v1'
STORE_KEY = 'accepted_contract_price_market_oi_observed'
CONTRACT = 'contracts/intelligence/contract_price_market_oi_observed_v1.json'
SOURCE_ADMISSION = 'state/acceptance/contract_price_market_oi_observed_v1/source_admission.json'
SCOPE = 'EXACT_SECID_FACTUAL_PRICE_MARKET_OI_OBSERVED_COMPARISONS'
ROLES = ('si_front','si_next','cr_front','cr_next')
INSTRUMENTS = dict(zip(ROLES, ('si_front_contract','si_next_contract','cr_front_contract','cr_next_contract')))
LAGS = (1,5,20)
FLAGS = dict(session_completion_authority=False, historical_pit_authority=False,
    model_input_allowed=False, signal_eligible=False, decision_eligible=False, action_authority=False)
MAX_BUFFER_BYTES = 8_000_000
MAX_AUDIT_BYTES = 64_000_000
MAX_AUDIT_BUFFERS = 1024
MAX_ARCHIVE_DIRECTORY_ENTRIES = 256


class CaptureBudgetExceeded(ValueError):
    """A whole-capture refusal, never an ordinary per-date source gap."""


class CaptureBudget:
    """Distinct original bytes processed in one capture, including discovery.

    An unknown digest requires one bounded read/hash before charging. This is
    not a claim that total duplicate reads or peak memory are at most 64 MB.
    """
    def __init__(self, *, max_bytes=None, max_buffers=None):
        self.max_bytes=MAX_AUDIT_BYTES if max_bytes is None else max_bytes
        self.max_buffers=MAX_AUDIT_BUFFERS if max_buffers is None else max_buffers
        self.buffers={};self.total_bytes=0;self.exhausted=False

    def ensure_active(self):
        if self.exhausted:raise CaptureBudgetExceeded('capture_original_artifact_budget_exceeded')

    def _check(self,count,total):
        self.ensure_active()
        if count>self.max_buffers or total>self.max_bytes:
            self.exhausted=True
            raise CaptureBudgetExceeded('capture_original_artifact_budget_exceeded')

    def preflight(self,sizes):
        self.ensure_active()
        new={digest:size for digest,size in sizes.items() if digest not in self.buffers}
        self._check(len(self.buffers)+len(new),self.total_bytes+sum(new.values()))

    def charge(self,raw):
        self.ensure_active();digest=sha256(raw).hexdigest()
        if digest in self.buffers:
            _require(self.buffers[digest]==raw,'capture_original_digest_bytes_mismatch')
            return
        self._check(len(self.buffers)+1,self.total_bytes+len(raw))
        self.buffers[digest]=raw;self.total_bytes+=len(raw)


def _source_json(content):
    from moex_data import step9_rub_analysis_bundle as step9
    if isinstance(content,(bytes,bytearray)): content=content.decode('utf-8')
    return json.loads(content,object_pairs_hook=step9._reject_duplicate_json_members,
        parse_constant=step9._reject_json_constant)


def _source_object(content):
    value=_source_json(content)
    _require(isinstance(value,dict),'source_JSON_must_contain_object')
    return value


def _encoded_size(encoded):
    # Valid base64 has an exact decoded size determined without decoding it.
    _require(isinstance(encoded,str) and len(encoded)<=4*((MAX_BUFFER_BYTES+2)//3),'audit_encoded_byte_limit')
    _require(len(encoded)%4==0,'audit_encoded_padding')
    padding=2 if encoded.endswith('==') else 1 if encoded.endswith('=') else 0
    size=len(encoded)//4*3-padding
    _require(0<=size<=MAX_BUFFER_BYTES,'audit_buffer_byte_limit')
    return size


def _inline_ref(raw):
    digest=sha256(raw).hexdigest()
    return {'ref':'inline-sha256:'+digest,'sha256':digest}


def _proof_ref(ref,digest):
    if ref=='inline-sha256:'+digest: return
    common._ref(ref)


def _native_buffers(body, budget=None):
    if budget is not None:budget.ensure_active()
    from moex_data import synchronized_live_market_oi_context as live
    responses=(body.get('original_forts_http_evidence') or {}).get('responses')
    _require(isinstance(responses,list) and 1<=len(responses)<=live.MAX_FORTS_PAGES+1,'current_response_bound')
    table={};total=0
    for value in responses:
        _require(isinstance(value,dict),'current_original_response_shape')
        digest=value['sha256'];common._hash(digest);encoded=value['content_base64']
        total+=_encoded_size(encoded)
        _require(total<=MAX_AUDIT_BYTES,'native_response_inventory_byte_limit')
        if digest in table: _require(table[digest]==encoded,'native_duplicate_digest_bytes_mismatch')
        table[digest]=encoded
    return _decode_buffers(table,budget=budget)


CURRENT_KEY='contract_price_market_oi_current_capture'


def capture_current(body, *, started, completed, now_fn=None):
    """Bounded original-byte current evidence; no filesystem writes or new request."""
    started=_stamp(started); completed=_stamp(completed)
    _require(started<=completed,'current_capture_clock_reversed')
    budget=CaptureBudget()
    try:
        facts=_original_current(None,body,completed,budget=budget)
        current={'causal_cutoff_at_utc':completed.isoformat(),'captured_at_utc':completed.isoformat(),
            'bindings':{role:body['bindings'][role] for role in ROLES},'facts':facts,'error':None,
            'original_byte_buffers':_buffer_table(None,facts,available=_native_buffers(body,budget),budget=budget)}
    except Exception as exc:
        current={'causal_cutoff_at_utc':completed.isoformat(),'captured_at_utc':completed.isoformat(),
            'bindings':{},'facts':None,'error':type(exc).__name__+': '+str(exc),'original_byte_buffers':{}}
    final=_stamp(now_fn()) if now_fn is not None else completed
    _require(final>=completed,'current_capture_validation_clock_reversed')
    current['captured_at_utc']=final.isoformat()
    return {'capture':current,'sha256':common._digest(current)}


def _buffer_table(root, value, *, available=None, budget=None):
    if budget is not None:budget.ensure_active()
    """One immutable byte copy per digest; never serialize reconstructed source rows."""
    refs = _proof_references(value)
    result = {}; total=0
    for ref, digest in refs:
        if digest not in result:
            raw = (available[digest] if available is not None and digest in available else _read_bytes(root, ref, digest,budget=budget))
            if budget is not None:budget.charge(raw)
            total+=len(raw)
            _require(len(result)<MAX_AUDIT_BUFFERS and len(raw) <= MAX_BUFFER_BYTES and total<=MAX_AUDIT_BYTES, 'audit_artifact_byte_limit')
            result[digest] = base64.b64encode(raw).decode('ascii')
    _decode_buffers(result)
    return result


def _proof_references(value):
    if isinstance(value, dict):
        if set(value) == {'ref', 'sha256'}:
            _proof_ref(value['ref'],value['sha256']); common._hash(value['sha256'])
            yield value['ref'], value['sha256']
        else:
            for key, child in value.items():
                if key != 'original_byte_buffers': yield from _proof_references(child)
    elif isinstance(value, list):
        for child in value: yield from _proof_references(child)


def _decode_buffers(table, *, budget=None):
    if budget is not None:budget.ensure_active()
    _require(isinstance(table,dict) and 1<=len(table)<=MAX_AUDIT_BUFFERS,'audit_buffer_count')
    total=0
    for digest,encoded in table.items():
        common._hash(digest);total+=_encoded_size(encoded)
        _require(total<=MAX_AUDIT_BYTES,'audit_total_byte_limit')
    if budget is not None:budget.preflight({digest:_encoded_size(encoded) for digest,encoded in table.items()})
    result={}
    for digest,encoded in table.items():
        raw=base64.b64decode(encoded,validate=True)
        if budget is not None:budget.charge(raw)
        _require(len(raw)==_encoded_size(encoded),'audit_decoded_size_mismatch')
        _require(sha256(raw).hexdigest()==digest,'audit_original_buffer_hash')
        result[digest]=raw
    return result


def _buffer_bytes(buffers, proof):
    _require(isinstance(proof, dict) and set(proof) == {'ref', 'sha256'}, 'audit_reference_shape')
    _proof_ref(proof['ref'],proof['sha256']); common._hash(proof['sha256'])
    _require(proof['sha256'] in buffers, 'audit_original_buffer_missing')
    return buffers[proof['sha256']]


def _frozen_ref(raw):
    digest=sha256(raw).hexdigest()
    return {'ref':'${MOEX_DATA_ROOT}/state/evidence/contract_price_market_oi_observed_v1/'+digest[:2]+'/'+digest+'.bin','sha256':digest}


def _portable_same_path(value, expected, label):
    _require(isinstance(value,str) and '..' not in type(expected)(value).parts and type(expected)(value)==expected, 'portable_path_mismatch:'+label)


def _stage3_audit(root, resolved, read, freeze):
    paths=[resolved['marker_path'],resolved['pilot_path']]
    if resolved['parent_path'] is not None: paths.append(resolved['parent_path'])
    for spec in resolved['specs']:
        paths.extend((spec.partition_path,spec.manifest_path,spec.quality_path))
    artifacts={'${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix():freeze(read('${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix())) for path in paths}
    return {'original_root':str(root),'artifacts':artifacts}


def _portable_stage3(record, buffers, now):
    """Recheck the original accepted marker and all ten pilot support identities."""
    from moex_data import step3_raw_acceptance as stage3
    p=record['proof']; audit=p['stage3_audit']
    _require(set(audit)=={'original_root','artifacts'} and isinstance(audit['original_root'],str),'stage3_audit_shape')
    root=(PureWindowsPath(audit['original_root']) if '\\' in audit['original_root'] or ':' in audit['original_root'] else PurePosixPath(audit['original_root']))
    _require(root.is_absolute() and '..' not in root.parts,'stage3_original_root_shape')
    artifacts=audit['artifacts']; run=p['acceptance_run_id']
    _require(isinstance(artifacts,dict) and len(artifacts)==(32 if p['parent'] is None else 33),'stage3_audit_inventory')
    def load(path):
        ref='${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix()
        return _source_object(_buffer_bytes(buffers,artifacts[ref]))
    marker_path=root/'state/acceptance/step3_canonical_raw'/('run_id='+run)/'accepted_pointers.json'
    pilot_path=marker_path.with_name('pilot_evidence.json')
    marker=load(marker_path); pilot=load(pilot_path)
    _require(artifacts['${MOEX_DATA_ROOT}/'+marker_path.relative_to(root).as_posix()]==p['marker']
        and artifacts['${MOEX_DATA_ROOT}/'+pilot_path.relative_to(root).as_posix()]==p['pilot'],'stage3_original_marker_link')
    for key,value in {'project':'MOEX_Bot','step':3,'status':'accepted','run_id':run,'acceptance_contract_id':stage3.CONTRACT_ID,
            'artifact_semantics':'immutable_run_scoped','accepted_pointer_count':10,'expected_pointer_count':10}.items():
        _require(type(marker.get(key)) is type(value) and marker[key]==value,'stage3_original_marker_identity:'+key)
    _require(marker['pilot_evidence_ref']=='${MOEX_DATA_ROOT}/'+pilot_path.relative_to(root).as_posix(),'stage3_original_pilot_ref')
    for key,value in {'project':'MOEX_Bot','step':3,'status':'pilot_passed','artifact_version':run,
            'latest_autodetect_used':False,'historical_backdating_used':False,'continuous_series_created':False,
            'run_artifacts_immutable':True,'run_id_reuse_allowed':False}.items():
        _require(type(pilot.get(key)) is type(value) and pilot[key]==value,'stage3_original_pilot_identity:'+key)
    day=_day(pilot['trade_date']); asof=_day(pilot['as_of_date']); _require(day<=asof and day==record['trade_date'],'stage3_original_pilot_date')
    run_root=root/'runs/step3_canonical_raw'/('run_id='+run)
    _require(pilot['materialization_root_ref']=='${MOEX_DATA_ROOT}/'+run_root.relative_to(root).as_posix(),'stage3_original_run_root_ref')
    _portable_same_path(pilot['materialization_root'],run_root,'materialization_root')
    _require(all(type(pilot['counts'].get(k)) is int and pilot['counts'][k]==v for k,v in stage3.EXPECTED_COUNTS.items()),'stage3_original_counts')
    bindings=stage3._validate_bindings(pilot,trade_date=day,as_of_date=asof)
    binding=_stamp(pilot['reference_observed_at_utc']); _require(binding==_stamp(p['binding_observed_at_utc']),'stage3_original_binding_clock')
    parent=parent_path=None
    if p['parent'] is not None:
        parent_run=run[:-7]; parent_path=root/'runs/step10_rub_daily_refresh'/('run_id='+parent_run)/'run_manifest.json'
        parent=load(parent_path); refresh=parent.get('source_refresh',{})
        _require(artifacts['${MOEX_DATA_ROOT}/'+parent_path.relative_to(root).as_posix()]==p['parent'],'stage3_original_parent_link')
        _require(parent.get('project')=='MOEX_Bot' and parent.get('stage')==10 and parent.get('run_id')==parent_run
            and parent.get('status')=='succeeded' and parent.get('current_pointer_rollback_status') in (None,'not_needed')
            and refresh.get('status')=='refreshed' and refresh.get('stage3_run_id')==run and refresh.get('trade_date')==day,'stage3_original_parent_admission')
        finished=_stamp(parent['finished_at_utc'])
    else:
        admission=_portable_source_admission(p,buffers,now)
        entry=next(e for e in admission['entries'] if e['kind']=='standalone_stage3_pilot')
        _require(set(entry)=={'kind','run_id','accepted_marker_ref','pilot_evidence_ref','marker_sha256','pilot_sha256'}
            and entry['run_id']==run=='step3_pilot_20260824_1705' and day=='2026-08-24','standalone_original_identity')
        _require(entry['accepted_marker_ref']=='${MOEX_DATA_ROOT}/'+marker_path.relative_to(root).as_posix()
            and entry['pilot_evidence_ref']=='${MOEX_DATA_ROOT}/'+pilot_path.relative_to(root).as_posix()
            and entry['marker_sha256']==p['marker']['sha256'] and entry['pilot_sha256']==p['pilot']['sha256'],'standalone_original_hash_links')
        finished=_stamp(admission['accepted_at_utc'])
    _require(binding<=finished<=now,'stage3_original_capture_causality')
    specs=[]
    for field,dataset,count in [('quote_partitions','futures_raw_5m',4),('open_interest_partitions','futures_open_interest_raw_5m',4),('tom_partitions','fx_spot_raw_5m',2)]:
        for item in stage3._require_list(pilot.get(field),field,count):
            quote=dataset=='futures_raw_5m'
            _require(item.get('dataset_id')==dataset and item.get('quality_status')=='pass','stage3_original_partition_status')
            instrument=stage3._single_scope(item.get('instrument_id_scope'),'instrument') if quote else stage3._require_token(item.get('instrument_id'),'instrument')
            secid=stage3._single_scope(item.get('secid_scope'),'secid') if quote else stage3._require_token(item.get('secid'),'secid')
            if not quote: _require(item.get('trade_date')==day,'stage3_original_partition_date')
            source=stage3._require_canonical_source(dataset,item.get('source_id'),'source')
            paths=[type(root)(item[k]) for k in (('manifest_reference','quality_report_reference','storage_partition_path') if quote else ('manifest_path','quality_report_path','partition_path'))]
            for path in paths: _require('..' not in path.parts and path.is_relative_to(run_root),'stage3_original_partition_path')
            manifest=load(paths[0]); quality=load(paths[1]); manifest_run=stage3._require_token(manifest.get('run_id'),'manifest.run_id')
            _require(manifest.get('refresh_status',manifest.get('status'))=='succeeded','stage3_original_manifest_status')
            spec=stage3.PointerSpec(dataset,instrument,source,secid,day,stage3._positive_row_count(item,'partition'),*paths,manifest_run)
            _validate_support_buffers(spec,manifest,quality); specs.append(spec)
    _require(len({(s.dataset_id,s.instrument_id) for s in specs})==10,'stage3_original_unique_specs')
    for dataset in ('futures_raw_5m','futures_open_interest_raw_5m'):
        _require({s.instrument_id:s.secid for s in specs if s.dataset_id==dataset}==bindings,'stage3_original_partition_bindings')
    _require({s.instrument_id:s.secid for s in specs if s.dataset_id=='fx_spot_raw_5m'}==stage3.EXPECTED_TOM_IDENTITIES,'stage3_original_tom_identity')
    pointers=stage3._require_list(marker.get('pointers'),'pointers',10)
    for spec in specs:
        matching=[v for v in pointers if v.get('dataset_id')==spec.dataset_id and v.get('instrument_id')==spec.instrument_id]
        _require(len(matching)==1,'stage3_original_pointer_identity'); pointer=matching[0]
        path=root/'state/datasets'/('dataset_id='+spec.dataset_id)/('instrument_id='+spec.instrument_id)/'current_accepted_manifest.json'
        _require(pointer.get('pointer_ref')=='${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix(),'stage3_original_pointer_ref')
        if 'pointer_path' in pointer: _portable_same_path(pointer['pointer_path'],path,'pointer')
        _require(pointer.get('manifest_ref')=='${MOEX_DATA_ROOT}/'+spec.manifest_path.relative_to(root).as_posix()
            and pointer.get('quality_report_ref')=='${MOEX_DATA_ROOT}/'+spec.quality_path.relative_to(root).as_posix(),'stage3_original_support_refs')
    return root,dict(run=run,marker_path=marker_path,marker=marker,pilot_path=pilot_path,pilot=pilot,parent_path=parent_path,parent=parent,
        finished=finished,binding=binding,specs=specs,source_artifacts=artifacts)


def _portable_source_admission(proof,buffers,now):
    value=_source_object(_buffer_bytes(buffers,proof['source_admission']))
    _require(set(value)=={'schema_version','project','task_id','accepted_at_utc','entries'}
        and value['schema_version']=='contract_price_market_oi_source_admission.v1' and value['project']=='MOEX_Bot'
        and value['task_id']=='contract_price_market_oi_observed_comparisons_v1','portable_source_admission_identity')
    _require(_stamp(value['accepted_at_utc'])<=now and isinstance(value['entries'],list) and len(value['entries'])==2
        and {v['kind'] for v in value['entries']}=={'official_paginated_tradestats','standalone_stage3_pilot'},'portable_source_admission_inventory')
    return value


def _portable_witness(e,buffers,now):
    import pandas as pd
    from moex_data import step9_rub_analysis_bundle as step9
    from moex_data.futures import futoi_delta_statistics_context as engine
    spec=engine._spec(stage=7,dataset_id=engine.OBSERVED_DATE_WITNESS_DATASET_ID,
        instrument_id=engine.OBSERVED_DATE_WITNESS_INSTRUMENT_ID,timeframe=engine.OBSERVED_DATE_WITNESS_TIMEFRAME)
    p=e['witness_proof']; pointer=_source_object(_buffer_bytes(buffers,p['pointer']))
    _require(pointer.get('dataset_id')==spec.dataset_id and pointer.get('instrument_id')==spec.instrument_id
        and pointer.get('timeframe')==spec.timeframe and pointer.get('quality_status')=='pass'
        and pointer.get('refresh_status','succeeded')=='succeeded','portable_witness_pointer_identity')
    run,_,_=step9._validate_pointer_provenance(pointer,spec)
    for key in ('partition','manifest','quality_report'):
        common._ref(pointer[key+'_ref'])
        _require(pointer[key+'_sha256']==p[key]['sha256'],'portable_witness_pointer_hash')
        if key!='partition': step9._validate_support_identity(_source_object(_buffer_bytes(buffers,p[key])),spec,key,
            quality_required=True,support_kind=key,producer_run_id=run)
    frame=pd.read_parquet(BytesIO(_buffer_bytes(buffers,p['partition'])))
    step9._selected_row(frame,spec,now)
    _require((step9._to_utc_series(frame,spec)<=pd.Timestamp(now)).all() and not frame['trade_date'].duplicated().any(),'portable_witness_causal_unique_dates')
    _require(not frame.empty and 'build_ts_utc' in frame,'portable_witness_build_missing')
    for value in frame['build_ts_utc']: _require(_stamp(str(value))<=now,'portable_witness_build_future')
    dates=sorted({_day(str(d)) for d in frame['trade_date']})
    _require(dates[-22:]==e['observed_dates'],'portable_witness_exact_date_inventory')


def _native_numbers(row):
    """Use the original live reader's numeric representation; keep proof rows raw."""
    from moex_data import synchronized_live_market_oi_context as live
    try:
        return (live._nonnegative_price(row['LAST'],secid=row['SECID'],field='LAST'),
                live._integer(row['OPENPOSITION']))
    except live.SynchronizedLiveMarketOIError as exc:
        raise ValueError(str(exc)) from exc


def _portable_native_body(binding_proof,buffers,bindings,now,facts=None):
    """Reconstruct the ordinary reader input from original HTTP buffers, not copied rows."""
    from moex_data import synchronized_live_market_oi_context as live
    inventory=binding_proof['retained_http_inventory']; clock=_stamp(binding_proof['binding_as_of_utc'])
    _require(clock<=now and isinstance(inventory,list) and 1<=len(inventory)<=live.MAX_FORTS_PAGES+1,'portable_native_binding_clock_or_count')
    _require(sum(len(_buffer_bytes(buffers,item['response'])) for item in inventory)<=MAX_AUDIT_BYTES,'native_response_inventory_byte_limit')
    responses=[]; payloads=[]; securities={}; selected={}
    for item in inventory:
        _require(set(item)=={'response','source_url','params','received_at_utc','requested_lower_bound_utc','role'},'portable_native_response_shape')
        raw=_buffer_bytes(buffers,item['response']); payload=_source_object(raw)
        _native_url(item['source_url'],item['params'])
        _require(_stamp(item['requested_lower_bound_utc'])<=_stamp(item['received_at_utc'])<=clock,'portable_native_response_clock')
        response={'content_base64':base64.b64encode(raw).decode('ascii'),'sha256':item['response']['sha256'],
            'source_url':item['source_url'],'params':item['params'],'received_at_utc':item['received_at_utc'],'http_status':200,'role':item['role']}
        responses.append(response); payloads.append((response,payload))
        if item['role']=='selected_values':
            for row in _table(payload,'securities'):
                _require(row['SECID'] not in securities,'portable_duplicate_security'); securities[row['SECID']]=row
            for row in _table(payload,'marketdata'):
                _require(row['SECID'] not in selected,'portable_duplicate_market_row'); selected[row['SECID']]=(row,item)
    _validate_native_inventory(payloads)
    actual=live._bindings_from_forts(live.pd.DataFrame(list(securities.values())),as_of_date=clock.astimezone(archive.MOSCOW).date().isoformat(),availability_ts_utc=clock.isoformat())
    _require({role:actual[role] for role in ROLES}==bindings,'portable_original_role_binding')
    nodes={}
    if facts is not None:
        for role,secid in bindings.items():
            row,item=selected[secid]
            price,oi=_native_numbers(row)
            nodes[role]={'secid':secid,'last':price,'oi':oi,
                'timestamp':live._source_event_time(row['SYSTIME'],'portable.SYSTIME').isoformat(),
                'received_at_utc':item['received_at_utc'],'source_trade_date':row['TRADEDATE'],
                'last_trade_time_moscow':live._optional_source_text(row.get('TIME')),
                'price_oi_same_source_row':True,'price_oi_usable':True,'stale':False}
    starts={i['requested_lower_bound_utc'] for i in inventory}; _require(len(starts)==1,'portable_native_request_clock_inventory')
    return {'bindings':bindings,'instruments':nodes,'snapshot_received_at_utc':clock.isoformat(),
        'original_forts_http_evidence':{'request_started_lower_bound_utc':next(iter(starts)),
            'request_clock_semantics':'batch_start_before_each_retained_request','responses':responses}}


def _validate_original_history(e):
    buffers=_decode_buffers(e['original_byte_buffers']); cutoff=_stamp(e['causal_cutoff_at_utc'])
    referenced={digest for _,digest in _proof_references(e)}
    _require(referenced==set(buffers),'audit_exact_buffer_inventory')
    _require(set(e['binding_proof'])=={'retained_http_inventory','binding_as_of_utc'}
        and e['binding_proof']['binding_as_of_utc']==e['role_binding_as_of_utc'],'dated_original_binding_proof_shape')
    _portable_native_body(e['binding_proof'],buffers,e['bindings'],cutoff)
    _portable_witness(e,buffers,cutoff)
    for day,pairs in e['history'].items():
        if not pairs: continue
        record=next(iter(pairs.values())); kind=record['source_kind']; proof=record['proof']
        _require(all(r['source_kind']==kind for r in pairs.values()),'historical_mixed_source_kind')
        if kind=='CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS':
            admission=_portable_source_admission(proof,buffers,cutoff)
            entry=next(v for v in admission['entries'] if v['kind']=='official_paginated_tradestats')
            derived,_=_official_pages(None,entry,accepted_at=_stamp(admission['accepted_at_utc']),now=cutoff,buffers=buffers)
            for r in derived.values(): r['proof']['source_admission']=proof['source_admission']
        else:
            root,resolved=_portable_stage3(record,buffers,cutoff)
            derived=_stage3_pairs(root,resolved,now=_stamp(proof['revalidated_at_utc']),kind=kind,buffers=buffers)
            if kind=='REVALIDATED_STANDALONE_STAGE3_PILOT':
                for r in derived.values(): r['proof']['source_admission']=proof['source_admission']
        expected={secid:r for secid,r in derived.items() if secid in e['bindings'].values()}
        _require(common._digest(expected)==common._digest(pairs),'historical_original_buffer_fact_or_inventory_mismatch')


CONTRACT_DOCUMENT = {'schema_version': 'contract_price_market_oi_observed_admission.v1',
 'project': 'MOEX_Bot',
 'task_id': 'contract_price_market_oi_observed_comparisons_v1',
 'scope': 'EXACT_SECID_FACTUAL_PRICE_MARKET_OI_OBSERVED_COMPARISONS',
 'admission': {'source_kinds': ['CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN',
                                'REVALIDATED_STANDALONE_STAGE3_PILOT',
                                'CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS',
                                'CURRENT_NATIVE_SAME_RESPONSE_ROW'],
               'standalone_run_id': 'step3_pilot_20260824_1705',
               'official_acquisition_trade_date': '2026-08-23',
               'official_acquisition_expected_market_rows': 9683,
               'lag_observations': [1, 5, 20],
               'max_witness_dates': 22,
               'max_contracts': 4,
               'max_original_buffer_bytes': 8000000,
               'max_original_audit_bytes': 64000000,
               'capture_original_artifact_max_bytes': 64000000,
               'capture_original_artifact_max_count': 1024,
               'capture_original_artifact_scope': 'distinct_verified_buffers_including_discovery_witness_history_alternate_native',
               'capture_budget_overflow_policy': 'sticky_whole_capture_refusal_before_further_parse_or_freeze',
               'capture_unknown_digest_read_bound_bytes': 8000000,
               'max_original_buffer_count': 1024,
               'native_current_byte_storage': 'inline_deduplicated_original_HTTP_bytes',
               'native_response_inventory_max_decoded_bytes': 64000000,
               'native_response_inventory_budget_counts_repeated_entries': True,
               'fast_current_total_state_max_bytes': 2000000,
               'max_accepted_run_candidates': 256,
               'max_archive_directory_entries_before_content_reads': 256,
               'archive_inventory_scope': 'all_entries_including_outside_lookback_before_pilot_date_filter',
               'lookback_calendar_days': 45,
               'current_exact_comparison_max_calendar_gap_from_witness': 1,
               'current_gap_policy': 'retain_fresh_anchor_refuse_exact_lags_without_non_session_inference',
               'dated_max_age_seconds': 345600,
               'price_oi_exact_join_required': True,
               'baseline_role_semantics': 'same_SECID_selected_at_explicit_role_binding_as_of_not_historical_role_proof',
               'session_completion_authority': False,
               'historical_pit_authority': False,
               'model_input_allowed': False,
               'signal_eligible': False,
               'decision_eligible': False,
               'action_authority': False,
               'market_open_interest_counting_basis': 'source_native_as_reported_no_rescaling_not_family_futoi_total'}}

def _json(value): return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def _stamp(value): return common._stamp(value)
def _day(value): return common._day(value)
def _require(ok, message):
    if not ok: raise ValueError(message)


def _read_bytes(root, ref, expected=None, *, budget=None):
    if budget is not None:budget.ensure_active()
    common._ref(ref)
    path=root/ref[len('${MOEX_DATA_ROOT}/'):]
    _require(not path.is_symlink() and path.is_file() and path.resolve().is_relative_to(root.resolve()), 'source_path_missing_or_escaped')
    _require(path.stat().st_size<=MAX_BUFFER_BYTES,'source_artifact_byte_limit')
    with path.open('rb') as source: content=source.read(MAX_BUFFER_BYTES+1)
    _require(len(content)<=MAX_BUFFER_BYTES,'source_artifact_byte_limit')
    if budget is not None:budget.charge(content)
    if expected is not None: _require(sha256(content).hexdigest()==expected, 'source_buffer_hash_mismatch')
    return content


def _freeze(root, content, *, budget=None):
    if budget is not None:budget.charge(content)
    digest=sha256(content).hexdigest()
    path=root/'state/evidence/contract_price_market_oi_observed_v1'/digest[:2]/(digest+'.bin')
    path.parent.mkdir(parents=True,exist_ok=True)
    _require(path.parent.resolve().is_relative_to(root.resolve()) and not path.is_symlink(),'immutable_source_copy_path')
    if path.exists(): _require(_read_bytes(root,'${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix(),budget=budget)==content,'immutable_source_copy_changed')
    else:
        with path.open('xb') as target: target.write(content)
    return {'ref':'${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix(),'sha256':digest}


def _freeze_json(root,path,expected, *, budget=None):
    content=_read_bytes(root,'${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix(),budget=budget)
    _require(common._digest(_source_object(content))==common._digest(expected),'source_json_changed_after_validation')
    return _freeze(root,content,budget=budget)


def _number(value, *, positive=False, integer=False):
    _require(type(value) in (int,float) and (not integer or type(value) is int),'numeric_type_invalid')
    parsed=Decimal(str(value)); _require(parsed.is_finite() and (parsed>0 if positive else parsed>=0),'numeric_value_invalid')
    return parsed


def _ohlc(values, prefix=''):
    nums=[_number(values[prefix+k],positive=not prefix) for k in ('open','high','low','close')]
    op,hi,lo,cl=nums
    _require(lo<=min(op,cl)<=max(op,cl)<=hi,'ohlc_range_invalid')
    if prefix: _require(all(n==n.to_integral_value() for n in nums),'oi_not_integral')


def _pair(secid, day, timestamp, published, received, price, oi, proof, *, source_kind):
    day=_day(day); event=_stamp(timestamp); pub=_stamp(published); receipt=_stamp(received)
    _require(event<=pub<=receipt,'paired_source_clock_order')
    if source_kind!='CURRENT_NATIVE_SAME_RESPONSE_ROW':
        _require(event.astimezone(archive.MOSCOW).date().isoformat()==day,'paired_source_date_mismatch')
    _number(price,positive=True); _number(oi,integer=True)
    return {'secid':secid,'trade_date':day,'source_timestamp_utc':event.isoformat(),
        'source_timestamp_moscow':event.astimezone(archive.MOSCOW).isoformat(),
        'endpoint_scope':'native_source_row_update_not_session_close' if source_kind=='CURRENT_NATIVE_SAME_RESPONSE_ROW' else 'observed_partial_day_endpoint_session_completion_unproven',
        'source_publication_at_utc':pub.isoformat(),'received_at_utc':receipt.isoformat(),
        'price_received_at_utc':receipt.isoformat(),'market_oi_received_at_utc':receipt.isoformat(),
        'price_source_publication_at_utc':None if source_kind in ('CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN','REVALIDATED_STANDALONE_STAGE3_PILOT') else pub.isoformat(),
        'publication_clock_semantics':'market_oi_publication_price_publication_unavailable' if source_kind in ('CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN','REVALIDATED_STANDALONE_STAGE3_PILOT') else 'same_source_row_update',
        'price':price,'market_open_interest':oi,'source_kind':source_kind,'proof':proof,
        'session_completion_proven':False}


def _stage3_pairs(root, resolved, *, now, kind, buffers=None, budget=None):
    """Derive values from the same verified buffers whose digests are retained."""
    import pandas as pd
    from moex_data import step3_raw_acceptance as stage3
    specs=resolved['specs']; result={}; paired_frames={}
    def read(ref, digest=None):
        if budget is not None:budget.ensure_active()
        if buffers is None:
            reader=resolved.get('byte_reader')
            raw=reader(root/ref[len('${MOEX_DATA_ROOT}/'):]) if reader is not None else _read_bytes(root,ref,digest,budget=budget)
            if digest is not None: _require(sha256(raw).hexdigest()==digest,'source_buffer_hash_mismatch')
            return raw
        proof = resolved['source_artifacts'][ref]
        if digest is not None: _require(proof['sha256'] == digest, 'stage3_buffer_reference_changed')
        return _buffer_bytes(buffers, proof)
    def freeze(raw):
        if buffers is None: return _freeze(root, raw,budget=budget)
        digest = sha256(raw).hexdigest()
        return {'ref':'${MOEX_DATA_ROOT}/state/evidence/contract_price_market_oi_observed_v1/'+digest[:2]+'/'+digest+'.bin','sha256':digest}
    def freeze_json(path, expected):
        raw = read('${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix())
        _require(common._digest(_source_object(raw)) == common._digest(expected), 'source_json_changed_after_validation')
        return freeze(raw)
    audit = _stage3_audit(root, resolved, read, freeze)
    for role,instrument in INSTRUMENTS.items():
        quote=next(s for s in specs if s.instrument_id==instrument and s.dataset_id=='futures_raw_5m')
        oi=next(s for s in specs if s.instrument_id==instrument and s.dataset_id=='futures_open_interest_raw_5m')
        _require(quote.secid==oi.secid and quote.trade_date==oi.trade_date,'price_oi_spec_join_mismatch')
        frames=[]; proofs=[]
        for spec in (quote,oi):
            paths={key:getattr(spec,attr) for key,attr in (('partition','partition_path'),('manifest','manifest_path'),('quality','quality_path'))}
            support={key:freeze(read('${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix())) for key,path in paths.items()}
            documents={key:_source_object((buffers[support[key]['sha256']] if buffers is not None else _read_bytes(root,support[key]['ref'],support[key]['sha256'],budget=budget))) for key in ('manifest','quality')}
            _validate_support_buffers(spec,documents['manifest'],documents['quality'])
            frame=pd.read_parquet(BytesIO((buffers[support['partition']['sha256']] if buffers is not None else _read_bytes(root,support['partition']['ref'],support['partition']['sha256'],budget=budget))))
            _require(len(frame)==spec.row_count and not frame.empty,'partition_row_count_mismatch')
            for key,val in (('instrument_id',instrument),('secid',spec.secid),('trade_date',spec.trade_date),('source_id',spec.source_id)):
                _require(key in frame and frame[key].eq(val).all(),'partition_row_identity_mismatch')
            stamps=pd.to_datetime(frame['ts'],errors='raise')
            if stamps.dt.tz is None: stamps=stamps.dt.tz_localize('Europe/Moscow')
            stamps=stamps.dt.tz_convert('UTC')
            receipts=pd.to_datetime(frame['ingest_ts'],errors='raise',utc=False)
            _require(receipts.dt.tz is not None and not receipts.isna().any(),'partition_receipt_timezone')
            receipts=receipts.dt.tz_convert('UTC')
            _require(not stamps.isna().any() and not stamps.duplicated().any() and stamps.is_monotonic_increasing,'partition_bar_order')
            _require((stamps<=receipts).all() and (receipts>=resolved['binding']).all() and (receipts<=resolved['finished']).all(),'partition_receipt_causality')
            _require(stamps.dt.tz_convert('Europe/Moscow').dt.date.astype(str).eq(spec.trade_date).all(),'partition_bar_source_date')
            if spec is oi:
                _require('systime_source' in frame and not frame['systime_source'].isna().any()
                    and not frame['systime_source'].astype(str).str.strip().eq('').any(),'oi_source_publication_evidence_missing')
                publications=pd.to_datetime(frame['availability_ts_utc'],errors='raise')
                _require(publications.dt.tz is not None and not publications.isna().any(),'oi_publication_timezone')
                publications=publications.dt.tz_convert('UTC')
                _require((stamps<=publications).all() and (publications<=receipts).all(),'oi_partition_publication_chain')
                for document in documents.values():
                    _require(_stamp(document['min_availability_ts_utc'])==publications.min().to_pydatetime()
                        and _stamp(document['max_availability_ts_utc'])==publications.max().to_pydatetime(),'oi_frozen_support_availability_bounds')
            frame=frame.copy(); frame['_event']=stamps; frame['_receipt']=receipts
            for row in frame.to_dict('records'): _ohlc(row,'' if spec is quote else 'oi_')
            frames.append(frame); proofs.append(support)
        q,o=frames; shared=sorted(set(q['_event']) & set(o['_event']))
        _require(bool(shared),'no_exact_price_oi_bar_join')
        paired_frames[quote.secid]=(q,o,set(shared))
        ts=shared[-1]; qr=q.loc[q['_event'].eq(ts)].iloc[0]; otr=o.loc[o['_event'].eq(ts)].iloc[0]
        pub=_stamp(str(otr['availability_ts_utc'])); receipt=max(qr['_receipt'].to_pydatetime(),otr['_receipt'].to_pydatetime())
        _require(ts.to_pydatetime()<=pub<=otr['_receipt'].to_pydatetime(),'oi_publication_causality')
        proof={'stage3_audit':audit,'acceptance_run_id':resolved['run'],'revalidated_at_utc':now.isoformat(),'quote':proofs[0],'open_interest':proofs[1],
            'marker':freeze_json(resolved['marker_path'],resolved['marker']),'pilot':freeze_json(resolved['pilot_path'],resolved['pilot']),
            'parent':freeze_json(resolved['parent_path'],resolved['parent']) if resolved['parent_path'] else None,
            'binding_observed_at_utc':resolved['binding'].isoformat(),
            'original_acceptance_digest_available':False,'hash_semantics':'computed_at_current_revalidation',
            'quote_source_row':_source_json(q.drop(columns=['_event','_receipt']).loc[q['_event'].eq(ts)].to_json(orient='records',date_format='iso'))[0],
            'oi_source_row':_source_json(o.drop(columns=['_event','_receipt']).loc[o['_event'].eq(ts)].to_json(orient='records',date_format='iso'))[0]}
        result[quote.secid]=_pair(quote.secid,quote.trade_date,ts.isoformat(),pub.isoformat(),receipt.isoformat(),float(qr['close']),int(otr['oi_close']),proof,source_kind=kind)
    common_times=set.intersection(*(entry[2] for entry in paired_frames.values()))
    _require(bool(common_times),'no_exact_common_four_contract_bar')
    selected_time=max(common_times)
    for secid,(q,o,_) in paired_frames.items():
        qr=q.loc[q['_event'].eq(selected_time)].iloc[0]; otr=o.loc[o['_event'].eq(selected_time)].iloc[0]
        proof=result[secid]['proof']
        proof['quote_source_row']=_source_json(q.drop(columns=['_event','_receipt']).loc[q['_event'].eq(selected_time)].to_json(orient='records',date_format='iso'))[0]
        proof['oi_source_row']=_source_json(o.drop(columns=['_event','_receipt']).loc[o['_event'].eq(selected_time)].to_json(orient='records',date_format='iso'))[0]
        result[secid]=_pair(secid,result[secid]['trade_date'],selected_time.isoformat(),str(otr['availability_ts_utc']),
            max(qr['_receipt'],otr['_receipt']).isoformat(),float(qr['close']),int(otr['oi_close']),proof,source_kind=kind)
        result[secid]['price_received_at_utc']=qr['_receipt'].isoformat()
        result[secid]['market_oi_received_at_utc']=otr['_receipt'].isoformat()
    return result


def _table(payload, name):
    block=payload[name]; columns=block['columns']; rows=block['data']
    _require(isinstance(columns,list) and len(set(str(k).upper() for k in columns))==len(columns),'source_column_inventory')
    _require(isinstance(rows,list) and all(isinstance(row,list) and len(row)==len(columns) for row in rows),'source_row_width')
    return [dict(zip((str(k).upper() for k in columns),row)) for row in rows]


def _official_pages(root, entry, *, accepted_at, now, buffers=None, budget=None):
    if budget is None and buffers is None:budget=CaptureBudget()
    from urllib.parse import urlsplit,parse_qs
    def read(ref,digest):
        return _read_bytes(root,ref,digest,budget=budget) if buffers is None else _buffer_bytes(buffers,{'ref':ref,'sha256':digest})
    def freeze(raw): return _freeze(root,raw,budget=budget) if buffers is None else _frozen_ref(raw)
    _require(set(entry)=={'kind','trade_date','expected_total_rows','pages'} and entry['trade_date']=='2026-08-23'
        and type(entry['expected_total_rows']) is int and entry['expected_total_rows']==9683,'bounded_official_admission_identity')
    pages=entry['pages']; _require(isinstance(pages,list) and len(pages)==10,'official_page_count')
    selected={}; keys=set(); page_proofs=[]; row_count=0
    for index,page in enumerate(pages):
        _require(set(page)=={'start','response_ref','response_sha256','receipt_ref','receipt_sha256'} and type(page['start']) is int and page['start']==index*1000,'official_page_inventory')
        raw=read(page['response_ref'],page['response_sha256']); receipt_raw=read(page['receipt_ref'],page['receipt_sha256'])
        receipt=_source_object(receipt_raw); received=_stamp(receipt['received_at_utc']); requested=_stamp(receipt['requested_at_utc'])
        _require(requested<=received<=accepted_at<=now,'official_receipt_causality')
        _require(receipt.get('http_status')==200 and receipt.get('sha256',receipt.get('response_sha256'))==sha256(raw).hexdigest(),'official_transport_or_hash')
        url=urlsplit(receipt['url']); query=parse_qs(url.query)
        _require(url.scheme=='https' and url.hostname=='apim.moex.com' and not url.username and not url.password
            and url.path=='/iss/datashop/algopack/fo/tradestats.json','official_source_url')
        _require(all(query.get(k)==['2026-08-23'] for k in ('date','from','till')) and query.get('start')==[str(index*1000)],'official_request_date_cursor')
        payload=_source_object(raw); cursor=_table(payload,'data.cursor')
        _require(cursor==[{'INDEX':index*1000,'TOTAL':9683,'PAGESIZE':1000}],'official_response_cursor')
        rows=_table(payload,'data'); _require(len(rows)==min(1000,9683-index*1000),'official_response_cardinality')
        proof={'response':freeze(raw),'receipt':freeze(receipt_raw),'start':index*1000}
        page_proofs.append(proof); row_count+=len(rows)
        for row in rows:
            key=(row['SECID'],row['TRADEDATE'],row['TRADETIME']); _require(key not in keys,'official_duplicate_source_key'); keys.add(key)
            _require(row['TRADEDATE']=='2026-08-23','official_foreign_source_date')
            if row['SECID'] not in ('SiU6','SiZ6','CRU6','CRZ6'): continue
            native={k.lower().removeprefix('pr_'):v for k,v in row.items()}
            _ohlc(native); _ohlc(native,'oi_')
            event=datetime.fromisoformat(row['TRADEDATE']+'T'+row['TRADETIME']).replace(tzinfo=archive.MOSCOW)
            publication=datetime.fromisoformat(row['SYSTIME'])
            if publication.utcoffset() is None: publication=publication.replace(tzinfo=archive.MOSCOW)
            oi=_number(row['OI_CLOSE']); _require(oi==oi.to_integral_value(),'official_fractional_oi')
            record=_pair(row['SECID'],row['TRADEDATE'],event.astimezone(timezone.utc).isoformat(),publication.astimezone(timezone.utc).isoformat(),received.isoformat(),row['PR_CLOSE'],int(oi),
                {'page':proof,'source_row':row,'bounded_admitted_at_utc':accepted_at.isoformat(),
                    'receipt_fields':{'requested_at_utc':requested.isoformat(),'received_at_utc':received.isoformat(),
                        'url':receipt['url'],'http_status':200,'response_sha256':sha256(raw).hexdigest()}},source_kind='CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS')
            selected.setdefault(row['SECID'],[]).append(record)
    _require(row_count==9683 and len(selected)==4,'official_complete_market_and_contract_universe')
    shared=set.intersection(*(set(r['source_timestamp_utc'] for r in rows) for rows in selected.values()))
    _require(bool(shared),'official_no_common_contract_timestamp'); latest=max(shared)
    result={secid:next(r for r in rows if r['source_timestamp_utc']==latest) for secid,rows in selected.items()}
    for record in result.values(): record['proof']['admitted_page_inventory']=page_proofs
    return result,page_proofs


def _bounded_sources(root, *, now, budget=None):
    ref='${MOEX_DATA_ROOT}/'+SOURCE_ADMISSION
    content=_read_bytes(root,ref,budget=budget); value=_source_object(content)
    _require(set(value)=={'schema_version','project','task_id','accepted_at_utc','entries'},'source_admission_shape')
    _require(value['schema_version']=='contract_price_market_oi_source_admission.v1' and value['project']=='MOEX_Bot'
        and value['task_id']=='contract_price_market_oi_observed_comparisons_v1','source_admission_identity')
    accepted=_stamp(value['accepted_at_utc']); _require(accepted<=now,'source_admission_future')
    entries=value['entries']; _require(isinstance(entries,list) and len(entries)==2,'source_admission_bounded_inventory')
    _require({e.get('kind') for e in entries}=={'official_paginated_tradestats','standalone_stage3_pilot'},'source_admission_kinds')
    return entries,accepted,_freeze(root,content,budget=budget)


def _witness(root, now, budget=None):
    if budget is None:budget=CaptureBudget()
    import pandas as pd
    from moex_data import step9_rub_analysis_bundle as step9
    from moex_data.futures import futoi_delta_statistics_context as engine
    spec=engine._spec(stage=7,dataset_id=engine.OBSERVED_DATE_WITNESS_DATASET_ID,
        instrument_id=engine.OBSERVED_DATE_WITNESS_INSTRUMENT_ID,timeframe=engine.OBSERVED_DATE_WITNESS_TIMEFRAME)
    pointer_path=step9._pointer_path(root,spec)
    pointer_raw=_read_bytes(root,'${MOEX_DATA_ROOT}/'+pointer_path.relative_to(root).as_posix(),budget=budget)
    pointer=_source_object(pointer_raw); raw={'pointer':pointer_raw}
    for key in ('partition','manifest','quality_report'):
        raw[key]=_read_bytes(root,pointer[key+'_ref'],pointer[key+'_sha256'],budget=budget)
    frame=pd.read_parquet(BytesIO(raw['partition']))
    dates=sorted({_day(str(day)) for day in frame['trade_date']})
    _require(bool(dates) and dates[-1]<=now.astimezone(archive.MOSCOW).date().isoformat(),'observed_witness_dates')
    proof={key:_frozen_ref(content) for key,content in raw.items()}
    _portable_witness({'witness_proof':proof,'observed_dates':dates[-22:]},
        {sha256(content).hexdigest():content for content in raw.values()},now)
    for content in raw.values(): _freeze(root,content,budget=budget)
    return dates[-22:],proof


def _memoized_source_reader(root, budget=None):
    """Each original artifact is bounded before parsing and shared through freeze."""
    cache={}; total=0
    def read(path):
        nonlocal total
        if budget is not None:budget.ensure_active()
        if path not in cache:
            raw=_read_bytes(root,'${MOEX_DATA_ROOT}/'+path.relative_to(root).as_posix(),budget=budget)
            _require(len(cache)<MAX_AUDIT_BUFFERS and total+len(raw)<=MAX_AUDIT_BYTES,'audit_artifact_byte_limit')
            cache[path]=raw;total+=len(raw)
        return cache[path]
    return read


def _bounded_archive_markers(base):
    """Bound directory enumeration before opening any source metadata.

    Dates live inside pilots, so outside-lookback entries must count too.
    Overflow is a refusal of the inventory, never a truncated sample of it.
    """
    import os
    from itertools import islice
    _require(not base.is_symlink(),'accepted_archive_directory_symlink')
    if not base.exists(): return []
    _require(base.is_dir(),'accepted_archive_directory_invalid')
    with os.scandir(base) as iterator:
        entries=list(islice(iterator,MAX_ARCHIVE_DIRECTORY_ENTRIES+1))
    _require(len(entries)<=MAX_ARCHIVE_DIRECTORY_ENTRIES,
        'accepted_archive_inventory_limit_before_content_reads: maximum=256; scope=all_directory_entries_including_outside_lookback')
    return sorted(base/entry.name/'accepted_pointers.json' for entry in entries
        if entry.name.startswith('run_id=') and entry.is_dir(follow_symlinks=False)
        and (base/entry.name/'accepted_pointers.json').is_file())


def _historical(root, now, budget=None):
    if budget is None:budget=CaptureBudget()
    from moex_data import rub_accepted_stage3_resolver as resolver
    base=root/'state/acceptance/step3_canonical_raw'; earliest=now.astimezone(archive.MOSCOW).date()-timedelta(days=45)
    candidates=[]; errors={}; records={}; unordered_dates=set()
    for marker in _bounded_archive_markers(base):
        day=None; parent_required=False
        try:
            pilot=marker.with_name('pilot_evidence.json')
            value=_source_object(_read_bytes(root,'${MOEX_DATA_ROOT}/'+pilot.relative_to(root).as_posix(),budget=budget)); day=_day(value['trade_date'])
            run=marker.parent.name.removeprefix('run_id=')
            if earliest.isoformat()<=day<=now.astimezone(archive.MOSCOW).date().isoformat() and run.endswith('_stage3'):
                parent_required=True; parent_run=run[:-7]
                parent=_source_object(_read_bytes(root,'${MOEX_DATA_ROOT}/runs/step10_rub_daily_refresh/run_id='+parent_run+'/run_manifest.json',budget=budget))
                # Rank observed attempts before validation: invalid newer parents remain decisive.
                finished=_stamp(parent['finished_at_utc'])
                _require(finished<=now,'future_binding_or_parent_completion')
                candidates.append((day,finished.timestamp(),run,marker))
        except CaptureBudgetExceeded:raise
        except (OSError,ValueError,KeyError,TypeError,OverflowError) as exc:
            if isinstance(exc,(json.JSONDecodeError,UnicodeError)) or (isinstance(exc,ValueError) and any(
                reason in str(exc) for reason in ('duplicate JSON object member:','JSON numeric constant must be finite:','source_JSON_must_contain_object'))):
                raise ValueError('accepted_archive_metadata_strict_json_refusal') from exc
            if isinstance(exc,ValueError) and str(exc)=='source_artifact_byte_limit':
                raise ValueError('accepted_archive_metadata_byte_limit') from exc
            if parent_required:
                unordered_dates.add(day);errors[day]='accepted_archive_parent_inventory_unavailable: '+type(exc).__name__+': '+str(exc)
            continue
    # Latest run per date is decisive; a rejected newer run cannot expose an older run.
    selected={}
    for day,finished,run,marker in sorted(candidates): selected[day]=marker
    for day,marker in selected.items():
        if day in unordered_dates:continue
        try:
            reader=_memoized_source_reader(root,budget)
            resolved=resolver.resolve(root,marker,now=now,earliest=earliest,byte_reader=reader)
            if resolved is not None: resolved['byte_reader']=reader
            if resolved is None: continue
            records[day]=_stage3_pairs(root,resolved,now=now,kind='CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN',budget=budget)
        except CaptureBudgetExceeded:raise
        except Exception as exc:
            budget.ensure_active()  # Nested legacy validators may wrap the typed budget error.
            errors[day]=type(exc).__name__+': '+str(exc)
    try: entries,accepted,admission=_bounded_sources(root,now=now,budget=budget)
    except CaptureBudgetExceeded:raise
    except Exception as exc:
        budget.ensure_active()
        for day in ('2026-08-23','2026-08-24'):
            if day not in records: errors[day]='bounded_source_admission_unavailable: '+type(exc).__name__+': '+str(exc)
        return records,errors
    for entry in entries:
        day='2026-08-23' if entry['kind']=='official_paginated_tradestats' else '2026-08-24'
        if day in selected or day in unordered_dates: continue
        try:
            if entry['kind']=='official_paginated_tradestats': pairs,_=_official_pages(root,entry,accepted_at=accepted,now=now,budget=budget)
            else:
                _require(set(entry)=={'kind','run_id','accepted_marker_ref','pilot_evidence_ref','marker_sha256','pilot_sha256'} and entry['run_id']=='step3_pilot_20260824_1705','standalone_source_admission')
                reader=_memoized_source_reader(root,budget)
                for key in ('accepted_marker','pilot_evidence'):
                    expected=entry['marker_sha256' if key=='accepted_marker' else 'pilot_sha256']
                    _require(sha256(reader(root/entry[key+'_ref'][len('${MOEX_DATA_ROOT}/'):])).hexdigest()==expected,'source_buffer_hash_mismatch')
                marker=root/entry['accepted_marker_ref'][len('${MOEX_DATA_ROOT}/'):]
                _require(marker.parent.name=='run_id='+entry['run_id'] and marker.name=='accepted_pointers.json','standalone_marker_path')
                resolved=resolver.resolve_standalone(root,marker,now=now,earliest=earliest,accepted_at=accepted,byte_reader=reader)
                if resolved is not None: resolved['byte_reader']=reader
                _require(resolved is not None and '${MOEX_DATA_ROOT}/'+resolved['pilot_path'].relative_to(root).as_posix()==entry['pilot_evidence_ref'],'standalone_pilot_ref')
                pairs=_stage3_pairs(root,resolved,now=now,kind='REVALIDATED_STANDALONE_STAGE3_PILOT',budget=budget)
                _require(all(r['proof']['marker']['sha256']==entry['marker_sha256'] and r['proof']['pilot']['sha256']==entry['pilot_sha256'] for r in pairs.values()), 'standalone_final_frozen_hash_mismatch')
            for pair in pairs.values(): pair['proof']['source_admission']=admission
            records[day]=pairs
        except CaptureBudgetExceeded:raise
        except Exception as exc:
            budget.ensure_active()  # Nested legacy validators may wrap the typed budget error.
            errors[day]=type(exc).__name__+': '+str(exc)
    return records,errors


def _original_current(root, body, now, *, buffers=None, binding_sink=None, budget=None):
    from moex_data import synchronized_live_market_oi_context as live
    from moex_data import rub_factual_projection as projection
    carrier=body.get('original_forts_http_evidence')
    _require(isinstance(carrier,dict),'current_original_http_bytes_unavailable')
    _require(set(carrier)=={'request_started_lower_bound_utc','request_clock_semantics','responses'} and carrier['request_clock_semantics']=='batch_start_before_each_retained_request','current_http_carrier_shape')
    requested=_stamp(carrier['request_started_lower_bound_utc']); _require(requested<=now,'current_request_future')
    responses=carrier['responses']; _require(isinstance(responses,list) and 1<=len(responses)<=live.MAX_FORTS_PAGES+1,'current_response_bound')
    verified_buffers=_native_buffers(body,budget)
    selected={}; proofs=[]; security_rows={}; payload_inventory=[]
    for item in responses:
        _require(set(item)=={'content_base64','sha256','source_url','params','received_at_utc','http_status','role'},'current_original_response_shape')
        raw=verified_buffers[item['sha256']]
        received=_stamp(item['received_at_utc']); _require(requested<=received<=now and item['http_status']==200,'current_response_causality')
        _require(item['role'] in ('selected_values','completeness_probe'),'current_response_role')
        _native_url(item['source_url'],item['params'])
        payload=_source_object(raw); rows=_table(payload,'marketdata'); securities=_table(payload,'securities')
        payload_inventory.append((item,payload))
        _require(len({r['SECID'] for r in rows})==len(rows),'current_duplicate_response_secid')
        proof={'response':_inline_ref(raw),'source_url':item['source_url'],'params':item['params'],
            'received_at_utc':received.isoformat(),'requested_lower_bound_utc':requested.isoformat(),'role':item['role']}
        proofs.append(proof)
        if item['role']=='selected_values':
            for security in securities:
                if security['SECID'] in security_rows: _require(security_rows[security['SECID']]==security,'current_duplicate_security_conflict')
                security_rows[security['SECID']]=security
            for row in rows:
                if row['SECID'] in selected: _require(selected[row['SECID']][0]==row,'current_duplicate_conflict')
                selected[row['SECID']]=(row,received,proof)
    _validate_native_inventory(payload_inventory)
    binding_clock=_stamp(body['snapshot_received_at_utc'])
    actual_bindings=live._bindings_from_forts(live.pd.DataFrame(list(security_rows.values())),
        as_of_date=binding_clock.astimezone(archive.MOSCOW).date().isoformat(),availability_ts_utc=binding_clock.isoformat())
    _require(all(actual_bindings[role]==body['bindings'][role] for role in ROLES),'current_original_role_binding_mismatch')
    if binding_sink is not None:
        binding_sink.update(retained_http_inventory=proofs, binding_as_of_utc=binding_clock.isoformat())
    result={}
    for role in ROLES:
        node=body['instruments'][role]; secid=body['bindings'][role]
        _require(node['secid']==secid and node.get('price_oi_same_source_row') is True and node.get('price_oi_usable') is True,'current_pair_binding_or_gate')
        _require(projection.fresh(node,now),'current_native_pair_expired')
        row,received,proof=selected[secid]
        _require(row['LAST']==node['last'] and row['OPENPOSITION']==node['oi'] and _stamp(node['received_at_utc'])==received,'current_original_row_value_mismatch')
        source_time=live._source_event_time(row['SYSTIME'],'B.SYSTIME')
        _require(source_time==_stamp(node['timestamp']) and source_time<=received,'current_original_source_clock_mismatch')
        _require(row.get('TRADEDATE')==node.get('source_trade_date'),'current_native_trade_date_mismatch')
        price,oi=_native_numbers(row)
        fact=_pair(secid,node['source_trade_date'],node['timestamp'],node['timestamp'],node['received_at_utc'],price,oi,
            {**proof,'source_row':row,'retained_http_inventory':proofs,
                'original_bindings':{role:actual_bindings[role] for role in ROLES},'binding_as_of_utc':binding_clock.isoformat()},source_kind='CURRENT_NATIVE_SAME_RESPONSE_ROW')
        fact['timestamp_semantics']='source_row_update_time_not_last_trade_time'
        fact['last_trade_time_moscow']=node.get('last_trade_time_moscow')
        result[secid]=fact
    return result


def _validate_pair(record, *, now):
    base={'secid','trade_date','source_timestamp_utc','source_publication_at_utc','received_at_utc','price','market_open_interest','source_kind','proof','session_completion_proven',
        'price_received_at_utc','market_oi_received_at_utc','price_source_publication_at_utc','publication_clock_semantics','source_timestamp_moscow','endpoint_scope'}
    native=record.get('source_kind')=='CURRENT_NATIVE_SAME_RESPONSE_ROW'
    _require(set(record)==base|({'timestamp_semantics','last_trade_time_moscow'} if native else set()),'paired_record_shape')
    _require(record['session_completion_proven'] is False,'paired_session_authority')
    _require(record['source_timestamp_moscow']==_stamp(record['source_timestamp_utc']).astimezone(archive.MOSCOW).isoformat()
        and record['endpoint_scope']==('native_source_row_update_not_session_close' if native else 'observed_partial_day_endpoint_session_completion_unproven'),'paired_endpoint_semantics')
    for key in ('source_timestamp_utc','source_publication_at_utc','received_at_utc'): _require(_stamp(record[key])<=now,'paired_record_future_clock')
    _pair(record['secid'],record['trade_date'],record['source_timestamp_utc'],record['source_publication_at_utc'],record['received_at_utc'],
        record['price'],record['market_open_interest'],record['proof'],source_kind=record['source_kind'])
    _require(max(_stamp(record['price_received_at_utc']),_stamp(record['market_oi_received_at_utc']))==_stamp(record['received_at_utc'])
        and _stamp(record['source_timestamp_utc'])<=_stamp(record['price_received_at_utc']) and _stamp(record['source_publication_at_utc'])<=_stamp(record['market_oi_received_at_utc']),'paired_individual_receipt_chain')
    _number(record['price'],positive=True); _number(record['market_open_interest'],integer=True)
    p=record['proof']; _require(isinstance(p,dict),'paired_source_proof')
    if native:
        row=p['source_row']; _require(row['SECID']==record['secid'] and row['TRADEDATE']==record['trade_date'] and row['LAST']==record['price'] and row['OPENPOSITION']==record['market_open_interest'],'current_retained_row_mismatch')
        _require(record['timestamp_semantics']=='source_row_update_time_not_last_trade_time','current_timestamp_semantics')
        from moex_data import synchronized_live_market_oi_context as live
        _require(record['last_trade_time_moscow']==live._optional_source_text(row.get('TIME')),'current_last_trade_time_semantics')
        _require(live._source_event_time(row['SYSTIME'],'retained.SYSTIME')==_stamp(record['source_timestamp_utc'])==_stamp(record['source_publication_at_utc']),'current_retained_source_time')
        _require(_stamp(p['received_at_utc'])==_stamp(record['received_at_utc']) and _stamp(p['requested_lower_bound_utc'])<=_stamp(p['received_at_utc']),'current_retained_receipt')
        _require(p['role']=='selected_values' and isinstance(p['retained_http_inventory'],list) and 1<=len(p['retained_http_inventory'])<=live.MAX_FORTS_PAGES+1,'current_retained_response_inventory')
        _require(set(p)=={'response','source_url','params','received_at_utc','requested_lower_bound_utc','role','source_row','retained_http_inventory','original_bindings','binding_as_of_utc'},'current_retained_proof_shape')
        _native_url(p['source_url'],p['params'])
        _require(set(p['original_bindings'])==set(ROLES) and len(set(p['original_bindings'].values()))==4 and record['secid'] in p['original_bindings'].values()
            and _stamp(p['binding_as_of_utc'])<=now,'current_retained_original_binding')
    elif record['source_kind']=='CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS':
        _require(record['trade_date']=='2026-08-23','official_retained_bounded_date')
        row=p['source_row']; _require(row['SECID']==record['secid'] and row['TRADEDATE']==record['trade_date'] and row['PR_CLOSE']==record['price'] and row['OI_CLOSE']==record['market_open_interest'],'official_retained_row_mismatch')
        _require(set(p)=={'page','source_row','bounded_admitted_at_utc','source_admission','receipt_fields','admitted_page_inventory'},'official_retained_proof_shape')
        receipt=p['receipt_fields']; _require(set(receipt)=={'requested_at_utc','received_at_utc','url','http_status','response_sha256'},'official_retained_receipt_shape')
        _require(_stamp(receipt['requested_at_utc'])<=_stamp(receipt['received_at_utc'])==_stamp(record['received_at_utc'])<=_stamp(p['bounded_admitted_at_utc'])
            and receipt['http_status']==200 and receipt['response_sha256']==p['page']['response']['sha256'],'official_retained_receipt_identity')
        event=datetime.fromisoformat(row['TRADEDATE']+'T'+row['TRADETIME']).replace(tzinfo=archive.MOSCOW)
        publication=datetime.fromisoformat(row['SYSTIME'])
        if publication.utcoffset() is None: publication=publication.replace(tzinfo=archive.MOSCOW)
        _require(event==_stamp(record['source_timestamp_utc']) and publication==_stamp(record['source_publication_at_utc']),'official_retained_source_clock')
        _require(_stamp(p['bounded_admitted_at_utc'])<=now,'official_retained_admission_future')
    elif record['source_kind'] in ('CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN','REVALIDATED_STANDALONE_STAGE3_PILOT'):
        required={'stage3_audit','acceptance_run_id','revalidated_at_utc','quote','open_interest','marker','pilot','parent','binding_observed_at_utc',
            'original_acceptance_digest_available','hash_semantics','quote_source_row','oi_source_row'}
        standalone=record['source_kind']=='REVALIDATED_STANDALONE_STAGE3_PILOT'
        _require(set(p)==required|({'source_admission'} if standalone else set()),'retained_stage3_proof_inventory')
        _require((p['acceptance_run_id']=='step3_pilot_20260824_1705' and record['trade_date']=='2026-08-24') if standalone else p['acceptance_run_id'].endswith('_stage3'),'retained_stage3_run_kind')
        q,o=p['quote_source_row'],p['oi_source_row']
        _require(q['secid']==o['secid']==record['secid'] and q['trade_date']==o['trade_date']==record['trade_date'] and q['ts']==o['ts'],'retained_exact_bar_join')
        _require(q['instrument_id']==o['instrument_id'] and q['instrument_id'] in INSTRUMENTS.values()
            and q['source_id']=='moex_algopack_fo_tradestats_5m' and o['source_id']=='moex_algopack_fo_open_interest_5m','retained_source_identity')
        _require(q['close']==record['price'] and o['oi_close']==record['market_open_interest'],'retained_bar_values')
        event=datetime.fromisoformat(q['ts'].replace('Z','+00:00'))
        if event.utcoffset() is None: event=event.replace(tzinfo=archive.MOSCOW)
        _require(event==_stamp(record['source_timestamp_utc']) and _stamp(o['availability_ts_utc'])==_stamp(record['source_publication_at_utc']),'retained_bar_source_clock')
        _require(max(_stamp(q['ingest_ts']),_stamp(o['ingest_ts']))==_stamp(record['received_at_utc']),'retained_bar_receipt')
        _require(_stamp(q['ingest_ts'])==_stamp(record['price_received_at_utc']) and _stamp(o['ingest_ts'])==_stamp(record['market_oi_received_at_utc'])
            and record['price_source_publication_at_utc'] is None and record['publication_clock_semantics']=='market_oi_publication_price_publication_unavailable','retained_individual_bar_clocks')
        _require(p['original_acceptance_digest_available'] is False and p['hash_semantics']=='computed_at_current_revalidation','retained_hash_scope')
        _require(_stamp(p['binding_observed_at_utc'])<=_stamp(record['received_at_utc'])<=_stamp(p['revalidated_at_utc'])<=now,'retained_binding_receipt_chain')
        _require((p['parent'] is not None)==(record['source_kind']=='CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN'),'retained_parent_kind')
    else: raise ValueError('unknown_paired_source_kind')
    if native or record['source_kind']=='CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS':
        _require(record['price_received_at_utc']==record['market_oi_received_at_utc']==record['received_at_utc']
            and record['price_source_publication_at_utc']==record['source_publication_at_utc'] and record['publication_clock_semantics']=='same_source_row_update','same_row_clock_semantics')
    def references(value):
        if isinstance(value,dict):
            if set(value)=={'ref','sha256'}: _proof_ref(value['ref'],value['sha256']); common._hash(value['sha256'])
            else:
                for child in value.values(): references(child)
        elif isinstance(value,list):
            for child in value: references(child)
    references(p)


def _contract():
    raw=(Path(__file__).resolve().parents[2]/CONTRACT).read_text(encoding='utf-8')
    _require(common._digest(_source_object(raw))==common._digest(CONTRACT_DOCUMENT),'paired_policy_contract_invalid_or_revoked')
    return raw


def _admit(snapshot,now):
    now=_stamp(now)
    store=snapshot[STORE_KEY]
    _require(set(store)=={'evidence','evidence_sha256','last_capture_attempt_at_utc','last_capture_error','current_capture','current_sha256','latest_source_errors'},'paired_store_shape')
    e=store['evidence']; _require(common._digest(e)==store['evidence_sha256'],'paired_evidence_hash')
    expected={'schema_version','accepted_at_utc','causal_cutoff_at_utc','contract_text','bindings','role_binding_as_of_utc','observed_dates','witness_proof','history','source_errors','original_byte_buffers','binding_proof'}
    _require(set(e)==expected and e['schema_version']==SCHEMA,'paired_evidence_shape')
    _require(_json(_source_object(e['contract_text']))==_json(_source_object(_contract())),'paired_contract_revoked')
    accepted=_stamp(e['accepted_at_utc']); cutoff=_stamp(e['causal_cutoff_at_utc']); attempt=_stamp(store['last_capture_attempt_at_utc'])
    _require(cutoff<=accepted<=now and accepted<=attempt and (now-accepted).total_seconds()<=345600,'paired_admission_clock_or_expired')
    _require(store['last_capture_error'] is None or isinstance(store['last_capture_error'],str),'paired_diagnostic_type')
    latest=store['latest_source_errors']
    _require(isinstance(latest,dict) and all(isinstance(k,str) and isinstance(v,str) for k,v in latest.items()),'paired_latest_diagnostic_shape')
    bindings=e['bindings']; _require(set(bindings)==set(ROLES) and len(set(bindings.values()))==4 and all(isinstance(s,str) and s for s in bindings.values()),'paired_binding_inventory')
    _require(_stamp(e['role_binding_as_of_utc'])<=cutoff,'paired_binding_future')
    dates=e['observed_dates']; _require(isinstance(dates,list) and 1<=len(dates)<=22 and dates==sorted(set(dates)) and all(_day(d)==d for d in dates),'paired_observed_witness')
    _require(date.fromisoformat(dates[-1])<=cutoff.astimezone(archive.MOSCOW).date()
        and (cutoff.astimezone(archive.MOSCOW).date()-date.fromisoformat(dates[0])).days<=45,'paired_witness_lookback')
    _require(set(latest)<=set(dates),'paired_latest_diagnostic_dates')
    _require(set(e['witness_proof'])=={'pointer','partition','manifest','quality_report'},'paired_witness_proof')
    _require(isinstance(e['history'],dict) and set(e['history'])<=set(dates),'paired_history_date_inventory')
    for day,pairs in e['history'].items():
        _require(isinstance(pairs,dict) and set(pairs)<=set(bindings.values()),'paired_contract_inventory')
        for secid,record in pairs.items():
            _require(record['secid']==secid and record['trade_date']==day,'paired_retained_identity'); _validate_pair(record,now=cutoff)
    _require(isinstance(e['source_errors'],dict) and all(k in dates and isinstance(v,str) for k,v in e['source_errors'].items()),'paired_source_errors')
    _validate_original_history(e)
    return e


def _compact_record(record):
    if record is None: return None
    return {key:deepcopy(value) for key,value in record.items() if key!='proof'}


def _change(anchor,baseline):
    if baseline is None: return None
    price=Decimal(str(anchor['price'])); prior=Decimal(str(baseline['price'])); oi=anchor['market_open_interest']; old=baseline['market_open_interest']
    return {'price_change':float(price-prior),'price_return_fraction':float(price/prior-1),
        'market_open_interest_change':oi-old,'market_open_interest_change_fraction':float(Decimal(oi-old)/Decimal(old)) if old else None,
        'market_open_interest_fraction_reason':None if old else 'zero_baseline_market_open_interest'}


def _view(e,anchor_day,anchors,dates,*,comparison_refusal=None):
    contracts={}
    for role in ROLES:
        secid=e['bindings'][role]; anchor=anchors.get(secid); changes={}
        for lag in LAGS:
            target=dates[-1-lag] if comparison_refusal is None and len(dates)>lag else None
            baseline=e['history'].get(target,{}).get(secid)
            changes[str(lag)]={'target_observed_trade_date':target,'baseline':_compact_record(baseline),
                'values':_change(anchor,baseline) if anchor is not None else None,
                'reason':comparison_refusal or (None if anchor is not None and baseline is not None else 'exact_anchor_or_observed_baseline_unavailable')}
        contracts[role]={'secid':secid,'status':'AVAILABLE' if anchor else 'UNAVAILABLE','anchor':_compact_record(anchor),'changes':changes}
    distributions={}
    for root in ('si','cr'):
        front=anchors.get(e['bindings'][root+'_front']); nxt=anchors.get(e['bindings'][root+'_next'])
        if front is None or nxt is None or front['source_timestamp_utc']!=nxt['source_timestamp_utc']:
            distributions[root]={'status':'UNAVAILABLE','reason':'exact_common_front_next_timestamp_unavailable'}; continue
        total=front['market_open_interest']+nxt['market_open_interest']
        distributions[root]={'status':'AVAILABLE' if total else 'UNAVAILABLE','reason':None if total else 'zero_two_contract_oi_denominator',
            'source_timestamp_utc':front['source_timestamp_utc'],'two_contract_market_open_interest':total,
            'front_share_fraction':front['market_open_interest']/total if total else None,'next_share_fraction':nxt['market_open_interest']/total if total else None,
            'scope':'two_anchor_bound_contracts_not_total_market'}
    return {'status':'AVAILABLE' if any(r['status']=='AVAILABLE' for r in contracts.values()) else 'UNAVAILABLE',
        'anchor_trade_date':anchor_day,'contracts':contracts,'front_next_market_oi_distribution':distributions}


def describe(snapshot,*,now):
    try:
        now=_stamp(now)
        failure=_capture_failure(snapshot,now)
        if STORE_KEY not in snapshot and failure is not None: raise ValueError(failure['error'])
        e=_admit(snapshot,now)
        dates=e['observed_dates']; dated_day=dates[-1]
        result={'schema_version':SCHEMA,'status':'AVAILABLE','scope':SCOPE,'checked_at_utc':now.isoformat(),
            'accepted_at_utc':e['accepted_at_utc'],'evidence_sha256':snapshot[STORE_KEY]['evidence_sha256'],
            'role_binding_as_of_utc':e['role_binding_as_of_utc'],'baseline_role_semantics':'same_SECID_selected_at_explicit_role_binding_as_of_not_historical_role_proof',
            'units':{'price_by_root':{'si':'RUB_per_1000_USD','cr':'RUB_per_CNY'},'price_return_fraction':'dimensionless_fraction','market_open_interest':'source_native_market_open_position_count_as_reported','market_open_interest_counting_basis':'no_rescaling_not_family_futoi_total','shares':'dimensionless_fraction'},
            'consumer_semantics':{
                'price_fields':{'CURRENT_NATIVE_SAME_RESPONSE_ROW':'marketdata.LAST_last_trade_price',
                    'CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN':'quote.close_5m_bar_CLOSE',
                    'REVALIDATED_STANDALONE_STAGE3_PILOT':'quote.close_5m_bar_CLOSE',
                    'CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS':'PR_CLOSE_5m_bar_CLOSE'},
                'oi_fields':'native_OPENPOSITION_or_historical_oi_close_OR_OI_CLOSE_without_rescaling',
                'oi_counting_side_convention':'not_provided_by_this_evidence; no_FUTOI_or_one_sided_conversion',
                'clock_meanings':{'source_timestamp_utc':'native_row_update_or_historical_5m_bar_endpoint',
                    'source_publication_at_utc':'native_row_update_time_reused_not_independently_proven_publication; official_SYSTIME_publication; Stage3_OI_publication_does_not_prove_price_publication',
                    'received_at_utc':'maximum_of_price_and_OI_receipts_not_atomic_simultaneous_receipt',
                    'price_source_publication_at_utc':'null_means_unavailable_not_inferred_from_endpoint'},
                'horizon_basis':'exact_1_5_20_indices_in_common_accepted_observed_trade_dates_not_calendar_days',
                'current_date_policy':'append_verified_current_date_if_later_than_witness_max; gap_over_one_calendar_day_refuses_all_exact_current_comparisons_without_non_session_inference',
                'missing_target_policy':'select_target_before_source_lookup; missing_target_stays_unavailable_without_older_replacement',
                'accepted_at_utc_scope':'dated_evidence_first_acceptance_only_not_current_source_acceptance',
                'dated_lifetime_seconds':345600,
                'current_lifetime':'source_rows_and_fast_generation_must_pass_existing_60_second_read_freshness; dated_expiry_does_not_extend_current',
                'current_pair_usable_at_read':'current_view_verified_price_OI_source_pair_passes_current_read_gates; does_not_assert_comparison_or_distribution_availability_or_action_authority',
                'role_selection':'four_exact_SECIDs_selected_from_native_security_reference_at_role_binding_as_of_utc; no_historical_front_next_assignment_or_continuous_series',
                'audit_hash_scope':'evidence_sha256_hashes_full_dated_evidence_in_audit_reference_including_original_byte_table; JSON_summary_alone_is_not_source_verification'},
            'dated_valid_until_utc':(_stamp(e['accepted_at_utc'])+timedelta(seconds=345600)).isoformat(),
            'observed_trade_dates':dates,'audit_reference':'input_snapshot.json#/'+STORE_KEY+'/evidence',
            'last_capture_error':snapshot[STORE_KEY]['last_capture_error'] if _stamp(snapshot[STORE_KEY]['last_capture_attempt_at_utc'])<=now else None,
            'latest_source_errors':deepcopy(snapshot[STORE_KEY]['latest_source_errors']) if _stamp(snapshot[STORE_KEY]['last_capture_attempt_at_utc'])<=now else {},
            'dated':_view(e,dated_day,e['history'].get(dated_day,{}),dates),**FLAGS}
        try:
            current=_current_capture(snapshot,now,e)
            from moex_data.rub_snapshot_read_freshness import MAX_LIVE_AGE_SECONDS
            _require(all(0<=(now-_stamp(r['source_timestamp_utc'])).total_seconds()<=MAX_LIVE_AGE_SECONDS for r in current.values()),'current_native_pair_expired')
            days={r['trade_date'] for r in current.values()}; _require(len(days)==1,'current_contract_trade_date_mismatch'); day=next(iter(days))
            _require(day>=dated_day,'current_observed_date_precedes_witness')
            current_dates=dates if day==dated_day else (dates+[day])[-22:]
            gap=(date.fromisoformat(day)-date.fromisoformat(dated_day)).days
            refusal='insufficient_observed_witness_coverage_for_exact_current_comparisons' if gap>1 else None
            result['current']=_view(e,day,current,current_dates,comparison_refusal=refusal)
            result['current'].update(observed_witness_continuity_status='UNAVAILABLE' if refusal else 'AVAILABLE',
                observed_witness_continuity_reason=refusal)
        except (ValueError,TypeError,KeyError) as exc: result['current']={'status':'UNAVAILABLE','reason':str(exc)}
        result['dated'].update(anchor_role='accepted_observed_bar_endpoint',current_pair_usable_at_read=False)
        result['current'].update(anchor_role='native_source_row_update',current_pair_usable_at_read=result['current']['status']=='AVAILABLE')
        return result
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as exc:
        return {'schema_version':SCHEMA,'status':'UNAVAILABLE','scope':SCOPE,'reason':str(exc),**FLAGS}


def capture_snapshot(snapshot,previous,*,now_fn,refresh_started_at,previous_capture_completed=None):
    body=((snapshot.get('components') or {}).get('synchronized_live_market_oi') or {}).get('data') or {}
    if not set(ROLES)<=set(body.get('instruments',{})) and STORE_KEY not in (previous or {}): return None
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    cutoff=_stamp(now_fn()); floors=[_stamp(refresh_started_at)]
    old=deepcopy((previous or {}).get(STORE_KEY))
    if previous_capture_completed is not None: floors.append(_stamp(previous_capture_completed))
    if old is not None: floors.append(_stamp(old['last_capture_attempt_at_utc']))
    if (previous or {}).get('contract_price_market_oi_capture_error') is not None:
        _capture_failure(previous,cutoff)
        floors.append(_stamp(previous['contract_price_market_oi_capture_error']['checked_at_utc']))
    _require(cutoff>=max(floors),'paired_capture_clock_reversed')
    candidate=None; error=None; current=None; current_buffers={}; current_error='current_capture_not_attempted'
    budget=CaptureBudget()
    try:
        root=source._data_root(); dates,witness=_witness(root,cutoff,budget); history,errors=_historical(root,cutoff,budget)
        bindings={role:body['bindings'][role] for role in ROLES}
        current=current_error=None; binding_proof={}
        try: current=_original_current(root,body,cutoff,binding_sink=binding_proof,budget=budget)
        except CaptureBudgetExceeded:raise
        except Exception as exc: current_error=type(exc).__name__+': '+str(exc)
        _require(bool(binding_proof),'original_native_role_binding_proof_unavailable')
        construction={'binding_proof':binding_proof,'schema_version':SCHEMA,'accepted_at_utc':cutoff.isoformat(),'causal_cutoff_at_utc':cutoff.isoformat(),
            'contract_text':_contract(),'bindings':bindings,'role_binding_as_of_utc':body['snapshot_received_at_utc'],
            'observed_dates':dates,'witness_proof':witness,
            'history':{day:{secid:r for secid,r in pairs.items() if secid in bindings.values()} for day,pairs in history.items() if day in dates},
            'source_errors':{day:reason for day,reason in errors.items() if day in dates}}
        available=_native_buffers(body,budget)
        construction['original_byte_buffers']=_buffer_table(root,construction,available=available,budget=budget)
        current_buffers=_buffer_table(root,current,available=available,budget=budget) if current else {}
        candidate=construction  # Only complete, byte-validated construction can reach admission.
    except Exception as exc: error=type(exc).__name__+': '+str(exc)
    completed=_stamp(now_fn()); _require(completed>=cutoff,'paired_capture_completion_reversed')
    if candidate is not None:
        candidate['accepted_at_utc']=completed.isoformat()
        current_carrier={'causal_cutoff_at_utc':cutoff.isoformat(),'captured_at_utc':completed.isoformat(),'bindings':candidate['bindings'],'facts':current,'error':current_error,
            'original_byte_buffers':current_buffers}
        store={'evidence':candidate,'evidence_sha256':common._digest(candidate),'last_capture_attempt_at_utc':completed.isoformat(),'last_capture_error':None,
            'current_capture':current_carrier,'current_sha256':common._digest(current_carrier),'latest_source_errors':candidate['source_errors']}
        try:
            _admit({STORE_KEY:store},completed)
            if old is not None:
                try:
                    # Fact identity does not become new merely because its own
                    # first-acceptance TTL elapsed during another capture.
                    prior=_admit({STORE_KEY:old},_stamp(old['evidence']['accepted_at_utc']))
                    if _semantic(prior)==_semantic(candidate): store['evidence'],store['evidence_sha256']=prior,old['evidence_sha256']
                except (ValueError,TypeError,KeyError,AttributeError): pass
            snapshot[STORE_KEY]=store
        except Exception as exc: error=type(exc).__name__+': '+str(exc)
    if error is not None:
        if old is not None:
            old.update(last_capture_attempt_at_utc=completed.isoformat(),last_capture_error=error,current_capture=None,current_sha256=None,latest_source_errors={}); snapshot[STORE_KEY]=old
        snapshot['contract_price_market_oi_capture_error']={'checked_at_utc':completed.isoformat(),'error':error}
    else: snapshot['contract_price_market_oi_capture_error']=None
    # Original HTTP bytes are now frozen in the B evidence carrier; avoid a second base64 copy.
    body.pop('original_forts_http_evidence',None)
    return completed


def attach_consumer(snapshot,release,*,now):
    release['contract_price_market_oi_context']=describe(snapshot,now=now)


def _semantic(e):
    def version(record):
        return {key:record[key] for key in ('secid','trade_date','source_timestamp_utc','source_publication_at_utc','price','market_open_interest','source_kind')}
    return {'bindings':e['bindings'],'dates':e['observed_dates'],'contract':e['contract_text'],
        'history':{day:{secid:version(r) for secid,r in pairs.items()} for day,pairs in e['history'].items()}}


def _current_capture(snapshot,now,e):
    store=snapshot[STORE_KEY]
    fast=snapshot.get('fast_market_read') is not None
    if fast:
        envelope=snapshot.get(CURRENT_KEY)
        _require(isinstance(envelope,dict) and set(envelope)=={'capture','sha256'},'fast_current_original_proof_unavailable')
        current=envelope['capture']; digest=envelope['sha256']
        expected_capture=_stamp(snapshot['fast_market_read']['completed_at'])
        _require(snapshot['fast_market_read'].get('error') is None,'fast_current_collection_unavailable')
    else:
        current=store['current_capture']; digest=store['current_sha256']; expected_capture=_stamp(store['last_capture_attempt_at_utc'])
    _require(isinstance(current,dict),store['last_capture_error'] or 'current_original_proof_unavailable')
    _require(set(current)=={'causal_cutoff_at_utc','captured_at_utc','bindings','facts','error','original_byte_buffers'} and common._digest(current)==digest,'current_capture_hash_or_shape')
    cutoff=_stamp(current['causal_cutoff_at_utc']); captured=_stamp(current['captured_at_utc'])
    _require(cutoff<=captured<=now and captured==expected_capture and current['bindings']==e['bindings'],'current_capture_clock_or_binding')
    _require(current['error'] is None or isinstance(current['error'],str),'current_capture_error_type')
    facts=current['facts']; _require(isinstance(facts,dict),current['error'] or 'current_original_proof_unavailable')
    _require(current['error'] is None and set(facts)==set(e['bindings'].values()),'current_capture_inventory')
    for secid,record in facts.items():
        _require(record['secid']==secid and record['source_kind']=='CURRENT_NATIVE_SAME_RESPONSE_ROW','current_capture_identity')
        _validate_pair(record,now=cutoff)
        _require(record['proof']['original_bindings']==e['bindings'],'current_capture_original_binding_mismatch')
        from moex_data.rub_snapshot_read_freshness import MAX_LIVE_AGE_SECONDS
        _require(0<=(cutoff-_stamp(record['source_timestamp_utc'])).total_seconds()<=MAX_LIVE_AGE_SECONDS,'current_original_admission_expired_at_capture')
    buffers=_decode_buffers(current['original_byte_buffers'])
    first=next(iter(facts.values()))['proof']
    body_at_capture=_portable_native_body(first,buffers,e['bindings'],cutoff,facts)
    derived=_original_current(None,body_at_capture,cutoff,buffers=buffers)
    _require(common._digest(derived)==common._digest(facts),'current_original_buffer_fact_mismatch')
    from moex_data import rub_factual_projection as projection
    body=projection.market_data(snapshot)
    for role,secid in e['bindings'].items():
        node=body['instruments'][role]; fact=facts[secid]
        _require(body['bindings'][role]==node['secid']==secid and node.get('price_oi_usable') is True and node.get('price_oi_same_source_row') is True
            and projection.fresh(node,now),'current_native_admission_revoked_or_expired')
        _require(node['last']==fact['price'] and node['oi']==fact['market_open_interest'] and _stamp(node['timestamp'])==_stamp(fact['source_timestamp_utc'])
            and _stamp(node['received_at_utc'])==_stamp(fact['received_at_utc']) and node.get('source_trade_date')==fact['trade_date'],'current_native_source_version_mismatch')
    return facts


def _capture_failure(snapshot,now):
    value=snapshot.get('contract_price_market_oi_capture_error')
    if value is None: return None
    _require(isinstance(value,dict) and set(value)=={'checked_at_utc','error'} and isinstance(value['error'],str) and bool(value['error']),'paired_capture_failure_shape')
    checked=_stamp(value['checked_at_utc']); store=snapshot.get(STORE_KEY)
    if store is not None: _require(checked==_stamp(store['last_capture_attempt_at_utc']) and value['error']==store['last_capture_error'],'paired_capture_failure_coherence')
    return deepcopy(value) if checked<=now else None


def _native_url(value,params):
    from urllib.parse import urlsplit
    from moex_data import synchronized_live_market_oi_context as live
    parsed=urlsplit(value)
    _require(parsed.scheme=='https' and parsed.hostname=='apim.moex.com' and not parsed.username and not parsed.password
        and parsed.path==live.FORTS_ENDPOINT,'current_native_source_url')
    _require(isinstance(params,dict) and set(params)<={'iss.meta','iss.only','securities.columns','marketdata.columns','start'},'current_native_request_params')


def _validate_native_inventory(inventory):
    selected=[(i,p) for i,p in inventory if i['role']=='selected_values']
    probes=[(i,p) for i,p in inventory if i['role']=='completeness_probe']
    _require(bool(selected),'current_selected_http_bytes_missing')
    if not isinstance(selected[0][1].get('securities.cursor'),dict):
        _require(len(selected)==len(probes)==1 and probes[0][0]['params'].get('start')==1_000_000_000,'current_full_response_probe_missing')
        from moex_data import synchronized_live_market_oi_context_apim as apim
        _require(not isinstance(probes[0][1].get('securities.cursor'),dict),'current_probe_pagination_changed')
        # The source compares normalized SECID sequences, not live value equality.
        try:
            first=apim._validate_apim_full_response(selected[0][1])
            second=apim._validate_apim_full_response(probes[0][1])
        except apim.core.SynchronizedLiveMarketOIError as exc:
            raise ValueError(str(exc)) from exc
        _require(first==second,'current_probe_universe_mismatch')
    else:
        _require(not probes,'current_cursor_probe_mixed')
        total=size=None; count=0; seen=set()
        for index,(item,payload) in enumerate(selected):
            cursor=_table(payload,'securities.cursor'); _require(len(cursor)==1,'current_cursor_shape'); cursor=cursor[0]
            if index==0: total,size=cursor['TOTAL'],cursor['PAGESIZE']
            _require(('start' not in item['params']) if index==0 else item['params'].get('start')==index*size,'current_cursor_request_progress')
            for name in ('securities','marketdata'):
                _require(payload[name]['columns']==selected[0][1][name]['columns'],'current_cursor_columns_changed')
            _require(type(total) is int and type(size) is int and total>0 and size>0 and cursor=={'INDEX':index*size,'TOTAL':total,'PAGESIZE':size},'current_cursor_progress')
            rows=_table(payload,'securities'); _require(len(rows)==min(size,total-index*size),'current_cursor_page_rows')
            count+=len(rows); seen.update(str(r['SECID']).upper() for r in rows)
        _require(count==total==len(seen),'current_cursor_incomplete')

def _oracle_view(e,day,anchors,dates,*,comparison_refusal=None):
    contracts={}
    for role,secid in e['bindings'].items():
        anchor=anchors.get(secid); changes={}
        for lag in (1,5,20):
            target=dates[len(dates)-1-lag] if comparison_refusal is None and len(dates)>lag else None
            baseline=e['history'].get(target,{}).get(secid); values=None
            if anchor is not None and baseline is not None:
                a,b=Decimal(str(anchor['price'])),Decimal(str(baseline['price'])); oi,old=anchor['market_open_interest'],baseline['market_open_interest']
                values={'price_change':float(a-b),'price_return_fraction':float(a/b-1),'market_open_interest_change':oi-old,
                    'market_open_interest_change_fraction':float(Decimal(oi-old)/old) if old else None,
                    'market_open_interest_fraction_reason':None if old else 'zero_baseline_market_open_interest'}
            changes[str(lag)]={'target_observed_trade_date':target,'baseline':{k:deepcopy(v) for k,v in baseline.items() if k!='proof'} if baseline else None,
                'values':values,'reason':comparison_refusal or (None if values is not None else 'exact_anchor_or_observed_baseline_unavailable')}
        contracts[role]={'secid':secid,'status':'AVAILABLE' if anchor else 'UNAVAILABLE',
            'anchor':{k:deepcopy(v) for k,v in anchor.items() if k!='proof'} if anchor else None,'changes':changes}
    distributions={}
    for root in ('si','cr'):
        a,b=(anchors.get(e['bindings'][root+'_'+role]) for role in ('front','next'))
        if a is None or b is None or a['source_timestamp_utc']!=b['source_timestamp_utc']:
            distributions[root]={'status':'UNAVAILABLE','reason':'exact_common_front_next_timestamp_unavailable'}
        else:
            total=a['market_open_interest']+b['market_open_interest']
            distributions[root]={'status':'AVAILABLE' if total else 'UNAVAILABLE','reason':None if total else 'zero_two_contract_oi_denominator',
                'source_timestamp_utc':a['source_timestamp_utc'],'two_contract_market_open_interest':total,
                'front_share_fraction':float(Decimal(a['market_open_interest'])/total) if total else None,
                'next_share_fraction':float(Decimal(b['market_open_interest'])/total) if total else None,'scope':'two_anchor_bound_contracts_not_total_market'}
    return {'status':'AVAILABLE' if any(value['status']=='AVAILABLE' for value in contracts.values()) else 'UNAVAILABLE',
        'anchor_trade_date':day,'contracts':contracts,'front_next_market_oi_distribution':distributions}


def verify_projection(snapshot,release,*,now):
    """Full independent projection inventory, refusal shape and Decimal arithmetic."""
    try:
        now=_stamp(now); failure=_capture_failure(snapshot,now)
        if STORE_KEY not in snapshot and failure is not None: raise ValueError(failure['error'])
        e=_admit(snapshot,now)
        dates=e['observed_dates']; day=dates[-1]
        expected={'schema_version':SCHEMA,'status':'AVAILABLE','scope':SCOPE,'checked_at_utc':now.isoformat(),
            'accepted_at_utc':e['accepted_at_utc'],'evidence_sha256':snapshot[STORE_KEY]['evidence_sha256'],
            'role_binding_as_of_utc':e['role_binding_as_of_utc'],'baseline_role_semantics':'same_SECID_selected_at_explicit_role_binding_as_of_not_historical_role_proof',
            'units':{'price_by_root':{'si':'RUB_per_1000_USD','cr':'RUB_per_CNY'},'price_return_fraction':'dimensionless_fraction','market_open_interest':'source_native_market_open_position_count_as_reported','market_open_interest_counting_basis':'no_rescaling_not_family_futoi_total','shares':'dimensionless_fraction'},
            'consumer_semantics':{
                'price_fields':{'CURRENT_NATIVE_SAME_RESPONSE_ROW':'marketdata.LAST_last_trade_price',
                    'CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN':'quote.close_5m_bar_CLOSE',
                    'REVALIDATED_STANDALONE_STAGE3_PILOT':'quote.close_5m_bar_CLOSE',
                    'CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS':'PR_CLOSE_5m_bar_CLOSE'},
                'oi_fields':'native_OPENPOSITION_or_historical_oi_close_OR_OI_CLOSE_without_rescaling',
                'oi_counting_side_convention':'not_provided_by_this_evidence; no_FUTOI_or_one_sided_conversion',
                'clock_meanings':{'source_timestamp_utc':'native_row_update_or_historical_5m_bar_endpoint',
                    'source_publication_at_utc':'native_row_update_time_reused_not_independently_proven_publication; official_SYSTIME_publication; Stage3_OI_publication_does_not_prove_price_publication',
                    'received_at_utc':'maximum_of_price_and_OI_receipts_not_atomic_simultaneous_receipt',
                    'price_source_publication_at_utc':'null_means_unavailable_not_inferred_from_endpoint'},
                'horizon_basis':'exact_1_5_20_indices_in_common_accepted_observed_trade_dates_not_calendar_days',
                'current_date_policy':'append_verified_current_date_if_later_than_witness_max; gap_over_one_calendar_day_refuses_all_exact_current_comparisons_without_non_session_inference',
                'missing_target_policy':'select_target_before_source_lookup; missing_target_stays_unavailable_without_older_replacement',
                'accepted_at_utc_scope':'dated_evidence_first_acceptance_only_not_current_source_acceptance',
                'dated_lifetime_seconds':345600,
                'current_lifetime':'source_rows_and_fast_generation_must_pass_existing_60_second_read_freshness; dated_expiry_does_not_extend_current',
                'current_pair_usable_at_read':'current_view_verified_price_OI_source_pair_passes_current_read_gates; does_not_assert_comparison_or_distribution_availability_or_action_authority',
                'role_selection':'four_exact_SECIDs_selected_from_native_security_reference_at_role_binding_as_of_utc; no_historical_front_next_assignment_or_continuous_series',
                'audit_hash_scope':'evidence_sha256_hashes_full_dated_evidence_in_audit_reference_including_original_byte_table; JSON_summary_alone_is_not_source_verification'},
            'dated_valid_until_utc':(_stamp(e['accepted_at_utc'])+timedelta(seconds=345600)).isoformat(),
            'observed_trade_dates':dates,'audit_reference':'input_snapshot.json#/'+STORE_KEY+'/evidence',
            'last_capture_error':snapshot[STORE_KEY]['last_capture_error'] if _stamp(snapshot[STORE_KEY]['last_capture_attempt_at_utc'])<=now else None,
            'latest_source_errors':deepcopy(snapshot[STORE_KEY]['latest_source_errors']) if _stamp(snapshot[STORE_KEY]['last_capture_attempt_at_utc'])<=now else {},
            'dated':_oracle_view(e,day,e['history'].get(day,{}),dates),**FLAGS}
        try:
            current=_current_capture(snapshot,now,e)
            from moex_data.rub_snapshot_read_freshness import MAX_LIVE_AGE_SECONDS
            _require(all(0<=(now-_stamp(r['source_timestamp_utc'])).total_seconds()<=MAX_LIVE_AGE_SECONDS for r in current.values()),'current_native_pair_expired')
            days={r['trade_date'] for r in current.values()}; _require(len(days)==1,'current_contract_trade_date_mismatch'); current_day=next(iter(days))
            _require(current_day>=day,'current_observed_date_precedes_witness')
            # Derive coverage independently of the producer's rendering branch.
            covered=date.fromisoformat(current_day)<=date.fromisoformat(day)+timedelta(days=1)
            refusal=None if covered else 'insufficient_observed_witness_coverage_for_exact_current_comparisons'
            expected['current']=_oracle_view(e,current_day,current,dates if current_day==day else (dates+[current_day])[-22:],comparison_refusal=refusal)
            expected['current'].update(observed_witness_continuity_status='AVAILABLE' if covered else 'UNAVAILABLE',
                observed_witness_continuity_reason=refusal)
        except (ValueError,TypeError,KeyError) as exc: expected['current']={'status':'UNAVAILABLE','reason':str(exc)}
        expected['dated'].update(anchor_role='accepted_observed_bar_endpoint',current_pair_usable_at_read=False)
        expected['current'].update(anchor_role='native_source_row_update',current_pair_usable_at_read=expected['current']['status']=='AVAILABLE')
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as exc:
        expected={'schema_version':SCHEMA,'status':'UNAVAILABLE','scope':SCOPE,'reason':str(exc),**FLAGS}
    common._require(common._digest(release.get('contract_price_market_oi_context'))==common._digest(expected),'contract price/OI independent complete projection')


def _validate_support_buffers(spec,manifest,quality):
    """Mirror existing Stage3 support predicates on the exact frozen JSON buffers."""
    from moex_data import step3_raw_acceptance as stage3
    identity={'dataset_id':spec.dataset_id,'run_id':spec.manifest_run_id,'instrument_id':spec.instrument_id,
        'source_id':spec.source_id,'secid':spec.secid,'trade_date':spec.trade_date}
    if spec.dataset_id=='futures_raw_5m':
        _require(manifest.get('run_id')==spec.manifest_run_id and manifest.get('refresh_status')=='succeeded','frozen_quote_manifest_status')
        _require(stage3._single_scope(manifest.get('instrument_scope'),'instrument')==spec.instrument_id
            and stage3._single_scope(manifest.get('source_scope'),'source')==spec.source_id,'frozen_quote_manifest_scope')
        partitions=manifest.get('partitions_written');_require(isinstance(partitions,list) and len(partitions)==1,'frozen_quote_partition_inventory')
        _portable_same_path(partitions[0],spec.partition_path,'partition');_portable_same_path(manifest.get('quality_report_ref'),spec.quality_path,'quality')
        _require(isinstance(manifest.get('source_contract'),dict) and all(manifest['source_contract'].get(k)==v for k,v in identity.items() if k not in ('dataset_id','run_id')),'frozen_quote_source_contract')
        _require(quality.get('run_id')==spec.manifest_run_id,'frozen_quote_quality_run')
        row=stage3._require_list(quality.get('rows'),'quality.rows',1)[0]
    else:
        _require(all(manifest.get(k)==v for k,v in identity.items()) and manifest.get('status')=='succeeded','frozen_oi_manifest_identity')
        _require(stage3._positive_int(manifest.get('row_count'),'row_count')==spec.row_count,'frozen_oi_manifest_rows')
        _portable_same_path(manifest.get('partition_path'),spec.partition_path,'partition');_portable_same_path(manifest.get('quality_report_path'),spec.quality_path,'quality')
        _portable_same_path(quality.get('partition_path'),spec.partition_path,'quality.partition')
        row=quality
    _require(all(row.get(k)==v for k,v in identity.items()) and row.get('quality_status')=='pass','frozen_quality_identity_or_status')
    _require(stage3._positive_int(row.get('rows'),'quality.rows')==spec.row_count,'frozen_quality_row_count')
