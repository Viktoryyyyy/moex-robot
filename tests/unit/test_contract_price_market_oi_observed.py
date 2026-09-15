"""Independent exact-SECID/observed-slot and source-proof regressions."""
from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import pytest
from moex_data import rub_contract_price_market_oi_observed as m

NOW=datetime(2026,9,15,12,tzinfo=timezone.utc)
BINDINGS=dict(zip(m.ROLES,('SiU6','SiZ6','CRU6','CRZ6')))
REF={'ref':'${MOEX_DATA_ROOT}/immutable/test.bin','sha256':'a'*64}


def _native_body(now=NOW,trade_date=None):
    import base64
    from moex_data import synchronized_live_market_oi_context as live
    event=now-timedelta(seconds=30); received=now-timedelta(seconds=20)
    rows=[]; nodes={}
    for index,(role,secid) in enumerate(BINDINGS.items()):
        row={'SECID':secid,'TRADEDATE':trade_date or now.date().isoformat(),'SYSTIME':event.astimezone(m.archive.MOSCOW).strftime('%Y-%m-%d %H:%M:%S'),
            'LAST':130.+index,'OPENPOSITION':1100+index,'TIME':None}
        rows.append(row)
        nodes[role]={'secid':secid,'last':row['LAST'],'oi':row['OPENPOSITION'],'timestamp':event.isoformat(),
            'received_at_utc':received.isoformat(),'source_trade_date':row['TRADEDATE'],
            'last_trade_time_moscow':None,'price_oi_same_source_row':True,'price_oi_usable':True,'stale':False}
    payload={'securities':{'columns':['SECID','BOARDID','LASTTRADEDATE','MINSTEP','STEPPRICE'],
        'data':[[secid,'RFUD','2026-09-17' if secid.endswith('U6') else '2026-12-17',1.,1.] for secid in BINDINGS.values()]},
        'marketdata':{'columns':list(rows[0]),'data':[list(r.values()) for r in rows]}}
    raw=json.dumps(payload).encode(); digest=m.sha256(raw).hexdigest()
    responses=[{'content_base64':base64.b64encode(raw).decode(),'sha256':digest,'source_url':'https://apim.moex.com'+live.FORTS_ENDPOINT,
        'params':{} if role=='selected_values' else {'start':1_000_000_000},'received_at_utc':received.isoformat(),'http_status':200,'role':role}
        for role in ('selected_values','completeness_probe')]
    return {'bindings':dict(BINDINGS),'instruments':nodes,'snapshot_received_at_utc':now.isoformat(),
        'original_forts_http_evidence':{'request_started_lower_bound_utc':(now-timedelta(seconds=40)).isoformat(),
            'request_clock_semantics':'batch_start_before_each_retained_request','responses':responses}}


def _consumer_bounded_sources(root,history):
    """Synthetic source buffers, explicitly for offline consumer examples only."""
    columns=['SECID','TRADEDATE','TRADETIME','SYSTIME','PR_OPEN','PR_HIGH','PR_LOW','PR_CLOSE','OI_OPEN','OI_HIGH','OI_LOW','OI_CLOSE']
    rows=[[f'SYNTHETIC{i}','2026-08-23','19:00:00','2026-08-23 19:00:48',100.,100.,100.,100.,1000,1000,1000,1000] for i in range(9683)]
    for i,secid in enumerate(BINDINGS.values()):rows[i][0]=secid
    pages=[]
    for start in range(0,9683,1000):
        payload={'data':{'columns':columns,'data':rows[start:start+1000]},'data.cursor':{'columns':['INDEX','TOTAL','PAGESIZE'],'data':[[start,9683,1000]]}}
        raw=json.dumps(payload).encode();response=m._freeze(root,raw)
        receipt={'requested_at_utc':(NOW-timedelta(seconds=20)).isoformat(),'received_at_utc':(NOW-timedelta(seconds=10)).isoformat(),
            'http_status':200,'sha256':response['sha256'],
            'url':f'https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json?date=2026-08-23&from=2026-08-23&till=2026-08-23&start={start}'}
        receipt_proof=m._freeze(root,json.dumps(receipt).encode())
        pages.append({'start':start,'response_ref':response['ref'],'response_sha256':response['sha256'],
            'receipt_ref':receipt_proof['ref'],'receipt_sha256':receipt_proof['sha256']})
    official={'kind':'official_paginated_tradestats','trade_date':'2026-08-23','expected_total_rows':9683,'pages':pages}
    old=history['2026-08-24']['SiU6']['proof'];run=old['acceptance_run_id']
    standalone={'kind':'standalone_stage3_pilot','run_id':run,
        'accepted_marker_ref':'${MOEX_DATA_ROOT}/state/acceptance/step3_canonical_raw/run_id='+run+'/accepted_pointers.json',
        'pilot_evidence_ref':'${MOEX_DATA_ROOT}/state/acceptance/step3_canonical_raw/run_id='+run+'/pilot_evidence.json',
        'marker_sha256':old['marker']['sha256'],'pilot_sha256':old['pilot']['sha256']}
    accepted=NOW-timedelta(seconds=5)
    document={'schema_version':'contract_price_market_oi_source_admission.v1','project':'MOEX_Bot',
        'task_id':'contract_price_market_oi_observed_comparisons_v1','accepted_at_utc':accepted.isoformat(),'entries':[official,standalone]}
    proof=m._freeze(root,json.dumps(document).encode())
    history['2026-08-23'],_=m._official_pages(root,official,accepted_at=accepted,now=NOW)
    for day in ('2026-08-23','2026-08-24'):
        for record in history[day].values():record['proof']['source_admission']=proof


from functools import lru_cache


@lru_cache(maxsize=8)
def _source_snapshot(revised=False,zero_oi=False,consumer=False,witness_end=None):
    """Real Parquet/JSON buffers; no admission or membership validator is mocked."""
    import tempfile
    import pandas as pd
    from test_step3_raw_acceptance import _evidence, _write_json
    from moex_data import step3_raw_acceptance as stage3
    from moex_data.futures import futoi_delta_statistics_context as engine
    with tempfile.TemporaryDirectory() as folder:
        root=Path(folder); history={}
        dates=[(NOW.date()-timedelta(days=i)).isoformat() for i in reversed(range(1,24 if consumer else 23))]
        if consumer:dates=[d for d in dates if d not in ('2026-09-12','2026-09-13')]
        if witness_end:dates=[d for d in dates if d<=witness_end]
        for index,day in enumerate(dates):
            run='step3_pilot_20260824_1705' if consumer and day=='2026-08-24' else 'step10_'+day.replace('-','')+'_stage3';pilot=_evidence(root,run)
            publication=day+('T14:05:48+00:00' if consumer and day=='2026-08-24' else 'T16:00:49+00:00' if revised and index==21 else 'T16:00:48+00:00')
            # Fixture producer uses Aug24; replace source dates in all original JSON.
            pilot=json.loads(json.dumps(pilot).replace('2026-08-24',day))
            run_root=root/'runs/step3_canonical_raw'/('run_id='+run)
            for path in run_root.rglob('*.json'):
                path.write_text(path.read_text().replace('2026-08-24',day))
            specs=[]
            for field,dataset in [('quote_partitions','futures_raw_5m'),('open_interest_partitions','futures_open_interest_raw_5m'),('tom_partitions','fx_spot_raw_5m')]:
                for item in pilot[field]:
                    quote=field=='quote_partitions'
                    instrument=item['instrument_id_scope'][0] if quote else item['instrument_id']
                    secid=item['secid_scope'][0] if quote else item['secid']
                    paths=[Path(item[k]) for k in (('manifest_reference','quality_report_reference','storage_partition_path') if quote else ('manifest_path','quality_report_path','partition_path'))]
                    manifest=json.loads(paths[0].read_text()); quality=json.loads(paths[1].read_text())
                    if field!='tom_partitions':
                        rows=[]
                        for bar in range(10):
                            event=datetime.fromisoformat(day+('T13:20:00+00:00' if consumer and day=='2026-08-24' else 'T15:15:00+00:00'))+timedelta(minutes=5*bar)
                            row={'secid':secid,'trade_date':day,'instrument_id':instrument,'source_id':item['source_id'],
                                'ts':event.astimezone(m.archive.MOSCOW).replace(tzinfo=None).isoformat(),'ingest_ts':day+('T14:08:00+00:00' if consumer and day=='2026-08-24' else 'T16:01:00+00:00')}
                            if quote: row.update(open=100.+index,high=100.+index,low=100.+index,close=100.+index)
                            else: row.update(oi_open=0 if zero_oi else 1000+index,oi_high=0 if zero_oi else 1000+index,oi_low=0 if zero_oi else 1000+index,oi_close=0 if zero_oi else 1000+index,
                                availability_ts_utc=publication,systime_source=day+' 19:00:48')
                            rows.append(row)
                        pd.DataFrame(rows).to_parquet(paths[2],index=False)
                        if not quote:
                            for document in (manifest,quality):document.update(min_availability_ts_utc=publication,max_availability_ts_utc=publication)
                    _write_json(paths[0],manifest);_write_json(paths[1],quality)
                    specs.append(stage3.PointerSpec(dataset,instrument,item['source_id'],secid,day,10,*paths,manifest['run_id']))
            pilot_path=root/'state/acceptance/step3_canonical_raw'/('run_id='+run)/'pilot_evidence.json'
            marker_path=pilot_path.with_name('accepted_pointers.json')
            marker={'project':'MOEX_Bot','step':3,'status':'accepted','run_id':run,'acceptance_contract_id':stage3.CONTRACT_ID,
                'artifact_semantics':'immutable_run_scoped','accepted_pointer_count':10,'expected_pointer_count':10,
                'pilot_evidence_ref':'${MOEX_DATA_ROOT}/'+pilot_path.relative_to(root).as_posix(),'pointers':[
                    {'dataset_id':s.dataset_id,'instrument_id':s.instrument_id,
                     'pointer_ref':'${MOEX_DATA_ROOT}/state/datasets/dataset_id='+s.dataset_id+'/instrument_id='+s.instrument_id+'/current_accepted_manifest.json',
                     'manifest_ref':'${MOEX_DATA_ROOT}/'+s.manifest_path.relative_to(root).as_posix(),
                     'quality_report_ref':'${MOEX_DATA_ROOT}/'+s.quality_path.relative_to(root).as_posix()} for s in specs]}
            parent_path=root/'runs/step10_rub_daily_refresh'/('run_id='+run[:-7])/'run_manifest.json'
            parent={'project':'MOEX_Bot','stage':10,'run_id':run[:-7],'status':'succeeded','finished_at_utc':day+'T17:00:00+00:00',
                'source_refresh':{'status':'refreshed','stage3_run_id':run,'trade_date':day}}
            for path,value in ((pilot_path,pilot),(marker_path,marker),(parent_path,parent)):_write_json(path,value)
            resolved=dict(run=run,marker_path=marker_path,marker=marker,pilot_path=pilot_path,pilot=pilot,parent_path=parent_path,parent=parent,
                finished=m._stamp(parent['finished_at_utc']),binding=m._stamp(pilot['reference_observed_at_utc']),specs=specs)
            kind='CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN'
            if consumer and day=='2026-08-24':
                resolved.update(parent_path=None,parent=None,finished=NOW-timedelta(seconds=5));kind='REVALIDATED_STANDALONE_STAGE3_PILOT'
            history[day]=m._stage3_pairs(root,resolved,now=NOW,kind=kind)
        if consumer:_consumer_bounded_sources(root,history)
        frame=pd.DataFrame([{'trade_date':d,'instrument_id':engine.OBSERVED_DATE_WITNESS_INSTRUMENT_ID,'timeframe':'1D',
            'availability_ts_utc':d+'T18:00:00+00:00','build_ts_utc':NOW.isoformat()} for d in dates])
        from io import BytesIO
        stream=BytesIO();frame.to_parquet(stream,index=False)
        witness={'partition':m._freeze(root,stream.getvalue())}
        identity={'dataset_id':engine.OBSERVED_DATE_WITNESS_DATASET_ID,'instrument_id':engine.OBSERVED_DATE_WITNESS_INSTRUMENT_ID,
            'timeframe':'1D','run_id':'witness_test','quality_status':'pass'}
        for key in ('manifest','quality_report'):witness[key]=m._freeze(root,json.dumps(identity).encode())
        pointer={**identity,'acceptance_run_id':'accepted_witness_test','acceptance_contract_id':'step7_rub_native_d1_w1_technical_acceptance.v1'}
        for key,proof in witness.items():pointer[key+'_ref']=proof['ref'];pointer[key+'_sha256']=proof['sha256']
        witness['pointer']=m._freeze(root,json.dumps(pointer).encode())
        binding={};m._original_current(root,_native_body(),NOW,binding_sink=binding)
        e={'schema_version':m.SCHEMA,'accepted_at_utc':NOW.isoformat(),'causal_cutoff_at_utc':NOW.isoformat(),'contract_text':m._contract(),
            'bindings':dict(BINDINGS),'role_binding_as_of_utc':NOW.isoformat(),'binding_proof':binding,
            'observed_dates':dates,'witness_proof':witness,'history':history,'source_errors':{}}
        e['original_byte_buffers']=m._buffer_table(root,e,available=m._native_buffers(_native_body()))
        return {m.STORE_KEY:{'evidence':e,'evidence_sha256':m.common._digest(e),'last_capture_attempt_at_utc':NOW.isoformat(),
            'last_capture_error':None,'current_capture':None,'current_sha256':None,'latest_source_errors':{}}}


def pair(secid,day,price=100.0,oi=1000):
    role=next(role for role,value in BINDINGS.items() if value==secid)
    ts=day+'T19:00:00'; pub=day+'T16:00:48+00:00'; receipt=day+'T16:01:00+00:00'
    shared={'secid':secid,'trade_date':day,'ts':ts,'instrument_id':m.INSTRUMENTS[role],'ingest_ts':receipt}
    q={**shared,'close':price,'source_id':'moex_algopack_fo_tradestats_5m'}
    o={**shared,'oi_close':oi,'availability_ts_utc':pub,'source_id':'moex_algopack_fo_open_interest_5m'}
    proof={'acceptance_run_id':'step10_'+day.replace('-','')+'_stage3','revalidated_at_utc':NOW.isoformat(),
        'quote':dict(partition=REF,manifest=REF,quality=REF),'open_interest':dict(partition=REF,manifest=REF,quality=REF),
        'marker':REF,'pilot':REF,'parent':REF,'binding_observed_at_utc':day+'T06:00:00+00:00',
        'original_acceptance_digest_available':False,'hash_semantics':'computed_at_current_revalidation',
        'quote_source_row':q,'oi_source_row':o}
    return m._pair(secid,day,day+'T16:00:00+00:00',pub,receipt,price,oi,proof,source_kind='CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN')


def snapshot(missing=()):
    value=deepcopy(_source_snapshot())
    e=value[m.STORE_KEY]['evidence']
    for index in missing: e['history'].pop(e['observed_dates'][index],None)
    _prune_buffers(e)
    value[m.STORE_KEY]['evidence_sha256']=m.common._digest(e)
    return value


def _prune_buffers(e):
    needed={digest for _,digest in m._proof_references(e)}
    e['original_byte_buffers']={k:v for k,v in e['original_byte_buffers'].items() if k in needed}




def release(s,now=NOW):
    r={};m.attach_consumer(s,r,now=now);m.verify_projection(s,r,now=now);return r['contract_price_market_oi_context']


def test_exact_observed_targets_do_not_shift_missing_days():
    s=snapshot(missing=(16,)); out=release(s)
    c=out['dated']['contracts']['si_front']
    assert c['changes']['5']['target_observed_trade_date']==s[m.STORE_KEY]['evidence']['observed_dates'][-6]
    assert c['changes']['5']['values'] is None
    assert c['changes']['1']['values']['market_open_interest_change']==1
    assert c['changes']['20']['values']['market_open_interest_change']==20
    assert out['units']['price_by_root']=={'si':'RUB_per_1000_USD','cr':'RUB_per_CNY'}
    assert out['current']['status']=='UNAVAILABLE'
    assert 'proof' not in c['anchor']


@pytest.mark.parametrize('mutation',['hash','foreign_secid','source_date','quote_oi_time','source_value','source_clock','receipt','fractional_oi','authority','contract','future_acceptance','old_witness','unknown_field'])
def test_self_hashed_retained_mutations_refuse(mutation):
    s=snapshot();e=s[m.STORE_KEY]['evidence'];r=e['history'][e['observed_dates'][-1]]['SiU6']
    if mutation=='hash': s[m.STORE_KEY]['evidence_sha256']='b'*64
    elif mutation=='foreign_secid': r['secid']='SiZ6'
    elif mutation=='source_date': r['trade_date']='1900-01-01'
    elif mutation=='quote_oi_time': r['proof']['oi_source_row']['ts']='2026-09-14T18:55:00'
    elif mutation=='source_value': r['price']+=1
    elif mutation=='source_clock': r['source_timestamp_utc']='2026-09-14T15:55:00+00:00'
    elif mutation=='receipt': r['received_at_utc']='2026-09-14T16:02:00+00:00'
    elif mutation=='fractional_oi': r['market_open_interest']=1000.5
    elif mutation=='authority': r['session_completion_proven']=True
    elif mutation=='contract':
        doc=json.loads(e['contract_text']);doc['admission']['action_authority']=True;e['contract_text']=json.dumps(doc)
    elif mutation=='future_acceptance': e['accepted_at_utc']=(NOW+timedelta(seconds=1)).isoformat()
    elif mutation=='old_witness': e['observed_dates']=['1900-01-01']
    else: r['invented_signal']='BUY'
    if mutation!='hash': s[m.STORE_KEY]['evidence_sha256']=m.common._digest(e)
    assert release(s)['status']=='UNAVAILABLE'


@pytest.mark.parametrize('tamper',['omit','price','oi','target','scope','authority','extra'])
def test_projection_tampering_rejected(tamper):
    s=snapshot();r={};m.attach_consumer(s,r,now=NOW);o=r['contract_price_market_oi_context']
    if tamper=='omit': r.clear()
    elif tamper=='scope': o['scope']='ACTIONABLE'
    elif tamper=='authority': o['action_authority']=True
    elif tamper=='extra': o['invented_signal']='BUY'
    else:
        change=o['dated']['contracts']['si_front']['changes']['1']
        if tamper=='target': change['target_observed_trade_date']='2026-08-23'
        elif tamper=='price': change['values']['price_return_fraction']=1
        else: change['values']['market_open_interest_change']=99
    with pytest.raises(AssertionError):m.verify_projection(s,r,now=NOW)


def install_current(s):
    import tempfile
    with tempfile.TemporaryDirectory() as folder:
        root=Path(folder);body=_native_body()
        facts=m._original_current(root,body,NOW)
        carrier={'causal_cutoff_at_utc':NOW.isoformat(),'captured_at_utc':NOW.isoformat(),'bindings':BINDINGS,
            'facts':facts,'error':None,'original_byte_buffers':m._buffer_table(root,facts,available=m._native_buffers(body))}
    s[m.STORE_KEY].update(current_capture=carrier,current_sha256=m.common._digest(carrier))
    body.pop('original_forts_http_evidence')
    s['components']={'synchronized_live_market_oi':{'data':body}}
    return s


def test_current_has_own_native_proof_and_expires_without_expiring_dated():
    s=install_current(snapshot());out=release(s)
    assert out['current']['status']=='AVAILABLE'
    assert out['current']['contracts']['si_front']['anchor']['timestamp_semantics']=='source_row_update_time_not_last_trade_time'
    assert out['current']['contracts']['si_front']['anchor']['last_trade_time_moscow'] is None
    from moex_data.rub_snapshot_read_freshness import MAX_LIVE_AGE_SECONDS
    later=NOW+timedelta(seconds=MAX_LIVE_AGE_SECONDS+1)
    expired=release(s,now=later)
    assert expired['current']['status']=='UNAVAILABLE' and expired['dated']['status']=='AVAILABLE'
    assert expired['evidence_sha256']==out['evidence_sha256']


@pytest.mark.parametrize('mutation',['digest','row_value','row_time','receipt','binding','gate','source_version','extra'])
def test_current_original_proof_or_read_governance_mutations_refuse(mutation):
    s=install_current(snapshot());carrier=s[m.STORE_KEY]['current_capture'];r=carrier['facts']['SiU6']
    if mutation=='digest': s[m.STORE_KEY]['current_sha256']='b'*64
    elif mutation=='row_value':r['proof']['source_row']['LAST']+=1
    elif mutation=='row_time':r['proof']['source_row']['SYSTIME']='2026-09-15 12:00:00'
    elif mutation=='receipt':r['received_at_utc']=NOW.isoformat()
    elif mutation=='binding':r['proof']['original_bindings']={**BINDINGS,'si_front':'SiZ6'}
    elif mutation=='gate':s['components']['synchronized_live_market_oi']['data']['instruments']['si_front']['price_oi_usable']=False
    elif mutation=='source_version':s['components']['synchronized_live_market_oi']['data']['instruments']['si_front']['last']+=1
    else:r['last_trade_time_moscow']={'invented_fact':'BUY'}
    if mutation!='digest':s[m.STORE_KEY]['current_sha256']=m.common._digest(carrier)
    result=release(s)
    assert result['current']['status']=='UNAVAILABLE' and result['dated']['status']=='AVAILABLE'


def test_zero_oi_denominators_are_explicit_and_never_nonfinite():
    s=deepcopy(_source_snapshot(zero_oi=True))
    out=release(s)
    delta=out['dated']['contracts']['si_front']['changes']['1']['values']
    assert delta['market_open_interest_change_fraction'] is None
    assert delta['market_open_interest_fraction_reason']=='zero_baseline_market_open_interest'
    assert out['dated']['front_next_market_oi_distribution']['si']['reason']=='zero_two_contract_oi_denominator'
    json.dumps(out,allow_nan=False)


def test_shared_renderer_omission_and_arithmetic_fault_are_caught(monkeypatch):
    original=m._view
    def broken(*args):
        value=original(*args);value['contracts'].pop('si_next');return value
    monkeypatch.setattr(m,'_view',broken)
    s=snapshot();r={};m.attach_consumer(s,r,now=NOW)
    with pytest.raises(AssertionError):m.verify_projection(s,r,now=NOW)


def test_whole_shared_renderer_refusal_is_caught(monkeypatch):
    monkeypatch.setattr(m,'describe',lambda *args,**kwargs:{'schema_version':m.SCHEMA,'status':'UNAVAILABLE','scope':m.SCOPE,'reason':'invented_refusal',**m.FLAGS})
    s=snapshot();r={};m.attach_consumer(s,r,now=NOW)
    with pytest.raises(AssertionError):m.verify_projection(s,r,now=NOW)


@pytest.mark.parametrize('change',['same','diagnostic_only','source_publication','missing_anchor'])
def test_capture_retention_tracks_fact_versions_not_attempts(monkeypatch,tmp_path,change):
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    old=snapshot(missing=(16,) if change=='diagnostic_only' else ())
    e=deepcopy(old[m.STORE_KEY]['evidence']);history=deepcopy(e['history']);errors={}
    if change=='diagnostic_only':errors[e['observed_dates'][16]]='ValidationError: latest diagnostic wording'
    elif change=='source_publication':
        e=deepcopy(_source_snapshot(True)[m.STORE_KEY]['evidence']);history=deepcopy(e['history'])
    elif change=='missing_anchor':history.pop(e['observed_dates'][-1]);errors[e['observed_dates'][-1]]='invalid_latest_accepted_source'
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    monkeypatch.setattr(m,'_witness',lambda *a:(e['observed_dates'],e['witness_proof']))
    monkeypatch.setattr(m,'_historical',lambda *a:(history,errors))
    def unavailable(*args,**kwargs):
        kwargs['binding_sink'].update(e['binding_proof'])
        raise PermissionError('current original byte read denied')
    monkeypatch.setattr(m,'_original_current',unavailable)
    buffers=m._decode_buffers(e['original_byte_buffers'])
    monkeypatch.setattr(m,'_read_bytes',lambda root,ref,expected=None:buffers[expected])
    current=deepcopy(old);current['components']={'synchronized_live_market_oi':{'data':_native_body()}}
    ticks=iter((NOW+timedelta(seconds=1),NOW+timedelta(seconds=2)))
    completed=m.capture_snapshot(current,old,now_fn=lambda:next(ticks),refresh_started_at=NOW)
    if change in ('same','diagnostic_only'):
        assert current[m.STORE_KEY]['evidence']==old[m.STORE_KEY]['evidence']
        assert current[m.STORE_KEY]['evidence_sha256']==old[m.STORE_KEY]['evidence_sha256']
    else:assert current[m.STORE_KEY]['evidence_sha256']!=old[m.STORE_KEY]['evidence_sha256']
    out=release(current,now=completed)
    assert out['latest_source_errors']==errors
    assert 'current original byte read denied' in out['current']['reason']
    if change=='missing_anchor':assert out['dated']['status']=='UNAVAILABLE'


def test_capture_completion_clock_reversal_is_atomic(monkeypatch,tmp_path):
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    old=snapshot();current=deepcopy(old);e=old[m.STORE_KEY]['evidence']
    current['components']={'synchronized_live_market_oi':{'data':{'instruments':dict.fromkeys(m.ROLES,{}),'bindings':BINDINGS,'snapshot_received_at_utc':NOW.isoformat()}}}
    before=deepcopy(current)
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    monkeypatch.setattr(m,'_witness',lambda *a:(e['observed_dates'],e['witness_proof']))
    monkeypatch.setattr(m,'_historical',lambda *a:(e['history'],{}))
    monkeypatch.setattr(m,'_original_current',lambda *a,**kw:kw['binding_sink'].update(e['binding_proof']))
    ticks=iter((NOW+timedelta(seconds=1),NOW))
    with pytest.raises(ValueError,match='completion_reversed'):
        m.capture_snapshot(current,old,now_fn=lambda:next(ticks),refresh_started_at=NOW)
    assert current==before


@pytest.mark.parametrize('error',[None,True,{},123])
def test_malformed_first_capture_diagnostic_refuses_without_leaking(error):
    s={'contract_price_market_oi_capture_error':{'checked_at_utc':NOW.isoformat(),'error':error}}
    out=release(s)
    assert out['status']=='UNAVAILABLE' and out['reason']=='paired_capture_failure_shape'


def test_original_first_acceptance_expiry_is_not_renewed_by_same_history(monkeypatch,tmp_path):
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    old=snapshot();e=old[m.STORE_KEY]['evidence'];later=NOW+timedelta(days=4,seconds=1)
    current=deepcopy(old);current['components']={'synchronized_live_market_oi':{'data':_native_body()}}
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    monkeypatch.setattr(m,'_witness',lambda *a:(e['observed_dates'],e['witness_proof']))
    monkeypatch.setattr(m,'_historical',lambda *a:(e['history'],{}))
    monkeypatch.setattr(m,'_original_current',lambda *a,**kw:kw['binding_sink'].update(e['binding_proof']))
    buffers=m._decode_buffers(e['original_byte_buffers'])
    monkeypatch.setattr(m,'_read_bytes',lambda root,ref,expected=None:buffers[expected])
    ticks=iter((later,later+timedelta(seconds=1)))
    m.capture_snapshot(current,old,now_fn=lambda:next(ticks),refresh_started_at=later)
    assert current[m.STORE_KEY]['evidence_sha256']==old[m.STORE_KEY]['evidence_sha256']
    assert release(current,now=later+timedelta(seconds=1))['status']=='UNAVAILABLE'


@pytest.mark.parametrize('mutation',['joint_price_copy','root_swap','source_buffer','missing_buffer','witness_date','missing_history_contract'])
def test_frozen_original_bytes_bind_values_roles_and_exact_inventory(mutation):
    s=snapshot();e=s[m.STORE_KEY]['evidence'];day=e['observed_dates'][-1];row=e['history'][day]['SiU6']
    if mutation=='joint_price_copy':row['price']=row['proof']['quote_source_row']['close']=777.
    elif mutation=='root_swap':e['bindings']['si_front'],e['bindings']['cr_front']=e['bindings']['cr_front'],e['bindings']['si_front']
    elif mutation=='source_buffer':
        digest=row['proof']['quote']['partition']['sha256'];e['original_byte_buffers'][digest]='e30='
    elif mutation=='missing_buffer':e['original_byte_buffers'].pop(row['proof']['quote']['partition']['sha256'])
    elif mutation=='witness_date':e['observed_dates'].pop(0)
    else:e['history'][day].pop('SiZ6')
    s[m.STORE_KEY]['evidence_sha256']=m.common._digest(e)
    assert release(s)['status']=='UNAVAILABLE'


@pytest.mark.parametrize('mutation',['joint_price_copy','source_date','probe_missing','binding_copy','source_buffer'])
def test_current_original_response_membership_and_read_identity(mutation):
    s=install_current(snapshot());carrier=s[m.STORE_KEY]['current_capture'];fact=carrier['facts']['SiU6']
    if mutation=='joint_price_copy':
        fact['price']=fact['proof']['source_row']['LAST']=777.
        s['components']['synchronized_live_market_oi']['data']['instruments']['si_front']['last']=777.
    elif mutation=='source_date':s['components']['synchronized_live_market_oi']['data']['instruments']['si_front']['source_trade_date']='1900-01-01'
    elif mutation=='probe_missing':
        for r in carrier['facts'].values():r['proof']['retained_http_inventory']=[v for v in r['proof']['retained_http_inventory'] if v['role']=='selected_values']
    elif mutation=='binding_copy':fact['proof']['original_bindings']['si_front']='CRU6'
    else:carrier['original_byte_buffers'][fact['proof']['response']['sha256']]='e30='
    s[m.STORE_KEY]['current_sha256']=m.common._digest(carrier)
    out=release(s)
    assert out['dated']['status']=='AVAILABLE' and out['current']['status']=='UNAVAILABLE'


def _fast_native_market():
    from test_rub_fast_market import market
    value=market(); native=_native_body()
    value['snapshot_received_at_utc']=NOW.isoformat()
    for node in value['instruments'].values():
        node['timestamp']=(NOW-timedelta(seconds=30)).isoformat();node['received_at_utc']=(NOW-timedelta(seconds=10)).isoformat()
    for role in m.ROLES:value['instruments'][role].update(native['instruments'][role],received_at_utc=(NOW-timedelta(seconds=10)).isoformat())
    value['original_forts_http_evidence']=native['original_forts_http_evidence']
    for response in value['original_forts_http_evidence']['responses']:response['received_at_utc']=(NOW-timedelta(seconds=10)).isoformat()
    return value


@pytest.mark.parametrize('defect',[None,'missing','hash','expired','source_date'])
def test_real_fast_refresh_persist_apply_uses_only_its_own_current_bytes(tmp_path,monkeypatch,defect):
    from moex_data import rub_fast_market as fast
    old=install_current(snapshot());original=deepcopy(old)
    old.update(authority={},analysis_views={},analysis_workflow={})
    value=fast.refresh(tmp_path,loader=_fast_native_market,clock=lambda:NOW)
    folder=fast.state_path(tmp_path);(folder/'enabled').write_text(fast.SCHEMA)
    assert (folder/'current.json').stat().st_size<=fast.MAX_BYTES
    assert 'original_byte_buffers' not in json.dumps(fast.collection_summary(value))
    if defect=='missing':value.pop(m.CURRENT_KEY)
    elif defect=='hash':value[m.CURRENT_KEY]['sha256']='b'*64
    elif defect=='source_date':
        value['market']['instruments']['si_front']['source_trade_date']='1900-01-01';value['market_sha256']=fast._digest(value['market'])
    if defect in ('missing','hash','source_date'):(folder/'current.json').write_text(json.dumps(value))
    monkeypatch.setattr('moex_data.synchronized_live_market_oi_context_partial.fetch_live_snapshot',lambda **kw:pytest.fail('network on read'))
    now=NOW+timedelta(seconds=61) if defect=='expired' else NOW
    before=(folder/'current.json').read_bytes();overlaid=fast.apply(old,root=tmp_path,now=now);out=release(overlaid,now=now)
    assert overlaid[m.STORE_KEY]['evidence']==original[m.STORE_KEY]['evidence']
    assert overlaid[m.STORE_KEY]['evidence_sha256']==original[m.STORE_KEY]['evidence_sha256']
    assert (folder/'current.json').read_bytes()==before
    assert out['dated']['status']=='AVAILABLE'
    assert out['current']['status']==('AVAILABLE' if defect is None else 'UNAVAILABLE')
    if defect is None:
        assert out['current']['contracts']['si_front']['anchor']['received_at_utc']==(NOW-timedelta(seconds=10)).isoformat()


def test_fast_validation_completion_is_after_verification_and_regression_refuses(tmp_path):
    from moex_data import rub_fast_market as fast
    ticks=iter((NOW,NOW+timedelta(seconds=1),NOW+timedelta(seconds=2)))
    value=fast.refresh(tmp_path,loader=_fast_native_market,clock=lambda:next(ticks))
    assert value[m.CURRENT_KEY]['capture']['causal_cutoff_at_utc']==(NOW+timedelta(seconds=1)).isoformat()
    assert value[m.CURRENT_KEY]['capture']['captured_at_utc']==value['completed_at']==(NOW+timedelta(seconds=2)).isoformat()
    before=(fast.state_path(tmp_path)/'current.json').read_bytes()
    ticks=iter((NOW+timedelta(seconds=3),NOW+timedelta(seconds=4),NOW+timedelta(seconds=3)))
    with pytest.raises(ValueError,match='validation_clock_reversed'):
        fast.refresh(tmp_path,loader=_fast_native_market,clock=lambda:next(ticks))
    assert (fast.state_path(tmp_path)/'current.json').read_bytes()==before
    with pytest.raises(ValueError,match='before_previous_completion'):
        fast.refresh(tmp_path,loader=lambda:pytest.fail('fetch after clock reversal'),clock=lambda:NOW)
    assert (fast.state_path(tmp_path)/'current.json').read_bytes()==before


def test_fast_full_state_byte_cap_is_enforced_and_cli_does_not_dump_buffers(tmp_path):
    from moex_data import rub_fast_market as fast
    body=_fast_native_market();body['oversized_fixture']='x'*fast.MAX_BYTES
    value=fast.refresh(tmp_path,loader=lambda:body,clock=lambda:NOW)
    assert value['status']=='FAILED' and value['error_class']=='FastMarketByteLimit'
    assert (fast.state_path(tmp_path)/'current.json').stat().st_size<fast.MAX_BYTES
    assert m.CURRENT_KEY not in value


def test_consumer_fixture_distinguishes_official_and_standalone_partial_endpoints():
    s=install_current(deepcopy(_source_snapshot(consumer=True)));out=release(s)
    dated=out['dated']['contracts']['si_front']['changes'];current=out['current']['contracts']['si_front']['changes']
    assert dated['1']['target_observed_trade_date']=='2026-09-11'
    assert dated['5']['target_observed_trade_date']=='2026-09-07'
    assert dated['20']['target_observed_trade_date']=='2026-08-23'
    assert current['20']['target_observed_trade_date']=='2026-08-24'
    assert dated['20']['baseline']['source_kind']=='CURRENT_ACCEPTED_OFFICIAL_PAGINATED_TRADESTATS'
    assert dated['20']['baseline']['source_timestamp_moscow']=='2026-08-23T19:00:00+03:00'
    assert current['20']['baseline']['source_kind']=='REVALIDATED_STANDALONE_STAGE3_PILOT'
    assert current['20']['baseline']['source_timestamp_moscow']=='2026-08-24T17:05:00+03:00'
    assert not current['20']['baseline']['session_completion_proven']


def _restore_archive(tmp_path,dates=None):
    e=snapshot()[m.STORE_KEY]['evidence'];buffers=m._decode_buffers(e['original_byte_buffers'])
    for day,pairs in e['history'].items():
        if dates is not None and day not in dates:continue
        audit=next(iter(pairs.values()))['proof']['stage3_audit']
        for ref,proof in audit['artifacts'].items():
            path=tmp_path/ref.removeprefix('${MOEX_DATA_ROOT}/');path.parent.mkdir(parents=True,exist_ok=True)
            raw=buffers[proof['sha256']]
            if path.suffix=='.json':
                # Relocate fixture source paths; source documents genuinely omit run_id.
                raw=raw.decode().replace(audit['original_root'].replace('\\','/'),tmp_path.as_posix()).encode()
            path.write_bytes(raw)
    return e


def test_real_archive_inventory_derives_run_from_marker_not_optional_pilot_field(tmp_path,monkeypatch):
    e=_restore_archive(tmp_path)
    monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    records,errors=m._historical(tmp_path,NOW)
    assert set(records)==set(e['observed_dates'])
    assert all(len(pairs)==4 for pairs in records.values())
    assert not (set(errors)&set(e['observed_dates']))
    for marker in tmp_path.glob('state/acceptance/step3_canonical_raw/run_id=*/pilot_evidence.json'):
        assert 'run_id' not in json.loads(marker.read_text())
    # Within the admitted inventory, retries cannot displace other observed dates.
    from test_step3_raw_acceptance import _write_json
    day=e['observed_dates'][-1]
    for number in range(100):
        run=f'burst_{number}_stage3';folder=tmp_path/'state/acceptance/step3_canonical_raw'/('run_id='+run)
        _write_json(folder/'accepted_pointers.json',{})
        _write_json(folder/'pilot_evidence.json',{'trade_date':day})
        _write_json(tmp_path/'runs/step10_rub_daily_refresh'/('run_id='+run[:-7])/'run_manifest.json',
            {'project':'MOEX_Bot','stage':10,'run_id':run[:-7],'status':'succeeded',
             'finished_at_utc':(datetime.fromisoformat(day+'T18:00:00+00:00')+timedelta(seconds=number)).isoformat(),
             'source_refresh':{'status':'refreshed','stage3_run_id':run,'trade_date':day}})
    records,errors=m._historical(tmp_path,NOW)
    assert set(records)==set(e['observed_dates'])-{day}
    assert day in errors  # Newest invalid source never falls back to that day's older run.


def _archive_inventory(root,count):
    base=root/'state/acceptance/step3_canonical_raw'
    for number in range(count):
        folder=base/f'run_id=old_retry_{number}_stage3';folder.mkdir(parents=True)
        (folder/'accepted_pointers.json').write_text('{}')
    return base


def test_archive_inventory_boundary_256_is_checked_before_date_filter(tmp_path,monkeypatch):
    _archive_inventory(tmp_path,256);reads=[]
    def read(root,ref,expected=None):
        if ref.endswith('/pilot_evidence.json'):
            reads.append(ref);return b'{"trade_date":"2020-01-01"}'
        if ref.endswith('/'+m.SOURCE_ADMISSION):raise FileNotFoundError('fixture has no bounded admission')
        pytest.fail('outside-lookback pilot must not cause a parent read: '+ref)
    monkeypatch.setattr(m,'_read_bytes',read)
    records,_=m._historical(tmp_path,NOW)
    assert records=={} and len(reads)==256


@pytest.mark.parametrize('extra_kind',['run_directory','other_entry'])
def test_archive_inventory_257_refuses_before_any_pilot_or_parent_file_read(tmp_path,monkeypatch,extra_kind):
    base=_archive_inventory(tmp_path,256)
    if extra_kind=='run_directory':(base/'run_id=newest_retry_stage3').mkdir()
    else:(base/'unclassified-entry').write_text('all directory entries count')
    reads=[];original=Path.open
    def tracked(path,*args,**kwargs):
        if path.name in ('pilot_evidence.json','run_manifest.json'):
            reads.append(str(path));pytest.fail('content read before archive inventory refusal')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',tracked)
    monkeypatch.setattr(m,'_read_bytes',lambda *a,**kw:pytest.fail('source content read on oversized inventory'))
    with pytest.raises(ValueError,match='accepted_archive_inventory_limit_before_content_reads'):
        m._historical(tmp_path,NOW)
    assert reads==[]


def test_inventory_refusal_preserves_prior_first_acceptance_without_new_admission(tmp_path,monkeypatch):
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    _archive_inventory(tmp_path,257)
    old=snapshot();before=deepcopy(old);e=old[m.STORE_KEY]['evidence']
    current={'components':{'synchronized_live_market_oi':{'data':_native_body()}}}
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    monkeypatch.setattr(m,'_witness',lambda *a:(e['observed_dates'],e['witness_proof']))
    ticks=iter((NOW+timedelta(seconds=1),NOW+timedelta(seconds=2)))
    completed=m.capture_snapshot(current,old,now_fn=lambda:next(ticks),refresh_started_at=NOW)
    assert old==before
    assert current[m.STORE_KEY]['evidence']==e
    assert current[m.STORE_KEY]['evidence_sha256']==old[m.STORE_KEY]['evidence_sha256']
    assert 'accepted_archive_inventory_limit_before_content_reads' in current[m.STORE_KEY]['last_capture_error']
    out=release(current,now=completed)
    assert out['dated']['status']=='AVAILABLE' and out['current']['status']=='UNAVAILABLE'
    assert out['accepted_at_utc']==e['accepted_at_utc']


@pytest.mark.parametrize('kind',['pilot','parent'])
def test_archive_metadata_byte_overflow_is_explicit_not_silent_older_fallback(tmp_path,kind):
    base=_archive_inventory(tmp_path,1)
    pilot=base/'run_id=old_retry_0_stage3'/'pilot_evidence.json'
    pilot.write_text('{"trade_date":"2026-09-14"}')
    target=pilot if kind=='pilot' else tmp_path/'runs/step10_rub_daily_refresh/run_id=old_retry_0/run_manifest.json'
    target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('wb') as output:output.truncate(m.MAX_BUFFER_BYTES+1)
    with pytest.raises(ValueError,match='accepted_archive_metadata_byte_limit'):
        m._historical(tmp_path,NOW)


@pytest.mark.parametrize('witness_end,trade_day,advance,covered',[
    (None,'2026-09-14',0,True),
    (None,'2026-09-15',0,True),
    (None,'2026-09-16',1,False),
    ('2026-09-11','2026-09-14',0,False),  # Friday -> Monday is uncertainty, not a non-session assertion.
])
def test_current_original_bytes_witness_continuity_keeps_anchor_but_refuses_unproven_lags(witness_end,trade_day,advance,covered):
    from moex_data.rub_factual_package import compact_values
    s=install_current(deepcopy(_source_snapshot(witness_end=witness_end)))
    original=deepcopy(s[m.STORE_KEY]);now=NOW+timedelta(days=advance)
    body=_native_body(now,trade_day)
    s[m.CURRENT_KEY]=m.capture_current(body,started=now-timedelta(seconds=40),completed=now)
    assert s[m.CURRENT_KEY]['capture']['error'] is None
    s['fast_market_read']={'completed_at':now.isoformat(),'error':None}
    body.pop('original_forts_http_evidence')
    s['components']={'synchronized_live_market_oi':{'data':body}}
    before=deepcopy(s);out=release(s,now=now)
    assert s==before and s[m.STORE_KEY]==original
    assert out['dated']['status']=='AVAILABLE' and out['current']['status']=='AVAILABLE'
    current=out['current']
    assert current['observed_witness_continuity_status']==('AVAILABLE' if covered else 'UNAVAILABLE')
    reason=None if covered else 'insufficient_observed_witness_coverage_for_exact_current_comparisons'
    assert current['observed_witness_continuity_reason']==reason
    for contract in current['contracts'].values():
        assert contract['anchor']['trade_date']==trade_day
        for lag,change in contract['changes'].items():
            if covered:
                assert change['target_observed_trade_date'] is not None and change['values'] is not None
            else:
                assert change['target_observed_trade_date'] is None
                assert change['baseline'] is None and change['values'] is None and change['reason']==reason
    compact=compact_values({'contract_price_market_oi_context':out})
    assert compact['contract_price_market_oi_context']['current']==out['current']
    assert compact['contract_price_market_oi_context']['dated']==out['dated']
    m.verify_projection(s,json.loads(json.dumps({'contract_price_market_oi_context':out})),now=now)
    if not covered:
        tampered=deepcopy(out)
        tampered['current']['observed_witness_continuity_status']='AVAILABLE'
        with pytest.raises(AssertionError):
            m.verify_projection(s,{'contract_price_market_oi_context':tampered},now=now)
        tampered=deepcopy(out)
        tampered['current']['contracts']['si_front']['changes']['1']['target_observed_trade_date']=s[m.STORE_KEY]['evidence']['observed_dates'][-1]
        with pytest.raises(AssertionError):
            m.verify_projection(s,{'contract_price_market_oi_context':tampered},now=now)


@pytest.mark.parametrize('kind',['marker','quote_manifest','quote_quality','oi_manifest','oi_quality','oi_partition','tom_manifest','tom_quality'])
def test_real_resolver_nested_first_reads_are_bounded_and_latest_date_never_falls_back(tmp_path,monkeypatch,kind):
    e=_restore_archive(tmp_path,dates={'2026-09-13','2026-09-14'})
    monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    audit=e['history']['2026-09-14']['SiU6']['proof']['stage3_audit']
    marker=tmp_path/'state/acceptance/step3_canonical_raw/run_id=step10_20260914_stage3/accepted_pointers.json'
    pilot=json.loads(marker.with_name('pilot_evidence.json').read_text())
    if kind=='marker':target=marker
    else:
        group,field=kind.split('_')
        row=pilot[{'quote':'quote_partitions','oi':'open_interest_partitions','tom':'tom_partitions'}[group]][0]
        key=({'manifest':'manifest_reference','quality':'quality_report_reference','partition':'storage_partition_path'} if group=='quote' else
             {'manifest':'manifest_path','quality':'quality_report_path','partition':'partition_path'})[field]
        target=Path(row[key])
    with target.open('wb') as handle:handle.truncate(m.MAX_BUFFER_BYTES+1)
    original=Path.open;reads=[]
    def bounded_spy(path,*args,**kwargs):
        if path==target:reads.append(path);pytest.fail('oversized original artifact opened before bound')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',bounded_spy)
    records,errors=m._historical(tmp_path,NOW)
    assert '2026-09-13' in records and '2026-09-14' not in records
    assert 'source_artifact_byte_limit' in errors['2026-09-14'] and reads==[]


def test_resolver_bounded_reader_preserves_legacy_specs_and_freezes_same_original_buffers(tmp_path,monkeypatch):
    from moex_data import rub_accepted_stage3_resolver as resolver
    _restore_archive(tmp_path,dates={'2026-09-14'});monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    marker=tmp_path/'state/acceptance/step3_canonical_raw/run_id=step10_20260914_stage3/accepted_pointers.json'
    kwargs={'now':NOW,'earliest':NOW.date()-timedelta(days=45)}
    legacy=resolver.resolve(tmp_path,marker,**kwargs)
    reader=m._memoized_source_reader(tmp_path)
    bounded=resolver.resolve(tmp_path,marker,byte_reader=reader,**kwargs)
    assert bounded==legacy
    # A replacement after validation cannot change the bytes frozen by this capture.
    original=reader(marker);marker.write_text('{}')
    bounded['byte_reader']=reader
    pairs=m._stage3_pairs(tmp_path,bounded,now=NOW,kind='CURRENT_REVALIDATED_ACCEPTED_STAGE10_RUN')
    assert all(row['proof']['marker']['sha256']==m.sha256(original).hexdigest() for row in pairs.values())
    assert marker.read_text()=='{}'


def _restore_witness(root):
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data import step9_rub_analysis_bundle as step9
    e=snapshot()[m.STORE_KEY]['evidence'];buffers=m._decode_buffers(e['original_byte_buffers'])
    p=e['witness_proof'];pointer=json.loads(buffers[p['pointer']['sha256']])
    for key in ('partition','manifest','quality_report'):
        path=root/pointer[key+'_ref'].removeprefix('${MOEX_DATA_ROOT}/');path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(buffers[p[key]['sha256']])
    spec=engine._spec(stage=7,dataset_id=engine.OBSERVED_DATE_WITNESS_DATASET_ID,
        instrument_id=engine.OBSERVED_DATE_WITNESS_INSTRUMENT_ID,timeframe=engine.OBSERVED_DATE_WITNESS_TIMEFRAME)
    path=step9._pointer_path(root,spec);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(buffers[p['pointer']['sha256']])
    return path,pointer,e


@pytest.mark.parametrize('kind',['pointer','manifest','quality_report','partition'])
def test_witness_first_read_is_bounded_before_legacy_or_portable_parsing(tmp_path,monkeypatch,kind):
    path,pointer,_=_restore_witness(tmp_path)
    target=path if kind=='pointer' else tmp_path/pointer[kind+'_ref'].removeprefix('${MOEX_DATA_ROOT}/')
    with target.open('wb') as handle:handle.truncate(m.MAX_BUFFER_BYTES+1)
    original=Path.open
    def spy(path,*args,**kwargs):
        if path==target:pytest.fail('oversized witness opened before bound')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',spy)
    with pytest.raises(ValueError,match='source_artifact_byte_limit'):m._witness(tmp_path,NOW)


@pytest.mark.parametrize('defect',[None,'governance','quality','hash','future_availability','future_build'])
def test_bounded_witness_preserves_pointer_support_and_causal_admission(tmp_path,defect):
    import pandas as pd
    from io import BytesIO
    path,pointer,e=_restore_witness(tmp_path)
    if defect=='governance':pointer['acceptance_contract_id']='unapproved'
    elif defect=='hash':pointer['partition_sha256']='0'*64
    elif defect in ('quality','future_availability','future_build'):
        key='quality_report' if defect=='quality' else 'partition'
        target=tmp_path/pointer[key+'_ref'].removeprefix('${MOEX_DATA_ROOT}/')
        if defect=='quality':
            value=json.loads(target.read_bytes());value['quality_status']='fail';raw=json.dumps(value).encode()
        else:
            frame=pd.read_parquet(target);column='availability_ts_utc' if defect=='future_availability' else 'build_ts_utc'
            frame.loc[frame.index[-1],column]=(NOW+timedelta(seconds=1)).isoformat()
            stream=BytesIO();frame.to_parquet(stream,index=False);raw=stream.getvalue()
        target.write_bytes(raw);pointer[key+'_sha256']=m.sha256(raw).hexdigest()
    path.write_text(json.dumps(pointer))
    if defect is None:
        dates,proof=m._witness(tmp_path,NOW)
        assert dates==e['observed_dates'] and proof==e['witness_proof']
    else:
        with pytest.raises(ValueError):m._witness(tmp_path,NOW)


def test_existing_frozen_copy_comparison_is_bounded_before_read(tmp_path,monkeypatch):
    raw=b'original';proof=m._freeze(tmp_path,raw);target=tmp_path/proof['ref'].removeprefix('${MOEX_DATA_ROOT}/')
    with target.open('wb') as handle:handle.truncate(m.MAX_BUFFER_BYTES+1)
    original=Path.open
    def spy(path,*args,**kwargs):
        if path==target:pytest.fail('oversized existing immutable copy opened')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',spy)
    with pytest.raises(ValueError,match='source_artifact_byte_limit'):m._freeze(tmp_path,raw)



def _invalid_json_member(raw,defect):
    prefix=b'"parser_probe":0,"parser_probe":1,' if defect=='duplicate' else b'"parser_probe":NaN,'
    assert raw.lstrip().startswith(b'{')
    return b'{'+prefix+raw.lstrip()[1:]


@pytest.mark.parametrize('kind',['marker','pilot','parent'])
@pytest.mark.parametrize('defect',['duplicate','nonfinite'])
def test_bounded_resolver_preserves_legacy_strict_json_hooks(tmp_path,monkeypatch,kind,defect):
    from moex_data import rub_accepted_stage3_resolver as resolver
    _restore_archive(tmp_path,dates={'2026-09-14'});monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    marker=tmp_path/'state/acceptance/step3_canonical_raw/run_id=step10_20260914_stage3/accepted_pointers.json'
    path={'marker':marker,'pilot':marker.with_name('pilot_evidence.json'),
        'parent':tmp_path/'runs/step10_rub_daily_refresh/run_id=step10_20260914/run_manifest.json'}[kind]
    path.write_bytes(_invalid_json_member(path.read_bytes(),defect))
    for reader in (None,m._memoized_source_reader(tmp_path)):
        with pytest.raises(ValueError,match='duplicate JSON|must be finite'):
            resolver.resolve(tmp_path,marker,now=NOW,earliest=NOW.date()-timedelta(days=45),byte_reader=reader)


@pytest.mark.parametrize('kind',['pointer','manifest','quality_report'])
@pytest.mark.parametrize('defect',['duplicate','nonfinite'])
def test_bounded_witness_strict_source_json(tmp_path,kind,defect):
    path,pointer,_=_restore_witness(tmp_path)
    target=path if kind=='pointer' else tmp_path/pointer[kind+'_ref'].removeprefix('${MOEX_DATA_ROOT}/')
    raw=_invalid_json_member(target.read_bytes(),defect)
    if kind=='pointer':target.write_bytes(raw)
    else:
        # Manifest and quality may share original bytes; mutate only the selected proof.
        proof=m._freeze(tmp_path,raw)
        pointer[kind+'_ref']=proof['ref'];pointer[kind+'_sha256']=proof['sha256']
        path.write_text(json.dumps(pointer))
    with pytest.raises(ValueError,match='duplicate JSON|must be finite'):m._witness(tmp_path,NOW)


def _replace_audit_buffer(e,old_digest,raw):
    import base64
    new=m._frozen_ref(raw)
    def replace(value):
        if isinstance(value,dict):
            if set(value)=={'ref','sha256'} and value['sha256']==old_digest:
                value.clear();value.update(new)
            else:
                for key,child in value.items():
                    if key!='original_byte_buffers':replace(child)
        elif isinstance(value,list):
            for child in value:replace(child)
    replace(e);e['original_byte_buffers'].pop(old_digest)
    e['original_byte_buffers'][new['sha256']]=base64.b64encode(raw).decode()


@pytest.mark.parametrize('kind',['marker','pilot','parent','witness_pointer'])
@pytest.mark.parametrize('defect',['duplicate','nonfinite'])
def test_self_hashed_portable_metadata_cannot_weaken_source_json_policy(kind,defect):
    s=snapshot();e=s[m.STORE_KEY]['evidence'];row=e['history'][e['observed_dates'][-1]]['SiU6']
    proof=e['witness_proof']['pointer'] if kind=='witness_pointer' else row['proof'][kind]
    digest=proof['sha256'];raw=m._decode_buffers(e['original_byte_buffers'])[digest]
    _replace_audit_buffer(e,digest,_invalid_json_member(raw,defect))
    s[m.STORE_KEY]['evidence_sha256']=m.common._digest(e)
    out=release(s)
    assert out['status']=='UNAVAILABLE' and ('duplicate JSON' in out['reason'] or 'must be finite' in out['reason'])


@pytest.mark.parametrize('defect',['encoded_length','aggregate','duplicate_digest_different_bytes'])
def test_current_inventory_preflight_refuses_before_any_decode_or_json(monkeypatch,defect):
    import base64
    body=_native_body();responses=body['original_forts_http_evidence']['responses'];size=len(base64.b64decode(responses[0]['content_base64']))
    if defect=='encoded_length':responses[-1]['content_base64']='A'*(4*((m.MAX_BUFFER_BYTES+2)//3)+4)
    elif defect=='aggregate':monkeypatch.setattr(m,'MAX_AUDIT_BYTES',size*2-1)
    else:responses[-1]['content_base64']='e30='
    monkeypatch.setattr(m.base64,'b64decode',lambda *a,**kw:pytest.fail('decoder reached before inventory refusal'))
    monkeypatch.setattr(m,'_source_json',lambda *a,**kw:pytest.fail('JSON parser reached before inventory refusal'))
    with pytest.raises(ValueError,match='byte_limit|bytes_mismatch'):
        m._original_current(None,body,NOW)


def test_current_inventory_exact_aggregate_boundary_decodes_duplicate_once(monkeypatch):
    import base64
    body=_native_body();encoded=body['original_forts_http_evidence']['responses'][0]['content_base64'];raw=base64.b64decode(encoded)
    monkeypatch.setattr(m,'MAX_BUFFER_BYTES',len(raw));monkeypatch.setattr(m,'MAX_AUDIT_BYTES',len(raw)*2)
    original=base64.b64decode;calls=[]
    def tracked(*args,**kwargs):calls.append(1);return original(*args,**kwargs)
    monkeypatch.setattr(m.base64,'b64decode',tracked)
    facts=m._original_current(None,body,NOW)
    assert set(facts)==set(BINDINGS.values()) and len(calls)==1


@pytest.mark.parametrize('defect',['first_encoded','last_encoded','aggregate'])
def test_retained_audit_table_preflights_all_entries_before_first_decode(monkeypatch,defect):
    import base64
    raws=[b'first',b'second'];table={m.sha256(raw).hexdigest():base64.b64encode(raw).decode() for raw in raws}
    if defect=='aggregate':monkeypatch.setattr(m,'MAX_AUDIT_BYTES',sum(map(len,raws))-1)
    else:
        key=list(table)[0 if defect=='first_encoded' else -1];table[key]='A'*(4*((m.MAX_BUFFER_BYTES+2)//3)+4)
    monkeypatch.setattr(m.base64,'b64decode',lambda *a,**kw:pytest.fail('decoder reached before table refusal'))
    with pytest.raises(ValueError,match='byte_limit'):m._decode_buffers(table)


def test_retained_audit_exact_size_boundary_and_bad_encoding_still_checked(monkeypatch):
    import base64
    raw=b'12345';table={m.sha256(raw).hexdigest():base64.b64encode(raw).decode()}
    monkeypatch.setattr(m,'MAX_BUFFER_BYTES',5);monkeypatch.setattr(m,'MAX_AUDIT_BYTES',5)
    assert m._decode_buffers(table)=={m.sha256(raw).hexdigest():raw}
    table[m.sha256(raw).hexdigest()]='!!!!!==='  # Small invalid alphabet still reaches strict decoder and is refused.
    with pytest.raises(ValueError):m._decode_buffers(table)


@pytest.mark.parametrize('kind',['pilot','parent'])
def test_strict_prefilter_error_cannot_skip_newest_run_and_select_older_source(tmp_path,monkeypatch,kind):
    _restore_archive(tmp_path,dates={'2026-09-13','2026-09-14'});monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    path=(tmp_path/'state/acceptance/step3_canonical_raw/run_id=step10_20260914_stage3/pilot_evidence.json' if kind=='pilot' else
        tmp_path/'runs/step10_rub_daily_refresh/run_id=step10_20260914/run_manifest.json')
    path.write_bytes(_invalid_json_member(path.read_bytes(),'duplicate'))
    with pytest.raises(ValueError,match='accepted_archive_metadata_strict_json_refusal'):m._historical(tmp_path,NOW)


@pytest.mark.parametrize('encoding',['utf16','utf8_bom','array','scalar'])
def test_bounded_resolver_preserves_legacy_utf8_and_object_requirement(tmp_path,monkeypatch,encoding):
    from moex_data import rub_accepted_stage3_resolver as resolver
    _restore_archive(tmp_path,dates={'2026-09-14'});monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    marker=tmp_path/'state/acceptance/step3_canonical_raw/run_id=step10_20260914_stage3/accepted_pointers.json'
    original=marker.read_bytes()
    malformed={'utf16':original.decode().encode('utf-16'),'utf8_bom':b'\xef\xbb\xbf'+original,'array':b'[]','scalar':b'1'}[encoding]
    marker.write_bytes(malformed)
    for reader in (None,m._memoized_source_reader(tmp_path)):
        with pytest.raises(ValueError):
            resolver.resolve(tmp_path,marker,now=NOW,earliest=NOW.date()-timedelta(days=45),byte_reader=reader)


@pytest.mark.parametrize('raw',[b'[]',b'1',b'\xef\xbb\xbf{}','{}'.encode('utf-16')])
def test_witness_source_objects_preserve_utf8_and_mapping_contract(tmp_path,raw):
    pointer,_,_=_restore_witness(tmp_path);pointer.write_bytes(raw)
    with pytest.raises(ValueError):m._witness(tmp_path,NOW)



@pytest.mark.parametrize('defect',[None,'failed','rollback','project','run','source_status','source_run','source_date','missing_finish','missing_parent','future_finish'])
def test_newest_parent_attempt_is_decisive_before_admission_filter(tmp_path,monkeypatch,defect):
    import shutil
    _restore_archive(tmp_path,dates={'2026-09-13','2026-09-14'});monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    old='step10_20260914';new=old+'_retry'
    for prefix in ('runs/step3_canonical_raw','state/acceptance/step3_canonical_raw'):
        origin=tmp_path/prefix/('run_id='+old+'_stage3');target=tmp_path/prefix/('run_id='+new+'_stage3')
        shutil.copytree(origin,target)
        for path in target.rglob('*.json'):path.write_text(path.read_text().replace(old,new))
    origin=tmp_path/'runs/step10_rub_daily_refresh'/('run_id='+old)/'run_manifest.json'
    parent=json.loads(origin.read_text().replace(old,new));parent['finished_at_utc']='2026-09-14T17:01:00+00:00'
    if defect=='failed':parent['status']='failed'
    elif defect=='rollback':parent['current_pointer_rollback_status']='rolled_back'
    elif defect=='project':parent['project']='other'
    elif defect=='run':parent['run_id']='other'
    elif defect=='source_status':parent['source_refresh']['status']='failed'
    elif defect=='source_run':parent['source_refresh']['stage3_run_id']='other'
    elif defect=='source_date':parent['source_refresh']['trade_date']='2026-09-13'
    elif defect=='missing_finish':parent.pop('finished_at_utc')
    elif defect=='future_finish':parent['finished_at_utc']=(NOW+timedelta(seconds=1)).isoformat()
    path=tmp_path/'runs/step10_rub_daily_refresh'/('run_id='+new)/'run_manifest.json'
    path.parent.mkdir(parents=True)
    if defect!='missing_parent':path.write_text(json.dumps(parent))
    records,errors=m._historical(tmp_path,NOW)
    assert '2026-09-13' in records
    if defect is None:
        assert records['2026-09-14']['SiU6']['proof']['acceptance_run_id']==new+'_stage3'
    else:
        assert '2026-09-14' not in records and errors['2026-09-14']
        if defect=='future_finish':assert 'future_binding_or_parent_completion' in errors['2026-09-14']


@pytest.mark.parametrize('previous_available',[False,True])
@pytest.mark.parametrize('stage',['candidate_buffers','current_buffers','native_inventory'])
def test_incomplete_capture_preserves_original_error_and_never_enters_admission(tmp_path,monkeypatch,previous_available,stage):
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    source_snapshot=snapshot();e=source_snapshot[m.STORE_KEY]['evidence'];previous=source_snapshot if previous_available else {}
    current={'components':{'synchronized_live_market_oi':{'data':_native_body()}}}
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    monkeypatch.setattr(m,'_witness',lambda *a:(e['observed_dates'],e['witness_proof']))
    monkeypatch.setattr(m,'_historical',lambda *a:(e['history'],{}))
    buffers=m._decode_buffers(e['original_byte_buffers'])
    monkeypatch.setattr(m,'_read_bytes',lambda root,ref,expected=None:buffers[expected])
    original_table=m._buffer_table;original_native=m._native_buffers;calls=[]
    def table(root,value,**kwargs):
        if (stage=='candidate_buffers' and 'history' in value) or (stage=='current_buffers' and 'SiU6' in value):
            raise PermissionError('exact '+stage+' source read refused')
        return original_table(root,value,**kwargs)
    native_calls=[]
    def native(body):
        native_calls.append(1)
        if stage=='native_inventory' and len(native_calls)==2:raise ValueError('exact native_inventory exceeded')
        return original_native(body)
    monkeypatch.setattr(m,'_buffer_table',table);monkeypatch.setattr(m,'_native_buffers',native)
    original_admit=m._admit
    def admit(*args,**kwargs):calls.append(1);return original_admit(*args,**kwargs)
    monkeypatch.setattr(m,'_admit',admit)
    ticks=iter((NOW+timedelta(seconds=1),NOW+timedelta(seconds=2)))
    completed=m.capture_snapshot(current,previous,now_fn=lambda:next(ticks),refresh_started_at=NOW)
    assert calls==[]
    message=current['contract_price_market_oi_capture_error']['error']
    assert 'exact '+stage in message and 'paired_evidence_shape' not in message
    if previous_available:
        assert current[m.STORE_KEY]['evidence']==e
        assert current[m.STORE_KEY]['evidence_sha256']==previous[m.STORE_KEY]['evidence_sha256']
        assert current[m.STORE_KEY]['last_capture_error']==message
    else:assert m.STORE_KEY not in current
    monkeypatch.setattr(m,'_admit',original_admit)
    out=release(current,now=completed)
    assert message==(out['last_capture_error'] if previous_available else out['reason'])


def _replace_native_payload(response,payload):
    import base64
    raw=json.dumps(payload).encode();response['sha256']=m.sha256(raw).hexdigest();response['content_base64']=base64.b64encode(raw).decode()


@pytest.mark.parametrize('defect',['probe_cursor','extra_security','extra_market'])
def test_native_full_response_reuses_source_universe_and_probe_mode_guards(defect):
    import base64
    body=_native_body()
    for response in body['original_forts_http_evidence']['responses']:
        payload=json.loads(base64.b64decode(response['content_base64']))
        if defect=='probe_cursor' and response['role']=='completeness_probe':
            payload['securities.cursor']={'columns':['INDEX','TOTAL','PAGESIZE'],'data':[[0,4,4]]}
        elif defect in ('extra_security','extra_market'):
            block=payload['securities' if defect=='extra_security' else 'marketdata'];row=list(block['data'][0]);row[block['columns'].index('SECID')]='OTHER';block['data'].append(row)
        _replace_native_payload(response,payload)
    with pytest.raises(ValueError) as failure:m._original_current(None,body,NOW)
    if defect!='probe_cursor':
        from moex_data import synchronized_live_market_oi_context as source
        assert isinstance(failure.value.__cause__,source.SynchronizedLiveMarketOIError)
        assert str(failure.value)==str(failure.value.__cause__)


@pytest.mark.parametrize('field',['LAST','OPENPOSITION','LASTTRADEDATE','STEPPRICE','secid_case'])
def test_source_probe_value_changes_do_not_invent_full_row_equality_policy(field):
    import base64
    body=_native_body();probe=body['original_forts_http_evidence']['responses'][1]
    payload=json.loads(base64.b64decode(probe['content_base64']))
    if field=='secid_case':
        for name in ('securities','marketdata'):
            for row in payload[name]['data']:row[payload[name]['columns'].index('SECID')]=row[payload[name]['columns'].index('SECID')].lower()
    else:
        block=payload['marketdata' if field in ('LAST','OPENPOSITION') else 'securities'];index=block['columns'].index(field)
        block['data'][0][index]='2026-09-18' if field=='LASTTRADEDATE' else block['data'][0][index]+1
    _replace_native_payload(probe,payload)
    facts=m._original_current(None,body,NOW)
    assert len(facts)==4 and facts['SiU6']['price']==body['instruments']['si_front']['last']


@pytest.mark.parametrize('defect',['cursor','universe'])
def test_portable_probe_cannot_change_source_completeness_after_capture(defect):
    import base64
    s=install_current(snapshot());store=s[m.STORE_KEY];carrier=store['current_capture'];facts=carrier['facts']
    inventory=facts['SiU6']['proof']['retained_http_inventory'];probe=inventory[1]
    raw=base64.b64decode(carrier['original_byte_buffers'][probe['response']['sha256']]);payload=json.loads(raw)
    if defect=='cursor':payload['securities.cursor']={'columns':['INDEX','TOTAL','PAGESIZE'],'data':[[0,4,4]]}
    else:
        row=list(payload['securities']['data'][0]);row[payload['securities']['columns'].index('SECID')]='OTHER'
        payload['securities']['data'].append(row)
    raw=json.dumps(payload).encode();new=m._inline_ref(raw);probe['response']=new
    carrier['original_byte_buffers'][new['sha256']]=base64.b64encode(raw).decode()
    store['current_sha256']=m.common._digest(carrier)
    out=release(s)
    assert out['dated']['status']=='AVAILABLE' and out['current']['status']=='UNAVAILABLE'
    expected='current_probe_pagination_changed' if defect=='cursor' else 'SECID universe mismatch'
    assert expected in out['current']['reason']


@pytest.mark.parametrize('defect',[None,'columns','request_start'])
def test_native_cursor_inventory_preserves_source_column_and_request_progress(defect):
    import base64
    payload=json.loads(base64.b64decode(_native_body()['original_forts_http_evidence']['responses'][0]['content_base64']))
    inventory=[]
    for index in range(2):
        page=deepcopy(payload)
        for block in ('securities','marketdata'):page[block]['data']=page[block]['data'][index*2:index*2+2]
        page['securities.cursor']={'columns':['INDEX','TOTAL','PAGESIZE'],'data':[[index*2,4,2]]}
        params={} if index==0 else {'start':2}
        if index==1 and defect=='columns':
            page['marketdata']['columns'].reverse()
            for row in page['marketdata']['data']:row.reverse()
        if index==1 and defect=='request_start':params['start']=3
        inventory.append(({'role':'selected_values','params':params},page))
    if defect is None:m._validate_native_inventory(inventory)
    else:
        with pytest.raises(ValueError,match='current_cursor_'):m._validate_native_inventory(inventory)



def test_cursor_case_duplicate_cardinality_matches_original_source_merge():
    import base64
    from moex_data import synchronized_live_market_oi_context as source
    payload=json.loads(base64.b64decode(_native_body()['original_forts_http_evidence']['responses'][0]['content_base64']))
    # Keep all four required contracts and add only a casing alias of an existing one.
    for name in ('securities','marketdata'):
        row=list(payload[name]['data'][0]);row[payload[name]['columns'].index('SECID')]='siu6'
        payload[name]['data'].append(row)
    inventory=[];aggregate={}
    for index in range(2):
        page=deepcopy(payload)
        for name in ('securities','marketdata'):
            page[name]['data']=page[name]['data'][index*3:index*3+3]
            source._merge_iss_block_by_secid(aggregate,page,name)
        page['securities.cursor']={'columns':['INDEX','TOTAL','PAGESIZE'],'data':[[index*3,5,3]]}
        inventory.append(({'role':'selected_values','params':{} if index==0 else {'start':3}},page))
    # The original acquisition rejects this mismatch after its normalized merge.
    assert source._aggregate_row_count(aggregate,'securities')==4
    assert sum(len(page['securities']['data']) for _,page in inventory)==5
    with pytest.raises(ValueError,match='current_cursor_incomplete'):m._validate_native_inventory(inventory)
