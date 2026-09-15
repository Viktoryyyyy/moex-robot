"""Synthetic immutable Stage4 buffers and independent C1 admission regressions."""
from copy import deepcopy
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from pathlib import Path
import json
import pytest
import pandas as pd
from moex_data import rub_historical_basis_carry_context as m
from moex_data import rub_accepted_stage4_resolver as resolver
from moex_data import step4_basis_carry_acceptance as acceptance
from moex_data.analytics import materialize_rub_basis_carry_5m as materializer
from moex_data.analytics import validate_rub_basis_carry_partition as physical

NOW=datetime(2026,9,15,12,tzinfo=timezone.utc)


def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value),encoding='utf-8')


def make_run(root,day,*,suffix='000001',finished_hour=22):
    run='step10_daily_'+day.replace('-','')+'_'+suffix+'_stage4';rr=root/resolver.RUNS/('run_id='+run)
    clock=day+'T21:00:00+00:00';receipt=day+'T21:01:00+00:00';built=day+'T21:02:00+00:00'
    flags={'alignment_policy':'exact_timestamp_inner_join','timestamp_policy':'naive_exchange_localize_europe_moscow_then_utc',
        'forward_fill_used':False,'asof_join_used':False,'continuous_series_used':False}
    pilot={'project':'MOEX_Bot','step':4,'status':'pilot_passed','trade_date':day,'artifact_version':run,
        'materialization_root':rr.as_posix(),'run_artifacts_immutable':True,'run_id_reuse_allowed':False,
        'front_next_minimum_days_to_expiry':1,'bindings':[],'quote_partitions':[],'tom_partitions':[],
        'reference_observed_at_utc':clock,'reference_source_url':'https://iss.moex.com/iss/engines/futures/markets/forts/securities.json',
        'counts':{'bindings':4,'perpetual_quote_partitions':2,'front_next_quote_partitions':4,'tom_partitions':2,'derived_partitions':2},
        'latest_autodetect_used':False,'derived_partitions':[],**flags}
    marker={'project':'MOEX_Bot','step':4,'status':'accepted','run_id':run,'acceptance_contract_id':acceptance.CONTRACT_ID,
        'accepted_pointer_count':2,'expected_pointer_count':2,'promotion_semantics':'transactional_with_rollback',
        'physical_partition_readback_required':True,'continuous_series_used':False,'pointers':[]}
    for instrument,(pair,pair_id,family,prefix,perpetual,spot) in resolver.PAIRS.items():
        bindings={}
        for role,letter,expiry in (('front','U','2026-09-17'),('next','Z','2026-12-17')):
            b={'root':family,'role':role,'instrument_id':prefix+'_'+role+'_contract','secid':family+letter+'6',
                'last_trade_date':expiry,'minimum_days_to_expiry':'1','as_of_date':day,'availability_ts_utc':clock,
                'mapping_fixed_ts_utc':clock,'source_id':'moex_iss_forts_securities_reference'}
            pilot['bindings'].append(b);bindings[role]=b
        ids={'spot':'usd_tom' if family=='Si' else 'cny_tom','perpetual':'usdrubf_futures_family' if family=='Si' else 'cnyrubf_futures_family',
            'front':prefix+'_front_contract','next':prefix+'_next_contract'}
        secids={'spot':spot,'perpetual':perpetual,**{k:v['secid'] for k,v in bindings.items()}}
        frames={};lineage={'binding_reference_observed_at_utc':clock,'binding_reference_url':pilot['reference_source_url']}
        for j,role in enumerate(resolver.ROLES):
            ident=ids[role];producer=run+'_'+ident+'_quote';folder=rr/ident;folder.mkdir(parents=True,exist_ok=True)
            partition=folder/'part.parquet';manifest=folder/'manifest.json';quality=folder/'quality.json'
            divisor=1000 if family=='Si' and role in ('front','next') else 1
            timestamps=[day+'T10:00:00+00:00',day+'T10:05:00+00:00']+([] if role=='spot' else [day+'T20:50:00+00:00'])
            pricebase=(80 if family=='Si' else 12)+j*.2+int(day[-2:])*.01
            sid='moex_iss_cets_tom_1m' if role=='spot' else 'moex_algopack_fo_tradestats_5m'
            frame=pd.DataFrame({'instrument_id':ident,'trade_date':day,'ts':timestamps,'secid':secids[role],'source_id':sid,
                'close':[(pricebase+i*.01)*divisor for i in range(len(timestamps))],'ingest_ts':receipt})
            frame.to_parquet(partition,index=False);frames[role]=frame
            identity={'dataset_id':'fx_spot_raw_5m' if role=='spot' else 'futures_raw_5m','run_id':producer,'instrument_id':ident,
                'source_id':sid,'secid':secids[role],'trade_date':day}
            row={**identity,'quality_status':'pass','rows':len(frame),'partition_path':partition.as_posix()}
            if role=='spot':
                support={**identity,'status':'succeeded','row_count':len(frame),'partition_path':partition.as_posix(),'quality_report_path':quality.as_posix()}
                q=row
            else:
                support={'run_id':producer,'refresh_status':'succeeded','instrument_scope':[ident],'source_scope':[sid],
                    'partitions_written':[partition.as_posix()],'quality_report_ref':quality.as_posix(),
                    'source_contract':{k:identity[k] for k in ('instrument_id','source_id','secid','trade_date')}}
                q={'run_id':producer,'rows':[row]}
            write_json(manifest,support);write_json(quality,q)
            entry={'dataset_id':identity['dataset_id'],'instrument_id':ident,'partition_path':partition.as_posix(),
                'manifest_path':manifest.as_posix(),'quality_report_path':quality.as_posix(),'quality_status':'pass','row_count':len(frame)}
            lineage[role]=entry
            pilot['tom_partitions' if role=='spot' else 'quote_partitions'].append(deepcopy(entry))
        derived=materializer.build_basis_carry_frame(instrument_id=instrument,trade_date=day,spot_frame=frames['spot'],
            perpetual_frame=frames['perpetual'],front_frame=frames['front'],next_frame=frames['next'],
            front_binding=bindings['front'],next_binding=bindings['next'],build_ts=built)
        folder=rr/instrument;folder.mkdir();partition=folder/'part.parquet';manifest=folder/'manifest.json';quality=folder/'quality.json'
        derived.to_parquet(partition,index=False);producer=run+'_'+instrument
        support={'dataset_id':'rub_basis_carry_5m','instrument_id':instrument,'pair_id':pair_id,'trade_date':day,'run_id':producer,
            'row_count':len(derived),'quality_status':'pass','refresh_status':'succeeded','partition_path':partition.as_posix(),
            'quality_report_path':quality.as_posix(),'input_lineage':lineage,**flags}
        write_json(manifest,support);write_json(quality,{**support,'exact_timestamp_inner_join':True,'duplicate_ts_count':0,
            'monotonic_ts':True,'positive_rate_check':True,'non_null_derived_metrics':True})
        pilot['derived_partitions'].append({'dataset_id':'rub_basis_carry_5m','instrument_id':instrument,'trade_date':day,
            'run_id':producer,'row_count':len(derived),'quality_status':'pass','partition_path':partition.as_posix(),
            'manifest_path':manifest.as_posix(),'quality_report_path':quality.as_posix(),**flags})
        pointer=root/'state/datasets/dataset_id=rub_basis_carry_5m'/('instrument_id='+instrument)/'current_accepted_manifest.json'
        marker['pointers'].append({'instrument_id':instrument,'run_id':producer,'acceptance_run_id':run,
            'pointer_path':pointer.as_posix(),'pointer_ref':'${MOEX_DATA_ROOT}/'+pointer.relative_to(root).as_posix(),
            'physical_readback':physical.validate_partition(partition,expected_instrument_id=instrument,expected_trade_date=day,expected_row_count=len(derived))})
    folder=root/resolver.ARCHIVE/('run_id='+run);write_json(folder/'pilot_evidence.json',pilot);write_json(folder/'accepted_pointers.json',marker)
    parent={'project':'MOEX_Bot','stage':10,'run_id':run[:-7],'status':'succeeded','started_at_utc':day+'T20:55:00+00:00',
        'finished_at_utc':day+f'T{finished_hour:02d}:00:00+00:00',
        'source_refresh':{'status':'refreshed','stage4_run_id':run,'stage3_run_id':run[:-7]+'_stage3','trade_date':day,'stage3_pointer_count':10,'stage4_pointer_count':2}}
    write_json(root/'runs/step10_rub_daily_refresh'/('run_id='+run[:-7])/'run_manifest.json',parent)
    return run


def fixture(root,*,weekend_observations=False):
    from test_contract_price_market_oi_observed import _restore_witness
    pointer_path,pointer,_=_restore_witness(root)
    if not weekend_observations:
        partition=root/pointer['partition_ref'].removeprefix('${MOEX_DATA_ROOT}/')
        frame=pd.read_parquet(partition)
        frame=frame[~frame.trade_date.astype(str).isin(['2026-09-12','2026-09-13'])]
        frame.to_parquet(partition,index=False)
        pointer['partition_sha256']=m.bytesource.sha256(partition.read_bytes()).hexdigest()
        write_json(pointer_path,pointer)
    for day in ('2026-09-07','2026-09-08','2026-09-11','2026-09-14'):make_run(root,day)
    e=m._capture(root,NOW);e['accepted_at_utc']=NOW.isoformat()
    return {m.STORE_KEY:{'evidence':e,'evidence_sha256':m.digest(e),'last_capture_attempt_at_utc':NOW.isoformat(),'last_capture_error':None}}


def read(s,now=NOW):
    out={};m.attach_consumer(s,out,now=now);m.verify_projection(s,out,now=now);return out[m.OUTPUT_KEY]


def test_stage4_same_bytes_replay_own_metric_endpoints_and_decimal(tmp_path):
    run=make_run(tmp_path,'2026-09-14');source=resolver.Source(tmp_path)
    result=resolver.validate_run(source,run,now=NOW)
    replay=resolver.validate_run(resolver.Source(proof=deepcopy(source.proof),buffers=source.buffers),run,now=NOW)
    assert result==replay
    usd=result['usd_rub']['metrics'];cny=result['cny_rub']['metrics']
    assert usd['front_spot_basis_abs']['status']=='UNAVAILABLE'
    assert usd['front_perpetual_basis_abs']['source_timestamp_utc']=='2026-09-14T20:50:00+00:00'
    assert cny['front_spot_basis_abs']['source_timestamp_utc']=='2026-09-14T10:05:00+00:00'
    assert cny['front_next_spread_abs']['own_pair_observation_count']==3
    for pair in result.values():
        for name,item in pair['metrics'].items():
            if item['status']=='AVAILABLE':assert item['value']==m._metric_value(item,name)


def test_c1_exact_horizons_previous_and_current_debt_preserve_input(tmp_path):
    s=fixture(tmp_path);before=deepcopy(s);out=read(s)
    assert out['status']=='PARTIAL' and out['current']=={'status':'UNAVAILABLE','reason':'native_current_proof_not_admitted'}
    metric=out['dated']['pairs']['cny_rub']['metrics']['front_next_spread_abs']
    assert metric['changes']['1']['target_observed_trade_date']=='2026-09-11'
    assert metric['changes']['5']['target_observed_trade_date']=='2026-09-07'
    assert metric['previous_comparable']['trade_date']=='2026-09-11' and metric['previous_comparable']['is_exact_previous_observation']
    assert s==before
    assert read(s,NOW+timedelta(hours=96))['dated']['status']=='AVAILABLE'
    assert read(s,NOW+timedelta(hours=96,seconds=1))['status']=='UNAVAILABLE'


@pytest.mark.parametrize('mutation',['price','expiry','omission','extra_metric','old_run','marker_ref'])
def test_rehashed_copies_cannot_replace_original_source_selection_or_facts(tmp_path,mutation):
    s=fixture(tmp_path);e=s[m.STORE_KEY]['evidence'];day='2026-09-14';metric=e['history'][day]['cny_rub']['metrics']['front_next_spread_abs']
    if mutation=='price':metric['value']=999
    elif mutation=='expiry':metric['comparison_leg']['expiry_date']='2030-01-01'
    elif mutation=='omission':e['history'].pop(day);e['runs'].pop(day);e['source_errors'][day]='accepted_Stage4_run_for_exact_observed_date_unavailable'
    elif mutation=='extra_metric':e['history'][day]['cny_rub']['metrics']['invented']={'value':1}
    elif mutation=='old_run':e['runs'][day]=e['runs']['2026-09-11']
    else:
        key=resolver.ARCHIVE+'/run_id='+e['runs'][day]+'/accepted_pointers.json';proof=e['original_artifacts'][key]
        raw=m.bytesource.base64.b64decode(e['original_byte_buffers'][proof['sha256']]);marker=json.loads(raw)
        marker['pointers'][0]['manifest_ref']='invented_redirect'
        raw=json.dumps(marker).encode();sha=m.bytesource.sha256(raw).hexdigest();e['original_byte_buffers'].pop(proof['sha256']);proof['sha256']=sha;e['original_byte_buffers'][sha]=m.bytesource.base64.b64encode(raw).decode()
    s[m.STORE_KEY]['evidence_sha256']=m.digest(e)
    assert read(s)['status']=='UNAVAILABLE'


@pytest.mark.parametrize('defect',['failed','future','rollback'])
def test_newer_parent_refuses_date_without_old_success_fallback(tmp_path,defect):
    old=make_run(tmp_path,'2026-09-14');new=make_run(tmp_path,'2026-09-14',suffix='000002',finished_hour=23)
    path=tmp_path/'runs/step10_rub_daily_refresh'/('run_id='+new[:-7])/'run_manifest.json';parent=json.loads(path.read_text())
    if defect=='failed':parent['status']='failed'
    elif defect=='rollback':parent['current_pointer_rollback_status']='rolled_back'
    else:parent['finished_at_utc']=(NOW+timedelta(days=1)).isoformat()
    write_json(path,parent)
    source=resolver.Source(tmp_path);history,runs,errors,discovery=m._scan(tmp_path,source,['2026-09-14'],NOW)
    assert history==runs=={} and errors['2026-09-14']
    actual=m._scan(None,resolver.Source(proof=source.proof,buffers=source.buffers),['2026-09-14'],NOW,discovery=discovery)
    assert actual[:3]==(history,runs,errors)


def test_archive_overflow_precedes_every_content_read(tmp_path,monkeypatch):
    for index in range(257):(tmp_path/resolver.ARCHIVE/('run_id='+str(index))).mkdir(parents=True)
    monkeypatch.setattr(resolver.Source,'read',lambda *a:pytest.fail('content read on overflow'))
    with pytest.raises(ValueError,match='archive_inventory_limit'):m._scan(tmp_path,resolver.Source(tmp_path),[],NOW)


def test_optional_reader_uses_exact_bytes_in_legacy_stage4_chain(tmp_path,monkeypatch):
    from test_step4_basis_carry_acceptance import _build_pilot_fixture
    run='legacy_bounded_fixture';_build_pilot_fixture(tmp_path,run);monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    pilot=json.loads((tmp_path/resolver.ARCHIVE/('run_id='+run)/'pilot_evidence.json').read_text())
    expected=acceptance.validate_pilot(pilot,run_id=run);reads=[]
    def reader(path):reads.append(path);return path.read_bytes()
    actual=acceptance.validate_pilot(pilot,run_id=run,byte_reader=reader)
    assert actual==expected and len(reads)==6


def test_capture_acceptance_after_validation_retention_and_clock_floor(tmp_path,monkeypatch):
    s=fixture(tmp_path);e=deepcopy(s[m.STORE_KEY]['evidence']);e.pop('accepted_at_utc')
    monkeypatch.setattr(m,'_capture',lambda *a:deepcopy(e))
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    out={};ticks=iter((NOW+timedelta(seconds=1),NOW+timedelta(seconds=2)))
    m.capture_snapshot(out,s,now_fn=lambda:next(ticks),refresh_started_at=NOW)
    assert out[m.STORE_KEY]['evidence']==s[m.STORE_KEY]['evidence']
    assert out[m.STORE_KEY]['evidence_sha256']==s[m.STORE_KEY]['evidence_sha256']
    assert out[m.STORE_KEY]['last_capture_attempt_at_utc']==(NOW+timedelta(seconds=2)).isoformat()
    monkeypatch.setattr(m,'_capture',lambda *a:pytest.fail('prework before clock gate'))
    with pytest.raises(ValueError,match='before_work'):m.capture_snapshot({},out,now_fn=lambda:NOW,refresh_started_at=NOW)


@pytest.mark.parametrize('clock',[None,'bad','2026-09-15T12:00:00',NOW-timedelta(days=1)])
def test_invalid_and_reverse_read_clock_canonical_refusal(tmp_path,clock):
    assert read(fixture(tmp_path),clock)['status']=='UNAVAILABLE'


def test_source_to_output_oracle_catches_consistent_renderer_omission(tmp_path,monkeypatch):
    s=fixture(tmp_path);original=m._view
    def omitted(*args,**kwargs):
        out=original(*args,**kwargs);out['pairs']['cny_rub']['metrics'].pop('front_next_spread_abs');return out
    monkeypatch.setattr(m,'_view',omitted)
    r={};m.attach_consumer(s,r,now=NOW)
    with pytest.raises(ValueError,match='independent projection'):m.verify_projection(s,r,now=NOW)


def test_missing_exact_baseline_does_not_become_previous_comparable(tmp_path):
    s=fixture(tmp_path);e=s[m.STORE_KEY]['evidence']
    # Remove an entire original captured run and its source bytes from a separate
    # synthetic inventory, preserving honest absence rather than edited copied facts.
    day='2026-09-11';run=e['runs'][day]
    for folder in (tmp_path/resolver.ARCHIVE/('run_id='+run),):
        for path in folder.iterdir():path.unlink()
        folder.rmdir()
    newer=m._capture(tmp_path,NOW);newer['accepted_at_utc']=NOW.isoformat()
    s[m.STORE_KEY].update(evidence=newer,evidence_sha256=m.digest(newer))
    out=read(s);item=out['dated']['pairs']['cny_rub']['metrics']['front_next_spread_abs']
    assert item['changes']['1']['target_observed_trade_date']==day and item['changes']['1']['change'] is None
    assert item['previous_comparable']['trade_date']=='2026-09-08'
    assert item['previous_comparable']['is_exact_previous_observation'] is False


def test_original_clock_revision_changes_semantic_but_validation_clock_does_not(tmp_path):
    e=fixture(tmp_path)[m.STORE_KEY]['evidence'];other=deepcopy(e)
    other['causal_cutoff_at_utc']=(NOW+timedelta(seconds=1)).isoformat();other['accepted_at_utc']=(NOW+timedelta(seconds=2)).isoformat()
    assert m.digest(m._semantic(e))==m.digest(m._semantic(other))
    item=next(iter(other['history'].values()))['cny_rub'];item['parent_finished_at_utc']='2026-09-15T01:00:00+00:00'
    assert m.digest(m._semantic(e))!=m.digest(m._semantic(other))


def test_capture_refusal_keeps_only_original_evidence_and_records_latest_failure(tmp_path,monkeypatch):
    s=fixture(tmp_path);before=deepcopy(s)
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    def unavailable(*args):raise resolver.Stage4SourceReadError('selected original partition read denied')
    monkeypatch.setattr(m,'_capture',unavailable)
    ticks=iter((NOW+timedelta(seconds=1),NOW+timedelta(seconds=2)));out={}
    completed=m.capture_snapshot(out,s,now_fn=lambda:next(ticks),refresh_started_at=NOW)
    assert s==before and out[m.STORE_KEY]['evidence']==s[m.STORE_KEY]['evidence']
    assert out[m.STORE_KEY]['evidence_sha256']==s[m.STORE_KEY]['evidence_sha256']
    assert read(out,completed)['latest_capture_error']=='Stage4SourceReadError: selected original partition read denied'


def test_capture_cannot_accept_before_validation_completion(tmp_path,monkeypatch):
    s=fixture(tmp_path);candidate=deepcopy(s[m.STORE_KEY]['evidence']);candidate.pop('accepted_at_utc')
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    state={'validated':False};original=m._validate_evidence
    monkeypatch.setattr(m,'_capture',lambda *a:candidate)
    def validate(*args):
        result=original(*args);state['validated']=True;return result
    monkeypatch.setattr(m,'_validate_evidence',validate)
    ticks=iter((NOW,NOW+timedelta(seconds=3)))
    def clock():
        value=next(ticks)
        if value>NOW:assert state['validated']
        return value
    out={};m.capture_snapshot(out,None,now_fn=clock,refresh_started_at=NOW)
    assert out[m.STORE_KEY]['evidence']['accepted_at_utc']==(NOW+timedelta(seconds=3)).isoformat()


def test_portable_original_run_path_keeps_nested_state_support_directory():
    value='/source/root/runs/step4_rub_basis_carry/run_id=test_stage4/state/refresh/manifest.json'
    assert resolver.logical(value)=='runs/step4_rub_basis_carry/run_id=test_stage4/state/refresh/manifest.json'


def test_expired_unchanged_original_sources_never_rejuvenate_first_acceptance(tmp_path,monkeypatch):
    previous=fixture(tmp_path);before=deepcopy(previous)
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    monkeypatch.setattr(source,'_data_root',lambda:tmp_path)
    later=NOW+timedelta(hours=97);ticks=iter((later,later+timedelta(seconds=1)));out={}
    m.capture_snapshot(out,previous,now_fn=lambda:next(ticks),refresh_started_at=later)
    assert previous==before
    assert out[m.STORE_KEY]['last_capture_error'] is None
    assert out[m.STORE_KEY]['evidence']==previous[m.STORE_KEY]['evidence']
    assert out[m.STORE_KEY]['evidence_sha256']==previous[m.STORE_KEY]['evidence_sha256']
    assert read(out,later+timedelta(seconds=1))['status']=='UNAVAILABLE'
    # A new original admitted run is a real source version, even if prices agree.
    make_run(tmp_path,'2026-09-14',suffix='000002',finished_hour=23)
    ticks=iter((later+timedelta(seconds=2),later+timedelta(seconds=3)));recovered={}
    m.capture_snapshot(recovered,out,now_fn=lambda:next(ticks),refresh_started_at=later)
    assert recovered[m.STORE_KEY]['last_capture_error'] is None
    assert recovered[m.STORE_KEY]['evidence_sha256']!=out[m.STORE_KEY]['evidence_sha256']
    assert recovered[m.STORE_KEY]['evidence']['accepted_at_utc']==(later+timedelta(seconds=3)).isoformat()
    assert read(recovered,later+timedelta(seconds=3))['dated']['status']=='AVAILABLE'


def test_explicit_weekend_observations_are_not_removed_by_calendar_assumption(tmp_path):
    out=read(fixture(tmp_path,weekend_observations=True))
    metric=out['dated']['pairs']['cny_rub']['metrics']['front_next_spread_abs']
    assert metric['changes']['1']['target_observed_trade_date']=='2026-09-13'
    assert metric['changes']['1']['change'] is None
    assert metric['previous_comparable']['trade_date']=='2026-09-11'
    assert metric['previous_comparable']['is_exact_previous_observation'] is False


@pytest.mark.parametrize('rows,columns',[(5001,1),(1,101)])
def test_original_parquet_metadata_bound_precedes_every_frame_decode(tmp_path,monkeypatch,rows,columns):
    path=tmp_path/resolver.RUNS/'run_id=metadata_bound'/'part.parquet'
    path.parent.mkdir(parents=True)
    pd.DataFrame({str(i):range(rows) for i in range(columns)}).to_parquet(path,index=False)
    monkeypatch.setattr(pd,'read_parquet',lambda *a,**k:pytest.fail('decode before metadata refusal'))
    source=resolver.Source(tmp_path)
    with pytest.raises(ValueError,match='stage4_physical_frame_bound'):source.read(path)
    replay=resolver.Source(proof=source.proof,buffers=source.buffers)
    with pytest.raises(ValueError,match='stage4_physical_frame_bound'):replay.frame(path)


@pytest.mark.parametrize('rows,columns',[(5000,1),(1,100)])
def test_original_parquet_metadata_exact_bound_remains_readable(tmp_path,rows,columns):
    path=tmp_path/resolver.RUNS/'run_id=metadata_bound'/'part.parquet'
    path.parent.mkdir(parents=True)
    pd.DataFrame({str(i):range(rows) for i in range(columns)}).to_parquet(path,index=False)
    source=resolver.Source(tmp_path)
    assert source.frame(path).shape==(rows,columns)
    assert resolver.Source(proof=source.proof,buffers=source.buffers).frame(path).shape==(rows,columns)
