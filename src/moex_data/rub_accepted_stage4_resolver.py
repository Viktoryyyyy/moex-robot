"""Bounded Stage4 run admission/replay from its own immutable input bytes."""
from datetime import date
from decimal import Decimal, localcontext
from io import BytesIO
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
import re
import pandas as pd
import pyarrow.parquet as pq
from moex_data import rub_contract_price_market_oi_observed as custody
from moex_data import step4_basis_carry_acceptance as stage4
from moex_data.analytics import validate_rub_basis_carry_partition as physical

PREFIX='${MOEX_DATA_ROOT}/'
ARCHIVE='state/acceptance/step4_rub_basis_carry'
RUNS='runs/step4_rub_basis_carry'
ROLES=('spot','perpetual','front','next')
PAIRS={'usd_rub_basis_carry':('usd_rub','USD/RUB','Si','si','USDRUBF','USD000UTSTOM'),
       'cny_rub_basis_carry':('cny_rub','CNY/RUB','CR','cr','CNYRUBF','CNYRUB_TOM')}
BASES=(('perpetual_spot_basis','perpetual','spot'),('front_spot_basis','front','spot'),
       ('next_spot_basis','next','spot'),('front_perpetual_basis','front','perpetual'),
       ('next_perpetual_basis','next','perpetual'),('front_next_spread','next','front'))
CARRIES=(('front_spot_implied_carry_annualized','front','spot','front'),
         ('next_spot_implied_carry_annualized','next','spot','next'),
         ('front_next_term_carry_annualized','next','front','term'))
require=custody._require
stamp=custody._stamp


def logical(value):
    """Portable identity of original run paths, never a filesystem read."""
    require(isinstance(value,str) and bool(value),'stage4_path_required')
    text=value.replace('\\','/')
    if text.startswith(PREFIX):text=text[len(PREFIX):]
    elif text.startswith('/') or re.match(r'^[A-Za-z]:/',text):
        candidates=[text.find('/'+prefix+'/') for prefix in ('runs','state')]
        hits=[i for i in candidates if i>=0]
        require(bool(hits),'stage4_absolute_source_path_scope');text=text[min(hits)+1:]
    require(not text.startswith('/') and all(p not in ('','..','.') for p in text.split('/')),'stage4_path_traversal')
    require(text.startswith(('runs/step4_rub_basis_carry/','state/acceptance/step4_rub_basis_carry/',
                             'runs/step10_rub_daily_refresh/')),'stage4_original_path_scope')
    return text


class Stage4SourceReadError(OSError):
    pass


class Source:
    def __init__(self,root=None,*,budget=None,proof=None,buffers=None):
        self.root=root;self.budget=budget or custody.CaptureBudget();self.proof={} if proof is None else proof
        self.buffers={} if buffers is None else buffers;self.cache={};self.used=set()
    def read(self,path):
        self.budget.ensure_active();key=logical(str(path));self.used.add(key)
        if key not in self.cache:
            if self.root is not None:
                try:raw=custody._read_bytes(self.root,PREFIX+key,budget=self.budget)
                except OSError as exc:raise Stage4SourceReadError(str(exc)) from exc
                self.proof[key]={'sha256':custody.sha256(raw).hexdigest()}
                self.buffers[self.proof[key]['sha256']]=raw
            else:
                item=self.proof[key];require(set(item)=={'sha256'},'stage4_original_reference_shape')
                raw=self.buffers[item['sha256']];self.budget.charge(raw)
                require(custody.sha256(raw).hexdigest()==item['sha256'],'stage4_original_hash_mismatch')
            if key.endswith('.json'):custody._source_object(raw)
            if key.endswith('.parquet'):
                # This reader also feeds the legacy physical validator: preflight
                # here, before either caller can materialize the verified bytes.
                metadata=pq.read_metadata(BytesIO(raw))
                require(0<metadata.num_rows<=5000 and metadata.num_columns<=100,'stage4_physical_frame_bound')
            self.cache[key]=raw
        return self.cache[key]
    def json(self,path):return custody._source_object(self.read(path))
    def frame(self,path):
        result=pd.read_parquet(BytesIO(self.read(path)))
        require(0<len(result)<=5000 and len(result.columns)<=100,'stage4_physical_frame_bound')
        return result


def utc(value):
    result=pd.Timestamp(value)
    require(not pd.isna(result),'stage4_source_timestamp_missing')
    return (result.tz_localize('Europe/Moscow') if result.tzinfo is None else result).tz_convert('UTC').to_pydatetime()


def finite(value):
    require(not isinstance(value,bool),'stage4_boolean_price')
    result=Decimal(str(value));require(result.is_finite() and result>0,'stage4_positive_finite_price')
    return result


def formulas(rates,days):
    result={role+'_rate':rate for role,rate in rates.items()}
    for name,c,r in BASES:
        result[name+'_abs']=rates[c]-rates[r];result[name+'_bps']=(rates[c]/rates[r]-1)*10000
    for name,c,r,h in CARRIES:result[name]=(rates[c]/rates[r]-1)*365/Decimal(days[h])
    return result


def _binding(pilot,prefix,root,day,binding_clock):
    out={}
    for role in ('front','next'):
        matches=[b for b in pilot['bindings'] if b.get('instrument_id')==prefix+'_'+role+'_contract']
        require(len(matches)==1,'stage4_binding_scope');b=matches[0]
        require(b.get('role')==role and b.get('root')==root and b.get('as_of_date')==day
                and b.get('source_id')=='moex_iss_forts_securities_reference','stage4_binding_identity')
        require(re.fullmatch(('Si' if root=='Si' else 'CR')+r'[HMUZ][0-9]',b['secid']) is not None,'stage4_binding_secid')
        require(stamp(b['availability_ts_utc'])==stamp(b['mapping_fixed_ts_utc'])==binding_clock,'stage4_binding_clock')
        require(date.fromisoformat(b['last_trade_date'])>date.fromisoformat(day),'stage4_binding_expired')
        out[role]=b
    require(out['front']['secid']!=out['next']['secid'] and out['front']['last_trade_date']<out['next']['last_trade_date'],'stage4_binding_order')
    return out


def validate_run(source,run,*,now):
    """Same validator for producer and portable audit; source owns bounded bytes."""
    require(isinstance(run,str) and re.fullmatch(r'[A-Za-z0-9_.-]+_stage4',run) is not None,'stage4_run_identity')
    run_root=Path(RUNS)/('run_id='+run);base=ARCHIVE+'/run_id='+run
    marker=source.json(base+'/accepted_pointers.json');pilot=source.json(base+'/pilot_evidence.json')
    day=custody._day(pilot['trade_date']);require(day<=now.astimezone(custody.archive.MOSCOW).date().isoformat(),'stage4_future_trade_date')
    parent_run=run[:-7];parent=source.json('runs/step10_rub_daily_refresh/run_id='+parent_run+'/run_manifest.json')
    refresh=parent.get('source_refresh',{})
    require(parent.get('project')=='MOEX_Bot' and parent.get('stage')==10 and parent.get('run_id')==parent_run
        and parent.get('status')=='succeeded' and parent.get('current_pointer_rollback_status') in (None,'not_needed')
        and refresh.get('status')=='refreshed' and refresh.get('stage4_run_id')==run and refresh.get('trade_date')==day
        and refresh.get('stage4_pointer_count')==2 and refresh.get('stage3_pointer_count')==10
        and refresh.get('stage3_run_id')==parent_run+'_stage3','stage4_parent_failed_rolled_back_or_identity_mismatch')
    started,finished=stamp(parent['started_at_utc']),stamp(parent['finished_at_utc'])
    require(started<=finished<=now,'stage4_parent_future_or_reversed')
    for key,value in {'project':'MOEX_Bot','step':4,'status':'accepted','run_id':run,'acceptance_contract_id':stage4.CONTRACT_ID,
        'accepted_pointer_count':2,'expected_pointer_count':2,'promotion_semantics':'transactional_with_rollback',
        'physical_partition_readback_required':True,'continuous_series_used':False}.items():
        require(type(marker.get(key)) is type(value) and marker.get(key)==value,'stage4_marker_identity_or_policy')
    def resolve(value):
        path=Path(logical(value));require(path==run_root or path.is_relative_to(run_root),'stage4_own_run_lineage_required')
        return path
    outputs=stage4.validate_pilot(pilot,run_id=run,byte_reader=source.read,run_root=run_root,path_resolver=resolve)
    pointers=marker.get('pointers');require(isinstance(pointers,list) and len(pointers)==2,'stage4_marker_pointer_inventory')
    result={}
    for output in outputs:
        instrument=output['instrument_id'];pair,pair_id,root,prefix,perpetual,spot=PAIRS[instrument]
        matches=[v for v in pointers if v.get('instrument_id')==instrument];require(len(matches)==1,'stage4_pointer_identity')
        pointer=matches[0]
        require(set(pointer)=={'instrument_id','run_id','acceptance_run_id','pointer_path','pointer_ref','physical_readback'},'stage4_pointer_original_schema')
        expected_ref=PREFIX+'state/current/market/derived/dataset_id=rub_basis_carry_5m/instrument_id='+instrument+'/current.json'
        # The canonical location is governed by the existing Stage4 pointer API.
        from moex_data import step9_rub_analysis_bundle as step9
        spec=next(v for v in step9._stage4_specs() if v.instrument_id==instrument)
        expected_ref=PREFIX+step9._pointer_path(Path('/C_ROOT'),spec).relative_to('/C_ROOT').as_posix()
        require(pointer.get('pointer_ref')==expected_ref and pointer.get('acceptance_run_id')==run
            and pointer.get('run_id')==output['manifest_run_id'],'stage4_pointer_run_or_ref')
        if 'pointer_path' in pointer:
            text=pointer['pointer_path'].replace('\\','/');require('..' not in text.split('/') and text.endswith('/'+expected_ref[len(PREFIX):]),'stage4_pointer_path')
        require(custody.common._digest(pointer.get('physical_readback'))==custody.common._digest(output['physical_readback']),'stage4_marker_physical_readback')
        manifest=source.json(output['manifest']);quality=source.json(output['quality']);derived=source.frame(output['partition'])
        require(manifest.get('dataset_id')=='rub_basis_carry_5m' and manifest.get('trade_date')==day
            and manifest.get('pair_id')==pair_id and manifest.get('refresh_status')=='succeeded','stage4_derived_manifest_scope')
        require(manifest.get('alignment_policy')=='exact_timestamp_inner_join' and manifest.get('forward_fill_used') is False
            and manifest.get('asof_join_used') is False and manifest.get('continuous_series_used') is False,'stage4_derived_join_policy')
        require(quality.get('exact_timestamp_inner_join') is True and quality.get('forward_fill_used') is False
            and quality.get('asof_join_used') is False and quality.get('duplicate_ts_count')==0
            and quality.get('monotonic_ts') is True and quality.get('positive_rate_check') is True
            and quality.get('non_null_derived_metrics') is True,'stage4_derived_quality_policy')
        lineage=manifest['input_lineage'];binding_clock=stamp(lineage['binding_reference_observed_at_utc'])
        require(started<=binding_clock<=finished and stamp(pilot['reference_observed_at_utc'])==binding_clock
            and pilot['reference_source_url']==lineage['binding_reference_url'],'stage4_binding_parent_clock')
        require(isinstance(lineage.get('binding_reference_url'),str) and lineage['binding_reference_url'].startswith('https://'),'stage4_reference_url')
        bindings=_binding(pilot,prefix,root,day,binding_clock)
        instruments={'spot':'usd_tom' if root=='Si' else 'cny_tom','perpetual':'usdrubf_futures_family' if root=='Si' else 'cnyrubf_futures_family',
            'front':prefix+'_front_contract','next':prefix+'_next_contract'}
        secids={'spot':spot,'perpetual':perpetual,**{k:b['secid'] for k,b in bindings.items()}}
        days={'front':(date.fromisoformat(bindings['front']['last_trade_date'])-date.fromisoformat(day)).days,
            'next':(date.fromisoformat(bindings['next']['last_trade_date'])-date.fromisoformat(day)).days,
            'term':(date.fromisoformat(bindings['next']['last_trade_date'])-date.fromisoformat(bindings['front']['last_trade_date'])).days}
        legs={};tables={}
        for role in ROLES:
            entry=lineage[role];partition=resolve(entry['partition_path']);support=source.json(resolve(entry['manifest_path']));q=source.json(resolve(entry['quality_report_path']))
            frame=source.frame(partition);require(len(frame)==entry['row_count'],'stage4_leg_lineage_count')
            source_id='moex_iss_cets_tom_1m' if role=='spot' else 'moex_algopack_fo_tradestats_5m'
            spec=SimpleNamespace(dataset_id=entry['dataset_id'],manifest_run_id=support['run_id'],instrument_id=instruments[role],
                source_id=source_id,secid=secids[role],trade_date=day,row_count=len(frame),partition_path=partition,quality_path=resolve(entry['quality_report_path']))
            def normalized(doc):
                from copy import deepcopy
                result=deepcopy(doc)
                for key in ('partition_path','quality_report_path','quality_report_ref'):
                    if key in result:result[key]=str(resolve(result[key]))
                if 'partitions_written' in result:result['partitions_written']=[str(resolve(v)) for v in result['partitions_written']]
                return result
            require(support['run_id']==run+'_'+instruments[role]+'_quote','stage4_leg_producer_run')
            from moex_data.step4_basis_carry_pilot_runner import _lineage
            acquisitions=pilot['tom_partitions'] if role=='spot' else pilot['quote_partitions']
            require(isinstance(acquisitions,list) and len(acquisitions)==(2 if role=='spot' else 6),'stage4_pilot_acquisition_inventory')
            linked=[_lineage(v) for v in acquisitions if _lineage(v).get('instrument_id')==instruments[role]]
            require(len(linked)==1 and custody.common._digest(linked[0])==custody.common._digest(entry),'stage4_pilot_original_lineage_mismatch')
            require(entry.get('instrument_id')==instruments[role] and entry.get('quality_status')=='pass'
                and entry.get('dataset_id')==('fx_spot_raw_5m' if role=='spot' else 'futures_raw_5m'),'stage4_lineage_identity')
            custody._validate_support_buffers(spec,normalized(support),normalized(q))
            required=['instrument_id','trade_date','ts','secid','source_id','close','ingest_ts']
            require(all(k in frame for k in required) and not frame[required].isna().any().any(),'stage4_leg_required_fields')
            require(set(frame.instrument_id)=={instruments[role]} and set(frame.secid)=={secids[role]} and set(frame.trade_date.astype(str))=={day} and set(frame.source_id)=={source_id},'stage4_leg_identity')
            frame=frame.copy();frame['utc']=frame.ts.map(utc)
            require(not frame.utc.duplicated().any(),'stage4_leg_duplicate_timestamp')
            records={}
            divisor=1000 if root=='Si' and role in ('front','next') else 1
            for row in frame.to_dict('records'):
                event=row['utc'];receipt=stamp(str(row['ingest_ts']));price=finite(row['close'])
                require(event.astimezone(custody.archive.MOSCOW).date().isoformat()==day and event<=receipt<=finished and binding_clock<=receipt,'stage4_leg_source_clock')
                records[event]={'secid':secids[role],'instrument_id':instruments[role],'trade_date':day,'source_timestamp_utc':event.isoformat(),
                    'received_at_utc':receipt.isoformat(),'price':float(price),'normalization_divisor':divisor,'normalized_rate':float(price/Decimal(divisor)),
                    'expiry_date':bindings[role]['last_trade_date'] if role in bindings else None,
                    'price_source_field':'close','timestamp_semantics':'five_minute_bar_endpoint_not_official_session_close',
                    'price_publication_at_utc':None}
            legs[role]=records;tables[role]=frame
        common=set.intersection(*(set(legs[k]) for k in ROLES));derived_times=[utc(x) for x in derived.ts]
        require(len(derived_times)==len(set(derived_times)) and set(derived_times)==common,'stage4_complete_original_intersection')
        maximum_receipt=max(stamp(r['received_at_utc']) for rows in legs.values() for r in rows.values())
        with localcontext() as context:
            context.prec=40
            for row,event in zip(derived.to_dict('records'),derived_times):
                require(maximum_receipt<=stamp(str(row['build_ts']))<=finished,'stage4_derived_build_clock')
                rates={k:Decimal(str(legs[k][event]['price']))/Decimal(legs[k][event]['normalization_divisor']) for k in ROLES}
                for key,expected in formulas(rates,days).items():
                    actual=Decimal(str(row[key]));require(actual.is_finite() and abs(actual-expected)<=Decimal('1e-9'),'stage4_original_leg_formula_mismatch:'+key)
                for key,expected in {'front_secid':secids['front'],'next_secid':secids['next'],'perpetual_secid':perpetual,
                    'front_expiry_date':bindings['front']['last_trade_date'],'next_expiry_date':bindings['next']['last_trade_date'],
                    'calendar_days_to_front_expiry':days['front'],'calendar_days_to_next_expiry':days['next'],'calendar_days_between_expiries':days['term']}.items():
                    require(row[key]==expected,'stage4_original_binding_metadata_mismatch')
        metrics={}
        for name,c,r in BASES:
            for suffix,unit in (('_abs','normalized_rate_difference'),('_bps','basis_points')):
                metrics[name+suffix]=metric_endpoint(name+suffix,c,r,legs,days,unit,None,root)
        for name,c,r,h in CARRIES:metrics[name]=metric_endpoint(name,c,r,legs,days,'annualized_fraction',h,root)
        result[pair]={'trade_date':day,'run_id':run,'source_kind':'CURRENT_REVALIDATED_STAGE4_OWN_INPUT_LEGS',
            'parent_finished_at_utc':finished.isoformat(),'role_binding_as_of_utc':binding_clock.isoformat(),
            'role_binding_scope':'run_pilot_selected_binding_not_original_full_reference_response',
            'metrics':metrics,'complete_materializer_row_count':len(derived)}
    return result


def metric_endpoint(name,c,r,legs,days,unit,horizon,root):
    if root=='Si' and 'spot' in (c,r):return {'status':'UNAVAILABLE','reason':'USD_spot_production_scope_not_admitted','value':None}
    times=set(legs[c])&set(legs[r])
    if not times:return {'status':'UNAVAILABLE','reason':'exact_own_leg_timestamp_intersection_missing','value':None}
    event=max(times);a,b=legs[c][event],legs[r][event]
    x=Decimal(str(a['price']))/Decimal(a['normalization_divisor']);y=Decimal(str(b['price']))/Decimal(b['normalization_divisor'])
    value=(x/y-1)*365/Decimal(days[horizon]) if horizon else x-y if unit=='normalized_rate_difference' else (x/y-1)*10000
    return {'status':'AVAILABLE','reason':None,'value':float(value),'unit':unit,'normalized_currency_unit':'RUB_per_USD' if root=='Si' else 'RUB_per_CNY',
        'comparison_leg':a,'reference_leg':b,'own_pair_observation_count':len(times),'source_timestamp_utc':event.isoformat(),
        'calendar_tenor_days':days[horizon] if horizon else None,'session_completion_proven':False}
