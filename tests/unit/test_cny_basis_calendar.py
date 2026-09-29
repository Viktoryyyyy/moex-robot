"""Synthetic calendar/source fixtures, never production replay."""
from copy import deepcopy
from datetime import date, timedelta
import json
import shutil

import pytest

from moex_data import rub_cny_basis_calendar as calendar
from moex_data import rub_exact_comparisons as exact
from moex_data import rub_historical_basis_carry_context as basis
from test_exact_comparison_sources import captured, http_factory, src, price, NOW, snapshot, basis_fixture


def days(end, count=22):
    return [(date.fromisoformat(end)-timedelta(days=i)).isoformat() for i in reversed(range(count))]


def test_september_28_target_plan_excludes_26_27_before_quality():
    selected=calendar.select(days('2026-09-28'))
    assert selected['target_dates']==['2026-09-28','2026-09-25','2026-09-21']
    assert {'2026-09-26','2026-09-27'} <= set(selected['excluded_nontrading_dates'])
    assert selected['session_completion_proven'] is False
    assert exact.targets(days('2026-09-28'),'2026-09-29')[1]==['2026-09-28','2026-09-27','2026-09-23']


def test_holidays_and_calendar_coverage_not_unbounded_weekdays():
    assert '2026-02-23' in calendar.select(days('2026-02-24'))['excluded_nontrading_dates']
    assert '2026-03-09' in calendar.select(days('2026-03-10'))['eligible_observed_dates']
    selected=calendar.select(days('2027-01-10'))
    assert selected['reason']=='CNY_calendar_coverage_unknown' and not selected['target_dates']


@pytest.mark.parametrize('defect',['empty','timeout','auth','ERROR_MESSAGE'])
def test_selected_CETS_trading_day_refusal_never_shifts_lag(tmp_path,defect):
    s=snapshot(missing=(1,16,20));basis_fixture(tmp_path,weekend_observations=True)
    # Remove only synthetic Stage4 runs so the real capture acquires both legs.
    shutil.rmtree(tmp_path/basis.resolver.ARCHIVE)
    e=basis._capture(tmp_path,NOW);e['accepted_at_utc']=NOW.isoformat()
    s[basis.STORE_KEY]={'evidence':e,'evidence_sha256':src.digest(e),'last_capture_attempt_at_utc':NOW.isoformat(),'last_capture_error':None}
    calls=[]
    exact.capture_snapshot(s,None,root=tmp_path,now_fn=lambda:NOW,refresh_started_at=NOW,
        http_get=http_factory(defects={('2026-09-11','CNYRUB_TOM'):defect},calls=calls),env={'MOEX_API_KEY':'synthetic'})
    assert s[exact.STORE_KEY]['error'] is None
    assert ('2026-09-13','CNYRUB_TOM') not in calls
    out=basis.describe(s,now=NOW);metric=out['dated']['pairs']['cny_rub']['metrics']['front_spot_basis_abs']
    assert metric['changes']['1']['target_observed_trade_date']=='2026-09-11'
    assert metric['changes']['1']['change'] is None
    assert metric['changes']['5']['change'] is not None
    assert out['dated']['pairs']['cny_rub']['metrics']['front_next_spread_abs']['changes']['1']['change'] is not None
    assert price.describe(s,now=NOW)['dated']['comparison_coverage']['1']['available']==4
    basis.verify_projection(s,{basis.OUTPUT_KEY:out},now=NOW)
    assert s[exact.STORE_KEY]['evidence']['entries']['2026-09-11/CNYRUB_TOM']['source'] is not None


@pytest.mark.parametrize('mutation',['target','calendar_hash','version','expired'])
def test_selected_v2_cannot_fall_back_to_futures_calendar(tmp_path,mutation):
    s=captured(tmp_path);store=s[exact.STORE_KEY];e=store['evidence'];now=NOW
    if mutation=='target':e['cny_spot_selection']['target_dates'][1]='2026-09-13'
    elif mutation=='calendar_hash':e['cny_spot_selection']['calendar_sha256']='0'*64
    elif mutation=='version':e['schema_version']='wrong'
    else:
        now=NOW+timedelta(hours=96,seconds=1)
        legacy=s[basis.STORE_KEY];legacy['evidence']['accepted_at_utc']=(NOW+timedelta(hours=1)).isoformat()
        legacy['last_capture_attempt_at_utc']=(NOW+timedelta(hours=1)).isoformat()
        legacy['evidence_sha256']=src.digest(legacy['evidence'])
    store['evidence_sha256']=src.digest(e)
    metric=basis.describe(s,now=now)['dated']['pairs']['cny_rub']['metrics']['front_spot_basis_abs']
    assert metric['anchor'] is None and metric['changes']['1']['target_observed_trade_date'] is None
    assert metric['changes']['1']['change'] is None


def test_frozen_v1_keeps_old_date_semantics(tmp_path):
    s=captured(tmp_path);e=s[exact.STORE_KEY]['evidence']
    e['schema_version']=exact.SCHEMA;e['contract']=src.contract();e.pop('cny_spot_selection')
    e['entries']={k:v for k,v in e['entries'].items() if k.split('/')[0] in e['price_target_dates']+e['basis_target_dates']}
    for day in e['basis_target_dates']:
        if exact._basis_missing(basis._admit(s,NOW),day,e['bindings']):
            item=src.acquire(day,'CNYRUB_TOM',now_fn=lambda:NOW,http_get=http_factory(),env={})
            e['entries'][day+'/CNYRUB_TOM']={'source':item,'source_sha256':src.digest(item),'refusal':None}
    s[exact.STORE_KEY]['evidence_sha256']=src.digest(e);before=deepcopy(s)
    out=basis.describe(s,now=NOW);cny=out['dated']['pairs']['cny_rub']['metrics']
    assert cny['front_spot_basis_abs']['changes']['1']['target_observed_trade_date']=='2026-09-13'
    assert out['dated']['comparison_coverage']['1']['available']==14
    assert s==before


def test_weekend_anchor_uses_own_eligible_date_through_real_capture(tmp_path):
    import pandas as pd
    from moex_data import step9_rub_analysis_bundle as step9
    from moex_data.futures import futoi_delta_statistics_context as engine
    from test_contract_price_market_oi_observed import _source_snapshot
    s=deepcopy(_source_snapshot(witness_end='2026-09-13'));basis_fixture(tmp_path,weekend_observations=True)
    spec=engine._spec(stage=7,dataset_id=engine.OBSERVED_DATE_WITNESS_DATASET_ID,
        instrument_id=engine.OBSERVED_DATE_WITNESS_INSTRUMENT_ID,timeframe=engine.OBSERVED_DATE_WITNESS_TIMEFRAME)
    path=step9._pointer_path(tmp_path,spec);pointer=json.loads(path.read_bytes())
    partition=tmp_path/pointer['partition_ref'].removeprefix('${MOEX_DATA_ROOT}/')
    frame=pd.read_parquet(partition);frame[frame.trade_date.astype(str)<='2026-09-13'].to_parquet(partition,index=False)
    pointer['partition_sha256']=src.sha256(partition.read_bytes()).hexdigest();path.write_text(json.dumps(pointer))
    e=basis._capture(tmp_path,NOW);e['accepted_at_utc']=NOW.isoformat()
    s[basis.STORE_KEY]={'evidence':e,'evidence_sha256':src.digest(e),'last_capture_attempt_at_utc':NOW.isoformat(),'last_capture_error':None}
    exact.capture_snapshot(s,None,root=tmp_path,now_fn=lambda:NOW,refresh_started_at=NOW,
        http_get=http_factory(),env={'MOEX_API_KEY':'synthetic'})
    assert s[exact.STORE_KEY]['error'] is None
    out=basis.describe(s,now=NOW);metric=out['dated']['pairs']['cny_rub']['metrics']['front_spot_basis_abs']
    assert out['dated']['anchor_trade_date']=='2026-09-13'
    assert metric['anchor_trade_date']=='2026-09-11'
    assert metric['anchor']['reference_leg']['trade_date']=='2026-09-11'
    assert metric['changes']['1']['target_observed_trade_date']=='2026-09-10'
    assert metric['changes']['5']['target_observed_trade_date']=='2026-09-04'
    basis.verify_projection(s,{basis.OUTPUT_KEY:out},now=NOW)


def test_new_capture_missing_binding_does_not_restore_old_v1_policy(tmp_path):
    s=basis_fixture(tmp_path,weekend_observations=True)
    exact.capture_snapshot(s,None,root=tmp_path,now_fn=lambda:NOW,refresh_started_at=NOW)
    assert s[exact.STORE_KEY]['error'] is not None
    metric=basis.describe(s,now=NOW)['dated']['pairs']['cny_rub']['metrics']['front_spot_basis_abs']
    assert metric['anchor'] is None and metric['changes']['1']['change'] is None


@pytest.mark.parametrize('version',['v1','v2'])
def test_publication_expiry_preserves_full_versioned_projection(tmp_path,version):
    s=captured(tmp_path);e=s[exact.STORE_KEY]['evidence']
    if version=='v1':
        e['schema_version']=exact.SCHEMA;e['contract']=src.contract();e.pop('cny_spot_selection')
        e['entries']={k:v for k,v in e['entries'].items() if k.split('/')[0] in e['price_target_dates']+e['basis_target_dates']}
    s[exact.STORE_KEY]['evidence_sha256']=src.digest(e)
    for key in (price.STORE_KEY,basis.STORE_KEY):
        store=s[key];store['evidence']['accepted_at_utc']=(NOW+timedelta(hours=1)).isoformat()
        store['last_capture_attempt_at_utc']=(NOW+timedelta(hours=1)).isoformat()
        store['evidence_sha256']=src.digest(store['evidence'])
    before=NOW+timedelta(hours=96)-timedelta(seconds=1)
    after=NOW+timedelta(hours=96,seconds=1)
    prepared={'release':{basis.OUTPUT_KEY:basis.describe(s,now=before)}}
    exact.prepare_publication_expiry(s,prepared,now=before)
    exact.apply_publication_expiry(prepared,now=after)
    expected=basis.describe(s,now=after)
    actual=prepared['release'][basis.OUTPUT_KEY]
    assert actual==expected
    assert actual['dated']['supplemental_evidence']['schema_version']=='exact_comparison_evidence.'+version
    assert actual['dated']['supplemental_evidence']['admission_contract'].endswith('_'+version+'.json')
    path=tmp_path/'published.json';path.write_text(json.dumps(prepared['release']))
    basis.verify_projection(s,json.loads(path.read_bytes()),now=after)
    metric=actual['dated']['pairs']['cny_rub']['metrics']['front_spot_basis_abs']
    if version=='v1':assert metric['anchor']['status']=='AVAILABLE' and 'date_selection' not in metric
    else:assert metric['anchor'] is None and metric['date_selection']['reason']=='exact_source_admission_expired'
