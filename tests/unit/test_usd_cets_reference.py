"""Synthetic/reconstructed responses, real admission, saved bytes and readers."""
from copy import deepcopy
from datetime import timedelta
import base64,json
import pytest
from moex_data import rub_usd_cets_reference as usd
from moex_data import rub_exact_comparisons as exact
from moex_data import rub_historical_basis_carry_context as basis
from moex_data import rub_factual_projection as projection
from test_exact_comparison_sources import Response, http_factory, snapshot, basis_fixture, src, NOW
from test_contract_price_market_oi_observed import _native_body

def market():
    result=_native_body()
    result.update(quality={'analysis_usable':True},status='READY')
    return result


def payload(*,age=5,status='A'):
    local=NOW.astimezone(usd.core.MOSCOW);trade=local-timedelta(seconds=age)
    def table(row):return {'columns':list(row),'data':[list(row.values())]}
    return {'securities':table({'SECID':usd.SECID,'BOARDID':'CETS','FACEUNIT':'USD','CURRENCYID':'RUB'}),
        'dataversion':table({'trade_date':local.date().isoformat(),'trade_session_date':local.date().isoformat()}),
        'marketdata':table({'SECID':usd.SECID,'BOARDID':'CETS','SYSTIME':local.replace(tzinfo=None).isoformat(sep=' '),
            'TRADEDATE':local.date().isoformat(),
            'TIME':trade.time().isoformat(),'TRADINGSTATUS':status,'OPEN':84.,'HIGH':85.,'LOW':83.,'LAST':84.5,
            'WAPRICE':84.,'VOLTODAY':None,'NUMTRADES':28,'BID':None,'OFFER':None})}


def evidence(**kwargs):
    def get(url,**kw):return Response(payload(**kwargs),url,kw['params'])
    return usd.capture(now_fn=lambda:NOW,http_get=get,env={'MOEX_API_KEY':'synthetic'})


def test_live_reference_replays_identity_without_deliverable_spot_claim():
    e=evidence();data=usd.attach(market(),e,now=NOW)
    assert usd.usable(data,now=NOW)
    row=data['instruments']['usd_tom']
    assert row['deliverable_spot'] is False and row['price_unit']=='RUB_per_USD'
    s={'identity':{'generated_at_utc':NOW.isoformat()},'components':{'synchronized_live_market_oi':{'status':'PARTIAL','data':data}}}
    assert projection.spot_usable(s,'usd_tom')
    assert not usd.usable(data,now=NOW+timedelta(seconds=61))


@pytest.mark.parametrize('kwargs',[{'age':3600},{'status':'N'}])
def test_new_heartbeat_and_receipt_do_not_renew_trade(kwargs):
    data=usd.attach(market(),evidence(**kwargs),now=NOW)
    assert not usd.usable(data,now=NOW)
    assert data['instruments']['usd_tom']['stale'] is True
    assert data['instruments']['si_front']['price_oi_usable'] is True


@pytest.mark.parametrize('defect',['outer_identity','contract','version','hash','SECID','BOARDID','TIME','native_date','duplicate','clock','stored_value'])
def test_full_original_and_outer_identity_required(defect):
    e=evidence();data=usd.attach(market(),e,now=NOW)
    if defect=='stored_value':data['instruments']['usd_tom']['last']=84.25
    elif defect=='outer_identity':e['identity']['secid']='CNYRUB_TOM'
    elif defect=='contract':e['contract']['current_last_price_ttl_seconds']=3600
    elif defect=='version':e['schema_version']='wrong'
    elif defect=='hash':e['response']['sha256']='0'*64
    elif defect=='clock':e['requested_at_utc']=(NOW+timedelta(seconds=1)).isoformat()
    else:
        obj=json.loads(base64.b64decode(e['response']['bytes_base64']))
        if defect=='duplicate':obj['marketdata']['data']*=2
        else:
            table=obj['dataversion'] if defect=='native_date' else obj['marketdata']
            field='trade_date' if defect=='native_date' else defect
            table['data'][0][table['columns'].index(field)]={'SECID':'CNYRUB_TOM','BOARDID':'OTCM','TIME':'23:59:59','native_date':'1900-01-01'}[defect]
        raw=json.dumps(obj).encode();e['response']['bytes_base64']=base64.b64encode(raw).decode();e['response']['sha256']=usd.sha256(raw).hexdigest()
    assert not usd.usable(data,now=NOW)


def captured_v3(tmp_path,*,defects=None):
    s=snapshot(missing=(1,16,20));s.update(basis_fixture(tmp_path,weekend_observations=True))
    exact.capture_snapshot(s,None,root=tmp_path,now_fn=lambda:NOW,refresh_started_at=NOW,
        http_get=http_factory(defects=defects),env={'MOEX_API_KEY':'synthetic'},version='v3')
    assert s[exact.STORE_KEY]['error'] is None
    return s


def test_exact_USD_1_5_from_same_dates_and_original_own_legs(tmp_path):
    s=captured_v3(tmp_path);view=basis.describe(s,now=NOW)
    for lag in ('1','5'):assert view['dated']['comparison_coverage'][lag]['available']==30
    m=view['dated']['pairs']['usd_rub']['metrics']['front_spot_basis_abs']
    assert m['changes']['1']['target_observed_trade_date']=='2026-09-11'
    assert m['anchor']['reference_identity']['deliverable_spot'] is False
    assert m['anchor']['reference_leg']['secid']==usd.SECID
    basis.verify_projection(s,{basis.OUTPUT_KEY:view},now=NOW)
    prior=deepcopy(s)
    exact.capture_snapshot(s,prior,root=tmp_path,now_fn=lambda:NOW+timedelta(seconds=10),refresh_started_at=NOW,
        http_get=lambda *a,**kw:pytest.fail('same observation refetched'),env={},version='v3')
    assert s[exact.STORE_KEY]['evidence_sha256']==prior[exact.STORE_KEY]['evidence_sha256']


@pytest.mark.parametrize('defect',['empty','auth','timeout','ERROR_MESSAGE'])
def test_USD_refusal_stays_at_selected_date_and_CNY_survives(tmp_path,defect):
    s=captured_v3(tmp_path,defects={('2026-09-11',usd.SECID):defect})
    view=basis.describe(s,now=NOW)['dated'];m=view['pairs']['usd_rub']['metrics']['front_spot_basis_abs']
    assert m['changes']['1']['target_observed_trade_date']=='2026-09-11' and m['changes']['1']['change'] is None
    assert m['changes']['5']['change'] is not None
    assert view['pairs']['cny_rub']['metrics']['front_spot_basis_abs']['changes']['1']['change'] is not None


def test_v3_corruption_does_not_restore_v1_USD_or_futures_dates(tmp_path):
    s=captured_v3(tmp_path);e=s[exact.STORE_KEY]['evidence'];e['usd_reference_selection']['calendar_sha256']='0'*64
    s[exact.STORE_KEY]['evidence_sha256']=src.digest(e)
    view=basis.describe(s,now=NOW)['dated'];m=view['pairs']['usd_rub']['metrics']['front_spot_basis_abs']
    assert m['anchor'] is None and m['changes']['1']['change'] is None


@pytest.mark.parametrize('defect',[None,'auth','outer_null','outer_list','raw_null','raw_list','table_null','row_null','url_list',
    'native_date_missing','native_date_prior','native_date_empty','native_date_bad','native_date_future','closed_missing_date'])
def test_real_fast_collection_disk_and_read_keep_USD_failure_independent(tmp_path,defect):
    from test_synchronized_live_market_oi_context import _payloads
    from moex_data import synchronized_live_market_oi_context_partial as live
    from moex_data import rub_fast_market as fast
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    forts,cny=_payloads()
    allowed={r[0] for r in forts['marketdata']['data']}
    forts['securities']['data']=[r for r in forts['securities']['data'] if r[0] in allowed]
    source_time=(NOW-timedelta(seconds=5)).astimezone(usd.core.MOSCOW).strftime('%Y-%m-%d %H:%M:%S')
    for body in (forts,cny):
        columns=body['marketdata']['columns']
        for row in body['marketdata']['data']:row[columns.index('SYSTIME')]=source_time
    def get(url,**kw):
        if usd.SECID in url:return Response(payload(),url,kw['params'],status=401 if defect=='auth' else 200)
        return Response(cny if 'CNYRUB_TOM' in url else forts,url,kw['params'])
    def loader():return live.fetch_live_snapshot_with_usd(http_get=get,now_fn=lambda:NOW,env={'MOEX_API_KEY':'synthetic'})
    value=fast.refresh(tmp_path,loader=loader,clock=lambda:NOW)
    assert value['status']=='COLLECTED'
    folder=fast.state_path(tmp_path);(folder/'enabled').write_text(fast.SCHEMA)
    # Hash-consistent corruption must reach the real original-evidence check.
    m=value['market'];e=m['usd_reference_evidence']
    if defect=='outer_null':m['usd_reference_evidence']=None
    elif defect=='outer_list':m['usd_reference_evidence']=[]
    elif defect=='row_null':m['instruments']['usd_tom']=None
    elif defect=='url_list':e['response']['url']=[]
    elif defect in ('raw_null','raw_list','table_null'):
        obj=None if defect=='raw_null' else [] if defect=='raw_list' else {**payload(),'marketdata':None}
        raw=json.dumps(obj).encode()
        e['response']['bytes_base64']=base64.b64encode(raw).decode()
        e['response']['sha256']=usd.sha256(raw).hexdigest()
    elif defect and (defect.startswith('native_date_') or defect=='closed_missing_date'):
        obj=payload(status='N' if defect=='closed_missing_date' else 'A')
        table=obj['marketdata'];index=table['columns'].index('TRADEDATE')
        if defect in ('native_date_missing','closed_missing_date'):
            table['columns'].pop(index);table['data'][0].pop(index)
        else:
            table['data'][0][index]={'native_date_prior':(NOW-timedelta(days=1)).date().isoformat(),
                'native_date_future':(NOW+timedelta(days=1)).date().isoformat(),
                'native_date_empty':'','native_date_bad':'not-a-date'}[defect]
        raw=json.dumps(obj).encode();e['response']['bytes_base64']=base64.b64encode(raw).decode()
        e['response']['sha256']=usd.sha256(raw).hexdigest()
        # Keep a producer's normalized observation; replay must still reject
        # current use after persisting the complete hash-consistent carrier.
        m['instruments']['usd_tom']=usd.replay(e,now=NOW)
    value['market_sha256']=fast._digest(m)
    (folder/'current.json').write_text(json.dumps(value))
    raw=(folder/'current.json').read_bytes();assert json.loads(raw)==value
    s={'identity':{'generated_at_utc':NOW.isoformat()},'components':{},'authority':{},'analysis_views':{},'analysis_workflow':{}}
    view=apply_read_freshness(fast.apply(s,root=tmp_path,now=NOW),now=NOW)
    assert view['fast_market_read']['error'] is None
    data=view['components']['synchronized_live_market_oi']['data']
    assert projection.spot_usable(view,'usd_tom') is (defect is None)
    assert data['instruments']['si_front']['price_oi_usable'] is True
    assert data['instruments']['cr_front']['price_oi_usable'] is True
    assert len(projection.basis_metrics(view))==(30 if defect is None else 22)
    if defect and (defect.startswith('native_date_') or defect=='closed_missing_date'):
        row=data['instruments']['usd_tom'];state=row['market_state']
        assert row['timestamp'] is None and row['native_trade_date_verified'] is False
        assert row['read_freshness_reason'].startswith('USD_native_trade_date_')
        assert state['current_price_usable'] is False
        assert state['last_observation']['values']['last']==84.5
        assert state['last_observation']['values']['timestamp'] is None
        assert state['last_observation']['current_use_allowed'] is False
        assert state['state']==('NOT_TRADING_OBSERVED' if defect=='closed_missing_date' else 'TRADING_OBSERVED')
    assert (folder/'current.json').read_bytes()==raw


@pytest.mark.parametrize('missing_native_date',[False,True])
def test_real_heavy_saved_canonical_read_and_final_delivery_recheck_currency_TTL(tmp_path,monkeypatch,missing_native_date):
    from test_stage9_analysis_bundle_v2 import source_io,live,shifted_market,NOW as clock
    from src.moex_research.consumers import usdrubf_chat_snapshot_consumer as consumer
    source_io(tmp_path,monkeypatch)
    monkeypatch.setitem(globals(),'NOW',clock)
    source=shifted_market(clock)
    source['instruments']['cnyrub_tom']['source_trading_status']='A'
    e=evidence()
    if missing_native_date:
        obj=json.loads(base64.b64decode(e['response']['bytes_base64']));t=obj['marketdata'];i=t['columns'].index('TRADEDATE')
        t['columns'].pop(i);t['data'][0].pop(i)
        raw=json.dumps(obj).encode();e['response']['bytes_base64']=base64.b64encode(raw).decode();e['response']['sha256']=usd.sha256(raw).hexdigest()
    source=usd.attach(source,e,now=clock)
    _,path=live.refresh_snapshot(now_fn=lambda:clock,live_loader=lambda:source)
    raw=path.read_bytes()
    before=consumer.load_market_factual(now_fn=lambda:clock,reader=live.base.read_current_snapshot,code_revision='a'*40)
    times=iter((clock,clock+timedelta(seconds=61)))
    after=consumer.load_market_factual(now_fn=lambda:next(times),reader=live.base.read_current_snapshot,code_revision='a'*40)
    for key in ('cnyrub_tom','usd_tom'):
        admitted=key!='usd_tom' or not missing_native_date
        assert before['prices'][key]['status']==('AVAILABLE' if admitted else 'UNAVAILABLE')
        assert before['prices'][key]['market_state']['current_price_usable'] is admitted
        assert after['prices'][key]['status']=='UNAVAILABLE'
        assert after['prices'][key]['market_state']['current_price_usable'] is False
        assert after['prices'][key]['market_state']['last_observation']['current_use_allowed'] is False
    if missing_native_date:
        row=before['prices']['usd_tom']
        assert row['source_identity']['timestamp'] is None
        assert row['reason']=='USD_native_trade_date_missing_or_invalid'
        assert row['market_state']['last_observation']['values']['last']==84.5
        assert row['market_state']['last_observation']['values']['native_trade_date_verified'] is False
        # This legacy heavy fixture lacks native expiry metadata, so its four
        # existing carry metrics are unavailable independently of USD admission.
        assert before['basis_carry']['admitted_metric_count']==18
        assert all('usd_tom' not in item['values']['legs'] for item in before['basis_carry']['metrics'])
    assert before['prices']['si_front']['status']=='AVAILABLE'
    assert before['prices']['cr_front']['status']=='AVAILABLE'
    assert path.read_bytes()==raw
