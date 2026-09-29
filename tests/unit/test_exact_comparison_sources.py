"""Synthetic exact-source responses; ordinary capture, admission and byte replay."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import base64
import json

import pytest
import requests

from moex_data import rub_exact_comparison_sources as src
from moex_data import rub_exact_comparisons as exact
from moex_data import rub_contract_price_market_oi_observed as price
from moex_data import rub_historical_basis_carry_context as basis
from test_contract_price_market_oi_observed import NOW, snapshot, _prune_buffers, _native_body, _restore_archive
from test_historical_basis_carry_context import fixture as basis_fixture


class Response:
    def __init__(self, payload, url, params, status=200):
        self.content = json.dumps(payload, allow_nan=False).encode()
        self.url = url+'?'+urlencode(params); self.status_code = status
    def json(self): return json.loads(self.content)
    def raise_for_status(self):
        if self.status_code >= 400: raise requests.HTTPError('synthetic HTTP refusal')


def http_factory(*, defects=None, calls=None):
    defects = defects or {}
    def get(url, **kwargs):
        query = kwargs['params']; day = query['from']; secid = url.rsplit('/',1)[-1].removesuffix('.json')
        spot = secid == 'candles'
        if spot: secid = 'CNYRUB_TOM'
        if calls is not None: calls.append((day,secid))
        defect = defects.get((day,secid)) or defects.get(secid)
        if defect == 'timeout': raise requests.Timeout('synthetic timeout')
        if defect == 'auth': return Response({},url,query,401)
        if defect == 'ERROR_MESSAGE': return Response({'ERROR_MESSAGE':'synthetic unavailable'},url,query)
        if spot:
            cols = ['open','high','low','close','volume','begin','end']
            rows = [] if query['start'] or date_weekend(day) or defect == 'empty' else [[12,12,12,12,2,day+' 18:59:00',day+' 18:59:59']]
            return Response({'candles':{'columns':cols,'data':rows}},url,query)
        cols = ['secid','tradedate','tradetime','pr_open','pr_high','pr_low','pr_close','oi_open','oi_high','oi_low','oi_close','SYSTIME']
        rows = [[secid,day,t,100,100,100,100,2**53+3,2**53+3,2**53+3,2**53+3,day+' '+t[:-2]+'45'] for t in ('18:55:00','19:00:00')]
        if defect == 'empty': rows=[]
        if defect == 'fractional': rows[-1][7:11]=[10.5]*4
        if defect == 'zero': rows[-1][7:11]=[0]*4
        if defect == 'price': rows[-1][6]=None
        if defect == 'date': rows[-1][1]='1900-01-01'
        if defect == 'secid': rows[-1][0]='SiZ9'
        if defect == 'duplicate': rows.append(deepcopy(rows[-1]))
        if defect == 'publication': rows[-1][-1]=day+' 18:59:00'
        if defect == 'schema': cols[-1]='WRONG_FIELD'
        if defect == 'sparse': rows=rows[:-1]
        return Response({'data':{'columns':cols,'data':rows},'data.cursor':{'columns':['INDEX','TOTAL','PAGESIZE'],'data':[[0,len(rows),1000]]}},url,query)
    return get


def date_weekend(day): return datetime.fromisoformat(day).weekday() >= 5


def source(day='2026-09-13', secid='SiU6', **kwargs):
    return src.acquire(day,secid,now_fn=lambda:NOW,http_get=http_factory(**kwargs),env={'MOEX_API_KEY':'synthetic-test-key'})


def captured(tmp_path, *, defects=None):
    s=snapshot(missing=(1,16,20))
    s.update(basis_fixture(tmp_path,weekend_observations=True))
    exact.capture_snapshot(s,None,root=tmp_path,now_fn=lambda:NOW,refresh_started_at=NOW,
        http_get=http_factory(defects=defects),env={'MOEX_API_KEY':'synthetic-test-key'})
    assert s[exact.STORE_KEY]['error'] is None
    return s


def test_exact_source_preserves_large_native_oi_and_same_row():
    value=source(); rows=src.replay(value,day='2026-09-13',secid='SiU6',now=NOW)
    pair=src.price_pair(rows,day='2026-09-13',secid='SiU6',source=value)
    assert pair['market_open_interest']==2**53+3 and type(pair['market_open_interest']) is int
    assert pair['price']==100 and pair['source_timestamp_moscow'].endswith('19:00:00+03:00')


@pytest.mark.parametrize('defect,reason',[
    ('empty','EMPTY'),('timeout','Timeout'),('auth','HTTPError'),('ERROR_MESSAGE','KeyError'),
    ('date','row_identity'),('secid','row_identity'),('duplicate','duplicate_bar')])
def test_exact_source_refusals_keep_original_responses(defect,reason):
    value=source(defects={'SiU6':defect})
    with pytest.raises(ValueError,match=reason): src.replay(value,day='2026-09-13',secid='SiU6',now=NOW)
    if defect!='timeout': assert value['pages'][0]['response_base64']


@pytest.mark.parametrize('defect',['fractional','price','publication','schema'])
def test_bad_latest_row_never_selects_earlier_valid_pair(defect):
    value=source(defects={'SiU6':defect}); rows=src.replay(value,day='2026-09-13',secid='SiU6',now=NOW)
    assert len(rows)==2
    with pytest.raises((ValueError,KeyError,TypeError)): src.price_pair(rows,day='2026-09-13',secid='SiU6',source=value)


@pytest.mark.parametrize('field,value', [('schema_version','wrong'),('trade_date','2026-09-12'),('secid','SiZ6'),
    ('capture_completed_at_utc','2026-09-15T12:00:01+00:00')])
def test_outer_source_identity_cannot_be_laundered(field,value):
    item=source();item[field]=value
    with pytest.raises(ValueError): src.replay(item,day='2026-09-13',secid='SiU6',now=NOW)


def test_raw_hash_and_same_bytes_replay():
    item=source();item['pages'][0]['response_base64']=base64.b64encode(b'{}').decode()
    with pytest.raises(ValueError,match='response_hash'):src.replay(item,day='2026-09-13',secid='SiU6',now=NOW)


def test_positive_exact_lags_and_weekend_without_CETS(tmp_path):
    s=captured(tmp_path);p=price.describe(s,now=NOW);b=basis.describe(s,now=NOW)
    for lag in ('1','5','20'):
        assert p['dated']['comparison_coverage'][lag]['available']==4
        for item in p['dated']['contracts'].values():
            change=item['changes'][lag]
            assert change['baseline']['secid']==item['secid'] and change['baseline']['trade_date']==change['target_observed_trade_date']
            assert change['baseline']['market_open_interest']==2**53+3
            assert change['baseline']['evidence_reference']['response_digest']
    assert b['dated']['comparison_coverage']['1']['available']==14
    assert b['dated']['comparison_coverage']['5']['available']==22
    cny=b['dated']['pairs']['cny_rub']['metrics']
    assert 'EMPTY' in cny['front_spot_basis_abs']['changes']['1']['reason']
    assert cny['front_perpetual_basis_abs']['changes']['1']['change'] is not None
    out={'contract_price_market_oi_context':p,basis.OUTPUT_KEY:b}
    price.verify_projection(s,out,now=NOW);basis.verify_projection(s,out,now=NOW)


def test_contract_and_price_vs_basis_failures_are_independent(tmp_path):
    s=captured(tmp_path,defects={('2026-09-13','SiU6'):'fractional'})
    p=price.describe(s,now=NOW)['dated']; b=basis.describe(s,now=NOW)['dated']
    assert p['contracts']['si_front']['changes']['1']['values'] is None
    assert p['contracts']['cr_front']['changes']['1']['values'] is not None
    assert p['contracts']['si_front']['changes']['5']['values'] is not None
    assert b['pairs']['usd_rub']['metrics']['front_perpetual_basis_abs']['changes']['1']['change'] is not None


def test_retry_reuses_bytes_preserves_first_acceptance_and_does_not_fetch_on_read(tmp_path):
    s=captured(tmp_path);before=deepcopy(s[exact.STORE_KEY]); prior=deepcopy(s)
    def forbidden(*a,**k): raise AssertionError('unexpected new HTTP')
    exact.capture_snapshot(s,prior,root=tmp_path,now_fn=lambda:NOW+timedelta(hours=1),refresh_started_at=NOW,
        http_get=forbidden,env={'MOEX_API_KEY':'synthetic-test-key'})
    assert s[exact.STORE_KEY]['evidence']==before['evidence']
    assert s[exact.STORE_KEY]['evidence_sha256']==before['evidence_sha256']
    assert price.describe(s,now=NOW+timedelta(hours=1))['dated']['comparison_coverage']['20']['available']==4
    assert price.describe(s,now=NOW+timedelta(hours=97))['status']=='UNAVAILABLE'


def test_corrupt_selected_evidence_is_refused_without_v1_or_earlier_date_replacement(tmp_path):
    s=captured(tmp_path); e=s[exact.STORE_KEY]['evidence']; key='2026-09-13/SiU6'
    item=e['entries'][key]; item['source']['pages'][0]['response_base64']=base64.b64encode(b'{}').decode()
    item['source_sha256']=src.digest(item['source']);s[exact.STORE_KEY]['evidence_sha256']=src.digest(e)
    p=price.describe(s,now=NOW)['dated']['contracts']
    assert p['si_front']['changes']['1']['baseline'] is None
    assert 'response_hash' in p['si_front']['changes']['1']['reason']
    assert p['cr_front']['changes']['1']['values'] is not None


def test_zero_oi_is_numeric_and_fraction_is_null():
    value=source(defects={'SiU6':'zero'}); rows=src.replay(value,day='2026-09-13',secid='SiU6',now=NOW)
    p=src.price_pair(rows,day='2026-09-13',secid='SiU6',source=value)
    change=price._change({**p,'market_open_interest':5},p)
    assert change['market_open_interest_change']==5 and change['market_open_interest_change_fraction'] is None


def test_sparse_contract_keeps_own_latest_bar_and_never_imputes_other_contract_time():
    item=source(defects={'SiU6':'sparse'});rows=src.replay(item,day='2026-09-13',secid='SiU6',now=NOW)
    p=src.price_pair(rows,day='2026-09-13',secid='SiU6',source=item)
    assert p['source_timestamp_moscow'].endswith('18:55:00+03:00')
    assert p['session_completion_proven'] is False


@pytest.mark.parametrize('mutation',['request_future','receipt_before_request','query','cursor','conflicting_duplicate','float_precision'])
def test_complete_receipt_and_source_inventory(mutation):
    item=source();page=item['pages'][0]
    if mutation=='request_future':page['requested_at_utc']=(NOW+timedelta(seconds=1)).isoformat()
    elif mutation=='receipt_before_request':page['received_at_utc']=(NOW-timedelta(seconds=1)).isoformat()
    elif mutation=='query':page['source_url']=page['source_url'].replace('2026-09-13','2026-09-12')
    else:
        payload=json.loads(base64.b64decode(page['response_base64']))
        if mutation=='cursor':payload['data.cursor']['data'][0][1]+=1
        elif mutation=='conflicting_duplicate':
            payload['data']['data'][0][2]=payload['data']['data'][1][2];payload['data']['data'][0][6]=99
        else:payload['data']['data'][-1][7:11]=[float(2**53+4)]*4
        raw=json.dumps(payload).encode();page['response_base64']=base64.b64encode(raw).decode();page['response_sha256']=src.sha256(raw).hexdigest()
    with pytest.raises((ValueError,KeyError,TypeError)):
        rows=src.replay(item,day='2026-09-13',secid='SiU6',now=NOW)
        src.price_pair(rows,day='2026-09-13',secid='SiU6',source=item)


def test_no_current_source_role_substitution_or_closest_date(tmp_path):
    s=captured(tmp_path);e=s[exact.STORE_KEY]['evidence'];e['bindings']['si_front']='SiM9'
    s[exact.STORE_KEY]['evidence_sha256']=src.digest(e)
    p=price.describe(s,now=NOW)['dated']['contracts']['si_front']['changes']['1']
    assert p['target_observed_trade_date']=='2026-09-13' and p['baseline'] is None


def test_real_heavy_refresh_saved_json_canonical_reader_stage9_and_compact(tmp_path,monkeypatch):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
    from moex_data import rub_factual_release as release
    from moex_data import rub_analysis_bundle_v2 as bundles
    # Original accepted Stage3/4 artifacts and real Parquet witness; missing
    # archives force the actual heavy caller to acquire exact source targets.
    basis_fixture(tmp_path,weekend_observations=True)
    _restore_archive(tmp_path,dates={'2026-09-14','2026-09-09'})
    monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path));monkeypatch.setenv('MOEX_API_KEY','synthetic-test-key')
    monkeypatch.setattr(requests,'get',http_factory())
    monkeypatch.setattr(live.base,'load_dotenv',lambda *a,**k:None)
    monkeypatch.setattr(live.base,'install_timestamp_policy',lambda:None)
    def missing(now):raise OSError('synthetic sibling source absent')
    def producers():return {k:(v if k.startswith('stage9_') else missing) for k,v in live.base.default_producers().items()}
    monkeypatch.setattr(live.current_context.current,'current_producers',producers)
    monkeypatch.setattr(live.current_context.context,'run_refresh_all',lambda **kw:{})
    monkeypatch.setattr(live.current_context.delta_context,'build_all',lambda **kw:{})
    monkeypatch.setattr(live.current_context,'_attach_futoi_context',lambda *a,**kw:None)
    monkeypatch.setattr('moex_data.rub_dated_hour_source.acquire',lambda **kw:{'latest_attempts':{}})
    saved,path=live.refresh_snapshot(now_fn=lambda:NOW,live_loader=_native_body)
    assert saved[exact.STORE_KEY]['error'] is None
    assert json.loads(path.read_bytes())==saved
    disk=path.read_bytes();read,_=live.base.read_current_snapshot(now_fn=lambda:NOW)
    assert path.read_bytes()==disk
    def no_network(*a,**kw):raise AssertionError('reader/publication attempted acquisition')
    monkeypatch.setattr(requests,'get',no_network)
    for value in (saved,read):
        for scope in ('daily','weekly'):
            data=value['components']['stage9_'+scope]['data']
            items=data['sections']['historical_comparisons']['items']
            p=items['contract_price_market_oi_context']['values']['dated']
            assert all(p['comparison_coverage'][lag]['available']==4 for lag in ('1','5','20'))
            b=items['historical_basis_carry_context']
            assert b['status']=='PARTIAL' and b['values']['dated']['comparison_coverage']['1']['available']==14
    compact=release.compact(read,now=NOW,code_revision='a'*40)
    assert compact['contract_price_market_oi_context']['dated']['comparison_coverage']['20']['available']==4
    target=compact['contract_price_market_oi_context']['dated']['contracts']['si_front']['changes']['1']
    assert target['baseline']['evidence_reference']['response_digest']
    export_input=deepcopy(read)
    # No Rosstat document source was supplied by this synthetic fixture.
    export_input['components'].pop('rosstat_monthly_cpi',None)
    exported=release.export(export_input,now=NOW,code_revision='a'*40,output=tmp_path/'export')
    raw=json.loads((exported/'input_snapshot.json').read_text())
    assert release.build(raw,now=NOW,code_revision='a'*40)==release.build(export_input,now=NOW,code_revision='a'*40)
    second,_=live.refresh_snapshot(now_fn=lambda:NOW,live_loader=_native_body)
    assert second[exact.STORE_KEY]['evidence_sha256']==saved[exact.STORE_KEY]['evidence_sha256']


def test_three_retries_and_expired_attempts_keep_first_acceptance(tmp_path):
    s=captured(tmp_path); original=deepcopy(s[exact.STORE_KEY]['evidence'])
    for offset in (60,120,180,345601,345602):
        prior=deepcopy(s)
        exact.capture_snapshot(s,prior,root=tmp_path,now_fn=lambda:NOW+timedelta(seconds=offset),refresh_started_at=NOW,
            http_get=http_factory(),env={'MOEX_API_KEY':'synthetic-test-key'})
        assert s[exact.STORE_KEY]['evidence']==original
        if offset>345600:
            assert exact._supplement(s,NOW+timedelta(seconds=offset))[0] is None


def test_failure_retry_clock_is_distinct_from_first_evidence(tmp_path):
    calls=[];get=http_factory(defects={'SiU6':'timeout'},calls=calls)
    values=[]
    for offset in (0,901,902):
        values.append(src.retained(tmp_path,'2026-09-13','SiU6',now_fn=lambda:NOW+timedelta(seconds=offset),
            http_get=get,env={'MOEX_API_KEY':'synthetic-test-key'}))
    assert len(calls)==2 and values[0]==values[1]==values[2]


def test_sticky_shared_budget_stops_before_cache_write(tmp_path):
    budget=src.Budget([b'a'*15_999_999]);calls=[]
    with pytest.raises(src.BudgetExceeded):
        src.retained(tmp_path,'2026-09-13','SiU6',now_fn=lambda:NOW,http_get=http_factory(calls=calls),
            env={'MOEX_API_KEY':'synthetic-test-key'},budget=budget)
    assert len(calls)==1 and not list(tmp_path.rglob('*.json'))
    with pytest.raises(src.BudgetExceeded):budget.charge(b'')


def test_source_failure_does_not_erase_independent_accepted_basis_metric(tmp_path,monkeypatch):
    s=captured(tmp_path);e=basis._admit(s,NOW);extra,rows,errors=exact._supplement(s,NOW)
    # An older selected role for one USD metric creates repair work. CR metrics
    # with matching identities remain valid despite a supplemental CR failure.
    day=e['observed_dates'][-1]; old=deepcopy(e['history'][day]['cny_rub']['metrics'])
    e['history'][day]['usd_rub']['metrics']['front_next_spread_abs']['comparison_leg']['secid']='SiM6'
    rows.pop(day+'/CNYRUBF',None);errors[day+'/CNYRUBF']='exact_source_acquisition_Timeout'
    monkeypatch.setattr(exact,'_supplement',lambda *args:(extra,rows,errors))
    repaired=exact.enrich_basis(e,s,NOW)
    assert repaired['history'][day]['cny_rub']['metrics']==old
