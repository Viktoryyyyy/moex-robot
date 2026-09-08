from copy import deepcopy
from datetime import datetime,timedelta,timezone
from hashlib import sha256
import json
from pathlib import Path

import pytest

from moex_research.external_data import cbr_liquidity_factual as liquidity

NOW=datetime(2026,9,8,13,5,31,tzinfo=timezone.utc)
FIXTURE=Path(__file__).parents[1]/'fixtures/cbr/banking_liquidity_20260908.html'


def raw():
    return FIXTURE.read_bytes()


def component(tmp_path):
    return {'status':'READY','data':liquidity.load(root=tmp_path,now_fn=lambda:NOW,fetch=lambda url:raw())}


def test_real_received_four_metrics_and_rounding_limitation(tmp_path):
    value=component(tmp_path); before=deepcopy(value)
    result=liquidity.reconcile(value,now=NOW)
    assert value==before
    assert result['status']=='READY'
    data=result['data']
    assert [o['value'] for o in data['observations']]==['2817.4','3675.6','6198.8','5340.7']
    assert all(o['units']=='RUB_bn' and o['displayed_decimal_places']==1 for o in data['observations'])
    assert all(o['source_publication_time'] is None for o in data['observations'])
    assert data['source_revision_status']=='latest_revised'
    assert data['quality_status']=='USABLE_WITH_LIMITATIONS'
    assert data['arithmetic_residual']['value']=='-0.1'
    assert data['arithmetic_residual']['informational_only'] is True
    assert data['arithmetic_residual']['raw_sha256']==sha256(raw()).hexdigest()
    assert len(data['limitations'])==3
    assert all(data[k] is False for k in liquidity.DENIED)
    assert liquidity.reconcile(result,now=NOW+timedelta(seconds=1200))['status']=='READY'


@pytest.mark.parametrize('old,new',[
    ('08.09.2026','broken-date'),('08.09.2026','09.09.2026'),
    ('08.09.2026','07.09.2026'),('08.09.2026','01.01.2026'),
    ('2,817.4','NaN'),('2,817.4','Infinity'),('2,817.4','2,81.4'),
    ('6,198.8','-6,198.8'),('5,340.7','-5,340.7'),
    ('billions of rubles','millions of rubles'),
    ('Liquidity deficit (+)/','Other balance/'),
])
def test_strict_raw_source_negative(old,new):
    changed=raw().decode().replace(old,new)
    assert changed!=raw().decode()
    with pytest.raises(ValueError):
        liquidity.parse(changed.encode(),source_url=liquidity._url(NOW.date()),received_at=NOW)


def test_signed_surplus_and_displayed_precision_preserved():
    changed=raw().replace(b'2,817.4',b'-2,817.400')
    fact=liquidity.parse(changed,source_url=liquidity._url(NOW.date()),received_at=NOW)[0]
    assert fact['value']=='-2817.400'
    assert fact['displayed_decimal_places']==3


@pytest.mark.parametrize('defect',['expired','future','raw','manifest','value','flags','retained','failed','receipt','naive','residual'])
def test_read_fail_closed(tmp_path,defect):
    value=component(tmp_path); data=value['data']; now=NOW
    if defect=='expired': now+=timedelta(seconds=1201)
    if defect=='future': now-=timedelta(seconds=1)
    if defect=='raw': (Path(data['manifest_path']).parent/(data['raw_sha256']+'.html')).write_bytes(b'changed')
    if defect=='manifest': Path(data['manifest_path']).unlink()
    if defect=='value': data['observations'][0]['value']='0'
    if defect=='flags': data['historical_pit_acceptance']=0
    if defect=='retained': value['status']='RETAINED'
    if defect=='failed': value['refresh_error']='failed'
    if defect=='receipt': data['receipt']=[]
    if defect=='naive': now=NOW.replace(tzinfo=None)
    if defect=='residual': data['arithmetic_residual']['value']='0'
    result=liquidity.reconcile(value,now=now)
    assert result['status']=='UNAVAILABLE'
    assert result['data']['factual_authority'] is False
    assert result['data']['consumer_factual_use_allowed'] is False
    assert result['data']['quality_status']=='UNAVAILABLE'


def test_rehashed_forged_projection_rejected_by_raw_replay(tmp_path):
    value=component(tmp_path); data=value['data']
    data['observations'][0]['value']='777'
    frozen={k:v for k,v in data.items() if k not in ('manifest_path','manifest_sha256')}
    encoded=json.dumps(frozen,sort_keys=True,separators=(',',':')).encode()
    digest=sha256(encoded).hexdigest()
    path=Path(data['manifest_path']).parent/(digest+'.json'); path.write_bytes(encoded)
    data.update(manifest_path=str(path),manifest_sha256=digest)
    assert liquidity.reconcile(value,now=NOW)['status']=='UNAVAILABLE'


@pytest.mark.parametrize('value',[None,[],{'status':'READY','data':[]},{'status':'READY','data':{}}])
def test_malformed_component(value):
    assert liquidity.reconcile(value,now=NOW)['status']=='UNAVAILABLE'


def test_reconcile_midnight_stays_receipt_bound(tmp_path):
    received=NOW.replace(hour=20,minute=59)
    value={'status':'READY','data':liquidity.load(root=tmp_path,now_fn=lambda:received,fetch=lambda url:raw())}
    assert liquidity.reconcile(value,now=received+timedelta(minutes=2))['status']=='READY'


def test_acquisition_date_change_rejected(tmp_path):
    times=iter([NOW.replace(hour=20,minute=59),NOW.replace(hour=21,minute=1)])
    with pytest.raises(ValueError): liquidity.load(root=tmp_path,now_fn=lambda:next(times),fetch=lambda url:raw())


def test_apply_expiry(tmp_path):
    snapshot={'components':{liquidity.COMPONENT:component(tmp_path)}}
    liquidity.apply(snapshot,now=NOW+timedelta(seconds=1201))
    assert snapshot['components'][liquidity.COMPONENT]['status']=='UNAVAILABLE'
