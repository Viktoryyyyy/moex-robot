from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from moex_research.external_data import cbr_meeting_calendar as calendar

NOW = datetime(2026,9,8,7,tzinfo=timezone.utc)


def raw():
    """Synthetic scoped DOM; no fixture claims of actual acquisition."""
    return ('''<a data-tabs-tab="current">2026 год</a>
    <div role="tabpanel" data-tabs-content="current">
    <table><td>Предполагаемое время публикации пресс-релиза — 13:30 по московскому времени</td>
    <td>Предполагаемое время начала пресс-конференции — 15:00 по московскому времени</td></table>
    <div class="main-events_day"><div class="date">24 июля 2026 года</div><div class="title">'''+calendar.MEETING+'''</div></div>
    <div class="main-events_day"><div class="date">5 августа 2026 года</div><div class="info">Резюме обсуждения ключевой ставки</div></div>
    <div class="main-events_day"><div class="date">11 сентября 2026 года</div><div class="title">'''+calendar.MEETING+'''</div></div>
    <div class="main-events_day"><div class="date">23 октября 2026 года</div><div class="title">'''+calendar.MEETING+'''</div></div>
    <div class="main-events_day"><div class="date">18 декабря 2026 года</div><div class="title">'''+calendar.MEETING+'''</div></div></div>''').encode()


def component(tmp_path, now=NOW):
    return {'status':'READY','data':calendar.load(root=tmp_path,now_fn=lambda:now,fetch=lambda url:raw())}


def test_frozen_replay_upcoming_and_no_authority(tmp_path):
    value=component(tmp_path)
    before=deepcopy(value)
    result=calendar.reconcile(value,now=NOW)
    assert value==before
    assert result['status']=='READY'
    data=result['data']
    assert len(data['events'])==4
    assert [e['scheduled_date'] for e in data['upcoming_events']]==['2026-09-11','2026-10-23','2026-12-18']
    assert all(data[k] is False for k in calendar.DENIED)
    event=data['upcoming_events'][0]
    assert event['scheduled_time'] is event['actual_event_time'] is event['source_publication_time'] is None
    assert event['planned_press_release_time']=='13:30'
    assert event['planned_times_kind']=='TENTATIVE_SOURCE_SCHEDULE'


@pytest.mark.parametrize('defect',['expired','future','raw','manifest','normalized','prior_failure','retained','url','receipt_shape','naive'])
def test_fail_closed(tmp_path,defect):
    value=component(tmp_path)
    data=value['data']; now=NOW
    if defect=='expired': now+=timedelta(seconds=1201)
    if defect=='future': now-=timedelta(seconds=1)
    if defect=='raw': (Path(data['manifest_path']).parent/(data['raw_sha256']+'.html')).write_bytes(b'changed')
    if defect=='manifest': Path(data['manifest_path']).unlink()
    if defect=='normalized': data['events'][0]['scheduled_date']='2026-01-01'
    if defect=='prior_failure': value['refresh_error']='failed'
    if defect=='retained': value['status']='RETAINED'
    if defect=='url': data['receipt']['source_url']='https://example.org/'
    if defect=='receipt_shape': data['receipt']=[]
    if defect=='naive': now=NOW.replace(tzinfo=None)
    result=calendar.reconcile(value,now=now)
    assert result['status']=='UNAVAILABLE'
    assert result['data']['calendar_schedule_usable'] is False
    assert result['data']['upcoming_events']==[]
    assert all(result['data'][k] is False for k in calendar.DENIED)


@pytest.mark.parametrize('old,new',[
    ('11 сентября 2026','31 сентября 2026'),('11 сентября 2026','11 сентября 2025'),
    ('11 сентября 2026','24 июля 2026'),('11 сентября 2026','broken date'),
    ('5 августа 2026','5 invalid 2026'),('13:30','14:00'),
    (calendar.MEETING,'Unknown meeting'),('2026 год</a>','2025 год</a>'),
    ('Резюме обсуждения ключевой ставки','Unknown activity')])
def test_strict_selected_year(old,new):
    with pytest.raises(ValueError): calendar.parse(raw().decode().replace(old,new).encode(),year=2026)


def test_midnight_recomputes_upcoming_without_rewriting_receipt(tmp_path):
    received=datetime(2026,9,11,20,59,tzinfo=timezone.utc)
    value=component(tmp_path,received)
    assert value['data']['upcoming_events'][0]['scheduled_date']=='2026-09-11'
    result=calendar.reconcile(value,now=received+timedelta(minutes=2))
    assert result['status']=='READY'
    assert result['data']['upcoming_events'][0]['scheduled_date']=='2026-10-23'
    assert result['data']['received_at']==value['data']['received_at']
    assert calendar.reconcile(result,now=received+timedelta(minutes=3))['status']=='READY'


def test_after_last_date_is_empty_not_complete(tmp_path):
    now=datetime(2026,12,19,7,tzinfo=timezone.utc)
    value=calendar.reconcile(component(tmp_path,now),now=now)
    assert value['status']=='READY'
    assert value['data']['upcoming_events']==[]
    assert value['data']['full_calendar_accepted'] is False


@pytest.mark.parametrize('value',[None,[],{'status':'READY','data':[]},{'status':'READY','data':{}}])
def test_malformed_component_safe(value):
    result=calendar.reconcile(value,now=NOW)
    assert result['status']=='UNAVAILABLE'
    assert result['data']['upcoming_events']==[]


def test_wrong_year_reference_rejects(tmp_path):
    now=datetime(2026,12,31,20,59,tzinfo=timezone.utc)
    assert calendar.reconcile(component(tmp_path,now),now=now+timedelta(minutes=2))['status']=='UNAVAILABLE'


def test_apply_reconciles_known_component(tmp_path):
    snapshot={'components':{calendar.COMPONENT:component(tmp_path)}}
    calendar.apply(snapshot,now=NOW+timedelta(seconds=1201))
    assert snapshot['components'][calendar.COMPONENT]['status']=='UNAVAILABLE'
