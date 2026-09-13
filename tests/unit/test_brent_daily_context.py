"""Synthetic explicit source dates; arithmetic expectations are independent."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
from urllib.parse import parse_qs, urlsplit

import pytest

from moex_research.external_data import brent_daily_context as context
from moex_research.external_data import moex_brent_factual as source

NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)


def block(rows):
    columns = list(rows[0]) if rows else ['SECID', 'BOARDID', 'ASSETCODE', 'TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE']
    return {'columns': columns, 'data': [[r[k] for k in columns] for r in rows]}


def native_rows(secid='BRV6'):
    return [{'SECID': secid, 'BOARDID': 'RFUD', 'ASSETCODE': 'BR',
        'TRADEDATE': (date(2026, 8, 13)+timedelta(days=i)).isoformat(),
        'OPEN': 99+i, 'HIGH': 102+i, 'LOW': 98+i, 'CLOSE': 100+i} for i in range(30)]


def latest(rows=None):
    rows = native_rows() if rows is None else rows
    row = rows[-1]
    return {'secid': row['SECID'], 'expiry': '2026-10-01', 'board': 'RFUD',
        'price_unit': 'USD/barrel', 'source_unit_text': source.UNIT_TEXT,
        'source_trade_date': row['TRADEDATE'], 'ohlc': source._ohlc(row), 'price': row['CLOSE'],
        'identity_evidence': {'first_trade_date': '2026-02-19'}}


def fetcher(rows, calls=None, page_size=100, defect=None):
    def fetch(url):
        if calls is not None: calls.append(url)
        args = parse_qs(urlsplit(url).query); offset = int(args['start'][0])
        selected = [r for r in rows if args['from'][0] <= r['TRADEDATE'] <= args['till'][0]]
        document = {'history': block(selected[offset:offset+page_size]),
            'history.cursor': block([dict(INDEX=offset, TOTAL=len(selected), PAGESIZE=page_size)])}
        if defect: defect(document)
        return json.dumps(document).encode()
    return fetch


def acquire(tmp_path, rows=None, now=NOW, **kwargs):
    rows = native_rows() if rows is None else rows
    return context.acquire(latest(rows), audit_root=tmp_path, clock=lambda: now, monotonic=lambda: 0,
        transport=kwargs.pop('transport', fetcher(rows)), **kwargs)


def test_independent_1_5_20_and_weekly_arithmetic(tmp_path):
    value = acquire(tmp_path); result = context.describe(value, latest(), now=NOW)
    assert result['status'] == 'AVAILABLE' and len(result['daily']) == 30
    for lag, ref, delta in ((1,128,1),(5,124,5),(20,109,20)):
        item = result['comparisons'][str(lag)+'obs']
        assert item['reference_close'] == ref and item['latest_close'] == 129
        assert item['change_abs'] == delta and item['change_pct'] == pytest.approx(delta/ref*100)
        assert item['observed_transitions'] == lag and item['secid'] == 'BRV6'
    week = result['weekly'][-1]
    assert week['period_start'] == '2026-09-07' and week['period_end'] == '2026-09-13'
    assert week['ohlc'] == {'open':124, 'high':131, 'low':123, 'close':129}
    assert week['observed_dates'] == ['2026-09-07','2026-09-08','2026-09-09','2026-09-10','2026-09-11']
    assert week['calendar_interval_ended'] is week['data_finality_proven'] is week['source_range_complete'] is False
    assert all(not result[k] for k in ('live_quote','historical_pit_eligible','model_usable','session_completion_proven'))


def test_missing20_does_not_erase5_or1(tmp_path):
    rows = native_rows()[-8:]
    result = context.describe(acquire(tmp_path,rows),latest(rows),now=NOW)
    assert result['comparisons']['20obs']['reason'] == 'insufficient_observed_dates'
    assert result['comparisons']['5obs']['change_abs'] == 5
    assert result['comparisons']['1obs']['change_abs'] == 1


@pytest.mark.parametrize('bad', [None, True, float('nan'), float('inf'), -1, '100'])
def test_bad_row_keeps_exact_ordinal_and_local_shorter_comparisons(tmp_path,bad):
    rows=native_rows(); rows[9]['CLOSE']=bad
    result=context.describe(acquire(tmp_path,rows),latest(rows),now=NOW)
    assert result['status']=='AVAILABLE' and result['daily'][9]['status']=='UNAVAILABLE'
    assert result['comparisons']['20obs']['reference_date']=='2026-08-22'
    assert result['comparisons']['20obs']['reason']=='invalid_source_row_in_observed_lag'
    assert result['comparisons']['5obs']['change_abs']==5
    assert result['comparisons']['1obs']['change_abs']==1


@pytest.mark.parametrize('defect', ['duplicate','secid','board','cursor','gap','ohlc'])
def test_native_quality_and_pagination(tmp_path,defect):
    rows=native_rows()
    if defect=='duplicate': rows.insert(0,deepcopy(rows[0]))
    elif defect=='secid': rows[0]['SECID']='OTHER'
    elif defect=='board': rows[0]['BOARDID']='OTHER'
    elif defect=='ohlc': rows[9]['HIGH']=1
    mutate = (lambda d: d['history.cursor']['data'][0].__setitem__(1, 99)) if defect=='cursor' else None
    value=acquire(tmp_path,rows,transport=fetcher(rows,page_size=5 if defect=='gap' else 100,defect=mutate))
    result=context.describe(value,latest(rows),now=NOW)
    if defect=='ohlc': assert result['daily'][9]['status']=='UNAVAILABLE'
    else: assert result['status']=='UNAVAILABLE' and value['last_attempt']['status']=='FAILED'


def test_complete_multi_page_and_immutable_reuse_then_revision(tmp_path):
    rows=native_rows();calls=[]
    first=acquire(tmp_path,rows,transport=fetcher(rows,calls,page_size=10))
    assert first['last_attempt']['request_count']==3
    files={p: p.read_bytes() for p in tmp_path.rglob('*.json')}
    second=acquire(tmp_path,rows,previous=first,transport=lambda url: pytest.fail('cache must not fetch'))
    assert second['evidence']==first['evidence'] and second['last_attempt']['status']=='CACHE_REUSED'
    revised=deepcopy(rows); revised[-1]['CLOSE']=128.5
    third=acquire(tmp_path,revised,previous=second)
    assert third['evidence']['source_revision_id']!=first['evidence']['source_revision_id']
    assert all(p.read_bytes()==body for p,body in files.items())
    assert context.describe(third,latest(revised),now=NOW)['comparisons']['1obs']['change_abs']==0.5


def test_failed_refresh_never_renews_receipt_and_retries(tmp_path):
    first=acquire(tmp_path)
    def fail(url): raise OSError('fixture outage')
    failed=acquire(tmp_path,now=NOW+timedelta(days=1),previous=first,transport=fail)
    assert failed['evidence']==first['evidence'] and failed['last_attempt']['request_count']==1
    assert context.describe(failed,latest(),now=NOW+timedelta(days=1))['status']=='UNAVAILABLE'
    recovered=acquire(tmp_path,now=NOW+timedelta(days=1),previous=failed)
    assert recovered['last_attempt']['status']=='RECEIVED'
    assert recovered['evidence']['first_accepted_at']==first['evidence']['first_accepted_at']


def test_roll_no_splice_and_review_week_without_observations(tmp_path):
    first=acquire(tmp_path)
    rows=native_rows('BRX6')[-3:]
    second=acquire(tmp_path,rows,previous=first,now=NOW+timedelta(days=1))
    result=context.describe(second,latest(rows),now=NOW+timedelta(days=1))
    assert len(result['daily'])==3 and result['comparisons']['5obs']['status']=='UNAVAILABLE'
    assert result['weekly'][-1]['period_start']=='2026-09-14'
    assert result['weekly'][-1]['reason']=='no_source_observations_in_period'
    assert result['weekly'][-2]['ohlc']['close']==129
    assert result['weekly'][-2]['calendar_interval_ended'] is True
    assert result['weekly'][-2]['data_finality_proven'] is False


@pytest.mark.parametrize('defect',['digest','units','date','duplicate'])
def test_reader_replay_rejects_corruption_without_io(tmp_path,defect):
    value=acquire(tmp_path); before=deepcopy(value)
    if defect=='digest': value['evidence']['source_revision_id']='0'*64
    elif defect=='units': value['evidence']['identity']['price_unit']='RUB'
    else:
        page=value['evidence']['batches'][0]['pages'][0]
        if defect=='duplicate': page['payload']['history']['data'][0]=deepcopy(page['payload']['history']['data'][1])
        else: page['received_at']=(NOW+timedelta(seconds=1)).isoformat()
        page['payload_digest']=context.payload_digest(page['payload'])
    assert context.describe(value,latest(),now=NOW)['status']=='UNAVAILABLE'
    assert context.describe(before,latest(),now=NOW)['status']=='AVAILABLE'


@pytest.mark.parametrize('field', ['raw_source_id', 'first_accepted_at', 'full_check_date_moscow', 'audit_version_ref'])
def test_rehashed_provenance_corruption_and_cache_recovery(tmp_path,field):
    value=acquire(tmp_path)
    evidence=value['evidence']
    if field=='raw_source_id': evidence['batches'][0]['pages'][0][field]='0'*64
    elif field=='first_accepted_at': evidence[field]=(NOW+timedelta(days=1)).isoformat()
    elif field=='full_check_date_moscow': evidence[field]='2026-09-14'
    else: evidence[field]='0'*64
    if field!='audit_version_ref': evidence['audit_version_ref']=context.payload_digest({k:v for k,v in evidence.items() if k!='audit_version_ref'})
    assert context.describe(value,latest(),now=NOW)['status']=='UNAVAILABLE'
    recovered=acquire(tmp_path,previous=value)
    assert recovered['last_attempt']['status']=='RECEIVED'
    assert context.describe(recovered,latest(),now=NOW)['status']=='AVAILABLE'


def test_extreme_finite_prices_refuse_nonfinite_comparison_only(tmp_path):
    rows=native_rows()
    rows[-2].update(OPEN=1e-310,HIGH=1e-310,LOW=1e-310,CLOSE=1e-310)
    result=context.describe(acquire(tmp_path,rows),latest(rows),now=NOW)
    assert result['comparisons']['1obs']['status']=='UNAVAILABLE'
    assert result['comparisons']['1obs']['reason']=='nonfinite_computed_change'
    assert result['comparisons']['5obs']['status']=='AVAILABLE'
    assert result['daily'][-2]['status']=='AVAILABLE'


def test_json_exponent_overflow_retains_invalid_ordinal(tmp_path):
    rows=native_rows(); rows[9]['CLOSE']='OVERFLOW'
    fetch=fetcher(rows)
    value=acquire(tmp_path,rows,transport=lambda url:fetch(url).replace(b'"OVERFLOW"',b'1e999'))
    result=context.describe(value,latest(rows),now=NOW)
    assert result['daily'][9]['status']=='UNAVAILABLE'
    assert result['comparisons']['20obs']['reason']=='invalid_source_row_in_observed_lag'
    assert result['comparisons']['5obs']['change_abs']==5


def test_request_before_collection_start_is_refused(tmp_path):
    stamps=iter([NOW,NOW-timedelta(seconds=1),NOW])
    result=context.acquire(latest(),audit_root=tmp_path,clock=lambda:next(stamps),
        transport=lambda url:pytest.fail('regressed request must not fetch'),monotonic=lambda:0)
    assert result['last_attempt']['status']=='FAILED'
    assert result['last_attempt']['reason']=='history request clock regressed before collection'


def test_same_date_incremental_overlap_and_no_prefix_splice(tmp_path):
    rows=native_rows(); first=acquire(tmp_path,rows)
    newer=deepcopy(rows)
    newer.append(dict(SECID='BRV6',BOARDID='RFUD',ASSETCODE='BR',TRADEDATE='2026-09-12',OPEN=129,HIGH=132,LOW=128,CLOSE=130))
    calls=[]
    second=acquire(tmp_path,newer,previous=first,transport=fetcher(newer,calls))
    assert parse_qs(urlsplit(calls[0]).query)['from']==['2026-09-07']
    result=context.describe(second,latest(newer),now=NOW)
    assert result['status']=='AVAILABLE' and result['comparisons']['20obs']['change_abs']==20
    assert len(result['daily'])==30 and result['observed_date_count']==31


def test_release_compact_and_reverse_projection_without_network(tmp_path,monkeypatch):
    from test_moex_brent_factual import documents, collect
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    docs=documents(published='2026-09-11')
    docs[-1]['history']=block([native_rows()[-1]])
    anchor=collect(docs,now=NOW)[0]
    anchor['daily_weekly_context']=context.acquire(anchor,audit_root=tmp_path,clock=lambda:NOW,transport=fetcher(native_rows()),monotonic=lambda:0)
    view={'identity':{'generated_at_utc':NOW.isoformat()},'components':{'oil':{'status':'READY','data_as_of':anchor['received_at'],'data':anchor}}}
    before=deepcopy(view)
    monkeypatch.setattr(source,'_fetch',lambda url:pytest.fail('reader must not fetch'))
    full=release.build(view,now=NOW,code_revision='a'*40)
    package=release.compact(view,now=NOW,code_revision='a'*40)
    fact=next(f for f in package['facts'] if f['factor']=='oil')['values']
    assert fact['price']==129 and fact['expiry']=='2026-10-01' and fact['price_field']=='CLOSE'
    assert fact['daily_weekly_context']['comparisons']['20obs']['change_abs']==20
    assert fact['daily_weekly_context']['audit_version_ref'] and fact['daily_weekly_context']['source_revision_id']
    assert 'raw_body' not in json.dumps(package) and 'payload_digest' not in json.dumps(package)
    assert release.compact(view,now=NOW,code_revision='a'*40)==package and view==before
    changed=deepcopy(full)
    next(f for f in changed['facts'] if f['factor']=='oil')['values']['daily_weekly_context']['comparisons']['1obs']['change_abs']=999
    with pytest.raises(AssertionError,match='oil history'): projection_completeness(view,changed,now=NOW)
    anchor['daily_weekly_context']['last_attempt']={'status':'FAILED','reason':'fixture outage'}
    failed=release.compact(view,now=NOW,code_revision='a'*40)
    fact=next(f for f in failed['facts'] if f['factor']=='oil')['values']
    assert fact['price']==129 and fact['daily_weekly_context']['status']=='UNAVAILABLE'
