"""Explicit factual admission of the ruble-settled CETS USD position instrument.

The legacy logical key is retained, but this is not deliverable USD spot. Live
admission replays the original response and uses the last-trade clock, never a
fresh exchange heartbeat to renew an old transaction.
"""
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
import base64
import json
import os
import requests

from moex_data import synchronized_live_market_oi_context as core

SCHEMA = 'usd_cets_reference_evidence.v1'
CONTRACT = 'contracts/intelligence/usd_cets_reference_v1.json'
SECID = 'USD000UTSTOM'
PATH = '/iss/engines/currency/markets/selt/boards/CETS/securities/'+SECID+'.json'
PARAMS = {'iss.meta':'off', 'iss.only':'securities,marketdata,dataversion'}
IDENTITY = {'logical_id':'usd_tom', 'secid':SECID, 'board_id':'CETS',
    'source_id':'moex_apim_cets_usd_ruble_settled_reference',
    'instrument_kind':'currency_position_instrument', 'settlement':'ruble_cash_settled_no_USD_delivery',
    'price_unit':'RUB_per_USD', 'deliverable_spot':False}
POLICY = {'project':'MOEX_Bot', 'schema_version':'usd_cets_reference_admission.v1',
    'identity':IDENTITY, 'effective_from':'2026-02-16',
    'official_semantics':'https://www.moex.com/n97672',
    'live_source':'https://apim.moex.com'+PATH,
    'dated_source':'https://iss.moex.com'+PATH.removesuffix('.json')+'/candles.json',
    'current_last_price_ttl_seconds':60, 'current_requires_trading_status':'A',
    'current_requires_native_marketdata_trade_date':True,
    'missing_native_trade_date':'last_received_observation_only_no_current_price_or_quote',
    'current_price_clock':'native_trade_date_and_last_trade_TIME',
    'dated_scope':'exact_own_5m_bar_intersection_no_fill',
    'basis_semantics':'futures_minus_ruble_settled_currency_position_reference',
    'carry_semantics':'annualized_relative_price_proxy_not_deliverable_currency_financing',
    'old_stage3_stage4_admission_changed':False, 'action_authority':False}


def require(ok, reason):
    if not ok: raise ValueError(reason)


def contract():
    value=json.loads((Path(__file__).resolve().parents[2]/CONTRACT).read_bytes())
    require(value == POLICY, 'USD_reference_contract_changed')
    return deepcopy(value)


def capture(*, now_fn=lambda:datetime.now(timezone.utc), http_get=requests.get, env=None, base_url=None, timeout=12, **ignored):
    active=os.environ if env is None else env
    start=core._aware_utc(now_fn(),'USD_capture_start')
    e={'schema_version':SCHEMA,'contract':contract(),'identity':deepcopy(IDENTITY),
       'requested_at_utc':start.isoformat(),'response':None,'error':None}
    def retain(url, **kwargs):
        response=http_get(url,**kwargs)
        raw=response.content
        require(isinstance(raw,bytes) and len(raw)<=1_000_000,'USD_response_byte_limit')
        e['response']={'bytes_base64':base64.b64encode(raw).decode(), 'sha256':sha256(raw).hexdigest(),
            'url':str(response.url),'http_status':response.status_code,
            'received_at_utc':core._aware_utc(now_fn(),'USD_receipt').isoformat()}
        return response
    try:
        core._fetch_json(url=core._api_base_url(base_url,active)+PATH,params=PARAMS,
            headers=core._auth_headers(active),timeout=timeout,http_get=retain,now_fn=now_fn)
    except Exception as exc: e['error']=type(exc).__name__
    return e


def _row(payload, table):
    block=payload[table];require(isinstance(block,dict),'USD_'+table+'_object')
    columns=block['columns']; rows=block['data']
    require(isinstance(columns,list) and all(isinstance(k,str) for k in columns)
        and isinstance(rows,list) and len(rows)==1 and isinstance(rows[0],list)
        and len(columns)==len(set(columns)) and len(rows[0])==len(columns), 'USD_'+table+'_shape')
    return dict(zip(columns,rows[0]))


def replay(e, *, now):
    now=core._aware_utc(now,'USD_read_clock')
    require(isinstance(e,dict) and set(e)=={'schema_version','contract','identity','requested_at_utc','response','error'}
        and e['schema_version']==SCHEMA and e['identity']==IDENTITY and e['contract']==contract(),'USD_outer_identity')
    response=e['response']
    require(isinstance(response,dict), 'USD_capture_'+str(e['error']))
    require(set(response)=={'bytes_base64','sha256','url','http_status','received_at_utc'}
        and isinstance(response['url'],str)
        and isinstance(response['bytes_base64'],str) and len(response['bytes_base64'])<=1_333_336,'USD_response_shape')
    raw=base64.b64decode(response['bytes_base64'],validate=True)
    require(sha256(raw).hexdigest()==response['sha256'],'USD_response_hash')
    start=core._aware_utc(e['requested_at_utc'],'USD_request')
    received=core._aware_utc(response['received_at_utc'],'USD_receipt')
    require(start<=received<=now,'USD_causal_clocks')
    url=urlsplit(response['url'])
    require((url.scheme,url.netloc,url.path)==('https','apim.moex.com',PATH) and not url.fragment
        and parse_qs(url.query)=={k:[v] for k,v in PARAMS.items()},'USD_response_route')
    require(e['error'] is None and response['http_status']==200,'USD_transport_refused')
    payload=json.loads(raw);require(isinstance(payload,dict),'USD_response_not_object')
    require(not payload.get('ERROR_MESSAGE'),'USD_ERROR_MESSAGE')
    security=_row(payload,'securities'); row=_row(payload,'marketdata'); version=_row(payload,'dataversion')
    require(all(x.get('SECID')==SECID and x.get('BOARDID')=='CETS' for x in (security,row))
        and security.get('FACEUNIT')=='USD' and security.get('CURRENCYID')=='RUB','USD_native_identity')
    day=version['trade_date']
    require(isinstance(day,str) and datetime.fromisoformat(day).date().isoformat()==day
        and version.get('trade_session_date')==day and '2026-02-16'<=day<=now.astimezone(core.MOSCOW).date().isoformat(),'USD_trade_date')
    require(type(row.get('NUMTRADES')) is int and row['NUMTRADES']>=0,'USD_invalid_numtrades')
    has_trades=row['NUMTRADES']>0
    update=core._source_event_time(row['SYSTIME'],'USD_source_update')
    require(update<=received,'USD_update_receipt_order')
    require(update.astimezone(core.MOSCOW).date().isoformat()==day,'USD_update_session_date_mismatch')
    native_day=row.get('TRADEDATE')
    try:
        valid_day=isinstance(native_day,str) and datetime.fromisoformat(native_day).date().isoformat()==native_day
    except ValueError: valid_day=False
    date_verified=valid_day and native_day==day
    trade=None
    if date_verified and has_trades:
        trade=core._source_event_time(native_day+'T'+row['TIME'],'USD_last_trade')
        require(trade<=update,'USD_event_update_receipt_order')
    normalized=core._normalize_row(logical_id='usd_tom',secid=SECID,row=row,source_id=IDENTITY['source_id'],
        received_at_utc=received,freshness_reference_utc=now,is_future=False)
    age=(now-trade).total_seconds() if trade is not None else None
    normalized.update(IDENTITY, timestamp=trade.isoformat() if trade is not None else None,
        source_trade_date=native_day if valid_day else None,native_trade_date=native_day,
        source_version_trade_date=day,native_trade_date_verified=date_verified,
        timestamp_semantics=('no_same_session_trade' if not has_trades else
            'native_last_trade_date_and_TIME' if date_verified else 'native_last_trade_date_unproven'),
        source_update_timestamp_utc=update.isoformat(),
        asset_type='ruble_settled_currency_position_reference',age_seconds=age,
        stale=not has_trades or not date_verified or age>60 or row.get('TRADINGSTATUS')!='A',
        reference_semantics=POLICY['basis_semantics'],carry_semantics=POLICY['carry_semantics'])
    normalized['spot_price_usable']=normalized['stale'] is False and normalized.get('last') is not None and normalized['last']>0
    if normalized['stale']:
        reason=('USD_native_trade_date_mismatch' if valid_day else 'USD_native_trade_date_missing_or_invalid') if not date_verified else (
            'source_not_trading' if row.get('TRADINGSTATUS')!='A' else 'source_age_exceeds_threshold')
        if not has_trades: reason='USD_no_same_session_trades'
        normalized.update(quote_usable=False,quote_stale=True,
            read_freshness_reason=reason)
    return normalized


def verified_observation(data, *, now):
    """Validate observation facts independently of their current-price admission."""
    row=replay(data['usd_reference_evidence'],now=now)
    stored=data['instruments']['usd_tom']
    require(isinstance(stored,dict),'USD_normalized_not_object')
    # Freshness diagnostics may be downgraded by the canonical reader. Facts,
    # identity and original clocks may never be substituted after replay.
    fields=(*IDENTITY,'last','open','high','low','timestamp','source_trade_date','source_trading_status',
            'native_trade_date','source_version_trade_date','native_trade_date_verified','last_trade_time_moscow',
            'source_update_timestamp_utc','received_at_utc','timestamp_semantics','reference_semantics','carry_semantics','bid','ask','spread','wap','volume','trades')
    require(type(stored.get('trades')) is int and all(stored.get(k)==row.get(k) for k in fields),
            'USD_normalized_original_mismatch')
    return row


def observation_verified(data, *, now):
    try:
        verified_observation(data,now=now)
        return True
    except (ValueError,KeyError,TypeError,core.SynchronizedLiveMarketOIError): return False


def usable(data, *, now):
    try:
        row=verified_observation(data,now=now)
        stored=data['instruments']['usd_tom']
        return row['spot_price_usable'] and stored.get('stale') is False
    except (ValueError,KeyError,TypeError,core.SynchronizedLiveMarketOIError): return False


def attach(snapshot,e,*,now):
    snapshot['usd_reference_evidence']=e
    snapshot['snapshot_received_at_utc']=core._aware_utc(now,'USD_attach_clock').isoformat()
    try: row=replay(e,now=now)
    except (ValueError,KeyError,TypeError,core.SynchronizedLiveMarketOIError) as exc:
        row={**IDENTITY,'stale':True,'spot_price_usable':False,'last':None,'quote_usable':False,
             'read_freshness_reason':str(exc),'evidence_ref':'usd_reference_evidence'}
    snapshot.setdefault('instruments',{})['usd_tom']=row
    snapshot.setdefault('bindings',{})['usd_tom']=SECID
    snapshot['quality']['usd_reference_usable']=row['spot_price_usable']
    recheck_synchronization(snapshot,now=now)
    return snapshot


def recheck_synchronization(snapshot,*,now):
    """Downgrade full-market gates over all eight legs; keep individual facts."""
    now=core._aware_utc(now,'USD_sync_clock')
    instruments=snapshot.get('instruments',{})
    sync=snapshot.setdefault('synchronization',{})
    quality=snapshot.setdefault('quality',{})
    if not isinstance(sync,dict): sync={};snapshot['synchronization']=sync
    if not isinstance(quality,dict): quality={};snapshot['quality']=quality
    required=(*core.LOGICAL_ORDER,'usd_tom')
    timestamps={}
    all_fresh=False
    try:
        timestamps={key:core._aware_utc(instruments[key]['timestamp'],key+'.timestamp') for key in required}
        all_fresh=all(instruments[key].get('stale') is False and
            -core.MAX_FUTURE_CLOCK_SKEW_SECONDS <= (now-value).total_seconds() <= core.MAX_FRESHNESS_SECONDS
            for key,value in timestamps.items())
    except (ValueError,KeyError,TypeError,core.SynchronizedLiveMarketOIError): pass
    complete=len(timestamps)==len(required)
    dates={value.astimezone(core.MOSCOW).date() for value in timestamps.values()}
    oldest=min(timestamps.values()) if complete else None
    newest=max(timestamps.values()) if complete else None
    skew=(newest-oldest).total_seconds() if complete else None
    usd_allowed=usable(snapshot,now=now)
    combined=bool(complete and all_fresh and len(dates)==1 and skew<=core.MAX_SKEW_SECONDS and usd_allowed)
    sync.update(synchronized=sync.get('synchronized') is True and combined,
        all_instruments_fresh=all_fresh,as_of_utc=core._iso(newest) if complete else None,
        oldest_timestamp_utc=core._iso(oldest) if complete else None,max_skew_seconds=round(skew,3) if skew is not None else None,
        freshness_reference_utc=now.isoformat(),instrument_scope=list(required),
        source_trade_dates_aligned=complete and len(dates)==1)
    sync['status']='PASS' if sync['synchronized'] else 'FAIL'
    quality['usd_reference_usable']=usd_allowed
    quality['analysis_usable']=quality.get('analysis_usable') is True and sync['synchronized'] and usd_allowed
    quotes=quality.setdefault('quote_usable_by_instrument',{})
    if not isinstance(quotes,dict): quotes={};quality['quote_usable_by_instrument']=quotes
    quotes['usd_tom']=usd_allowed and instruments.get('usd_tom',{}).get('quote_usable') is True
    quality['quote_all_instruments_usable']=bool(set(quotes)==set(required) and all(quotes.values()))
    if not quality['analysis_usable']:
        if snapshot.get('status')=='READY': snapshot['status']='PARTIAL'
        if quality.get('status')=='PASS': quality['status']='PARTIAL'


def annotate_basis(derived):
    pair=derived.get('pairs',{}).get('usd_rub',{})
    for metric in pair.get('metrics',[]):
        if 'usd_tom' in metric.get('legs',[]):
            metric.update(reference_identity=deepcopy(IDENTITY),reference_semantics=POLICY['basis_semantics'],
                          carry_semantics=POLICY['carry_semantics'])
    return derived
