from copy import deepcopy
from datetime import datetime,timedelta,timezone
from moex_data import rub_currency_market_state as state
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

NOW=datetime(2026,9,29,17,49,tzinfo=timezone.utc)

def test_nontrading_source_heartbeat_cannot_make_CNY_current():
    row={'last':12.4,'secid':'CNYRUB_TOM','source_id':'test','timestamp':NOW.isoformat(),
         'source_update_timestamp_utc':NOW.isoformat(),'received_at_utc':NOW.isoformat(),
         'source_trading_status':'N','stale':False,'spot_price_usable':True,'quote_usable':True}
    snapshot={'components':{'synchronized_live_market_oi':{'status':'READY','data':{
        'instruments':{'cnyrub_tom':row},'synchronization':{},'quality':{'spot_price_usable':True}}}}}
    before=deepcopy(snapshot)
    read=apply_read_freshness(snapshot,now=NOW)
    result=read['components']['synchronized_live_market_oi']['data']['instruments']['cnyrub_tom']
    assert result['stale'] and not result['spot_price_usable'] and not result['quote_usable']
    presentation=result['market_state']
    assert presentation['state']=='NOT_TRADING_OBSERVED'
    assert presentation['last_observation']['values']['last']==12.4
    assert presentation['last_observation']['current_use_allowed'] is False
    assert presentation['session_completion_proven'] is False
    assert snapshot==before

def test_stale_active_row_and_missing_source_do_not_become_closed_session():
    row={'last':12.4,'timestamp':(NOW-timedelta(hours=2)).isoformat(),'source_trading_status':'A',
         'stale':True,'read_freshness_reason':'source_age_exceeds_threshold'}
    result=state.describe(row,now=NOW)
    assert result['state']=='TRADING_OBSERVED' and result['current_price_usable'] is False
    assert state.describe({},now=NOW)['state']=='SOURCE_UNAVAILABLE'
    assert state.describe({'last':12,'timestamp':NOW.isoformat()},now=NOW)['state']=='TRADING_STATE_UNKNOWN'
