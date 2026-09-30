"""Display native non-trading state without making an old price current."""
from copy import deepcopy
from datetime import datetime, timezone

SCHEMA = 'rub_currency_market_state.v1'


def describe(row, *, now, current_admitted=False, usd_evidence_verified=None):
    usd_selected = (row.get('source_id') == 'moex_apim_cets_usd_ruble_settled_reference'
                    or usd_evidence_verified is not None)
    invalid_usd = usd_selected and usd_evidence_verified is not True
    status = row.get('source_trading_status')
    reason = row.get('read_freshness_reason')
    # A valid USD response with no executions is source availability evidence,
    # even when LAST is null. Do not infer a trade or decode other native status
    # codes (for example T) as an active/closed session.
    no_trades = (usd_selected and not invalid_usd
                 and type(row.get('trades')) is int and row['trades'] == 0)
    observed = not invalid_usd and row.get('last') is not None and (row.get('timestamp') is not None or row.get('received_at_utc') is not None)
    try:
        age=(now-datetime.fromisoformat(row['timestamp'])).total_seconds()
        fresh=-5<=age<=60
    except (KeyError,ValueError,TypeError): fresh=False
    state = ('SOURCE_UNAVAILABLE' if invalid_usd else 'NOT_TRADING_OBSERVED' if status == 'N' else 'NO_TRADES_OBSERVED' if no_trades else 'TRADING_OBSERVED' if status == 'A'
             else 'SOURCE_UNAVAILABLE' if not observed else 'TRADING_STATE_UNKNOWN')
    return {'schema_version':SCHEMA, 'state':state, 'native_trading_status':None if invalid_usd else status,
        **({'source_evidence_verified':not invalid_usd} if usd_selected else {}),
        'reported_trade_count':None if invalid_usd else row.get('trades'),
        'trade_activity':'NO_TRADES_OBSERVED' if no_trades else 'NOT_CLASSIFIED',
        'checked_at_utc':now.isoformat(), 'source_status_at_utc':None if invalid_usd else row.get('source_update_timestamp_utc'),
        'current_price_usable':bool(current_admitted and fresh and observed and row.get('stale') is False and status != 'N' and not no_trades),
        'freshness_reason':'USD_reference_evidence_invalid' if invalid_usd else reason, 'session_completion_proven':False,
        'last_observation':({'status':'AVAILABLE_OBSERVED', 'current_use_allowed':False,
            'scope':'last_received_source_observation_not_verified_session_close',
            'values':{k:deepcopy(row[k]) for k in ('last','secid','source_id','source_trade_date','timestamp',
                'timestamp_semantics','source_update_timestamp_utc','last_trade_time_moscow','received_at_utc',
                'native_trade_date','source_version_trade_date','native_trade_date_verified','trades','source_trading_status',
                'price_unit','instrument_kind','settlement','deliverable_spot') if k in row}}
            if observed else None)}


def annotate(row, *, now, current_admitted=False, usd_evidence_verified=None):
    row['market_state'] = describe(row, now=now, current_admitted=current_admitted,
                                   usd_evidence_verified=usd_evidence_verified)


def enforce(row):
    """Native N revokes current use even if SYSTIME continues ticking."""
    if row.get('source_trading_status') == 'N':
        row.update(stale=True,spot_price_usable=False,quote_usable=False,quote_stale=True)
        row['read_freshness_reason'] = row.get('read_freshness_reason') or 'source_not_trading'
