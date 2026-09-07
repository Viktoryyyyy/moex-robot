"""Describe observed rows without inferring a trading calendar or session close."""
from collections.abc import Mapping
from datetime import datetime, timezone
from moex_data.synchronized_live_market_oi_context import LOGICAL_ORDER

POLICY = "observed_source_rows_without_calendar_inference.v1"


def build_context(instruments, *, now):
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("aware read time required")
    instruments = instruments if isinstance(instruments, Mapping) else {}
    rows = {}
    for key in LOGICAL_ORDER:
        item = instruments.get(key)
        item = item if isinstance(item, Mapping) else {}
        raw_time = item.get("timestamp")
        age = None
        try:
            timestamp = datetime.fromisoformat(raw_time)
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                raise ValueError("naive source time")
            age = (now - timestamp).total_seconds()
            if age < -5:
                state, reason = "INVALID_SOURCE_TIME", "future_source_timestamp"
            elif age > 60:
                state, reason = "STALE_SOURCE_ROW", "source_age_exceeds_threshold"
            elif item.get("stale") is not False:
                state, reason = "SOURCE_ROW_REJECTED", "existing_source_gate_rejected"
            else:
                state, reason = "FRESH_SOURCE_ROW", None
        except (ValueError, TypeError, OverflowError):
            state, reason = "SOURCE_TIME_UNKNOWN", "missing_or_invalid_source_timestamp"
        rows[key] = {
            "observation_state": state,
            "reason": reason,
            "source_age_seconds": age,
            "source_update_timestamp_utc": raw_time,
            "source_trade_date": item.get("source_trade_date"),
            "source_trading_status_raw": item.get("source_trading_status"),
            "last_trade_time_moscow_raw": item.get("last_trade_time_moscow"),
            "session_state": "UNKNOWN",
            "session_close_proven": False,
            "last_trade_freshness_proven": False,
            "price_oi_usable": item.get("price_oi_usable") is True and state == "FRESH_SOURCE_ROW",
            "quote_usable": item.get("quote_usable") is True and state == "FRESH_SOURCE_ROW",
        }
    return {
        "policy": POLICY,
        "read_at_utc": now.astimezone(timezone.utc).isoformat(),
        "calendar_dependency": False,
        "calendar_acceptance_status": "NOT_ESTABLISHED",
        "weekday_weekend_inference": False,
        "absence_proves_nontrading_day": False,
        "stale_row_proves_session_closed": False,
        "fresh_update_proves_recent_trade": False,
        "future_session_schedule_available": False,
        "instruments": rows,
    }
