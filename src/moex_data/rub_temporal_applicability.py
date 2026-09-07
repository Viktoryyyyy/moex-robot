"""Separate current facts, dated observations and slow published prices at read time."""
from collections.abc import Mapping
from datetime import date, datetime, timezone

from moex_data.futures.futoi_current_pair_authority import check_time, MAX_AGE_SECONDS

POLICY = "rub_temporal_applicability.v1"


def _previous_witness(context, now):
    try:
        values = context['observed_trade_dates']
        if not isinstance(values, list) or not values or values != sorted(set(values)):
            return None
        for value in values:
            if date.fromisoformat(value).isoformat() != value:
                return None
        current = context.get('observed_current_trade_date')
        if current is not None and current != values[-1]:
            return None
        prior = values[:-1] if current is not None else values
        expected = context.get('previous_observed_trade_date')
        # Compare only dates proven by the source witness, never infer weekdays.
        return expected if prior and expected == prior[-1] else None
    except (ValueError, TypeError, KeyError):
        return None


def _receipt_fresh(value, now):
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.utcoffset() is not None and 0 <= (now - parsed).total_seconds() <= MAX_AGE_SECONDS
    except (ValueError, TypeError, OverflowError):
        return False


def apply(snapshot, *, now):
    """Mutate only the independent read view; never upgrade consumer permissions."""
    views = {}
    components = snapshot.get("components", {})
    for name, instrument in (("futoi_live", "si_futures_family"), ("futoi_live_cr", "cr_futures_family")):
        component = components.get(name, {})
        data = component.get("data")
        if not isinstance(data, dict) or "current_intraday" not in data:
            continue
        record = data.get("current_intraday")
        try:
            check_time(record, now)
            reason = None
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            reason = str(exc)
        current_allowed = (reason is None and component.get("status") == "READY"
                           and data.get("consumer_factual_use_allowed") is True
                           and data.get("factual_authority") is True)
        if not current_allowed:
            # A fresh previous-date observation cannot keep current authority alive.
            component["status"] = "UNAVAILABLE"
            data["factual_authority"] = data["consumer_factual_use_allowed"] = False
            if isinstance(record, dict):
                record["consumer_factual_use_allowed"] = False
            authority = snapshot.get("authority", {})
            authority.get("futoi_by_instrument", {}).get(instrument, {})["factual_authority"] = False
            if instrument == "si_futures_family":
                authority["futoi_factual_authority"] = False
        previous = data.get("previous_completed_session")
        previous = previous if isinstance(previous, Mapping) else {}
        fact = previous.get("factual")
        fact = fact if isinstance(fact, Mapping) else {}
        context = data.get("context_refresh")
        context = context if isinstance(context, Mapping) else {}
        expected = _previous_witness(context, now)
        dated_available = bool(expected and fact.get("trade_date") == expected
            and previous.get("status") == "FRESH"
            and _receipt_fresh(previous.get("last_success_at"), now))
        views[name] = {
            "current": {"kind": "CURRENT_SOURCE_PAIR", "usable": current_allowed,
                "reason": reason if reason else (None if current_allowed else "persisted_admission_blocked"),
                "maximum_source_and_receipt_age_seconds": MAX_AGE_SECONDS},
            "previous": {"kind": "PREVIOUS_OBSERVED_DATE_FACT",
                "source_trade_date": fact.get("trade_date"), "expected_observed_date": expected,
                "dated_observation_available": dated_available,
                "session_completion_state": "UNKNOWN", "session_completion_proven": False,
                "current_use_allowed": False, "authority_granted_by_this_view": False,
                "legacy_field": "previous_completed_session"},
        }
    oil = components.get("oil", {})
    oil_data = oil.get("data")
    if isinstance(oil_data, Mapping):
        views["oil"] = {"kind": "SLOW_PUBLISHED_DAILY_PRICE",
            "source_trade_date": oil_data.get("source_trade_date"),
            "received_at": oil_data.get("received_at"),
            "price_event_time": oil_data.get("source_event_time"),
            "intraday_use_allowed": False,
            "dated_price_use_allowed": oil.get("status") == "READY" and oil_data.get("consumer_factual_use_allowed") is True,
            "freshness_basis": "receipt_recheck_does_not_change_price_date",
            "historical_pit_acceptance": False}
    snapshot["temporal_applicability"] = {"policy": POLICY,
        "read_at_utc": now.astimezone(timezone.utc).isoformat(), "components": views,
        "session_state": "UNKNOWN", "calendar_coverage_accepted": False,
        "absence_or_expiry_proves_closed_session": False,
        "authority_granted_by_this_view": False}
