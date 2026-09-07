from copy import deepcopy
from datetime import datetime, timedelta, timezone
import pytest
from moex_data.rub_source_observation import build_context
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

NOW = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)  # Sunday, no weekday inference.


def row(timestamp=None):
    return {"timestamp": timestamp or NOW.isoformat(), "stale": False,
            "price_oi_usable": True, "quote_usable": False,
            "source_trade_date": "2026-09-07", "source_trading_status": "opaque"}


@pytest.mark.parametrize("offset,expected", [(0,"FRESH_SOURCE_ROW"),(-60,"FRESH_SOURCE_ROW"),
    (-60.001,"STALE_SOURCE_ROW"),(6,"INVALID_SOURCE_TIME")])
def test_observation_is_not_session_state(offset, expected):
    source = {"si_front": row((NOW + timedelta(seconds=offset)).isoformat())}
    before = deepcopy(source)
    result = build_context(source, now=NOW)
    assert source == before
    item = result["instruments"]["si_front"]
    assert item["observation_state"] == expected
    assert item["session_state"] == "UNKNOWN"
    assert not item["session_close_proven"]
    assert item["source_trade_date"] == "2026-09-07"
    assert item["source_trading_status_raw"] == "opaque"
    assert not result["weekday_weekend_inference"]


@pytest.mark.parametrize("timestamp", [None, "", "bad", "2026-09-06T12:00:00"])
def test_missing_time_is_unknown_not_closed(timestamp):
    result = build_context({"si_front": {"timestamp":timestamp}}, now=NOW)
    item = result["instruments"]["si_front"]
    assert item["observation_state"] == "SOURCE_TIME_UNKNOWN"
    assert not item["price_oi_usable"]
    assert not item["session_close_proven"]


def test_missing_instruments_have_explicit_unknown_state():
    result = build_context({}, now=NOW)
    assert len(result["instruments"]) == 7
    assert all(v["observation_state"] == "SOURCE_TIME_UNKNOWN" for v in result["instruments"].values())
    assert not result["absence_proves_nontrading_day"]


def test_quality_rejection_is_preserved():
    item = row()
    item["stale"] = True
    assert build_context({"si_front":item},now=NOW)["instruments"]["si_front"]["observation_state"] == "SOURCE_ROW_REJECTED"


def test_midnight_does_not_invent_trade_date_or_session_close():
    now = datetime(2026,9,6,21,0,10,tzinfo=timezone.utc)
    item = row((now-timedelta(seconds=20)).isoformat())
    item["source_trade_date"] = None
    result = build_context({"si_front":item},now=now)["instruments"]["si_front"]
    assert result["observation_state"] == "FRESH_SOURCE_ROW"
    assert result["source_trade_date"] is None
    assert result["session_state"] == "UNKNOWN"


def test_api_context_uses_downgraded_rows_and_never_adds_authority():
    source = {"components":{"synchronized_live_market_oi":{"status":"UNAVAILABLE","data":{
        "instruments":{"si_front":row((NOW-timedelta(hours=2)).isoformat())},
        "quality":{}, "synchronization":{}}}}, "authority":{"broker_execution":False}}
    result = apply_read_freshness(source,now=NOW)
    item = result["source_observation_context"]["instruments"]["si_front"]
    assert item["observation_state"] == "STALE_SOURCE_ROW"
    assert not item["price_oi_usable"]
    assert not result["authority"]["broker_execution"]
