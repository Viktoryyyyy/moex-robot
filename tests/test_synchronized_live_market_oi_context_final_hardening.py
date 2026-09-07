from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as live


RECEIVED = datetime(2026, 9, 2, 10, 0, 5, tzinfo=timezone.utc)


def _future_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "SYSTIME": "2026-09-02 13:00:00",
        "OPEN": 90.0,
        "HIGH": 92.0,
        "LOW": 89.0,
        "LAST": 91.0,
        "VOLTODAY": 100,
        "VALTODAY": 9050,
        "NUMTRADES": 10,
        "OPENPOSITION": 1000,
        "BID": 90.9,
        "OFFER": 91.1,
    }
    row.update(overrides)
    return row


def _normalize_future(**overrides: object) -> dict[str, object]:
    return live._normalize_row(
        logical_id="si_front",
        secid="SiU6",
        row=_future_row(**overrides),
        source_id="test",
        received_at_utc=RECEIVED,
        freshness_reference_utc=RECEIVED,
        is_future=True,
        security_row={"MINSTEP": 1.0, "STEPPRICE": 1.0},
    )


def test_source_update_never_becomes_last_trade_time() -> None:
    item = _normalize_future(TIME="12:00:01", TRADEDATE="2026-09-02")
    assert item["timestamp"] == item["source_update_timestamp_utc"]
    assert item["timestamp_semantics"] == "source_row_update_time_not_last_trade_time"
    assert item["last_trade_time_moscow"] == "12:00:01"
    assert item["source_trade_date"] == "2026-09-02"
    assert item["price_oi_usable"] is False


def test_missing_trade_date_is_not_inferred_from_source_update() -> None:
    item = _normalize_future(TIME="18:59:59", TRADEDATE=float("nan"))
    assert item["source_trade_date"] is None
    assert item["source_trading_status"] is None
    assert item["last_trade_time_moscow"] == "18:59:59"


def _normalize_spot(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "SYSTIME": "2026-09-02 13:00:00",
        "OPEN": 12.0,
        "HIGH": 12.2,
        "LOW": 11.9,
        "LAST": 12.1,
        "WAPRICE": 12.05,
        "VOLTODAY": 100,
        "NUMTRADES": 10,
        "BID": 12.09,
        "OFFER": 12.11,
    }
    row.update(overrides)
    return live._normalize_row(
        logical_id="cnyrub_tom",
        secid="CNYRUB_TOM",
        row=row,
        source_id="test",
        received_at_utc=RECEIVED,
        freshness_reference_utc=RECEIVED,
        is_future=False,
    )


def test_api_base_url_accepts_only_canonical_https_origin() -> None:
    assert live._api_base_url(None, {}) == "https://apim.moex.com"
    assert live._api_base_url("https://apim.moex.com/", {}) == "https://apim.moex.com"

    rejected = (
        "http://apim.moex.com",
        "https://example.com",
        "https://apim.moex.com:8443",
        "https://user:pass@apim.moex.com",
        "https://apim.moex.com/path",
        "https://apim.moex.com?x=1",
    )
    for value in rejected:
        with pytest.raises(live.SynchronizedLiveMarketOIError):
            live._api_base_url(value, {})


def test_crossed_bid_offer_fails_quote_closed_without_poisoning_factual_row() -> None:
    item = _normalize_future(BID=91.2, OFFER=91.1)

    assert item["bid"] == pytest.approx(91.2)
    assert item["ask"] == pytest.approx(91.1)
    assert item["spread"] is None
    assert item["quote_usable"] is False
    assert item["quote_status"] == "crossed_quote_unusable"
    assert item["quote_temporal_coherence"] == "unproven_for_crossed_quote"
    assert item["last"] == pytest.approx(91.0)
    assert item["oi"] == 1000


def test_futures_wap_outside_session_range_fails_closed() -> None:
    with pytest.raises(live.SynchronizedLiveMarketOIError, match="WAP must not exceed HIGH"):
        _normalize_future(VALTODAY=9300)


def test_spot_wap_outside_session_range_fails_closed() -> None:
    with pytest.raises(live.SynchronizedLiveMarketOIError, match="WAP must not be below LOW"):
        _normalize_spot(WAPRICE=11.8)


def test_wap_rounding_tolerance_is_allowed() -> None:
    item = _normalize_spot(WAPRICE=12.200000001)
    assert item["wap"] == pytest.approx(12.200000001)
