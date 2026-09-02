from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as live


@pytest.mark.parametrize("field", ["OPEN", "HIGH", "LOW"])
def test_negative_futures_ohlc_price_fails_closed(field: str) -> None:
    row: dict[str, object] = {
        "SECID": "USDRUBF",
        "OPEN": 90.0,
        "HIGH": 92.0,
        "LOW": 89.0,
        "LAST": 91.0,
        "VOLTODAY": 100,
        "VALTODAY": 9_100_000,
        "NUMTRADES": 10,
        "OPENPOSITION": 1000,
        "BID": 90.9,
        "OFFER": 91.1,
        "SYSTIME": "2026-09-02 13:00:00",
    }
    row[field] = -1
    now = datetime(2026, 9, 2, 10, 0, 1, tzinfo=timezone.utc)

    with pytest.raises(
        live.SynchronizedLiveMarketOIError,
        match=rf"USDRUBF\.{field} must be nonnegative",
    ):
        live._normalize_row(
            logical_id="usdrubf",
            secid="USDRUBF",
            row=row,
            source_id=live.FORTS_SOURCE_ID,
            received_at_utc=now,
            freshness_reference_utc=now,
            is_future=True,
            security_row={
                "SECID": "USDRUBF",
                "BOARDID": "RFUD",
                "LASTTRADEDATE": "2099-12-31",
                "MINSTEP": 0.01,
                "STEPPRICE": 10.0,
            },
        )


def test_negative_spot_wap_price_fails_closed() -> None:
    row: dict[str, object] = {
        "SECID": "CNYRUB_TOM",
        "OPEN": 12.0,
        "HIGH": 12.2,
        "LOW": 11.9,
        "LAST": 12.1,
        "WAPRICE": -1,
        "VOLTODAY": 100,
        "NUMTRADES": 10,
        "BID": 12.09,
        "OFFER": 12.11,
        "SYSTIME": "2026-09-02 13:00:00",
    }
    now = datetime(2026, 9, 2, 10, 0, 1, tzinfo=timezone.utc)

    with pytest.raises(
        live.SynchronizedLiveMarketOIError,
        match=r"CNYRUB_TOM\.WAPRICE must be nonnegative",
    ):
        live._normalize_row(
            logical_id="cnyrub_tom",
            secid="CNYRUB_TOM",
            row=row,
            source_id=live.CETS_SOURCE_ID,
            received_at_utc=now,
            freshness_reference_utc=now,
            is_future=False,
        )
