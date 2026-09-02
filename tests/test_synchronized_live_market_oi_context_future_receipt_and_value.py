from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as live


def test_future_timestamp_is_bounded_against_row_receipt_not_snapshot_completion() -> None:
    row = {
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
        "SYSTIME": "2026-09-02 13:00:20",
    }
    received_at = datetime(2026, 9, 2, 10, 0, 0, tzinfo=timezone.utc)
    completion_at = datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc)

    with pytest.raises(
        live.SynchronizedLiveMarketOIError,
        match=r"USDRUBF\.SYSTIME is 20\.000s ahead of row receipt",
    ):
        live._normalize_row(
            logical_id="usdrubf",
            secid="USDRUBF",
            row=row,
            source_id=live.FORTS_SOURCE_ID,
            received_at_utc=received_at,
            freshness_reference_utc=completion_at,
            is_future=True,
            security_row={
                "SECID": "USDRUBF",
                "BOARDID": "RFUD",
                "LASTTRADEDATE": "2099-12-31",
                "MINSTEP": 0.01,
                "STEPPRICE": 10.0,
            },
        )


def test_negative_forts_traded_value_fails_closed_before_wap_derivation() -> None:
    with pytest.raises(
        live.SynchronizedLiveMarketOIError,
        match=r"USDRUBF\.VALTODAY must be nonnegative",
    ):
        live._forts_session_wap(
            secid="USDRUBF",
            marketdata_row={"VOLTODAY": 100, "VALTODAY": -1},
            security_row={"MINSTEP": 0.01, "STEPPRICE": 10.0},
        )
