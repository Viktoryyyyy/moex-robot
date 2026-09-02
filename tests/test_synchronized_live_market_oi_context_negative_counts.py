from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as live


@pytest.mark.parametrize(
    ("field", "value"),
    [("VOLTODAY", -1), ("NUMTRADES", -1)],
)
def test_negative_session_counts_fail_closed(field: str, value: int) -> None:
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
    row[field] = value
    now = datetime(2026, 9, 2, 10, 0, 10, tzinfo=timezone.utc)

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
