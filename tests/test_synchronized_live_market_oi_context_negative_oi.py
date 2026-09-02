from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as live


def test_negative_futures_open_interest_fails_closed() -> None:
    row = {
        "OPEN": 90.0,
        "HIGH": 92.0,
        "LOW": 89.0,
        "LAST": 91.0,
        "VOLTODAY": 100,
        "VALTODAY": 9_050_000,
        "NUMTRADES": 10,
        "OPENPOSITION": -1,
        "BID": 90.9,
        "OFFER": 91.1,
        "SYSTIME": "2026-09-02 13:00:00",
    }
    observed_at = datetime(2026, 9, 2, 10, 0, 5, tzinfo=timezone.utc)

    with pytest.raises(live.SynchronizedLiveMarketOIError, match="OPENPOSITION must be nonnegative"):
        live._normalize_row(
            logical_id="usdrubf",
            secid="USDRUBF",
            row=row,
            source_id=live.FORTS_SOURCE_ID,
            received_at_utc=observed_at,
            freshness_reference_utc=observed_at,
            is_future=True,
            security_row={"MINSTEP": 0.01, "STEPPRICE": 10.0},
        )
