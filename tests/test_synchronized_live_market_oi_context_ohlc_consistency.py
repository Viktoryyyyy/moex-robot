from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as live


def _normalize(*, open_price: float = 90.0, high: float = 92.0, low: float = 89.0, last: float = 91.0) -> dict[str, object]:
    row = {
        "SYSTIME": "2026-09-02 13:00:00",
        "OPEN": open_price,
        "HIGH": high,
        "LOW": low,
        "LAST": last,
        "VOLTODAY": 100,
        "VALTODAY": 9050,
        "NUMTRADES": 10,
        "OPENPOSITION": 1000,
        "BID": 90.9,
        "OFFER": 91.1,
    }
    security = {"MINSTEP": 1.0, "STEPPRICE": 1.0}
    received = datetime(2026, 9, 2, 10, 0, 5, tzinfo=timezone.utc)
    return live._normalize_row(
        logical_id="si_front",
        secid="SiU6",
        row=row,
        source_id="test",
        received_at_utc=received,
        freshness_reference_utc=received,
        is_future=True,
        security_row=security,
    )


def test_inverted_high_low_range_fails_closed() -> None:
    with pytest.raises(live.SynchronizedLiveMarketOIError, match="HIGH must be greater than or equal to LOW"):
        _normalize(high=88.0, low=89.0)


@pytest.mark.parametrize(
    ("open_price", "last", "message"),
    [
        (93.0, 91.0, "OPEN must not exceed HIGH"),
        (88.0, 91.0, "OPEN must not be below LOW"),
        (90.0, 93.0, "LAST must not exceed HIGH"),
        (90.0, 88.0, "LAST must not be below LOW"),
    ],
)
def test_open_and_last_must_stay_inside_session_range(
    open_price: float,
    last: float,
    message: str,
) -> None:
    with pytest.raises(live.SynchronizedLiveMarketOIError, match=message):
        _normalize(open_price=open_price, last=last)
