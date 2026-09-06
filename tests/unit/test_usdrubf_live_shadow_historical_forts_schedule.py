from __future__ import annotations

from datetime import datetime

import pytest

from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    FORTS_UNIFIED_SESSION_START_DATE,
    LiveShadowBridgeError,
    MOSCOW,
    build_closed_15m_bars,
)


def _bar(day: str, clock: str, value: float) -> dict[str, object]:
    return {
        "end": datetime.fromisoformat(f"{day}T{clock}:00").replace(tzinfo=MOSCOW),
        "open": value,
        "high": value + 0.1,
        "low": value - 0.1,
        "close": value,
        "volume": 100.0,
    }


def test_pre_unified_session_1400_clearing_gap_skips_only_incomplete_bucket() -> None:
    bars = (
        _bar("2025-08-15", "13:45", 80.00),
        _bar("2025-08-15", "13:50", 80.05),
        _bar("2025-08-15", "13:55", 80.10),
        _bar("2025-08-15", "14:05", 80.15),
        _bar("2025-08-15", "14:10", 80.20),
        _bar("2025-08-15", "14:15", 80.25),
        _bar("2025-08-15", "14:20", 80.30),
        _bar("2025-08-15", "14:25", 80.35),
    )

    aggregated = build_closed_15m_bars(bars)

    assert [row["end"] for row in aggregated] == [
        "2025-08-15T13:45:00+03:00",
        "2025-08-15T14:15:00+03:00",
    ]
    assert aggregated[-1]["close"] == pytest.approx(80.35)


def test_pre_unified_session_1900_evening_clearing_gap_skips_only_incomplete_bucket() -> None:
    bars = (
        _bar("2025-08-15", "18:30", 80.00),
        _bar("2025-08-15", "18:35", 80.05),
        _bar("2025-08-15", "18:40", 80.10),
        _bar("2025-08-15", "18:45", 80.15),
        _bar("2025-08-15", "18:50", 80.20),
        _bar("2025-08-15", "19:05", 80.25),
        _bar("2025-08-15", "19:10", 80.30),
        _bar("2025-08-15", "19:15", 80.35),
        _bar("2025-08-15", "19:20", 80.40),
        _bar("2025-08-15", "19:25", 80.45),
    )

    aggregated = build_closed_15m_bars(bars)

    assert [row["end"] for row in aggregated] == [
        "2025-08-15T18:30:00+03:00",
        "2025-08-15T19:15:00+03:00",
    ]
    assert aggregated[-1]["close"] == pytest.approx(80.45)


def test_unified_session_start_date_does_not_allow_legacy_1400_gap() -> None:
    day = FORTS_UNIFIED_SESSION_START_DATE.isoformat()
    bars = (
        _bar(day, "13:45", 80.00),
        _bar(day, "13:50", 80.05),
        _bar(day, "13:55", 80.10),
        _bar(day, "14:05", 80.15),
        _bar(day, "14:10", 80.20),
    )

    with pytest.raises(LiveShadowBridgeError, match="broken 15m bucket.*14:00:00"):
        build_closed_15m_bars(bars)


def test_unified_session_start_date_does_not_allow_legacy_1900_gap() -> None:
    day = FORTS_UNIFIED_SESSION_START_DATE.isoformat()
    bars = (
        _bar(day, "18:45", 80.00),
        _bar(day, "18:50", 80.05),
        _bar(day, "19:05", 80.10),
        _bar(day, "19:10", 80.15),
    )

    with pytest.raises(LiveShadowBridgeError, match="broken 15m bucket.*19:00:00"):
        build_closed_15m_bars(bars)


def test_non_clearing_historical_gap_remains_fail_closed() -> None:
    bars = (
        _bar("2025-08-15", "13:00", 80.00),
        _bar("2025-08-15", "13:10", 80.05),
        _bar("2025-08-15", "13:25", 80.10),
    )

    with pytest.raises(LiveShadowBridgeError, match="broken 15m bucket.*13:15:00"):
        build_closed_15m_bars(bars)


def test_broken_bucket_reports_exact_missing_source_bars() -> None:
    bars = (
        _bar("2026-09-06", "11:45", 80.00),
        _bar("2026-09-06", "11:50", 80.05),
        _bar("2026-09-06", "11:55", 80.10),
        _bar("2026-09-06", "12:05", 80.15),
        _bar("2026-09-06", "12:10", 80.20),
    )
    with pytest.raises(LiveShadowBridgeError) as error:
        build_closed_15m_bars(bars)
    assert "missing=2026-09-06T12:00:00+03:00;" in str(error.value)
    assert "observed=2026-09-06T12:05:00+03:00,2026-09-06T12:10:00+03:00" in str(error.value)
    assert "synthetic_fill_allowed=false" in str(error.value)
