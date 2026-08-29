from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from src.moex_research.intelligence.usdrubf_historical_sparse_bridge import (
    HISTORICAL_SPARSE_15M_SOURCE,
    build_historical_sparse_closed_15m_bars,
    build_historical_sparse_decision_input,
)
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    LiveShadowBridgeError,
    build_live_decision_input,
)
from src.moex_research.runners.usdrubf_s7_2_historical_sparse_component_benchmark import (
    build_historical_sparse_replay_with_exclusions,
)


def _bar(day: str, clock: str, close: float, *, volume: float = 100.0) -> dict[str, object]:
    ts = pd.Timestamp(f"{day} {clock}", tz="Europe/Moscow").to_pydatetime()
    return {
        "end": ts,
        "open": close - 0.02,
        "high": close + 0.05,
        "low": close - 0.05,
        "close": close,
        "volume": volume,
    }


def _frame(day: str, bars: list[tuple[str, float]]) -> pd.DataFrame:
    trade_date = pd.Timestamp(day)
    return pd.DataFrame(
        [
            {
                "end": pd.Timestamp(f"{day} {clock}"),
                "open": close - 0.02,
                "high": close + 0.05,
                "low": close - 0.05,
                "close": close,
                "volume": 100.0,
                "trade_date": trade_date,
            }
            for clock, close in bars
        ]
    )


def _prior_bars(day: str) -> tuple[dict[str, object], ...]:
    return (
        _bar(day, "10:00", 79.90),
        _bar(day, "10:05", 80.00),
        _bar(day, "10:10", 80.10),
        _bar(day, "18:45", 80.20),
        _bar(day, "18:50", 80.15),
    )


def _sparse_current_bars(day: str) -> tuple[dict[str, object], ...]:
    return (
        _bar(day, "10:00", 80.10),
        _bar(day, "10:05", 80.15),
        _bar(day, "10:10", 80.20),
        _bar(day, "14:05", 80.30),
        _bar(day, "14:10", 80.40),
    )


def test_sparse_15m_aggregates_only_observed_rows_without_imputation() -> None:
    bars = _sparse_current_bars("2026-08-10")

    aggregated = build_historical_sparse_closed_15m_bars(
        bars,
        as_of_timestamp=bars[-1]["end"],
    )

    assert len(aggregated) == 2
    first, sparse = aggregated
    assert first["constituent_count"] == 3
    assert sparse["constituent_count"] == 2
    assert sparse["end"] == "2026-08-10T14:00:00+03:00"
    assert sparse["open"] == pytest.approx(80.28)
    assert sparse["close"] == pytest.approx(80.40)
    assert sparse["volume"] == pytest.approx(200.0)
    assert sparse["source_available_at"] == bars[-1]["end"]
    assert sparse["last_observed_at"] == bars[-1]["end"]


def test_sparse_bucket_availability_is_nominal_close_when_final_5m_row_is_absent() -> None:
    bars = (
        _bar("2026-08-10", "14:00", 80.20),
        _bar("2026-08-10", "14:05", 80.30),
    )
    as_of = _bar("2026-08-10", "14:15", 80.35)["end"]

    aggregated = build_historical_sparse_closed_15m_bars(
        bars,
        as_of_timestamp=as_of,
    )

    assert len(aggregated) == 1
    sparse = aggregated[0]
    assert sparse["constituent_count"] == 2
    assert sparse["last_observed_at"] == bars[-1]["end"]
    assert sparse["source_available_at"] == pd.Timestamp(
        "2026-08-10 14:10", tz="Europe/Moscow"
    ).to_pydatetime()


def test_sparse_15m_does_not_use_bucket_before_nominal_close() -> None:
    bars = (
        _bar("2026-08-10", "14:05", 80.30),
        _bar("2026-08-10", "14:10", 80.40),
    )

    with pytest.raises(LiveShadowBridgeError, match="zero closed rows"):
        build_historical_sparse_closed_15m_bars(
            bars,
            as_of_timestamp=bars[0]["end"],
        )


def test_historical_sparse_bridge_accepts_native_gap_while_live_bridge_stays_fail_closed() -> None:
    prior = _prior_bars("2026-08-07")
    current = _sparse_current_bars("2026-08-10")

    with pytest.raises(LiveShadowBridgeError, match="broken 15m bucket"):
        build_live_decision_input(
            current_session_bars=current,
            prior_session_bars=prior,
            wall_clock_as_of=current[-1]["end"],
        )

    historical = build_historical_sparse_decision_input(
        current_session_bars=current,
        prior_session_bars=prior,
        wall_clock_as_of=current[-1]["end"],
    )

    details = dict(historical.ema_3_19_ai.details or {})
    assert details["source"] == HISTORICAL_SPARSE_15M_SOURCE
    assert details["sparse_bucket_count"] == 1
    assert details["min_constituent_count"] == 2
    assert details["missing_5m_imputation"] is False
    assert details["nominal_close_guard"] is True
    assert details["availability_semantics"] == "nominal_bucket_close"
    assert historical.as_of_timestamp == current[-1]["end"]


def test_sparse_runner_keeps_prediction_day_that_strict_live_bridge_would_exclude() -> None:
    daily = pd.DataFrame(
        [
            {"end": pd.Timestamp("2026-08-07 18:50"), "close": 80.15},
            {"end": pd.Timestamp("2026-08-10 14:10"), "close": 80.40},
            {"end": pd.Timestamp("2026-08-11 18:50"), "close": 80.55},
        ]
    )
    intraday = pd.concat(
        [
            _frame(
                "2026-08-07",
                [("10:00", 79.90), ("10:05", 80.00), ("10:10", 80.10), ("18:45", 80.20), ("18:50", 80.15)],
            ),
            _frame(
                "2026-08-10",
                [("10:00", 80.10), ("10:05", 80.15), ("10:10", 80.20), ("14:05", 80.30), ("14:10", 80.40)],
            ),
            _frame(
                "2026-08-11",
                [("10:00", 80.35), ("10:05", 80.40), ("10:10", 80.45), ("18:45", 80.50), ("18:50", 80.55)],
            ),
        ],
        ignore_index=True,
    )

    replay, exclusions = build_historical_sparse_replay_with_exclusions(
        daily,
        intraday,
        horizons=(1,),
    )

    assert replay["trade_date"].tolist() == ["2026-08-10", "2026-08-11"]
    assert exclusions.empty
    first = replay.iloc[0]
    assert first["ema_sparse_bucket_count"] == 1
    assert first["ema_min_constituent_count"] == 2
    assert first["future_price_h1"] == pytest.approx(80.55)
    parsed = datetime.fromisoformat(str(first["as_of_timestamp"]))
    assert parsed.tzinfo is not None
