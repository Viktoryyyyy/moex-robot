from __future__ import annotations

import pandas as pd
import pytest

from src.moex_research.intelligence.usdrubf_historical_sparse_bridge import (
    build_historical_sparse_closed_15m_bars,
)
from src.moex_research.runners.usdrubf_s7_2_empirical_stability_analysis import (
    _ema_slice_metrics,
    build_ema_stability,
    build_sparse_complete_sensitivity,
    build_structure_stability_summary,
    build_structure_yearly_stability,
)


def _bar(clock: str, close: float) -> dict[str, object]:
    ts = pd.Timestamp(f"2026-08-10 {clock}", tz="Europe/Moscow").to_pydatetime()
    return {
        "end": ts,
        "open": close - 0.02,
        "high": close + 0.05,
        "low": close - 0.05,
        "close": close,
        "volume": 100.0,
    }


def _replay_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2025-01-10",
                "price": 100.0,
                "ema_direction": "BULLISH_USD",
                "market_regime": "R1",
                "structure_signature": "HIGH:AWAY|LOW:UNTOUCHED",
                "future_price_h1": 101.0,
                "future_price_h5": 102.0,
            },
            {
                "trade_date": "2025-02-10",
                "price": 100.0,
                "ema_direction": "BEARISH_USD",
                "market_regime": "R1",
                "structure_signature": "HIGH:AWAY|LOW:UNTOUCHED",
                "future_price_h1": 101.0,
                "future_price_h5": 103.0,
            },
            {
                "trade_date": "2026-01-10",
                "price": 100.0,
                "ema_direction": "BEARISH_USD",
                "market_regime": "R1",
                "structure_signature": "HIGH:AWAY|LOW:UNTOUCHED",
                "future_price_h1": 101.0,
                "future_price_h5": 104.0,
            },
            {
                "trade_date": "2026-02-10",
                "price": 100.0,
                "ema_direction": "BEARISH_USD",
                "market_regime": "R1",
                "structure_signature": "HIGH:AWAY|LOW:UNTOUCHED",
                "future_price_h1": 99.0,
                "future_price_h5": 101.0,
            },
        ]
    )


def test_complete_only_sensitivity_drops_incomplete_bucket_without_imputation() -> None:
    bars = (
        _bar("10:00", 80.00),
        _bar("10:05", 80.10),
        _bar("10:10", 80.20),
        _bar("10:15", 80.30),
        _bar("10:20", 80.40),
        _bar("10:30", 80.50),
    )
    as_of = bars[-1]["end"]

    sparse = build_historical_sparse_closed_15m_bars(
        bars,
        as_of_timestamp=as_of,
        min_constituents=1,
    )
    complete = build_historical_sparse_closed_15m_bars(
        bars,
        as_of_timestamp=as_of,
        min_constituents=3,
    )

    assert [row["constituent_count"] for row in sparse] == [3, 2]
    assert [row["constituent_count"] for row in complete] == [3]
    assert sparse[1]["close"] == pytest.approx(80.40)
    assert complete[0]["close"] == pytest.approx(80.20)


def test_ema_metrics_report_post_hoc_majority_reference_and_delta() -> None:
    metrics = _ema_slice_metrics(
        _replay_rows(),
        horizon=1,
        neutral_band_bps=0.0,
    )

    assert metrics["eligible_count"] == 4
    assert metrics["ema_accuracy"] == pytest.approx(0.5)
    assert metrics["majority_class"] == "BULLISH_USD"
    assert metrics["majority_accuracy"] == pytest.approx(0.75)
    assert metrics["ema_accuracy_minus_majority_accuracy"] == pytest.approx(-0.25)
    assert metrics["directional_prediction_count"] == 4


def test_ema_yearly_stability_is_split_by_trade_year() -> None:
    overall, yearly = build_ema_stability(
        _replay_rows(),
        horizons=(1, 5),
        neutral_band_bps=0.0,
    )

    assert set(yearly["year"]) == {2025, 2026}
    assert set(yearly["horizon"]) == {1, 5}
    assert len(yearly) == 4
    assert overall["by_horizon"]["1"]["majority_accuracy"] == pytest.approx(0.75)
    assert overall["reference_semantics"].startswith("majority_class is a post-hoc")


def test_structure_yearly_sample_gate_and_consistency_are_descriptive() -> None:
    yearly = build_structure_yearly_stability(
        _replay_rows(),
        horizons=(5,),
        neutral_band_bps=0.0,
        min_group_sample=2,
    )
    structure = yearly[
        yearly["grouping_field"] == "structure_signature"
    ].reset_index(drop=True)

    assert len(structure) == 2
    assert structure["sample_gate_pass"].tolist() == [True, True]
    summary = build_structure_stability_summary(yearly)
    row = summary[
        (summary["grouping_field"] == "structure_signature")
        & (summary["group_value"] == "HIGH:AWAY|LOW:UNTOUCHED")
        & (summary["horizon"] == 5)
    ].iloc[0]
    assert row["years_passing_sample_gate"] == 2
    assert row["directional_consistency"] == "BULLISH_EVERY_PASSING_YEAR"


def test_sparse_complete_sensitivity_reports_direction_changes_and_metric_delta() -> None:
    sparse = _replay_rows().iloc[:2].copy()
    complete = sparse.copy()
    complete.loc[complete.index[1], "ema_direction"] = "BULLISH_USD"

    result = build_sparse_complete_sensitivity(
        sparse,
        complete,
        horizons=(1,),
        neutral_band_bps=0.0,
        candidate_days=2,
        sparse_excluded_days=0,
        complete_excluded_days=0,
    )

    assert result["common_trade_dates"] == 2
    assert result["ema_direction_match_count"] == 1
    assert result["ema_direction_match_rate"] == pytest.approx(0.5)
    assert result["ema_direction_changed_days"] == 1
    h1 = result["by_horizon"]["1"]
    assert h1["sparse"]["ema_accuracy"] == pytest.approx(0.5)
    assert h1["complete_only"]["ema_accuracy"] == pytest.approx(1.0)
    assert h1["complete_minus_sparse_accuracy"] == pytest.approx(0.5)
