from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.moex_research.intelligence.usdrubf_intelligence_benchmark import (
    BenchmarkObservation,
    IntelligenceBenchmarkError,
    evaluate_intelligence_quality,
    realized_bias,
)


T0 = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 1, 6, 12, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 1, 7, 12, 0, tzinfo=timezone.utc)


def _row(
    ts: datetime,
    *,
    price: float = 80.0,
    bias: str = "BULLISH_USD",
    trade_state: str = "ENTER",
    confidence: float = 0.8,
    future_prices: dict[int, float] | None = None,
) -> BenchmarkObservation:
    return BenchmarkObservation(
        as_of_timestamp=ts,
        price=price,
        final_bias=bias,
        trade_state=trade_state,
        confidence=confidence,
        future_prices=future_prices or {1: 80.8, 3: 81.6},
        trend=bias,
        market_regime="TREND",
    )


def test_realized_bias_respects_neutral_band() -> None:
    assert realized_bias(start_price=80.0, future_price=80.8, neutral_band_bps=50.0) == "BULLISH_USD"
    assert realized_bias(start_price=80.0, future_price=79.2, neutral_band_bps=50.0) == "BEARISH_USD"
    assert realized_bias(start_price=80.0, future_price=80.2, neutral_band_bps=50.0) == "NEUTRAL"


def test_trade_state_and_bias_map_to_bounded_exposure() -> None:
    assert _row(T0, bias="BULLISH_USD", trade_state="HOLD").exposure == "LONG_USD"
    assert _row(T0, bias="BEARISH_USD", trade_state="ADD").exposure == "SHORT_USD"
    assert _row(T0, bias="BULLISH_USD", trade_state="WAIT").exposure == "OUT"
    assert _row(T0, bias="NEUTRAL", trade_state="ENTER").exposure == "OUT"


def test_metrics_are_deterministic_on_known_synthetic_sample() -> None:
    rows = [
        _row(T0, bias="BULLISH_USD", trade_state="ENTER", confidence=0.9, future_prices={1: 80.8}),
        _row(T1, bias="BEARISH_USD", trade_state="HOLD", confidence=0.8, future_prices={1: 79.2}),
        _row(T2, bias="BULLISH_USD", trade_state="WAIT", confidence=0.9, future_prices={1: 79.2}),
    ]

    result = evaluate_intelligence_quality(
        rows,
        horizons=(1,),
        neutral_band_bps=0.0,
        high_confidence_threshold=0.75,
    )
    h1 = result["by_horizon"]["1"]

    assert h1["eligible_count"] == 3
    assert h1["bias_correct_count"] == 2
    assert h1["bias_accuracy"] == pytest.approx(2 / 3)
    assert h1["high_confidence_count"] == 3
    assert h1["high_confidence_error_rate"] == pytest.approx(1 / 3)
    assert h1["active_count"] == 2
    assert h1["active_directional_success_rate"] == 1.0
    assert h1["positive_active_return_rate"] == 1.0
    assert h1["mean_signed_return_bps_when_active"] == pytest.approx(100.0)
    assert h1["out_count"] == 1
    assert h1["missed_directional_opportunity_rate"] == 1.0
    assert h1["exposure_distribution"] == {"LONG_USD": 1, "OUT": 1, "SHORT_USD": 1}


def test_missing_terminal_horizons_are_reported_not_silently_imputed() -> None:
    rows = [
        _row(T0, future_prices={1: 80.8, 5: 82.0}),
        _row(T1, future_prices={1: 80.4}),
    ]

    result = evaluate_intelligence_quality(rows, horizons=(1, 5))
    h1 = result["by_horizon"]["1"]
    h5 = result["by_horizon"]["5"]

    assert h1["eligible_count"] == 2
    assert h1["missing_label_count"] == 0
    assert h1["label_coverage"] == 1.0
    assert h5["eligible_count"] == 1
    assert h5["missing_label_count"] == 1
    assert h5["label_coverage"] == 0.5


def test_duplicate_as_of_timestamps_are_rejected() -> None:
    with pytest.raises(IntelligenceBenchmarkError, match="must be unique"):
        evaluate_intelligence_quality([_row(T0), _row(T0)])


def test_invalid_observation_and_thresholds_fail_closed() -> None:
    with pytest.raises(IntelligenceBenchmarkError, match="confidence"):
        _row(T0, confidence=1.1)
    with pytest.raises(IntelligenceBenchmarkError, match="future price horizon"):
        _row(T0, future_prices={0: 81.0})
    with pytest.raises(IntelligenceBenchmarkError, match="neutral_band_bps"):
        evaluate_intelligence_quality([_row(T0)], neutral_band_bps=-1.0)
    with pytest.raises(IntelligenceBenchmarkError, match="high_confidence_threshold"):
        evaluate_intelligence_quality([_row(T0)], high_confidence_threshold=1.1)
