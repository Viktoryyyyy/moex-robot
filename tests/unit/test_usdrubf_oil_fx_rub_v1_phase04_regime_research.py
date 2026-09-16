from __future__ import annotations

import numpy as np
import pandas as pd

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase04_regime_research as phase04


def test_rolling_percentile_is_past_only() -> None:
    values = np.arange(100, dtype=float)
    original = phase04._rolling_percentile(values, window=20, min_history=5)
    mutated = values.copy()
    mutated[80:] = mutated[80:] + 10000.0
    changed = phase04._rolling_percentile(mutated, window=20, min_history=5)
    np.testing.assert_allclose(original[:80], changed[:80], equal_nan=True)


def test_regime_classification_is_fixed() -> None:
    assert phase04._classify_regime(0.90, 0.90) == "high_high"
    assert phase04._classify_regime(0.90, 0.10) == "high_low"
    assert phase04._classify_regime(0.10, 0.90) == "low_high"
    assert phase04._classify_regime(0.10, 0.10) == "low_low"
    assert phase04._classify_regime(0.50, 0.90) == "neutral"
    assert phase04._classify_regime(float("nan"), 0.90) == "warmup"


def test_high_high_negative_catchup_can_be_detected_without_model_fit() -> None:
    n = 300
    frame = pd.DataFrame({
        "target_trade_date": pd.date_range("2025-01-01", periods=n, freq="D").strftime("%Y-%m-%d"),
        "regime": ["high_high"] * 60 + ["neutral"] * (n - 60),
    })
    for horizon in phase04.HORIZONS:
        values = np.zeros(n, dtype=float)
        values[:60] = -0.03 - horizon * 0.0001
        values[60:] = np.linspace(-0.001, 0.001, n - 60)
        frame[f"fwd_usdrubf_close_return_{horizon}session"] = values

    metrics, stability, summary = phase04.analyze_regimes(frame)
    high_high = metrics.loc[metrics["regime"].eq("high_high")]

    assert set(high_high["evidence_status"]) == {"robust"}
    assert (high_high["mean_forward_return"] < 0.0).all()
    assert (high_high["short_hit_rate"] == 1.0).all()
    assert summary["H5_divergence_catch_up"]["status"] == "supported_robust"
    assert summary["H6_regime_dependency"]["status"] == "supported_robust"
    assert not stability.empty


def test_confirmation_horizons_are_predeclared_not_optimized() -> None:
    assert phase04.HORIZONS == (1, 3, 5, 10, 20)
    assert phase04.CONFIRMATION_HORIZONS == (5, 10, 20)
    assert phase04.WINDOW == 126
    assert phase04.MIN_HISTORY == 63
    assert phase04.HIGH_THRESHOLD == 0.75
    assert phase04.LOW_THRESHOLD == 0.25
