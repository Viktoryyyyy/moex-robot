from __future__ import annotations

import json

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


def test_identity_alignment_is_dtype_insensitive_but_value_strict() -> None:
    observed = pd.DataFrame({
        "target_trade_date": pd.Series(["2025-01-01", "2025-01-02"], dtype="string"),
        "target_instrument_id": ["forts.usdrubf", "forts.usdrubf"],
    })
    expected = pd.DataFrame({
        "target_trade_date": ["2025-01-01", "2025-01-02"],
        "target_instrument_id": pd.Series(["forts.usdrubf", "forts.usdrubf"], dtype="string"),
    })

    assert phase04._identity_values_equal(observed, expected)
    expected.loc[1, "target_trade_date"] = "2025-01-03"
    assert not phase04._identity_values_equal(observed, expected)


def test_block_bootstrap_indices_are_deterministic_and_complete() -> None:
    rng1 = np.random.default_rng(123)
    rng2 = np.random.default_rng(123)
    first = phase04._circular_block_indices(53, rng1)
    second = phase04._circular_block_indices(53, rng2)
    np.testing.assert_array_equal(first, second)
    assert len(first) == 53
    assert first.min() >= 0
    assert first.max() < 53
    assert phase04.BOOTSTRAP_BLOCK_LENGTH == 20


def _synthetic_frame(*, late_only: bool = False) -> pd.DataFrame:
    n = 300
    dates = pd.date_range("2025-01-01", periods=n, freq="D")
    regimes = np.full(n, "neutral", dtype=object)
    if late_only:
        regimes[220:280] = "high_high"
    else:
        regimes[10:30] = "high_high"
        regimes[110:130] = "high_high"
        regimes[210:230] = "high_high"
    frame = pd.DataFrame({
        "target_trade_date": dates.strftime("%Y-%m-%d"),
        "regime": regimes,
    })
    high_high = frame["regime"].eq("high_high").to_numpy()
    for horizon in phase04.HORIZONS:
        values = np.linspace(-0.001, 0.001, n)
        values[high_high] = -0.03 - horizon * 0.0001
        frame[f"fwd_usdrubf_close_return_{horizon}session"] = values
    return frame


def test_high_high_negative_catchup_can_be_robust_across_calendar_thirds() -> None:
    metrics, stability, summary = phase04.analyze_regimes(_synthetic_frame())
    high_high = metrics.loc[metrics["regime"].eq("high_high")]
    high_high_stability = stability.loc[stability["regime"].eq("high_high")]

    assert set(high_high["h5_evidence_status"]) == {"robust"}
    assert set(high_high["h6_evidence_status"]) == {"robust"}
    assert (high_high["mean_forward_return"] < 0.0).all()
    assert (high_high["short_hit_rate"] == 1.0).all()
    assert (high_high_stability["negative_mean_segment_count"] == 3).all()
    assert summary["H5_divergence_catch_up"]["status"] == "supported_robust"
    assert summary["H6_regime_dependency"]["status"] == "supported_robust"


def test_late_only_high_high_cluster_cannot_pass_temporal_stability() -> None:
    metrics, stability, summary = phase04.analyze_regimes(_synthetic_frame(late_only=True))
    high_high = metrics.loc[metrics["regime"].eq("high_high")]
    high_high_stability = stability.loc[stability["regime"].eq("high_high")]

    assert set(high_high["h5_evidence_status"]) == {"not_supported"}
    assert set(high_high["h6_evidence_status"]) == {"not_supported"}
    assert (high_high_stability["negative_mean_segment_count"] == 1).all()
    assert summary["H5_divergence_catch_up"]["status"] == "not_supported_in_phase04"
    assert summary["H6_regime_dependency"]["status"] == "not_supported_in_phase04"


def test_joint_bootstrap_preserves_zero_difference_when_every_row_is_regime() -> None:
    n = 100
    frame = pd.DataFrame({
        "regime": ["high_high"] * n,
        "fwd_usdrubf_close_return_5session": np.linspace(-0.02, 0.01, n),
    })
    _, _, diff_low, diff_high = phase04._bootstrap_regime_ci(
        frame,
        regime="high_high",
        label="fwd_usdrubf_close_return_5session",
        seed=123,
    )
    assert abs(diff_low) < 1e-15
    assert abs(diff_high) < 1e-15


def test_h6_can_be_supported_when_high_high_return_is_positive_but_below_unconditional() -> None:
    n = 300
    dates = pd.date_range("2025-01-01", periods=n, freq="D")
    regimes = np.full(n, "neutral", dtype=object)
    regimes[10:30] = "high_high"
    regimes[110:130] = "high_high"
    regimes[210:230] = "high_high"
    frame = pd.DataFrame({
        "target_trade_date": dates.strftime("%Y-%m-%d"),
        "regime": regimes,
    })
    high_high = frame["regime"].eq("high_high").to_numpy()
    for horizon in phase04.HORIZONS:
        values = np.full(n, 0.05, dtype=float)
        values[high_high] = 0.01
        frame[f"fwd_usdrubf_close_return_{horizon}session"] = values

    metrics, _, summary = phase04.analyze_regimes(frame)
    hh = metrics.loc[metrics["regime"].eq("high_high")]

    assert set(hh["h5_evidence_status"]) == {"not_supported"}
    assert set(hh["h6_evidence_status"]) == {"robust"}
    assert summary["H5_divergence_catch_up"]["status"] == "not_supported_in_phase04"
    assert summary["H6_regime_dependency"]["status"] == "supported_robust"


def test_json_safe_converts_non_finite_values_to_null() -> None:
    payload = {
        "nan": float("nan"),
        "pos_inf": float("inf"),
        "neg_inf": float("-inf"),
        "finite": 1.25,
        "nested": [np.float64(np.nan)],
    }
    safe = phase04._json_safe(payload)
    encoded = json.dumps(safe, allow_nan=False)
    decoded = json.loads(encoded)

    assert decoded["nan"] is None
    assert decoded["pos_inf"] is None
    assert decoded["neg_inf"] is None
    assert decoded["nested"] == [None]
    assert decoded["finite"] == 1.25


def test_confirmation_horizons_are_predeclared_not_optimized() -> None:
    assert phase04.HORIZONS == (1, 3, 5, 10, 20)
    assert phase04.CONFIRMATION_HORIZONS == (5, 10, 20)
    assert phase04.WINDOW == 126
    assert phase04.MIN_HISTORY == 63
    assert phase04.HIGH_THRESHOLD == 0.75
    assert phase04.LOW_THRESHOLD == 0.25
    assert phase04.BOOTSTRAP_BLOCK_LENGTH == 20
