from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from moex_research.runners.usdrubf_phase7_4_feature_policy_revision_builder import (
    MATRIX_CATEGORICAL_FEATURES,
    MATRIX_NUMERIC_FEATURES,
    M1_CATEGORICAL_FEATURES,
    M1_NUMERIC_FEATURES,
    NEW_NUMERIC_FEATURES,
    Phase74FeaturePolicyBuilderError,
    build_feature_matrices,
)


def _source(rows: int = 45, instruments: tuple[str, ...] = ("SiA", "SiB")) -> pd.DataFrame:
    frames = []
    for offset, instrument in enumerate(instruments):
        index = np.arange(rows, dtype=float)
        close = 100 + 25 * offset + index * (1.0 + 0.1 * offset) + np.sin(index / 3)
        volume = 1000 + 100 * offset + 9 * index
        trades = 100 + 10 * offset + index
        frames.append(pd.DataFrame({
            "trade_date": pd.date_range("2024-01-01", periods=rows, freq="D"),
            "instrument_id": instrument,
            "open": close * 0.998, "high": close * 1.012,
            "low": close * 0.988, "close": close, "volume": volume,
            "value": close * volume, "num_trades": trades,
        }))
    return pd.concat(frames, ignore_index=True)


def _phase6(source: pd.DataFrame) -> pd.DataFrame:
    records = []
    for instrument, group in source.groupby("instrument_id", sort=False):
        group = group.sort_values("trade_date").reset_index(drop=True)
        close = group["close"].astype(float)
        returns = close.pct_change()
        ranges = (group["high"] - group["low"]) / close
        ema3 = close.ewm(span=3, adjust=False).mean()
        ema19 = close.ewm(span=19, adjust=False).mean()
        for index in range(1, len(group)):
            spread = ema3.iloc[index] - ema19.iloc[index]
            records.append({
                "target_phase_label": ("B", "S", "OUT")[index % 3],
                "target_is_labeled": True, "target_source": "manual_phase_labels_v1",
                "target_trade_date": (group["trade_date"].iloc[index] + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                "target_instrument_id": instrument,
                "prior_trade_date": group["trade_date"].iloc[index].strftime("%Y-%m-%d"),
                "session_index": index, "days_since_prior_trade_date": 1,
                "lag1_close_return_1d": returns.iloc[index],
                "lag1_intraday_return": close.iloc[index] / group["open"].iloc[index] - 1,
                "rolling_past_return_mean": returns.iloc[max(1, index - 4):index + 1].mean(),
                "rolling_past_return_std": returns.iloc[max(1, index - 4):index + 1].std(ddof=1),
                "lag1_hl_range_pct": ranges.iloc[index],
                "rolling_past_hl_range_mean": ranges.iloc[max(0, index - 4):index + 1].mean(),
                "rolling_past_hl_range_std": ranges.iloc[max(0, index - 4):index + 1].std(ddof=1),
                "lag1_volume": group["volume"].iloc[index],
                "lag1_value": group["value"].iloc[index],
                "lag1_num_trades": group["num_trades"].iloc[index],
                "rolling_past_volume_mean": group["volume"].iloc[max(0, index - 4):index + 1].mean(),
                "lag1_ema_3": ema3.iloc[index], "lag1_ema_19": ema19.iloc[index],
                "lag1_ema_3_19_spread": spread,
                "lag1_ema_3_19_state": "ema3_above_ema19" if spread > 0 else "ema3_below_ema19" if spread < 0 else "ema3_equal_ema19",
            })
    return pd.DataFrame(records)


def test_exact_formulas_ema_windows_and_retained_copy() -> None:
    source = _source()
    phase6 = _phase6(source)
    result = build_feature_matrices(source, phase6)
    matrix = result.matrices["M1_REVISED_FULL"]
    group = source[source["instrument_id"].eq("SiA")].reset_index(drop=True)
    index = 25
    target = (group["trade_date"].iloc[index] + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    row = matrix[(matrix["target_instrument_id"] == "SiA") & (matrix["target_trade_date"] == target)].iloc[0]
    close = group["close"].to_numpy(float)
    returns = pd.Series(close).pct_change().to_numpy(float)
    ranges = ((group["high"] - group["low"]) / group["close"]).to_numpy(float)
    ema3 = pd.Series(close).ewm(span=3, adjust=False).mean().to_numpy(float)
    ema19 = pd.Series(close).ewm(span=19, adjust=False).mean().to_numpy(float)
    spread = (ema3 - ema19) / close
    assert row["lag1_intersession_gap_days"] == 1
    assert row["lag1_ema_3_19_spread_pct"] == pytest.approx(spread[index])
    assert row["lag1_close_ema_19_distance_pct"] == pytest.approx(close[index] / ema19[index] - 1)
    assert row["lag1_ema_3_slope_5_pct"] == pytest.approx(ema3[index] / ema3[index - 5] - 1)
    assert row["lag1_ema_19_slope_5_pct"] == pytest.approx(ema19[index] / ema19[index - 5] - 1)
    assert row["lag1_ema_3_19_spread_pct_change_5"] == pytest.approx(spread[index] - spread[index - 5])
    assert row["rolling_past_return_std_5"] == pytest.approx(np.std(returns[index - 4:index + 1], ddof=1))
    assert row["rolling_past_return_std_20"] == pytest.approx(np.std(returns[index - 19:index + 1], ddof=1))
    assert row["lag1_hl_range_to_prior20_mean"] == pytest.approx(ranges[index] / np.mean(ranges[index - 20:index]) - 1)
    assert row["rolling_hl_range_mean_ratio_5_20"] == pytest.approx(np.mean(ranges[index - 4:index + 1]) / np.mean(ranges[index - 19:index + 1]) - 1)
    frozen = phase6[(phase6["target_instrument_id"] == "SiA") & (phase6["target_trade_date"] == target)].iloc[0]
    for feature in ("lag1_close_return_1d", "lag1_intraday_return", "rolling_past_return_mean", "lag1_hl_range_pct", "lag1_ema_3_19_state"):
        assert row[feature] == frozen[feature]


def test_prior20_activity_formulas_and_state_run() -> None:
    source = _source(instruments=("SiA",))
    phase6 = _phase6(source)
    matrix = build_feature_matrices(source, phase6).matrices["M1_REVISED_FULL"]
    index = 30
    group = source.reset_index(drop=True)
    target = (group["trade_date"].iloc[index] + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    row = matrix[matrix["target_trade_date"] == target].iloc[0]
    volume = group["volume"].to_numpy(float)
    trades = group["num_trades"].to_numpy(float)
    value = group["value"].to_numpy(float)
    assert row["lag1_log_volume_rel_prior20"] == pytest.approx(np.log1p(volume[index]) - np.mean(np.log1p(volume[index - 20:index])))
    assert row["lag1_log_num_trades_rel_prior20"] == pytest.approx(np.log1p(trades[index]) - np.mean(np.log1p(trades[index - 20:index])))
    assert row["lag1_log_avg_trade_value_rel_prior20"] == pytest.approx(np.log1p(value[index] / trades[index]) - np.mean(np.log1p(value[index - 20:index] / trades[index - 20:index])))
    assert row["rolling_log_volume_mean_diff_5_20"] == pytest.approx(np.mean(np.log1p(volume[index - 4:index + 1])) - np.mean(np.log1p(volume[index - 19:index + 1])))
    assert row["lag1_ema_3_19_state_run_length_log"] > 0


def test_same_instrument_isolation_no_target_session_use_and_order() -> None:
    source = _source(rows=35)
    phase6 = _phase6(source)
    phase6 = phase6[phase6["target_trade_date"] <= "2024-01-31"].copy()
    baseline = build_feature_matrices(source, phase6).matrices["M1_REVISED_FULL"]
    changed = source.copy()
    changed.loc[(changed["instrument_id"] == "SiB") & (changed["trade_date"] == pd.Timestamp("2024-01-31")), ["open", "high", "low", "close"]] *= 7
    altered = build_feature_matrices(changed, phase6).matrices["M1_REVISED_FULL"]
    pd.testing.assert_frame_equal(
        baseline[baseline["target_instrument_id"] == "SiA"].reset_index(drop=True),
        altered[altered["target_instrument_id"] == "SiA"].reset_index(drop=True),
    )
    before_target = baseline[(baseline["target_instrument_id"] == "SiB") & (baseline["target_trade_date"] <= "2024-01-31")].reset_index(drop=True)
    after_target = altered[(altered["target_instrument_id"] == "SiB") & (altered["target_trade_date"] <= "2024-01-31")].reset_index(drop=True)
    pd.testing.assert_frame_equal(before_target, after_target)
    assert result_order(baseline) == sorted(result_order(baseline))


def result_order(frame: pd.DataFrame) -> list[tuple[str, str]]:
    return list(zip(frame["target_trade_date"].astype(str), frame["target_instrument_id"].astype(str)))


def test_warmup_and_denominator_failures_are_separate_and_no_inf() -> None:
    source = _source(instruments=("SiA",))
    phase6 = _phase6(source)
    source.loc[source["trade_date"] == pd.Timestamp("2024-01-26"), "num_trades"] = 0
    result = build_feature_matrices(source, phase6)
    diagnostics = result.diagnostics
    assert diagnostics["warmup_null"].any()
    assert diagnostics["denominator_failure"].any()
    assert not (diagnostics["warmup_null"] & diagnostics["denominator_failure"]).any()
    numeric = result.matrices["M1_REVISED_FULL"].loc[:, M1_NUMERIC_FEATURES].to_numpy(float)
    assert not np.isinf(numeric).any()


def test_exact_m1_m5_inventory_and_m2_removes_state() -> None:
    assert tuple(MATRIX_NUMERIC_FEATURES) == (
        "M1_REVISED_FULL", "M2_MINUS_NORMALIZED_EMA_TREND",
        "M3_MINUS_VOLATILITY_RANGE", "M4_MINUS_VOLUME_ACTIVITY",
        "M5_MINUS_LAGGED_INTERSESSION_GAP",
    )
    assert MATRIX_NUMERIC_FEATURES["M1_REVISED_FULL"] == M1_NUMERIC_FEATURES
    assert MATRIX_CATEGORICAL_FEATURES["M1_REVISED_FULL"] == M1_CATEGORICAL_FEATURES
    assert MATRIX_CATEGORICAL_FEATURES["M2_MINUS_NORMALIZED_EMA_TREND"] == ()
    assert set(NEW_NUMERIC_FEATURES).issubset(set(M1_NUMERIC_FEATURES))


def test_missing_duplicate_and_invalid_source_fail_closed() -> None:
    source = _source(instruments=("SiA",))
    phase6 = _phase6(source)
    with pytest.raises(Phase74FeaturePolicyBuilderError, match="missing required"):
        build_feature_matrices(source.drop(columns=["value"]), phase6)
    duplicate = pd.concat([source, source.iloc[[0]]], ignore_index=True)
    with pytest.raises(Phase74FeaturePolicyBuilderError, match="duplicate source"):
        build_feature_matrices(duplicate, phase6)
    invalid = source.copy()
    invalid.loc[0, "close"] = 0
    with pytest.raises(Phase74FeaturePolicyBuilderError, match="positive"):
        build_feature_matrices(invalid, phase6)
    invalid = source.copy()
    invalid.loc[0, "high"] = invalid.loc[0, "low"] - 1
    with pytest.raises(Phase74FeaturePolicyBuilderError, match="lower"):
        build_feature_matrices(invalid, phase6)


def test_deterministic_and_no_filesystem_writes(tmp_path, monkeypatch) -> None:
    source = _source()
    phase6 = _phase6(source)
    monkeypatch.chdir(tmp_path)
    first = build_feature_matrices(source, phase6)
    second = build_feature_matrices(source, phase6)
    for matrix_id in first.matrices:
        pd.testing.assert_frame_equal(first.matrices[matrix_id], second.matrices[matrix_id])
    assert list(tmp_path.iterdir()) == []
