from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.moex_features.daily.usdrubf_d1_ema_3_19_classical_indicators import (
    INDICATOR_COLUMNS,
    OUTPUT_COLUMNS,
    build_classical_indicators_frame,
    wilder_average,
)


def _d1_frame(closes: list[float], *, flat_range: bool = False) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for index, close in enumerate(closes):
        spread = 0.0 if flat_range else 1.0
        rows.append(
            {
                "instrument_id": "usdrubf",
                "end": pd.Timestamp("2024-01-01 18:50:00") + pd.Timedelta(days=index),
                "open": close - (0.25 if not flat_range else 0.0),
                "high": close + spread,
                "low": close - spread,
                "close": close,
                "volume": 1000.0 + index,
            }
        )
    return pd.DataFrame(rows)


def test_wilder_average_uses_simple_seed_then_recursive_smoothing() -> None:
    values = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])

    actual = wilder_average(values, period=3)

    assert actual.iloc[:2].isna().all()
    assert actual.iloc[2] == pytest.approx(2.0)
    assert actual.iloc[3] == pytest.approx((2.0 * 2.0 + 4.0) / 3.0)
    assert actual.iloc[4] == pytest.approx((((2.0 * 2.0 + 4.0) / 3.0) * 2.0 + 5.0) / 3.0)


def test_all_ten_indicators_session_index_and_ready_contract() -> None:
    closes = [100.0 + index + 3.0 * np.sin(index / 3.0) for index in range(80)]

    actual = build_classical_indicators_frame(_d1_frame(closes))

    assert tuple(actual.columns) == OUTPUT_COLUMNS
    assert tuple(column for column in actual.columns if column in INDICATOR_COLUMNS) == INDICATOR_COLUMNS
    assert actual["session_index"].tolist() == list(range(len(actual)))
    assert actual["indicator_ready"].equals(actual[list(INDICATOR_COLUMNS)].notna().all(axis=1))
    assert actual["indicator_ready"].any()
    assert actual.loc[10, "roc_10"] == pytest.approx(closes[10] / closes[0] - 1.0)
    assert np.isfinite(actual.loc[actual["indicator_ready"], list(INDICATOR_COLUMNS)].to_numpy()).all()


def test_rsi_atr_adx_and_directional_spread_use_wilder_path() -> None:
    closes = [100.0 + index for index in range(70)]

    actual = build_classical_indicators_frame(_d1_frame(closes))

    assert actual.loc[20, "rsi_14"] == pytest.approx(100.0)
    assert actual.loc[20, "atr_14_pct"] == pytest.approx(2.0 / closes[20])
    assert actual.loc[40, "adx_14"] == pytest.approx(100.0)
    assert actual.loc[40, "di_spread_14"] > 0.0


def test_zero_denominators_never_emit_infinity_and_structural_undefined_stays_null() -> None:
    actual = build_classical_indicators_frame(_d1_frame([100.0] * 60, flat_range=True))

    numeric = actual[list(INDICATOR_COLUMNS)].to_numpy(dtype=float, na_value=np.nan)
    assert not np.isinf(numeric).any()
    assert actual["stoch_k_14"].isna().all()
    assert actual["adx_14"].isna().all()
    assert actual["bb_percent_b_20_2"].isna().all()
    assert not actual["indicator_ready"].any()


def test_future_row_mutation_does_not_change_previous_indicator_rows() -> None:
    closes = [100.0 + 0.2 * index + 5.0 * np.sin(index / 5.0) for index in range(80)]
    base = _d1_frame(closes)
    mutated = base.copy()
    mutated.loc[len(mutated) - 1, ["open", "high", "low", "close"]] = [499.0, 501.0, 498.0, 500.0]

    before = build_classical_indicators_frame(base)
    after = build_classical_indicators_frame(mutated)

    pd.testing.assert_frame_equal(before.iloc[:-1].reset_index(drop=True), after.iloc[:-1].reset_index(drop=True))


def test_builder_rejects_non_chronological_or_duplicate_d1_keys() -> None:
    frame = _d1_frame([100.0 + index for index in range(40)])
    frame.loc[5, "end"] = frame.loc[4, "end"]

    with pytest.raises(ValueError, match="duplicate D1 instrument_id/end"):
        build_classical_indicators_frame(frame)
