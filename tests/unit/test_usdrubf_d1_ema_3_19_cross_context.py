from __future__ import annotations

import pandas as pd
import pytest

from src.moex_features.daily.usdrubf_d1_ema_3_19_cross_context import (
    calculate_ema,
    build_ema_3_19_cross_context_frame,
)


def _d1_frame(closes: list[float]) -> pd.DataFrame:
    rows = []
    for index, close in enumerate(closes):
        open_value = close - 0.5
        rows.append(
            {
                "instrument_id": "usdrubf",
                "end": pd.Timestamp("2024-01-01 18:50:00") + pd.Timedelta(days=index),
                "open": open_value,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1000.0 + index,
            }
        )
    return pd.DataFrame(rows)


def test_feature_builder_rejects_missing_required_ohlc_columns() -> None:
    frame = _d1_frame([100.0] * 25).drop(columns=["open"])

    with pytest.raises(ValueError, match="missing required columns: open"):
        build_ema_3_19_cross_context_frame(frame)


def test_feature_builder_enforces_monotonic_finalized_timestamps() -> None:
    frame = _d1_frame([100.0] * 25)
    frame.loc[3, "end"] = frame.loc[2, "end"] - pd.Timedelta(hours=12)

    with pytest.raises(ValueError, match="strictly increasing"):
        build_ema_3_19_cross_context_frame(frame)


def test_ema_formula_uses_alpha_two_over_window_plus_one() -> None:
    values = pd.Series([10.0, 20.0])

    ema3 = calculate_ema(values, window=3)
    ema19 = calculate_ema(values, window=19)

    assert ema3.iloc[0] == 10.0
    assert ema3.iloc[1] == 15.0
    assert ema19.iloc[0] == 10.0
    assert ema19.iloc[1] == 11.0


def test_no_crossover_event_is_emitted_before_warmup() -> None:
    frame = _d1_frame([100.0] * 10 + [120.0] * 9)

    events = build_ema_3_19_cross_context_frame(frame)

    assert events.empty


def test_cross_up_and_cross_down_semantics_are_detected_after_warmup() -> None:
    closes = [100.0 - i for i in range(25)]
    closes.extend([76.0 + i * 3.0 for i in range(25)])
    closes.extend([148.0 - i * 4.0 for i in range(25)])
    frame = _d1_frame(closes)

    events = build_ema_3_19_cross_context_frame(frame)

    assert "cross_up" in set(events["cross_dir"])
    assert "cross_down" in set(events["cross_dir"])

    cross_up = events[events["cross_dir"] == "cross_up"].iloc[0]
    assert cross_up["ema_diff_prev"] <= 0.0
    assert cross_up["ema_diff"] > 0.0

    cross_down = events[events["cross_dir"] == "cross_down"].iloc[0]
    assert cross_down["ema_diff_prev"] >= 0.0
    assert cross_down["ema_diff"] < 0.0


def test_event_feature_rows_do_not_contain_d_plus_1_or_label_values() -> None:
    closes = [100.0 - i for i in range(25)] + [76.0 + i * 3.0 for i in range(25)]
    events = build_ema_3_19_cross_context_frame(_d1_frame(closes))

    forbidden = {
        "entry_open",
        "outcome_open",
        "next_open",
        "signed_ret_o2o_h1",
        "signed_ret_o2o_h2",
        "signed_ret_o2o_h5",
        "allow_trade_h5",
        "max_adverse_excursion_h5",
        "max_favorable_excursion_h5",
    }
    assert forbidden.isdisjoint(set(events.columns))
