from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.moex_features.daily.usdrubf_d1_ema_3_19_classical_indicators import (
    INDICATOR_COLUMNS,
    build_classical_indicators_frame,
)
from src.moex_features.daily.usdrubf_d1_ema_3_19_indicator_context import (
    build_ema_3_19_indicator_context_frame,
    forbidden_context_columns,
)


def _d1_frame(rows: int = 80) -> pd.DataFrame:
    values: list[dict[str, object]] = []
    for index in range(rows):
        close = 100.0 + 0.25 * index + 4.0 * np.sin(index / 4.0)
        values.append(
            {
                "instrument_id": "usdrubf",
                "end": pd.Timestamp("2024-01-01 18:50:00") + pd.Timedelta(days=index),
                "open": close - 0.2,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
            }
        )
    return pd.DataFrame(values)


def _events(d1: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": d1.loc[20, "end"],
                "cross_dir": "cross_up",
                "ema3": 101.0,
                "ema19": 100.0,
                "ret_1d": 0.01,
                "signed_return_h5": 999.0,
                "allow_trade_h5": 1,
            },
            {
                "instrument_id": "usdrubf",
                "end": d1.loc[45, "end"],
                "cross_dir": "cross_down",
                "ema3": 102.0,
                "ema19": 103.0,
                "ret_3d": -0.02,
                "reverse_label_censored": False,
            },
        ]
    )


def test_join_retains_event_rows_preserves_cross_dir_and_adds_all_indicators() -> None:
    d1 = _d1_frame()
    indicators = build_classical_indicators_frame(d1)
    events = _events(d1)

    actual = build_ema_3_19_indicator_context_frame(events, indicators)

    assert len(actual) == len(events)
    assert actual["end"].tolist() == events["end"].tolist()
    assert actual["cross_dir"].tolist() == events["cross_dir"].tolist()
    assert actual["session_index"].tolist() == [20, 45]
    assert set(INDICATOR_COLUMNS).issubset(actual.columns)
    assert "indicator_ready" in actual.columns
    assert forbidden_context_columns(actual) == []
    assert "ret_1d" not in actual.columns
    assert "ret_3d" not in actual.columns
    assert "signed_return_h5" not in actual.columns
    assert "allow_trade_h5" not in actual.columns
    assert "reverse_label_censored" not in actual.columns


def test_join_recomputes_indicator_ready_from_all_ten_indicator_values() -> None:
    d1 = _d1_frame()
    indicators = build_classical_indicators_frame(d1)
    events = _events(d1)

    # Use object dtype so the test can represent a stale serialized flag on pandas 2.x and 3.x.
    indicators["indicator_ready"] = indicators["indicator_ready"].astype(object)
    indicators.loc[45, "indicator_ready"] = "False"
    actual = build_ema_3_19_indicator_context_frame(events, indicators)
    assert bool(actual.loc[1, "indicator_ready"]) is True

    indicators.loc[45, "rsi_14"] = np.nan
    indicators.loc[45, "indicator_ready"] = "True"
    actual = build_ema_3_19_indicator_context_frame(events, indicators)
    assert bool(actual.loc[1, "indicator_ready"]) is False


def test_join_is_exact_one_to_one_and_rejects_unmatched_event_key() -> None:
    d1 = _d1_frame()
    indicators = build_classical_indicators_frame(d1)
    events = _events(d1)
    events.loc[1, "end"] = pd.Timestamp("2030-01-01 18:50:00")

    with pytest.raises(ValueError, match="match exactly one D1 indicator row"):
        build_ema_3_19_indicator_context_frame(events, indicators)


def test_join_rejects_duplicate_event_or_indicator_keys() -> None:
    d1 = _d1_frame()
    indicators = build_classical_indicators_frame(d1)
    events = _events(d1)

    duplicate_events = pd.concat([events.iloc[[0]], events.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate crossover"):
        build_ema_3_19_indicator_context_frame(duplicate_events, indicators)

    duplicate_indicators = pd.concat([indicators, indicators.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate indicator"):
        build_ema_3_19_indicator_context_frame(events, duplicate_indicators)


def test_join_rejects_non_zero_based_indicator_session_index() -> None:
    d1 = _d1_frame()
    indicators = build_classical_indicators_frame(d1)
    indicators.loc[10, "session_index"] = 999

    with pytest.raises(ValueError, match="zero-based chronological"):
        build_ema_3_19_indicator_context_frame(_events(d1), indicators)
