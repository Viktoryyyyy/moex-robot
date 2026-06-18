from __future__ import annotations

import pandas as pd
import pytest

from src.moex_features.labels.usdrubf_d1_ema_3_19_multi_horizon_labels import (
    OUTPUT_COLUMNS,
    build_multi_horizon_labels_frame,
)


def _d1_frame(rows: int = 16) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": pd.Timestamp("2024-01-01 18:50:00") + pd.Timedelta(days=index),
                "open": 100.0 + 10.0 * index,
                "high": 101.0 + 10.0 * index,
                "low": 99.0 + 10.0 * index,
                "close": 100.5 + 10.0 * index,
            }
            for index in range(rows)
        ]
    )


def _events(d1: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"instrument_id": "usdrubf", "end": d1.loc[1, "end"], "cross_dir": "cross_up"},
            {"instrument_id": "usdrubf", "end": d1.loc[3, "end"], "cross_dir": "cross_down"},
            {"instrument_id": "usdrubf", "end": d1.loc[6, "end"], "cross_dir": "cross_up"},
            {"instrument_id": "usdrubf", "end": d1.loc[15, "end"], "cross_dir": "cross_down"},
        ]
    )


def test_fixed_horizon_indices_returns_allow_trade_and_opposite_timing() -> None:
    d1 = _d1_frame()
    actual = build_multi_horizon_labels_frame(_events(d1), d1)
    first = actual.iloc[0]

    assert tuple(actual.columns) == OUTPUT_COLUMNS
    assert first["event_session_index"] == 1
    assert first["entry_session_index"] == 2
    assert first["h1_completion_index"] == 3
    assert first["h1_signed_return"] == pytest.approx(130.0 / 120.0 - 1.0)
    assert first["h1_allow_trade"] == 1
    assert bool(first["h1_opposite_cross_before_exit"]) is False

    assert first["h2_completion_index"] == 4
    assert first["h2_signed_return"] == pytest.approx(140.0 / 120.0 - 1.0)
    assert bool(first["h2_opposite_cross_before_exit"]) is True

    assert first["h10_completion_index"] == 12
    assert first["h10_signed_return"] == pytest.approx(220.0 / 120.0 - 1.0)


def test_reverse_exit_uses_first_opposite_event_R_plus_1_open() -> None:
    d1 = _d1_frame()
    actual = build_multi_horizon_labels_frame(_events(d1), d1)
    first = actual.iloc[0]
    second = actual.iloc[1]

    assert first["reverse_event_session_index"] == 3
    assert first["reverse_completion_index"] == 4
    assert first["holding_sessions_to_reverse_exit"] == 2
    assert first["reverse_signed_return"] == pytest.approx(140.0 / 120.0 - 1.0)
    assert first["reverse_allow_trade"] == 1
    assert bool(first["reverse_label_censored"]) is False

    assert second["reverse_event_session_index"] == 6
    assert second["reverse_completion_index"] == 7
    assert second["holding_sessions_to_reverse_exit"] == 3
    assert second["reverse_signed_return"] == pytest.approx(-(170.0 / 140.0 - 1.0))
    assert second["reverse_allow_trade"] == 0
    assert bool(second["reverse_label_censored"]) is False


def test_missing_fixed_or_reverse_outcomes_stay_null_and_never_become_class_zero() -> None:
    d1 = _d1_frame()
    actual = build_multi_horizon_labels_frame(_events(d1), d1)
    last = actual.iloc[-1]
    no_later_opposite = actual.iloc[2]

    assert pd.isna(last["entry_session_index"])
    assert pd.isna(last["h1_completion_index"])
    assert pd.isna(last["h1_signed_return"])
    assert pd.isna(last["h1_allow_trade"])

    assert pd.isna(no_later_opposite["reverse_event_session_index"]) is False
    assert no_later_opposite["reverse_event_session_index"] == 15
    assert pd.isna(no_later_opposite["reverse_completion_index"])
    assert pd.isna(no_later_opposite["reverse_signed_return"])
    assert pd.isna(no_later_opposite["reverse_allow_trade"])
    assert bool(no_later_opposite["reverse_label_censored"]) is True

    assert pd.isna(last["reverse_event_session_index"])
    assert pd.isna(last["reverse_signed_return"])
    assert pd.isna(last["reverse_allow_trade"])
    assert bool(last["reverse_label_censored"]) is True


def test_every_event_must_match_exactly_one_d1_session() -> None:
    d1 = _d1_frame()
    events = _events(d1)
    events.loc[len(events) - 1, "end"] = pd.Timestamp("2030-01-01 18:50:00")

    with pytest.raises(ValueError, match="match exactly one D1 session"):
        build_multi_horizon_labels_frame(events, d1)


def test_duplicate_event_keys_are_rejected() -> None:
    d1 = _d1_frame()
    events = pd.concat([_events(d1).iloc[[0]], _events(d1).iloc[[0]]], ignore_index=True)

    with pytest.raises(ValueError, match="duplicate crossover"):
        build_multi_horizon_labels_frame(events, d1)
