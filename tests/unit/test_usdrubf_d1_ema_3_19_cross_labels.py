from __future__ import annotations

import pandas as pd

from src.moex_research.labels.usdrubf_d1_ema_3_19_cross_labels import build_ema_3_19_cross_labels


def _d1_frame(count: int = 30) -> pd.DataFrame:
    rows = []
    for index in range(count):
        open_value = 100.0 + index
        rows.append(
            {
                "instrument_id": "usdrubf",
                "end": pd.Timestamp("2024-01-01 18:50:00") + pd.Timedelta(days=index),
                "open": open_value,
                "high": open_value + 2.0,
                "low": open_value - 2.0,
                "close": open_value + 0.5,
            }
        )
    return pd.DataFrame(rows)


def test_label_builder_uses_d_plus_1_open_as_earliest_outcome_anchor() -> None:
    daily = _d1_frame(30)
    events = pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": daily.loc[19, "end"],
                "cross_dir": "cross_up",
            }
        ]
    )

    labels = build_ema_3_19_cross_labels(events, daily)

    expected = (daily.loc[21, "open"] - daily.loc[20, "open"]) / daily.loc[20, "open"]
    assert labels.loc[0, "signed_ret_o2o_h1"] == expected
    assert labels.loc[0, "allow_trade_h5"] == 1


def test_cross_down_labels_are_signed_by_cross_direction() -> None:
    daily = _d1_frame(30)
    events = pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": daily.loc[19, "end"],
                "cross_dir": "cross_down",
            }
        ]
    )

    labels = build_ema_3_19_cross_labels(events, daily)

    raw_h1 = (daily.loc[21, "open"] - daily.loc[20, "open"]) / daily.loc[20, "open"]
    assert labels.loc[0, "signed_ret_o2o_h1"] == -raw_h1


def test_label_builder_uses_future_outcomes_only_not_event_day_ohlc() -> None:
    daily = _d1_frame(30)
    events = pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": daily.loc[19, "end"],
                "cross_dir": "cross_up",
            }
        ]
    )

    labels_before = build_ema_3_19_cross_labels(events, daily)
    changed = daily.copy()
    changed.loc[19, ["open", "high", "low", "close"]] = [1.0, 2.0, 0.5, 1.5]
    labels_after = build_ema_3_19_cross_labels(events, changed)

    columns = [
        "signed_ret_o2o_h1",
        "signed_ret_o2o_h2",
        "signed_ret_o2o_h5",
        "allow_trade_h5",
        "max_adverse_excursion_h5",
        "max_favorable_excursion_h5",
    ]
    assert labels_before.loc[0, columns].to_dict() == labels_after.loc[0, columns].to_dict()


def test_allow_trade_h5_is_one_only_when_signed_ret_h5_is_positive() -> None:
    daily = _d1_frame(30)
    cross_up_events = pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": daily.loc[19, "end"],
                "cross_dir": "cross_up",
            }
        ]
    )
    cross_down_events = pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": daily.loc[19, "end"],
                "cross_dir": "cross_down",
            }
        ]
    )

    cross_up_labels = build_ema_3_19_cross_labels(cross_up_events, daily)
    cross_down_labels = build_ema_3_19_cross_labels(cross_down_events, daily)

    assert cross_up_labels.loc[0, "signed_ret_o2o_h5"] > 0.0
    assert cross_up_labels.loc[0, "allow_trade_h5"] == 1
    assert cross_down_labels.loc[0, "signed_ret_o2o_h5"] < 0.0
    assert cross_down_labels.loc[0, "allow_trade_h5"] == 0


def test_allow_trade_h5_is_zero_when_future_anchor_is_unavailable() -> None:
    daily = _d1_frame(24)
    events = pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": daily.loc[20, "end"],
                "cross_dir": "cross_up",
            }
        ]
    )

    labels = build_ema_3_19_cross_labels(events, daily)

    assert labels.loc[0, "allow_trade_h5"] == 0
    assert pd.isna(labels.loc[0, "signed_ret_o2o_h5"])
    assert pd.isna(labels.loc[0, "max_adverse_excursion_h5"])
    assert pd.isna(labels.loc[0, "max_favorable_excursion_h5"])
