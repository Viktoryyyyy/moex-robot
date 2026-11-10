from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).parents[1] / "research/usdrubf_multisession_sequence_20261110/study.py"
SPEC = importlib.util.spec_from_file_location("multisession_sequence_study", MODULE_PATH)
assert SPEC and SPEC.loader
study = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = study
SPEC.loader.exec_module(study)


def _daily(rows: list[dict[str, float | str | bool]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["rvol20"] = 1.0
    frame["delta"] = 0.0
    frame["oi_pct"] = 0.0
    return frame


def test_setup_uses_sequence_without_five_session_extreme() -> None:
    daily = _daily(
        [
            {"date": "2026-01-01", "open": 100.0, "high": 100.2, "low": 99.0, "close": 99.2, "atr20_prior": 1.0},
            {"date": "2026-01-02", "open": 99.2, "high": 99.4, "low": 98.8, "close": 98.9, "atr20_prior": 1.0},
            {"date": "2026-01-03", "open": 98.9, "high": 99.6, "low": 98.7, "close": 99.5, "atr20_prior": 1.0},
        ]
    )
    # There is deliberately no prior5_low/new_low field. Detection must still work.
    setup = study.detect_setup(daily, 2, 1, "stall_turn", 3)
    assert setup is not None
    assert setup["formation_start_date"] == "2026-01-01"
    assert setup["setup_date"] == "2026-01-03"
    assert setup["trigger"] == 99.61


def test_two_close_turn_requires_both_transition_closes() -> None:
    daily = _daily(
        [
            {"date": "2026-01-01", "open": 100.0, "high": 100.1, "low": 98.8, "close": 99.0, "atr20_prior": 1.0},
            {"date": "2026-01-02", "open": 99.0, "high": 99.5, "low": 98.7, "close": 99.4, "atr20_prior": 1.0},
            {"date": "2026-01-03", "open": 99.4, "high": 100.0, "low": 99.1, "close": 99.9, "atr20_prior": 1.0},
        ]
    )
    assert study.detect_setup(daily, 2, 1, "two_close_turn", 3) is not None
    daily.loc[2, "close"] = 99.2
    assert study.detect_setup(daily, 2, 1, "two_close_turn", 3) is None


def test_confirmation_modes_are_symmetric() -> None:
    long_setup = {"trigger": 101.01}
    short_setup = {"trigger": 98.99}
    long_bar = pd.Series(
        {"close": 101.1, "delta": 10, "running_vwap": 100.8, "session_cum_delta": 25, "is_last": True}
    )
    short_bar = pd.Series(
        {"close": 98.9, "delta": -10, "running_vwap": 99.2, "session_cum_delta": -25, "is_last": True}
    )
    long_day = {"close": 101.1, "vwap": 100.8, "delta": 100, "rvol20": 1.2, "oi_pct": -0.6}
    short_day = {"close": 98.9, "vwap": 99.2, "delta": -100, "rvol20": 1.2, "oi_pct": -0.6}
    for mode in study.CONFIRMATIONS:
        assert study.confirmed(mode, 1, long_bar, long_setup, long_day)
        assert study.confirmed(mode, -1, short_bar, short_setup, short_day)


def test_invalidation_uses_full_formation_edge() -> None:
    setup = {"formation_low": 98.0, "formation_high": 102.0, "setup_atr": 1.0}
    assert study._invalidated(setup, 1, {"low": 97.89}, 0.1)
    assert not study._invalidated(setup, 1, {"low": 97.91}, 0.1)
    assert study._invalidated(setup, -1, {"high": 102.11}, 0.1)


def test_neighbor_changes_exactly_one_dimension() -> None:
    center = study.Variant("stall_turn", 3, "price", 3, 0.1, 3)
    one = study.Variant("stall_turn", 4, "price", 3, 0.1, 3)
    two = study.Variant("two_close_turn", 4, "price", 3, 0.1, 3)
    assert study._direct_neighbors(center, [center, one, two]) == [one]


def test_candidate_confirmation_is_later_session_and_invalidation_wins() -> None:
    bars = pd.DataFrame(
        [
            # Price is already beyond the trigger, but this is the setup session.
            {"session": 0, "close": 101.2, "high": 101.3, "low": 100.0, "delta": 1, "running_vwap": 100.5, "session_cum_delta": 1, "is_last": True},
            # The next session touches the invalidation level on the same bar
            # that price might otherwise confirm, so no entry is allowed.
            {"session": 1, "close": 101.2, "high": 101.3, "low": 98.8, "delta": 1, "running_vwap": 100.5, "session_cum_delta": 1, "is_last": True},
            {"session": 2, "close": 101.3, "high": 101.4, "low": 100.5, "delta": 1, "running_vwap": 100.6, "session_cum_delta": 2, "is_last": True},
        ]
    )
    daily = pd.DataFrame(
        [
            {"close": 101.2, "vwap": 100.5, "delta": 1, "rvol20": 1.2, "oi_pct": -1.0},
            {"close": 101.2, "vwap": 100.5, "delta": 1, "rvol20": 1.2, "oi_pct": -1.0},
            {"close": 101.3, "vwap": 100.6, "delta": 2, "rvol20": 1.2, "oi_pct": -1.0},
        ]
    )
    setup = {
        "side": 1,
        "setup_session": 0,
        "trigger": 101.01,
        "formation_low": 99.0,
        "formation_high": 101.0,
        "setup_atr": 1.0,
    }
    variant = study.Variant("stall_turn", 3, "price", 3, 0.1, 3)
    assert study._candidate_confirmations(bars, daily, variant, [setup]) == []
