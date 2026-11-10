from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).parents[1] / "research/usdrubf_multiday_reversal_20261110/study.py"
SPEC = importlib.util.spec_from_file_location("multiday_reversal_study", MODULE_PATH)
assert SPEC and SPEC.loader
study = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = study
SPEC.loader.exec_module(study)


def test_confirmation_modes_are_causal_and_symmetric() -> None:
    setup_long = {"trigger": 101.01}
    setup_short = {"trigger": 98.99}
    day_long = pd.Series({"close": 102.0, "vwap": 101.5, "delta": 50, "rvol20": 1.2, "oi_pct": -0.6})
    day_short = pd.Series({"close": 98.0, "vwap": 98.5, "delta": -50, "rvol20": 1.2, "oi_pct": -0.6})
    bar_long = pd.Series({"high": 101.2, "low": 100.0, "close": 101.1, "delta": 10, "is_last": True})
    bar_short = pd.Series({"high": 100.0, "low": 98.8, "close": 98.9, "delta": -10, "is_last": True})
    for mode in study.MODES:
        assert study._confirmed(mode, 1, bar_long, setup_long, day_long)
        assert study._confirmed(mode, -1, bar_short, setup_short, day_short)


def test_setup_trigger_requires_one_tick_beyond_candle() -> None:
    bar = pd.Series({"high": 101.0, "low": 99.0, "close": 101.0, "delta": 1, "is_last": False})
    day = pd.Series({"close": 101.0, "vwap": 100.0, "delta": 1, "rvol20": 2.0, "oi_pct": -2.0})
    assert not study._confirmed("touch_setup_extreme", 1, bar, {"trigger": 101.01}, day)


def test_metrics_exclude_trade_that_exits_after_fold() -> None:
    trade = {
        "entry_date": "2026-09-30", "exit_date": "2026-10-01", "side": "long",
        "net_r": 1.0, "net_points": 1.0, "net_return_pct": 1.0,
        "exit_reason": "time", "mae_r": -0.2, "mfe_r": 1.5,
    }
    result = study.metrics([trade], "2026-07-01", "2026-09-30")
    assert result["trades"] == 0


def test_stop_rounds_away_from_position() -> None:
    assert study._round_stop(99.999, 1) == 99.99
    assert study._round_stop(100.001, -1) == 100.01
