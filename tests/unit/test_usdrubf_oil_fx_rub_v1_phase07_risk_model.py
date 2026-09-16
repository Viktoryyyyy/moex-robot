from __future__ import annotations

import numpy as np
import pandas as pd

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase07_risk_model as phase07


def _panel() -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-01", periods=40).strftime("%Y-%m-%d")
    base = np.full(40, 100.0)
    return pd.DataFrame(
        {
            "trade_date": dates,
            "instrument_id": ["forts.usdrubf"] * 40,
            "open": base.copy(),
            "high": base + 0.5,
            "low": base - 0.5,
            "close": base.copy(),
        }
    )


def _signal(panel: pd.DataFrame, entry_idx: int = 5) -> pd.Series:
    return pd.Series(
        {
            "signal_id": "oil_fx_rub_v1_hh_test",
            "target_trade_date": panel.loc[entry_idx, "trade_date"],
            "prior_trade_date": panel.loc[entry_idx - 1, "trade_date"],
            "entry_source_session_index": entry_idx,
            "execution_eligible": True,
        }
    )


def test_intratrade_drawdown_for_short_mark_path() -> None:
    closes = np.array([99.0, 97.0, 101.0, 96.0])
    value = phase07._intratrade_max_drawdown(100.0, closes)
    equity = np.array([1.0, 1.01, 1.03, 0.99, 1.04])
    peaks = np.maximum.accumulate(equity)
    expected = float(np.max(1.0 - equity / peaks))
    assert np.isclose(value, expected)


def test_excursion_row_uses_high_for_mae_and_low_for_mfe() -> None:
    panel = _panel()
    entry_idx = 5
    panel.loc[entry_idx : entry_idx + 5, "high"] = [101.0, 103.0, 102.0, 101.5, 100.8, 100.6]
    panel.loc[entry_idx : entry_idx + 5, "low"] = [99.0, 98.0, 95.0, 96.0, 97.0, 98.0]
    panel.loc[entry_idx : entry_idx + 5, "close"] = [99.5, 98.5, 96.0, 97.0, 98.0, 97.5]
    row = phase07._excursion_row(_signal(panel, entry_idx), panel, 5)
    assert np.isclose(row["mae_adverse_excursion"], 0.03)
    assert row["mae_sessions_from_entry"] == 1
    assert np.isclose(row["mfe_favorable_excursion"], 0.05)
    assert row["mfe_sessions_from_entry"] == 2


def test_stop_fill_is_gap_aware() -> None:
    panel = _panel()
    entry_idx = 5
    panel.loc[entry_idx + 1, "open"] = 103.0
    panel.loc[entry_idx + 1, "high"] = 104.0
    panel.loc[entry_idx + 1, "low"] = 102.5
    panel.loc[entry_idx + 1, "close"] = 103.5
    row = phase07._simulate_stop(_signal(panel, entry_idx), panel, 5, 0.02)
    assert row["stop_triggered"] is True
    assert np.isclose(row["stop_price"], 102.0)
    assert np.isclose(row["actual_exit_price"], 103.0)
    assert np.isclose(row["gross_short_return"], -0.03)
    assert np.isclose(row["net_short_return"], -0.032)


def test_stop_without_trigger_uses_fixed_horizon_close() -> None:
    panel = _panel()
    entry_idx = 5
    panel.loc[entry_idx : entry_idx + 5, "high"] = 100.5
    panel.loc[entry_idx + 5, "close"] = 95.0
    row = phase07._simulate_stop(_signal(panel, entry_idx), panel, 5, 0.05)
    assert row["stop_triggered"] is False
    assert row["actual_exit_execution"] == "fixed_horizon_close"
    assert np.isclose(row["gross_short_return"], 0.05)
    assert np.isclose(row["net_short_return"], 0.048)


def test_cost_aware_sizing_and_gap_breach() -> None:
    rows = []
    for horizon in phase07.HORIZONS:
        for stop_pct in phase07.STOP_GRID:
            rows.append(
                {
                    "horizon_sessions": horizon,
                    "stop_pct": stop_pct,
                    "worst_trade_net_return": -0.032 if (horizon == 20 and np.isclose(stop_pct, 0.02)) else -float(stop_pct + phase07.COST_RETURN),
                }
            )
    sizing = phase07._sizing_sensitivity(pd.DataFrame(rows))
    row = sizing.loc[
        (sizing["horizon_sessions"] == 20)
        & np.isclose(sizing["stop_pct"], 0.02)
        & np.isclose(sizing["risk_budget_pct_nav"], 0.005)
    ].iloc[0]
    expected_exposure = 0.005 / 0.022
    assert np.isclose(row["normalized_exposure_multiple"], expected_exposure)
    assert np.isclose(row["nominal_exact_stop_loss_pct_nav"], 0.005)
    assert row["observed_worst_trade_loss_pct_nav"] > 0.005
    assert bool(row["gap_risk_budget_breached_in_sample"]) is True
    assert len(sizing) == 45


def test_grids_are_fixed_ex_ante() -> None:
    assert phase07.HORIZONS == (5, 10, 20)
    assert phase07.STOP_GRID == (0.01, 0.02, 0.03, 0.04, 0.05)
    assert phase07.RISK_BUDGET_GRID == (0.0025, 0.005, 0.01)
    assert phase07.ROUND_TRIP_COST_BPS == 20
