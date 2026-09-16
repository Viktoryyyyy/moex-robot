from __future__ import annotations

import numpy as np
import pandas as pd

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase06a_robustness_sensitivity as phase06a


def _panel(rows: int = 80) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-01", periods=rows).strftime("%Y-%m-%d")
    values = np.linspace(80.0, 84.0, rows)
    return pd.DataFrame(
        {
            "trade_date": dates,
            "instrument_id": ["forts.usdrubf"] * rows,
            "open": values,
            "high": values + 0.5,
            "low": values - 0.5,
            "close": values - 0.2,
        }
    )


def _observations(panel: pd.DataFrame, count: int = 70) -> pd.DataFrame:
    indices = np.arange(1, count + 1)
    return pd.DataFrame(
        {
            "target_trade_date": panel.loc[indices, "trade_date"].to_numpy(),
            "target_instrument_id": ["forts.usdrubf"] * count,
            "prior_trade_date": panel.loc[indices - 1, "trade_date"].to_numpy(),
            "brent_prior_close": np.linspace(60.0, 100.0, count),
            "usdrubf_prior_close": np.linspace(70.0, 100.0, count),
            "entry_source_session_index": indices,
        }
    )


def test_execution_eligibility_respects_configured_cooldown() -> None:
    assert phase06a._execution_eligibility([10, 24, 25, 40], 15) == [True, False, True, True]
    assert phase06a._execution_eligibility([10, 30, 31, 52], 21) == [True, False, True, True]


def test_config_candidates_exclude_terminal_entry_ohlc() -> None:
    panel = _panel(80)
    observations = _observations(panel, 79)
    candidates, raw_count, unbound_count = phase06a._build_config_candidates(
        observations,
        panel,
        window=63,
        threshold=0.70,
        cooldown=15,
    )
    assert raw_count > 0
    assert unbound_count in (0, 1)
    if not candidates.empty:
        assert (candidates["entry_source_session_index"].astype(int) < len(panel) - 1).all()


def test_trade_rows_apply_short_return_and_cost_once() -> None:
    panel = _panel(80)
    candidate = pd.DataFrame(
        {
            "target_trade_date": [panel.loc[10, "trade_date"]],
            "prior_trade_date": [panel.loc[9, "trade_date"]],
            "entry_source_session_index": [10],
            "brent_percentile": [0.90],
            "usdrubf_percentile": [0.90],
            "execution_eligible": [True],
        }
    )
    rows = phase06a._trade_rows_for_config(
        candidate,
        panel,
        window=126,
        threshold=0.75,
        cooldown=21,
    )
    five = next(row for row in rows if row["horizon_sessions"] == 5)
    expected_gross = (five["entry_price"] - five["exit_price"]) / five["entry_price"]
    assert np.isclose(five["gross_short_return"], expected_gross)
    assert np.isclose(five["net_short_return"], expected_gross - 0.002)


def test_metric_grid_constants_are_fixed_ex_ante() -> None:
    assert len(phase06a.WINDOW_GRID) * len(phase06a.THRESHOLD_GRID) * len(phase06a.COOLDOWN_GRID) == 27
    assert 27 * len(phase06a.HORIZON_GRID) == 135
    assert phase06a.PRIMARY_COST_BPS == 20


def test_aggregate_summary_does_not_select_winner() -> None:
    rows = []
    for horizon in phase06a.HORIZON_GRID:
        rows.append(
            {
                "config_id": "w126_t75_c21",
                "rolling_window_sessions": 126,
                "minimum_history_sessions": 63,
                "high_threshold": 0.75,
                "cooldown_source_sessions": 21,
                "horizon_sessions": horizon,
                "round_trip_cost_bps": 20,
                "raw_signal_count": 3,
                "independent_trade_count": 3,
                "unbound_entry_excluded_count": 0,
                "trade_count": 3,
                "mean_net_return": 0.01,
                "median_net_return": 0.01,
                "hit_rate_net_positive": 1.0,
                "compounded_net_return": 0.03,
                "worst_trade_net_return": 0.005,
                "trade_sequence_max_drawdown": 0.0,
            }
        )
    summary = phase06a._build_aggregate_summary(pd.DataFrame(rows))
    assert summary["winner_selection_performed"] is False
    assert summary["parameter_ranking_performed"] is False
    assert summary["parameter_optimization_performed"] is False
