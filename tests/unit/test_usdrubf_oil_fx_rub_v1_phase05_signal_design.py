from __future__ import annotations

import pandas as pd

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase05_signal_design as phase05


def test_execution_eligibility_uses_fixed_21_session_separation() -> None:
    assert phase05._execution_eligibility([10, 15, 31, 52]) == [True, False, True, True]
    assert phase05.MIN_ENTRY_SEPARATION == 21


def _synthetic_observations() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "target_trade_date": ["2025-01-06", "2025-01-10", "2025-02-10"],
            "target_instrument_id": ["forts.usdrubf"] * 3,
            "prior_trade_date": ["2025-01-05", "2025-01-09", "2025-02-09"],
            "brent_percentile_126": [0.80, 0.90, 0.85],
            "usdrubf_percentile_126": [0.82, 0.95, 0.88],
            "regime": ["high_high", "high_high", "high_high"],
            "entry_source_session_index": [5, 9, 30],
        }
    )


def _synthetic_panel() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=60, freq="D")
    return pd.DataFrame(
        {
            "trade_date": dates.strftime("%Y-%m-%d"),
            "instrument_id": ["forts.usdrubf"] * len(dates),
            "open": [100.0] * len(dates),
            "high": [101.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [100.0] * len(dates),
        }
    )


def test_candidates_are_schedule_only_and_deoverlapped() -> None:
    candidates = phase05._build_signal_candidates(
        _synthetic_observations(), _synthetic_panel()
    )

    assert len(candidates) == 3
    assert candidates["execution_eligible"].tolist() == [True, False, True]
    assert candidates["suppression_reason"].tolist() == [
        "",
        "overlap_cooldown_21_sessions",
        "",
    ]
    assert set(candidates["direction"]) == {"short_usdrubf_long_rub"}
    assert set(candidates["signal_regime"]) == {"high_high"}

    lowered = [column.lower() for column in candidates.columns]
    assert not any("return" in column for column in lowered)
    assert not any("pnl" in column for column in lowered)
    assert not any("entry_open" in column for column in lowered)
    assert not any("entry_price" in column for column in lowered)
    assert not any("exit_price" in column for column in lowered)
    assert not any("exit_close" in column for column in lowered)


def test_candidates_are_invariant_to_target_open_values() -> None:
    panel_a = _synthetic_panel()
    panel_b = panel_a.copy()
    panel_b["open"] = [10_000.0 + index for index in range(len(panel_b))]

    first = phase05._build_signal_candidates(_synthetic_observations(), panel_a)
    second = phase05._build_signal_candidates(_synthetic_observations(), panel_b)

    pd.testing.assert_frame_equal(first, second)


def test_fixed_exit_schedule_uses_panel_session_indices_only() -> None:
    candidates = phase05._build_signal_candidates(
        _synthetic_observations(), _synthetic_panel()
    )
    first = candidates.iloc[0]
    panel = _synthetic_panel()

    assert first["exit_5session_trade_date"] == panel.iloc[10]["trade_date"]
    assert first["exit_10session_trade_date"] == panel.iloc[15]["trade_date"]
    assert first["exit_20session_trade_date"] == panel.iloc[25]["trade_date"]
    assert first["exit_5session_execution"] == "close"
    assert first["exit_10session_execution"] == "close"
    assert first["exit_20session_execution"] == "close"


def test_fixed_exit_schedule_can_emit_terminal_date_without_price() -> None:
    panel = _synthetic_panel()
    terminal_index = len(panel) - 1
    observations = pd.DataFrame(
        {
            "target_trade_date": [panel.iloc[terminal_index - 20]["trade_date"]],
            "target_instrument_id": ["forts.usdrubf"],
            "prior_trade_date": [panel.iloc[terminal_index - 21]["trade_date"]],
            "brent_percentile_126": [0.90],
            "usdrubf_percentile_126": [0.90],
            "regime": ["high_high"],
            "entry_source_session_index": [terminal_index - 20],
        }
    )

    candidates = phase05._build_signal_candidates(observations, panel)
    row = candidates.iloc[0]

    assert row["exit_20session_available"]
    assert row["exit_20session_trade_date"] == panel.iloc[terminal_index]["trade_date"]
    assert "exit_price" not in " ".join(candidates.columns).lower()
    assert "exit_close" not in " ".join(candidates.columns).lower()


def test_summary_does_not_evaluate_performance() -> None:
    candidates = phase05._build_signal_candidates(
        _synthetic_observations(), _synthetic_panel()
    )
    summary = phase05._build_summary(candidates)

    assert summary["status"] == "signal_design_ready_for_phase06"
    assert summary["raw_high_high_observation_count"] == 3
    assert summary["execution_eligible_entry_count"] == 2
    assert summary["entry_price_value_published"] is False
    assert summary["future_exit_prices_used"] is False
    assert summary["future_returns_used"] is False
    assert summary["pnl_computed"] is False
    assert summary["performance_ranking_performed"] is False
    assert summary["phase06_round_trip_cost_bps_sensitivity"] == [0, 5, 10, 20]


def test_signal_parameters_are_predeclared_not_optimized() -> None:
    assert phase05.EXIT_HORIZONS == (5, 10, 20)
    assert phase05.COST_GRID_BPS == (0, 5, 10, 20)
    assert phase05.MIN_ENTRY_SEPARATION == 21
