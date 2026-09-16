from __future__ import annotations

import math

import pandas as pd

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase06_backtest as phase06


def _synthetic_panel() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=90, freq="D")
    return pd.DataFrame(
        {
            "trade_date": dates.strftime("%Y-%m-%d"),
            "instrument_id": ["forts.usdrubf"] * len(dates),
            "open": [100.0 + i * 0.1 for i in range(len(dates))],
            "high": [101.0 + i * 0.1 for i in range(len(dates))],
            "low": [99.0 + i * 0.1 for i in range(len(dates))],
            "close": [100.2 + i * 0.08 for i in range(len(dates))],
        }
    )


def _synthetic_candidates(panel: pd.DataFrame) -> pd.DataFrame:
    entry_indices = [5, 30, 55]
    rows = []
    for ordinal, entry_idx in enumerate(entry_indices, start=1):
        row = {
            "signal_id": f"s{ordinal}",
            "target_trade_date": panel.iloc[entry_idx]["trade_date"],
            "prior_trade_date": panel.iloc[entry_idx - 1]["trade_date"],
            "direction": "short_usdrubf_long_rub",
            "signal_regime": "high_high",
            "entry_source_session_index": entry_idx,
            "execution_eligible": True,
        }
        for horizon in phase06.EXIT_HORIZONS:
            row[f"exit_{horizon}session_available"] = True
            row[f"exit_{horizon}session_trade_date"] = panel.iloc[entry_idx + horizon][
                "trade_date"
            ]
        rows.append(row)
    return pd.DataFrame(rows)


def test_short_return_and_cost_math_are_frozen() -> None:
    gross = phase06._gross_short_return(100.0, 95.0)
    assert math.isclose(gross, 0.05)
    assert math.isclose(phase06._net_short_return(gross, 0), 0.05)
    assert math.isclose(phase06._net_short_return(gross, 20), 0.048)


def test_trade_sequence_max_drawdown_uses_compounded_equity() -> None:
    drawdown = phase06._trade_sequence_max_drawdown([0.10, -0.05, 0.02])
    assert math.isclose(drawdown, 0.05, rel_tol=0.0, abs_tol=1e-12)


def test_materialized_backtest_uses_only_frozen_horizons_and_cost_grid() -> None:
    panel = _synthetic_panel()
    candidates = _synthetic_candidates(panel)
    trades = phase06._materialize_trade_results(candidates, panel)

    assert len(trades) == 36
    assert set(trades["horizon_sessions"]) == {5, 10, 20}
    assert set(trades["round_trip_cost_bps"]) == {0, 5, 10, 20}
    assert set(trades["direction"]) == {"short_usdrubf_long_rub"}
    assert set(trades["entry_execution"]) == {"open"}
    assert set(trades["exit_execution"]) == {"close"}

    one = trades.loc[
        trades["signal_id"].eq("s1")
        & trades["horizon_sessions"].eq(5)
        & trades["round_trip_cost_bps"].eq(20)
    ].iloc[0]
    expected_gross = (panel.iloc[5]["open"] - panel.iloc[10]["close"]) / panel.iloc[5][
        "open"
    ]
    assert math.isclose(one["gross_short_return"], expected_gross)
    assert math.isclose(one["net_short_return"], expected_gross - 0.002)


def test_backtest_metrics_have_exact_twelve_variants() -> None:
    panel = _synthetic_panel()
    trades = phase06._materialize_trade_results(_synthetic_candidates(panel), panel)
    metrics = phase06._build_backtest_metrics(trades)

    assert len(metrics) == 12
    assert set(metrics["horizon_sessions"]) == {5, 10, 20}
    assert set(metrics["round_trip_cost_bps"]) == {0, 5, 10, 20}
    assert set(metrics["trade_count"]) == {3}
    assert metrics["trade_sequence_max_drawdown"].ge(0.0).all()


def test_price_binding_rejects_terminal_ohlc_consumption() -> None:
    panel = _synthetic_panel()
    candidates = _synthetic_candidates(panel)
    terminal_entry = candidates.copy()
    terminal_entry.loc[0, "entry_source_session_index"] = len(panel) - 1
    terminal_entry.loc[0, "target_trade_date"] = panel.iloc[-1]["trade_date"]

    try:
        phase06._validate_price_binding(terminal_entry, panel)
    except phase06.Phase06BacktestError as exc:
        assert "entry open is not bound" in str(exc)
    else:
        raise AssertionError("terminal entry OHLC must fail closed")


def test_frozen_schedule_constant_has_three_independent_entries() -> None:
    assert len(phase06.FROZEN_SCHEDULE) == 3
    assert [row[0] for row in phase06.FROZEN_SCHEDULE] == [
        "2025-01-13",
        "2025-09-29",
        "2026-03-17",
    ]
