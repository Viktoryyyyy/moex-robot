from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).parents[1] / "research/usdrubf_multisession_sequence_20261110/rework.py"
SPEC = importlib.util.spec_from_file_location("multisession_rework", MODULE_PATH)
assert SPEC and SPEC.loader
rework = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = rework
SPEC.loader.exec_module(rework)

HOLDOUT_PATH = Path(__file__).parents[1] / "research/usdrubf_multisession_sequence_20261110/forward_holdout.py"
HOLDOUT_SPEC = importlib.util.spec_from_file_location("multisession_forward_holdout", HOLDOUT_PATH)
assert HOLDOUT_SPEC and HOLDOUT_SPEC.loader
holdout = importlib.util.module_from_spec(HOLDOUT_SPEC)
sys.modules[HOLDOUT_SPEC.name] = holdout
HOLDOUT_SPEC.loader.exec_module(holdout)


def test_costs_expose_slippage_separately() -> None:
    assert rework.BASE_COSTS.commission_per_side == 0.005
    assert rework.BASE_COSTS.half_spread_per_side == 0.010
    assert rework.BASE_COSTS.slippage_per_side == 0.010
    assert rework.BASE_COSTS.all_in_per_side == 0.025
    assert rework.STRESS_COSTS.slippage_per_side == 0.035
    assert rework.STRESS_COSTS.all_in_per_side == 0.05


def test_execution_prices_are_always_adverse() -> None:
    costs = rework.Costs()
    assert rework._execution_price(100.0, 1, True, costs) == 100.02
    assert rework._execution_price(100.0, 1, False, costs) == 99.98
    assert rework._execution_price(100.0, -1, True, costs) == 99.98
    assert rework._execution_price(100.0, -1, False, costs) == 100.02


def test_atr_trail_uses_only_completed_sessions() -> None:
    daily = pd.DataFrame(
        [
            {"high": 101.0, "low": 98.0, "atr20_prior": 1.0},
            {"high": 110.0, "low": 99.0, "atr20_prior": 1.0},
        ]
    )
    variant = rework.Variant("stall_turn", 4, "price", 5, 0.1, "atr_trailing", 1.5, 0)
    stop, source = rework._trail_stop(variant, 1, 97.0, 0, 1, daily)
    # At the open of session 1, its high=110 is not known. Only session 0
    # may tighten the stop: 101 - 1.5 * 1 = 99.5.
    assert stop == 99.5
    assert source == "atr_trailing"


def test_structure_exit_uses_prior_completed_low() -> None:
    daily = pd.DataFrame(
        [
            {"high": 101.0, "low": 98.0, "atr20_prior": 1.0},
            {"high": 102.0, "low": 99.0, "atr20_prior": 1.0},
            {"high": 103.0, "low": 100.0, "atr20_prior": 1.0},
        ]
    )
    variant = rework.Variant("stall_turn", 4, "price", 5, 0.1, "structure", 0.0, 1)
    stop, source = rework._trail_stop(variant, 1, 97.0, 0, 2, daily)
    assert stop == 98.99
    assert source == "structure"


def test_variant_grid_has_no_fixed_time_exit() -> None:
    values = rework.variants()
    assert len(values) == 480
    assert {item.exit_mode for item in values} == {"atr_trailing", "structure", "hybrid"}
    assert all(not hasattr(item, "hold") for item in values)


def test_walk_forward_selection_dates_are_strictly_ordered() -> None:
    for fold in rework.FOLDS:
        assert fold.train_end < fold.validate_start <= fold.validate_end
    assert rework.FOLDS[-1].validate_end < rework.HISTORIC_HOLDOUT[0]


def test_training_selection_cannot_see_future_trade() -> None:
    left = rework.Variant("stall_turn", 3, "price", 3, 0.1, "atr_trailing", 1.5, 0)
    right = rework.Variant("stall_turn", 4, "price", 3, 0.1, "atr_trailing", 1.5, 0)

    def trade(entry: str, exit_date: str, net_r: float) -> dict[str, object]:
        return {
            "entry_date": entry,
            "exit_date": exit_date,
            "side": 1,
            "net_r": net_r,
            "net_points": net_r,
            "net_return_pct": net_r,
            "exit_reason": "atr_trailing",
            "mae_r": -0.2,
            "mfe_r": max(0.0, net_r),
        }

    mapping = {
        left.key: [trade("2024-01-01", "2024-01-02", -1.0), trade("2025-01-01", "2025-01-02", 100.0)],
        right.key: [trade("2024-01-01", "2024-01-02", 1.0), trade("2025-01-01", "2025-01-02", -100.0)],
    }
    winner, _table = rework.select_on_training([left, right], mapping, "2024-12-31")
    assert winner == right


def test_future_gate_never_promotes_automatically() -> None:
    gate = {
        "minimum_closed_trades": 20,
        "minimum_each_direction": 5,
        "promotion_allowed": False,
    }
    trades = []
    for index in range(20):
        trades.append(
            {
                "entry_date": f"2027-01-{index + 1:02d}",
                "exit_date": f"2027-01-{index + 1:02d}",
                "side": 1 if index < 10 else -1,
                "net_r": 0.2,
                "net_points": 0.2,
                "net_return_pct": 0.2,
                "exit_reason": "atr_trailing",
                "mae_r": -0.1,
                "mfe_r": 0.3,
            }
        )
    result = holdout.evaluate_gate(trades, gate)
    assert result["status"] == "READY_FOR_USER_REVIEW"
    assert result["automatic_review_ready"] is True
    assert result["promotion_allowed"] is False
