from __future__ import annotations

import json
from pathlib import Path

from src.moex_research.runners.usdrubf_ema_3_19_d1_economic_cost_capacity import (
    DECLARED_OUTPUT_FILES,
    EXPERIMENT_ID,
    M2_EXPERIMENT_ID,
    M4B_EXPERIMENT_ID,
    M4C_EXPERIMENT_ID,
    _build_parser,
)

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "src/moex_research/runners/usdrubf_ema_3_19_d1_economic_cost_capacity.py"
CONTRACT = ROOT / "contracts/experiments/usdrubf_ema_3_19_d1_economic_cost_capacity_v1.json"
APPROVED_FILE_SCOPE = {
    "src/moex_research/runners/usdrubf_ema_3_19_d1_economic_cost_capacity.py",
    "contracts/experiments/usdrubf_ema_3_19_d1_economic_cost_capacity_v1.json",
    "tests/unit/test_usdrubf_ema_3_19_d1_economic_cost_capacity.py",
    "tests/contract/test_usdrubf_ema_3_19_d1_economic_cost_capacity_contract.py",
}


def _contract() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def test_exact_four_file_m5a_scope_is_present() -> None:
    assert len(APPROVED_FILE_SCOPE) == 4
    assert all((ROOT / path).is_file() for path in APPROVED_FILE_SCOPE)


def test_contract_binds_exact_inputs_lineage_and_outputs() -> None:
    contract = _contract()
    assert contract["experiment_id"] == EXPERIMENT_ID
    assert contract["required_cli_args"] == [
        "--d1-ohlc-path",
        "--cross-context-path",
        "--m2-quality-report-path",
        "--m4b-decision-path",
        "--m4c-decision-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ]
    assert all(item["contract_class"] == "cli_argument" for item in contract["input_artifacts"])
    assert all(item["implicit_discovery_allowed"] is False for item in contract["input_artifacts"])
    assert [item["source_experiment_id"] for item in contract["input_artifacts"]] == [
        M2_EXPERIMENT_ID,
        M2_EXPERIMENT_ID,
        M2_EXPERIMENT_ID,
        M4B_EXPERIMENT_ID,
        M4C_EXPERIMENT_ID,
    ]
    assert contract["lineage"] == {
        "required_m4b_result": "rule_gate_not_supported",
        "required_m4b_selected_rule": None,
        "required_m4c_result": "technical_ml_not_supported",
        "required_m4c_selected_feature_group": None,
        "economic_candidate": "unfiltered EMA(3/19) crossover baseline",
    }
    assert [item["filename"] for item in contract["output_artifacts"]] == list(
        DECLARED_OUTPUT_FILES
    )


def test_contract_requires_canonical_engine_and_freezes_execution_semantics() -> None:
    contract = _contract()
    semantics = contract["canonical_backtest_semantics"]
    assert semantics == {
        "engine": "moex_backtest.engine.canonical.CanonicalBacktestEngine",
        "engine_id": "canonical_backtest_engine_minimal_v1",
        "fill_model_id": "next_bar_open",
        "execution_delay": "strictly next finalized D1 bar after signal timestamp",
        "signal_trade_price": "next D1 open",
        "reversal": "target-position delta at next D1 open; quantity two when reversing between +1 and -1",
        "terminal_close": "last valid D1 close",
        "missing_next_bar": "reject signal",
        "invalid_next_bar": "reject signal",
        "position_size": "one normalized contract unit",
        "commission_bps": 0.0,
        "slippage_bps": 0.0,
        "simulation_role": "gross execution baseline and turnover source only",
    }
    assert contract["strategy_signal_semantics"]["cross_up_target_position"] == 1
    assert contract["strategy_signal_semantics"]["cross_down_target_position"] == -1
    assert contract["strategy_signal_semantics"]["flat_between_crosses"] is False


def test_economic_measurement_is_cost_capacity_not_an_actual_cost_claim() -> None:
    contract = _contract()
    measurement = contract["economic_measurement"]
    assert measurement["break_even_all_in_bps_per_traded_notional"] == (
        "gross total PnL / gross turnover notional * 10000"
    )
    assert measurement["net_pnl_equation"] == (
        "gross_pnl_price_units - gross_turnover_notional * all_in_bps / 10000"
    )
    assert measurement["actual_market_cost_claim_allowed"] is False
    assert contract["status"]["actual_market_costs_bound"] is False
    assert contract["status"]["contract_multiplier_bound"] is False
    assert contract["status"]["funding_or_roll_bound"] is False
    assert len(contract["required_unbound_economic_inputs"]) == 6


def test_decision_cannot_promote_strategy_or_runtime() -> None:
    contract = _contract()
    decision = contract["decision_conditions"]
    assert decision == {
        "gross_rejection_if": "gross total PnL <= 0",
        "gross_rejection_result": "economic_baseline_not_supported_gross",
        "positive_gross_result": "economic_cost_binding_required",
        "positive_gross_interpretation": (
            "positive gross result establishes only cost capacity; it does not establish net profitability"
        ),
        "full_economic_support_result_available": False,
        "strategy_promotion_allowed": False,
        "runtime_or_trading_action_allowed": False,
    }
    assert contract["known_limitations"]["daily_mark_to_market_path_available"] is False
    assert contract["known_limitations"]["ruble_pnl_available"] is False


def test_cli_has_no_cost_override_or_strategy_promotion_surface() -> None:
    parser = _build_parser()
    options = {
        option
        for action in parser._actions
        for option in action.option_strings
    }
    for forbidden in (
        "--commission-bps",
        "--slippage-bps",
        "--contract-multiplier",
        "--funding-rate",
        "--threshold",
        "--promote",
        "--runtime",
    ):
        assert forbidden not in options


def test_runner_delegates_execution_and_costs_to_canonical_backtest_engine() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    for required in (
        "CanonicalBacktestEngine",
        "CanonicalBacktestInput",
        "CostConfig",
        "ExecutionConfig",
        "custom_strategy_pnl_engine_used",
        "break_even_all_in_bps_per_traded_notional",
    ):
        assert required in source
    for forbidden in (
        "LogisticRegression",
        "predict_proba",
        "GridSearchCV",
        "XGBClassifier",
        "_execute_transition",
        "_apply_costs",
        "joblib.dump",
        "pickle.dump",
        "broker_adapter",
        "live_adapter",
    ):
        assert forbidden not in source
