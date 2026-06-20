from __future__ import annotations

import json
from pathlib import Path

from src.moex_research.runners.usdrubf_ema_3_19_d1_rule_gate_benchmark import (
    BOOTSTRAP_CONFIDENCE_LEVEL,
    BOOTSTRAP_REPETITIONS,
    BOOTSTRAP_SEED,
    CANDIDATE_RULES,
    DECLARED_OUTPUT_FILES,
    EXPERIMENT_ID,
    MAXIMUM_ACCEPTANCE_RATE,
    MINIMUM_ACCEPTANCE_RATE,
    MINIMUM_ACCEPTED_EVENTS,
    RANDOM_REPETITIONS,
    RANDOM_SEED,
    RULE_NAMES,
    _build_parser,
)

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts/experiments/usdrubf_ema_3_19_d1_rule_gate_benchmark_v1.json"
RUNNER_PATH = ROOT / "src/moex_research/runners/usdrubf_ema_3_19_d1_rule_gate_benchmark.py"
APPROVED_FILE_SCOPE = {
    "src/moex_research/runners/usdrubf_ema_3_19_d1_rule_gate_benchmark.py",
    "contracts/experiments/usdrubf_ema_3_19_d1_rule_gate_benchmark_v1.json",
    "tests/unit/test_usdrubf_ema_3_19_d1_rule_gate_benchmark.py",
    "tests/contract/test_usdrubf_ema_3_19_rule_gate_benchmark_contract.py",
}


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_exact_four_file_m4b_scope_is_present() -> None:
    assert len(APPROVED_FILE_SCOPE) == 4
    assert all((ROOT / path).is_file() for path in APPROVED_FILE_SCOPE)


def test_contract_binds_exact_cli_inputs_and_outputs() -> None:
    contract = _contract()
    assert contract["experiment_id"] == EXPERIMENT_ID
    assert contract["producer"] == {
        "module": "src.moex_research.runners.usdrubf_ema_3_19_d1_rule_gate_benchmark",
        "invocation": "python -m src.moex_research.runners.usdrubf_ema_3_19_d1_rule_gate_benchmark",
    }
    assert contract["required_cli_args"] == [
        "--indicator-context-path",
        "--labels-path",
        "--quality-report-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ]
    assert [item["contract_class"] for item in contract["input_artifacts"]] == [
        "cli_argument",
        "cli_argument",
        "cli_argument",
    ]
    assert all(item["implicit_discovery_allowed"] is False for item in contract["input_artifacts"])
    assert [item["filename"] for item in contract["output_artifacts"]] == list(
        DECLARED_OUTPUT_FILES
    )
    assert contract["artifact_write_policy"]["declared_outputs_only"] is True
    assert contract["artifact_write_policy"]["stdout_only_result_allowed"] is False


def test_frozen_rules_targets_and_controls_match_the_m4b_envelope() -> None:
    contract = _contract()
    assert list(contract["frozen_rules"]) == list(RULE_NAMES)
    assert contract["frozen_rules"] == {
        "no_gate": ["indicator_ready"],
        "adx_di": ["adx_14 >= 25", "dir_di_spread > 0"],
        "adx_di_momentum": [
            "adx_14 >= 25",
            "dir_di_spread > 0",
            "dir_roc_10 > 0",
            "dir_rsi_centered > 0",
        ],
        "moderate_trend_confirmation": [
            "adx_14 >= 20",
            "dir_di_spread > 0",
            "dir_macd_hist > 0",
            "dir_bb_position > 0",
        ],
    }
    assert tuple(contract["frozen_rules"])[1:] == CANDIDATE_RULES
    assert contract["targets"]["primary"] == {
        "horizon": "h2",
        "fields": ["h2_signed_return", "h2_allow_trade"],
    }
    assert contract["targets"]["secondary"] == {
        "target": "h10_no_opposite_cross",
        "derive_as": "not h10_opposite_cross_before_exit",
        "restriction": "evaluation label only, never a gate feature",
    }
    assert contract["random_gate_control"]["seed"] == RANDOM_SEED == 319
    assert contract["random_gate_control"]["repetitions"] == RANDOM_REPETITIONS == 10_000
    assert contract["bootstrap_control"]["seed"] == BOOTSTRAP_SEED == 319
    assert contract["bootstrap_control"]["repetitions"] == BOOTSTRAP_REPETITIONS == 5_000
    assert contract["bootstrap_control"]["confidence_level"] == BOOTSTRAP_CONFIDENCE_LEVEL == 0.90


def test_decision_limits_and_fallback_are_frozen() -> None:
    contract = _contract()
    limits = contract["decision_conditions"]["candidate_limits"]
    assert MINIMUM_ACCEPTED_EVENTS == 12
    assert MINIMUM_ACCEPTANCE_RATE == 0.20
    assert MAXIMUM_ACCEPTANCE_RATE == 0.60
    assert limits == {
        "minimum_accepted_events": MINIMUM_ACCEPTED_EVENTS,
        "minimum_acceptance_rate": MINIMUM_ACCEPTANCE_RATE,
        "maximum_acceptance_rate": MAXIMUM_ACCEPTANCE_RATE,
    }
    assert contract["decision_conditions"]["fallback_result"] == "rule_gate_not_supported"
    assert len(contract["decision_conditions"]["h2_rule_supported_only_if"]) == 6
    assert len(contract["decision_conditions"]["h10_persistence_supported_only_if"]) == 5


def test_runner_exposes_no_control_override_or_model_path() -> None:
    parser = _build_parser()
    option_strings = {
        option
        for action in parser._actions
        for option in action.option_strings
    }
    assert "--random-seed" not in option_strings
    assert "--random-repetitions" not in option_strings
    assert "--bootstrap-seed" not in option_strings
    assert "--bootstrap-repetitions" not in option_strings
    assert "--threshold" not in option_strings

    source = RUNNER_PATH.read_text(encoding="utf-8")
    for forbidden in (
        "sklearn",
        "LogisticRegression",
        "HistGradientBoosting",
        "xgboost",
        "XGBClassifier",
        "predict_proba",
        "threshold_grid",
    ):
        assert forbidden not in source
    assert '"h10_no_opposite_cross_use": "evaluation label only"' in source
    assert '"label_or_future_fields_used_in_gate": []' in source
