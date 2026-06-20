from __future__ import annotations

import json
from pathlib import Path

from src.moex_research.runners.usdrubf_ema_3_19_d1_technical_ml_benchmark import (
    CALIBRATION_BINS,
    CLASSIFICATION_THRESHOLD,
    DECLARED_OUTPUT_FILES,
    EXPERIMENT_ID,
    FEATURE_GROUPS,
    FEATURE_GROUP_NAMES,
    MAXIMUM_RETENTION_RATE,
    MINIMUM_INITIAL_TRAIN_EVENTS,
    MINIMUM_RETAINED_EVENTS,
    MINIMUM_RETENTION_RATE,
    MODEL_C,
    MODEL_CANDIDATES,
    MODEL_RANDOM_STATE,
    PERMUTATION_REPETITIONS,
    PERMUTATION_SEED,
    PURGE_D1_SESSIONS,
    TEST_BLOCK_EVENTS,
    _build_parser,
)

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "src/moex_research/runners/usdrubf_ema_3_19_d1_technical_ml_benchmark.py"
CONTRACT = ROOT / "contracts/experiments/usdrubf_ema_3_19_d1_technical_ml_benchmark_v1.json"
APPROVED_FILE_SCOPE = {
    "src/moex_research/runners/usdrubf_ema_3_19_d1_technical_ml_benchmark.py",
    "contracts/experiments/usdrubf_ema_3_19_d1_technical_ml_benchmark_v1.json",
    "tests/unit/test_usdrubf_ema_3_19_d1_technical_ml_benchmark.py",
    "tests/contract/test_usdrubf_ema_3_19_d1_technical_ml_benchmark_contract.py",
}


def _contract() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def test_exact_four_file_scope_is_present() -> None:
    assert len(APPROVED_FILE_SCOPE) == 4
    assert all((ROOT / path).is_file() for path in APPROVED_FILE_SCOPE)


def test_contract_binds_exact_cli_inputs_outputs_and_lineage() -> None:
    contract = _contract()
    assert contract["experiment_id"] == EXPERIMENT_ID
    assert contract["producer"] == {
        "module": "src.moex_research.runners.usdrubf_ema_3_19_d1_technical_ml_benchmark",
        "invocation": "python -m src.moex_research.runners.usdrubf_ema_3_19_d1_technical_ml_benchmark",
    }
    assert contract["required_cli_args"] == [
        "--indicator-context-path",
        "--labels-path",
        "--quality-report-path",
        "--m4b-decision-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ]
    assert all(item["contract_class"] == "cli_argument" for item in contract["input_artifacts"])
    assert all(item["implicit_discovery_allowed"] is False for item in contract["input_artifacts"])
    assert contract["lineage"]["m4a_source_experiment_id"] == (
        "usdrubf_ema_3_19_d1_indicators_horizons_v1"
    )
    assert contract["lineage"]["m4b_source_experiment_id"] == (
        "usdrubf_ema_3_19_d1_rule_gate_benchmark_v1"
    )
    assert contract["lineage"]["required_m4b_result"] == "rule_gate_not_supported"
    assert [item["filename"] for item in contract["output_artifacts"]] == list(
        DECLARED_OUTPUT_FILES
    )


def test_feature_groups_model_and_walk_forward_protocol_are_frozen() -> None:
    contract = _contract()
    assert list(contract["feature_groups"]) == list(FEATURE_GROUP_NAMES)
    assert contract["feature_groups"] == {
        name: {
            "numeric": list(definition["numeric"]),
            "categorical": list(definition["categorical"]),
            "candidate": definition["candidate"],
        }
        for name, definition in FEATURE_GROUPS.items()
    }
    assert tuple(
        name for name, definition in contract["feature_groups"].items() if definition["candidate"]
    ) == MODEL_CANDIDATES
    assert contract["model"] == {
        "estimator": "sklearn.linear_model.LogisticRegression",
        "C": MODEL_C,
        "class_weight": "balanced",
        "solver": "liblinear",
        "random_state": MODEL_RANDOM_STATE,
        "probability_mode": "native logistic probability",
        "post_hoc_calibrator": None,
        "persistent_model_artifact": False,
    }
    assert contract["validation_protocol"] == {
        "mode": "expanding_walk_forward",
        "shuffle": False,
        "minimum_initial_train_events_after_purge": MINIMUM_INITIAL_TRAIN_EVENTS,
        "test_block_events": TEST_BLOCK_EVENTS,
        "final_partial_test_block_allowed": True,
        "purge_d1_sessions": PURGE_D1_SESSIONS,
        "classification_threshold": CLASSIFICATION_THRESHOLD,
        "threshold_tuning": False,
        "hyperparameter_search": False,
        "preprocessing_fit_scope": "training fold only",
    }


def test_negative_controls_and_decision_boundaries_are_frozen() -> None:
    contract = _contract()
    assert contract["negative_controls"] == {
        "direction_only_feature_group": True,
        "training_label_permutation": {
            "seed": PERMUTATION_SEED,
            "repetitions": PERMUTATION_REPETITIONS,
            "scope": "training labels independently inside each walk-forward fold",
            "oos_labels_unchanged": True,
            "adjustment": "one-sided max-statistic across four candidate feature groups",
        },
    }
    limits = contract["decision_conditions"]["candidate_limits"]
    assert limits == {
        "minimum_oos_events": 24,
        "minimum_retained_events": MINIMUM_RETAINED_EVENTS,
        "minimum_retention_rate": MINIMUM_RETENTION_RATE,
        "maximum_retention_rate": MAXIMUM_RETENTION_RATE,
    }
    assert contract["decision_conditions"]["fallback_result"] == "technical_ml_not_supported"
    assert contract["decision_conditions"]["model_promotion_allowed"] is False
    assert contract["decision_conditions"]["strategy_promotion_allowed"] is False


def test_cli_has_no_override_for_frozen_model_controls() -> None:
    parser = _build_parser()
    options = {
        option
        for action in parser._actions
        for option in action.option_strings
    }
    for forbidden in (
        "--threshold",
        "--C",
        "--model-c",
        "--random-state",
        "--permutation-seed",
        "--permutation-repetitions",
        "--purge-sessions",
        "--test-block-events",
        "--feature-group",
    ):
        assert forbidden not in options


def test_runner_reuses_m4b_semantics_and_contains_no_search_or_runtime_surface() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert "build_analysis_frame" in source
    assert "build_rule_masks" in source
    assert "LogisticRegression" in source
    assert "expanding_walk_forward" in source
    assert "training labels permuted independently inside each walk-forward fold" in source
    for forbidden in (
        "GridSearchCV",
        "RandomizedSearchCV",
        "HistGradientBoosting",
        "XGBClassifier",
        "xgboost",
        "threshold_grid",
        "joblib.dump",
        "pickle.dump",
        "broker",
        "live_adapter",
    ):
        assert forbidden not in source
    assert "label_or_future_fields_used_as_features" in source
    assert "persistent_model_artifact_emitted" in source
    assert CALIBRATION_BINS == 5
