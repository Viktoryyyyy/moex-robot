from __future__ import annotations

import argparse
import ast
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moex_research.runners import (  # noqa: E402
    usdrubf_phase7_1_internal_baseline_model as runner,
)


CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/usdrubf_phase7_1_internal_baseline_model_v1.json"
)
RUNNER_PATH = (
    ROOT
    / "src/moex_research/runners/usdrubf_phase7_1_internal_baseline_model.py"
)
READINESS_PATH = (
    ROOT
    / "contracts/validation/usdrubf_phase7_modeling_readiness_target_policy_v1.yaml"
)
APPROVED_SCOPE = [
    "src/moex_research/runners/usdrubf_phase7_1_internal_baseline_model.py",
    "contracts/experiments/usdrubf_phase7_1_internal_baseline_model_v1.json",
    "tests/unit/test_usdrubf_phase7_1_internal_baseline_model.py",
    "tests/contract/test_usdrubf_phase7_1_internal_baseline_model_contract.py",
]


def _contract() -> dict[str, object]:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_contract_has_all_required_sections_and_exact_identity() -> None:
    contract = _contract()
    required_sections = {
        "experiment_identity",
        "producer",
        "approved_file_scope",
        "authority_and_status",
        "required_cli_args",
        "input_artifacts",
        "immutable_input_identity",
        "target_policy_binding",
        "supervised_row_eligibility",
        "feature_policy",
        "preprocessing",
        "model",
        "validation_protocol",
        "train_only_baselines",
        "required_metrics",
        "undefined_metric_policy",
        "leakage_fail_closed_policy",
        "declared_output_artifacts",
        "artifact_write_policy",
        "forbidden_capabilities",
        "acceptance_invariants",
    }
    assert required_sections.issubset(contract)

    identity = contract["experiment_identity"]
    assert identity["contract_id"] == runner.EXPERIMENT_CONTRACT_ID
    assert identity["contract_version"] == runner.EXPERIMENT_CONTRACT_VERSION
    assert identity["project"] == "MOEX Bot"
    assert identity["lane"] == "ema_3_19_ai"
    assert (
        identity["task_id"]
        == "ema_3_19_ai_market_phase_phase_7_1_internal_baseline_model"
    )


def test_contract_declares_exact_four_new_file_scope_only() -> None:
    contract = _contract()
    scope = contract["approved_file_scope"]
    assert scope["create_only"] == APPROVED_SCOPE
    assert scope["existing_files_to_modify"] == []
    assert scope["exact_file_count"] == 4
    assert scope["scope_widening_allowed"] is False

    forbidden_existing_or_shared_paths = {
        "requirements.txt",
        "pyproject.toml",
        ".github/workflows/tests.yml",
        "contracts/datasets/usdrubf_phase6_internal_modeling_dataset.v1.yaml",
        "contracts/features/usdrubf_phase6_internal_factor_batches_v1.json",
        "contracts/validation/usdrubf_phase7_modeling_readiness_target_policy_v1.yaml",
    }
    assert forbidden_existing_or_shared_paths.isdisjoint(scope["create_only"])
    assert all(path in APPROVED_SCOPE for path in scope["create_only"])


def test_runner_and_contract_agree_on_exact_cli_arguments() -> None:
    contract = _contract()
    assert contract["required_cli_args"] == list(runner.REQUIRED_CLI_ARGS)

    parser = runner.build_argument_parser()
    observed_options = [
        option
        for action in parser._actions
        for option in action.option_strings
        if option.startswith("--") and option != "--help"
    ]
    assert observed_options == list(runner.REQUIRED_CLI_ARGS)

    forbidden_mutable_controls = {
        "--C",
        "--class-weight",
        "--solver",
        "--max-iter",
        "--n-splits",
        "--test-size",
        "--gap",
        "--shuffle",
        "--random-state",
        "--feature-list",
        "--threshold",
        "--calibration",
    }
    assert forbidden_mutable_controls.isdisjoint(observed_options)


def test_exact_feature_lists_and_exclusions_are_frozen() -> None:
    contract = _contract()
    policy = contract["feature_policy"]
    assert policy["numeric_features"] == list(runner.NUMERIC_FEATURES)
    assert policy["categorical_features"] == list(runner.CATEGORICAL_FEATURES)
    assert policy["excluded_target_and_metadata_columns"] == list(
        runner.EXCLUDED_TARGET_AND_METADATA_COLUMNS
    )
    assert runner.MODEL_FEATURES == (
        *runner.NUMERIC_FEATURES,
        *runner.CATEGORICAL_FEATURES,
    )
    assert set(runner.MODEL_FEATURES).isdisjoint(
        runner.EXCLUDED_TARGET_AND_METADATA_COLUMNS
    )
    assert policy["exact_model_matrix_required"] is True
    assert policy["unknown_or_extra_dataset_columns_silently_added"] is False


def test_exact_logistic_regression_constructor_and_preprocessing() -> None:
    contract = _contract()
    assert contract["model"]["estimator"] == (
        "sklearn.linear_model.LogisticRegression"
    )
    assert contract["model"]["constructor"] == runner.MODEL_CONSTRUCTOR == {
        "C": 1.0,
        "class_weight": "balanced",
        "solver": "lbfgs",
        "max_iter": 1000,
    }
    assert set(runner.MODEL_CONSTRUCTOR).isdisjoint(
        {"multi_class", "random_state", "n_jobs", "penalty"}
    )

    pipeline = runner.build_candidate_pipeline()
    classifier = pipeline.named_steps["classifier"]
    assert classifier.C == 1.0
    assert classifier.class_weight == "balanced"
    assert classifier.solver == "lbfgs"
    assert classifier.max_iter == 1000

    preprocessor = pipeline.named_steps["preprocessor"]
    numeric = preprocessor.transformers[0][1]
    categorical = preprocessor.transformers[1][1]
    assert numeric.named_steps["imputer"].strategy == "median"
    assert numeric.named_steps["scaler"].__class__.__name__ == "StandardScaler"
    assert categorical.named_steps["imputer"].strategy == "most_frequent"
    assert categorical.named_steps["encoder"].handle_unknown == "ignore"


def test_exact_time_series_split_constructor_and_invariants() -> None:
    contract = _contract()
    protocol = contract["validation_protocol"]
    assert protocol["splitter"] == "sklearn.model_selection.TimeSeriesSplit"
    assert protocol["constructor"] == runner.SPLITTER_CONSTRUCTOR == {
        "n_splits": 5,
        "test_size": 64,
        "gap": 0,
    }
    assert protocol["shuffle"] is False
    assert protocol["expected_validation_rows_per_fold"] == 64
    assert protocol["expected_total_validation_rows"] == 320
    assert protocol["all_three_classes_required_in_each_training_fold"] is True
    assert protocol["missing_validation_class_allowed"] is True
    assert protocol["fold_failure_policy"] == "fail_closed"


def test_exact_train_only_baseline_definitions() -> None:
    contract = _contract()
    baselines = contract["train_only_baselines"]
    assert baselines["majority_class_train_only"] == {
        "definition": (
            "predict the most frequent eligible class observed in the training fold"
        ),
        "validation_information_used": False,
    }
    assert baselines["class_prior_train_only"] == {
        "definition": (
            "use training-fold B/S/OUT class proportions as fixed validation probabilities"
        ),
        "validation_information_used": False,
    }
    assert (
        baselines["candidate_and_baselines_use_identical_validation_identities"]
        is True
    )
    assert (
        baselines[
            "baseline_state_recomputed_independently_for_every_training_fold"
        ]
        is True
    )
    assert baselines["validation_labels_influence_baseline_construction"] is False


def test_metrics_class_order_and_undefined_policy_are_exact() -> None:
    contract = _contract()
    metrics = contract["required_metrics"]
    assert metrics["common"] == list(runner.REQUIRED_COMMON_METRICS)
    assert metrics["probability"] == [runner.PROBABILITY_METRIC]
    assert metrics["fixed_class_order"] == list(runner.CLASS_ORDER)

    undefined = contract["undefined_metric_policy"]
    assert undefined["zero_support_class"] == {
        "support": 0,
        "precision_recall_f1": "explicit_null_or_explicit_undefined_state",
    }
    assert undefined["favorable_zero_substitution"] is False
    assert undefined["confusion_matrix_class_order"] == list(runner.CLASS_ORDER)
    assert undefined["probability_alignment"] == list(runner.CLASS_ORDER)


def test_exact_eight_artifact_inventory_and_write_policy() -> None:
    contract = _contract()
    assert contract["declared_output_artifacts"] == list(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )
    assert len(runner.DECLARED_OUTPUT_ARTIFACTS) == 8

    policy = contract["artifact_write_policy"]
    assert policy["synthetic_test_outputs_allowed"] is True
    assert policy["synthetic_output_location"] == "pytest_tmp_path_only"
    assert policy["generated_repository_artifacts_allowed"] is False
    assert policy["real_phase6_output_creation_allowed"] is False
    assert policy["real_runtime_deferred_to"] == "Phase 7.2"
    assert policy["outputs_outside_explicit_output_dir_allowed"] is False
    assert policy["undeclared_outputs_allowed"] is False
    assert policy["non_empty_output_dir_allowed"] is False
    assert policy["model_pickle_or_joblib_allowed"] is False


def test_frozen_readiness_contract_sha_and_markers_match_repository_bytes() -> None:
    contract = _contract()
    binding = contract["target_policy_binding"]
    observed_sha = hashlib.sha256(READINESS_PATH.read_bytes()).hexdigest()
    assert observed_sha == runner.READINESS_CONTRACT_SHA256
    assert observed_sha == binding["canonical_sha256"]
    assert binding["canonical_repository_commit"] == (
        "6e8530e8d548d0016d17758785d797fe9995ace4"
    )
    assert binding["required_markers"] == list(runner.REQUIRED_READINESS_MARKERS)
    assert binding["yaml_parser_dependency_allowed"] is False
    assert binding["mismatch_policy"] == "fail_closed"


def test_forbidden_runtime_external_serialization_and_trading_capabilities() -> None:
    contract = _contract()
    authority = contract["authority_and_status"]
    assert authority == {
        "merge_authority": "PM_L2_ONLY",
        "model_promotion_allowed": False,
        "persistent_model_artifact_allowed": False,
        "real_dataset_fit_allowed": False,
        "real_prediction_generation_allowed": False,
        "repository_research_runner": True,
        "runtime_ready": False,
        "server_apply_allowed": False,
        "strategy_promotion_allowed": False,
        "synthetic_test_fit_allowed": True,
        "trading_conclusion_allowed": False,
    }

    forbidden = contract["forbidden_capabilities"]
    assert all(forbidden.values())
    assert set(forbidden) == {
        "network_calls",
        "subprocess_calls",
        "provider_calls",
        "external_data_ingestion",
        "broker_or_trading_actions",
        "live_prediction",
        "signal_generation",
        "persistent_model_serialization",
        "server_commands",
        "server_apply",
        "direct_main_write",
        "merge",
    }


def test_runner_ast_has_no_forbidden_import_or_external_action_surface() -> None:
    source = RUNNER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden_import_roots = {
        "requests",
        "urllib",
        "http",
        "socket",
        "subprocess",
        "pickle",
        "joblib",
    }
    imported_roots: set[str] = set()
    forbidden_calls = {
        "system",
        "popen",
        "run",
        "check_call",
        "check_output",
        "urlopen",
        "request",
        "dump",
        "dumps_model",
        "load_model",
        "create_order",
        "place_order",
        "submit_order",
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call):
            function_name = getattr(node.func, "attr", None) or getattr(
                node.func, "id", None
            )
            assert function_name not in forbidden_calls

    assert forbidden_import_roots.isdisjoint(imported_roots)
    assert "/home/trader" not in source
    assert "moex_robot" not in source
    assert ".pkl" not in source
    assert ".pickle" not in source
    assert ".joblib" not in source


def test_contract_scope_excludes_dependencies_workflows_shared_existing_files_and_route_b() -> None:
    contract = _contract()
    scope = contract["approved_file_scope"]["create_only"]
    forbidden_prefixes = (
        ".github/",
        "docs/",
        "contracts/datasets/",
        "contracts/features/",
        "contracts/validation/",
        "src/moex_core/",
        "src/moex_data/",
        "src/moex_features/",
        "src/moex_backtest/",
        "src/moex_strategy_sdk/",
        "docs/sot/context/schemas/route_b_",
        "docs/sot/route_b/",
    )
    assert "requirements.txt" not in scope
    assert "pyproject.toml" not in scope
    assert all(not path.startswith(forbidden_prefixes) for path in scope)
    assert all("usdrubf_ema_3_19_d1_economic_cost_capacity" not in path for path in scope)
