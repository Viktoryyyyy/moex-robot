from __future__ import annotations

import ast
import json
from pathlib import Path


ROOT = Path(__file__).parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/usdrubf_phase7_4_feature_policy_revision_experiment_v1.json"
)
BUILDER_PATH = (
    ROOT
    / "src/moex_research/runners/usdrubf_phase7_4_feature_policy_revision_builder.py"
)
EXPERIMENT_PATH = (
    ROOT
    / "src/moex_research/runners/usdrubf_phase7_4_feature_policy_revision_experiment.py"
)
APPROVED_FILES = [
    "contracts/experiments/usdrubf_phase7_4_feature_policy_revision_experiment_v1.json",
    "src/moex_research/runners/usdrubf_phase7_4_feature_policy_revision_builder.py",
    "src/moex_research/runners/usdrubf_phase7_4_feature_policy_revision_experiment.py",
    "tests/unit/test_usdrubf_phase7_4_feature_policy_revision_builder.py",
    "tests/unit/test_usdrubf_phase7_4_feature_policy_revision_experiment.py",
    "tests/contract/test_usdrubf_phase7_4_feature_policy_revision_experiment_contract.py",
]
CLI_ARGS = [
    "--source-panel-path",
    "--source-panel-manifest-path",
    "--modeling-dataset-path",
    "--dataset-manifest-path",
    "--feature-schema-path",
    "--readiness-contract-path",
    "--m0-validation-predictions-path",
    "--experiment-contract-path",
    "--output-dir",
    "--run-id",
    "--git-commit-sha",
]
OUTPUTS = [
    "input_identity_verification.json",
    "feature_matrix_inventory.json",
    "feature_nullness_by_matrix_and_fold.csv",
    "feature_shift_by_matrix_group_and_fold.csv",
    "fold_metrics_by_matrix.csv",
    "aggregate_metrics_by_matrix.json",
    "per_class_metrics_by_matrix.csv",
    "ablation_effects.csv",
    "gate_results.json",
]


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_scope_branch_and_shared_lock() -> None:
    contract = _contract()
    identity = contract["experiment_identity"]
    assert identity == {
        "contract_id": "usdrubf_phase7_4_feature_policy_revision_experiment_v1",
        "contract_version": "1.0",
        "design_recommendation_id": "usdrubf_phase7_4_feature_policy_revision_design_v4_complete_corrected",
        "execution_mode": "browser_chatgpt_github_direct",
        "lane": "ema_3_19_ai",
        "phase": "7.4",
        "project": "MOEX Bot",
        "status": "repository_implementation_only",
        "task_id": "ema_3_19_ai_market_phase_phase_7_4_feature_policy_revision_implementation",
    }
    assert contract["approved_branch"] == (
        "strategy/ema-3-19-ai/phase-7-4-feature-policy-revision-implementation"
    )
    scope = contract["approved_file_scope"]
    assert scope["create_only"] == APPROVED_FILES
    assert scope["existing_files_to_modify"] == []
    assert scope["exact_file_count"] == 6
    assert not scope["scope_widening_allowed"]
    lock = contract["shared_file_lock"]
    assert lock["required"]
    assert lock["lock_status"] == "granted_by_PM_L2"
    assert lock["owner_lane"] == "ema_3_19_ai"
    assert lock["branch"] == contract["approved_branch"]


def test_exact_cli_builder_import_only_and_output_inventory() -> None:
    contract = _contract()
    assert contract["required_cli_args"] == CLI_ARGS
    assert len(contract["required_cli_args"]) == 11
    assert contract["declared_output_artifacts"] == OUTPUTS
    assert contract["artifact_write_policy"]["exact_count"] == 9

    producer = contract["producer"]
    assert producer["builder_import_only"]
    assert not producer["builder_standalone_cli_allowed"]
    assert producer["sole_cli_entrypoint"]
    builder_source = BUILDER_PATH.read_text(encoding="utf-8")
    experiment_source = EXPERIMENT_PATH.read_text(encoding="utf-8")
    assert 'if __name__ == "__main__"' not in builder_source
    assert 'if __name__ == "__main__"' in experiment_source
    assert "argparse" not in builder_source
    assert "subprocess" not in builder_source
    assert "socket" not in builder_source


def test_exact_matrix_roles_inventories_and_ablation_policy() -> None:
    contract = _contract()
    matrices = contract["matrix_inventory"]
    assert list(matrices) == [
        "M0_FROZEN_PHASE7_2_CONTROL",
        "M1_REVISED_FULL",
        "M2_MINUS_NORMALIZED_EMA_TREND",
        "M3_MINUS_VOLATILITY_RANGE",
        "M4_MINUS_VOLUME_ACTIVITY",
        "M5_MINUS_LAGGED_INTERSESSION_GAP",
    ]
    assert matrices["M0_FROZEN_PHASE7_2_CONTROL"]["role"] == (
        "immutable_historical_control"
    )
    assert matrices["M1_REVISED_FULL"]["role"] == "sole_acceptance_candidate"
    assert matrices["M1_REVISED_FULL"]["numeric_count"] == 20
    assert matrices["M1_REVISED_FULL"]["categorical_count"] == 1
    assert matrices["M1_REVISED_FULL"]["total_pre_encoding"] == 21
    assert matrices["M2_MINUS_NORMALIZED_EMA_TREND"]["categorical_features"] == []
    assert "lag1_ema_3_19_state" in matrices[
        "M2_MINUS_NORMALIZED_EMA_TREND"
    ]["removed_from_M1"]
    assert matrices["M3_MINUS_VOLATILITY_RANGE"]["removed_from_M1"] == [
        "rolling_past_return_std_5",
        "rolling_past_return_std_20",
        "rolling_return_std_ratio_5_20",
        "lag1_hl_range_pct",
        "lag1_hl_range_to_prior20_mean",
        "rolling_hl_range_mean_ratio_5_20",
    ]
    assert matrices["M4_MINUS_VOLUME_ACTIVITY"]["removed_from_M1"] == [
        "lag1_log_volume_rel_prior20",
        "lag1_log_num_trades_rel_prior20",
        "lag1_log_avg_trade_value_rel_prior20",
        "rolling_log_volume_mean_diff_5_20",
    ]
    assert matrices["M5_MINUS_LAGGED_INTERSESSION_GAP"]["removed_from_M1"] == [
        "lag1_intersession_gap_days"
    ]
    assert contract["matrix_policy"] == {
        "M1_only_acceptance_candidate": True,
        "ablation_promotion_allowed": False,
        "combinatorial_search_allowed": False,
        "maximum_matrix_count": 6,
    }


def test_exact_formulas_windows_model_folds_and_preprocessing() -> None:
    contract = _contract()
    formulas = contract["exact_feature_definitions"]
    assert formulas["lag1_intersession_gap_days"]["formula"] == (
        "calendar_days(trade_date_(t-1) - trade_date_(t-2))"
    )
    assert formulas["lag1_ema_3_slope_5_pct"]["formula"] == (
        "EMA3_(t-1) / EMA3_(t-6) - 1"
    )
    assert formulas["lag1_hl_range_to_prior20_mean"]["formula"] == (
        "h_(t-1) / mean(h_s for s in [t-21,t-2]) - 1"
    )
    assert formulas["lag1_log_avg_trade_value_rel_prior20"]["formula"] == (
        "log1p(A_(t-1)/N_(t-1)) - mean(log1p(A_s/N_s) for s in [t-21,t-2])"
    )
    assert contract["ema_semantics"]["EMA3"] == (
        "pandas ewm(span=3, adjust=False).mean()"
    )
    assert contract["ema_semantics"]["EMA19"] == (
        "pandas ewm(span=19, adjust=False).mean()"
    )
    time = contract["time_semantics"]
    assert time["latest_permitted_source_session"] == "t-1"
    assert time["prior20_reference_windows_end"] == "t-2"
    assert not time["target_session_OHLCV_allowed"]
    assert not time["cross_instrument_state_allowed"]

    fixed = contract["fixed_non_feature_components"]
    assert fixed["splitter"] == {
        "gap": 0,
        "n_splits": 5,
        "test_size": 64,
        "type": "sklearn.model_selection.TimeSeriesSplit",
    }
    assert fixed["candidate_model"] == {
        "constructor": {
            "C": 1.0,
            "class_weight": "balanced",
            "max_iter": 1000,
            "solver": "lbfgs",
        },
        "estimator": "sklearn.linear_model.LogisticRegression",
    }
    assert fixed["preprocessing_fit_scope"] == "each_training_fold_only"
    assert not fixed["calibration_allowed"]
    assert not fixed["threshold_optimization_allowed"]
    assert not fixed["hyperparameter_search_allowed"]


def test_confidence_smd_success_and_failure_contracts_are_exact() -> None:
    contract = _contract()
    confidence = contract["confidence_bucket_policy"]
    assert confidence["method"] == "method_B"
    assert confidence["inclusive_bucket"] == {
        "lower_bound": 0.9,
        "upper_bound": 1.0,
    }
    assert confidence["bucket_gap"] == (
        "bucket_mean_confidence - bucket_accuracy"
    )
    assert confidence["M1_gate"][-1] == "M1_gap <= 0.70 * M0_gap"

    shift = contract["feature_shift_policy"]
    assert shift["feature_level_SMD"]["formula"] == (
        "abs(validation_mean - training_mean) / training_standard_deviation"
    )
    assert shift["feature_level_SMD"]["training_standard_deviation_ddof"] == 1
    assert set(shift["revised_M1_groups"]) == {
        "lagged_intersession_gap",
        "normalized_ema_trend",
        "volatility_range",
        "volume_activity",
    }
    assert set(shift["mapped_M0_groups"]) == set(
        shift["revised_M1_groups"]
    )
    assert shift["aggregation"]["pooled_group_median"] == (
        "median of five per-fold group medians"
    )

    success = contract["success_criteria"]
    failure = contract["failure_criteria"]
    assert set(success) == {f"S{index}" for index in range(1, 19)}
    assert set(failure) == {f"F{index}" for index in range(1, 12)}
    assert success["S1"]["condition"] == ">= 0.2458904109589041"
    assert success["S13"]["condition"] == (
        "<= 0.70 * identically recomputed M0 gap"
    )
    assert success["S18"]["condition"] == "all S1 through S17 pass for M1"
    assert failure["F1"]["condition"] == (
        "M1 aggregate_B_recall <= 0.1458904109589041"
    )
    assert failure["F10"]["condition"] == (
        "M0 bucket reference empty undefined nonfinite or nonpositive gap"
    )


def test_no_real_data_evaluation_promotion_server_or_merge_authority() -> None:
    contract = _contract()
    restrictions = contract["no_real_data_restrictions"]
    assert restrictions == {
        "controlled_server_evaluation_during_implementation": False,
        "real_Phase6_dataset_access_during_implementation": False,
        "real_Phase7_2_output_directory_access_during_implementation": False,
        "real_fit_during_implementation": False,
        "real_internal_D1_source_panel_access_during_implementation": False,
        "real_predictions_during_implementation": False,
        "repository_or_server_evaluation_artifacts_during_implementation": False,
    }
    authority = contract["authority_and_status"]
    assert not authority["evaluation_task_authorized"]
    assert not authority["direct_main_write_allowed"]
    assert not authority["merge_allowed"]
    assert authority["merge_authority"] == "PM_L2_ONLY"
    assert not authority["server_apply_allowed"]
    assert not authority["server_commands_allowed"]
    assert authority["runtime_allowed"] == "synthetic_pytest_only"
    assert not authority["real_model_fitting_allowed"]
    assert not authority["real_prediction_generation_allowed"]
    assert not authority["persistent_model_allowed"]
    assert not authority["model_promotion_allowed"]
    assert not authority["strategy_promotion_allowed"]
    assert not authority["live_prediction_allowed"]
    assert not authority["trading_allowed"]
    assert not authority["external_ingestion_allowed"]


def test_modules_use_one_phase_specific_exception_and_typed_dataclasses() -> None:
    for path, exception_name in (
        (BUILDER_PATH, "Phase74FeaturePolicyBuilderError"),
        (EXPERIMENT_PATH, "Phase74FeaturePolicyExperimentError"),
    ):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        exception_classes = [
            node.name
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and any(
                isinstance(base, ast.Name)
                and base.id in {"ValueError", "Exception", "RuntimeError"}
                for base in node.bases
            )
        ]
        assert exception_classes == [exception_name]
        dataclasses = [
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and any(
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "dataclass"
                for decorator in node.decorator_list
            )
        ]
        assert dataclasses
        for dataclass_node in dataclasses:
            assert all(
                not isinstance(statement, ast.AnnAssign)
                or statement.annotation is not None
                for statement in dataclass_node.body
            )
