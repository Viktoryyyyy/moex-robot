from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts"
    / "experiments"
    / "usdrubf_phase8_3_external_factor_incremental_value_experiment_v1.json"
)


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_exact_project_phase_lane_task_and_branch() -> None:
    contract = _contract()
    identity = contract["experiment_identity"]
    assert identity == {
        "contract_id": "usdrubf_phase8_3_external_factor_incremental_value_experiment_v1",
        "contract_version": "1.0",
        "project": "MOEX Bot",
        "phase": "8.3",
        "lane": "ema_3_19_ai",
        "task_id": "ema_3_19_ai_market_phase_phase_8_3_external_factor_incremental_value_experiment_v1",
        "execution_mode": "browser_chatgpt_github_direct",
        "status": "repository_implementation_only",
    }
    assert contract["approved_branch"] == (
        "research/ema-3-19-ai/phase-8-3-external-factor-incremental-value"
    )


def test_exact_six_create_only_files() -> None:
    scope = _contract()["approved_file_scope"]
    assert scope["exact_file_count"] == 6
    assert scope["existing_files_to_modify"] == []
    assert scope["scope_widening_allowed"] is False
    assert scope["create_only"] == [
        "contracts/experiments/usdrubf_phase8_3_external_factor_incremental_value_experiment_v1.json",
        "src/moex_research/runners/usdrubf_phase8_3_external_factor_builder.py",
        "src/moex_research/runners/usdrubf_phase8_3_external_factor_incremental_value_experiment.py",
        "tests/unit/test_usdrubf_phase8_3_external_factor_builder.py",
        "tests/unit/test_usdrubf_phase8_3_external_factor_incremental_value_experiment.py",
        "tests/contract/test_usdrubf_phase8_3_external_factor_incremental_value_experiment_contract.py",
    ]


def test_exact_upstream_hashes_and_phase82_identity() -> None:
    contract = _contract()
    assert contract["upstream_sha256"] == {
        "modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
        "dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
        "feature_schema": "8f08802c7fb0a4cc43ab4ba072ee22ff9edd92fe8d674ea0515545d20d143238",
        "m0_validation_predictions": "9769d00a49adeb54c016d965387774e46a3e09e09f895aa61d48a90bbf3568cf",
        "phase82_input_identity": "3285631da929d5b8a8b3399b4ac02304a2b4819f611514ba9a9b294bd238f243",
        "phase82_source_fetch_manifest": "88df6150db6c70d9d7e3177169c00b49ae1e9880bf175921f58b4041e424cd96",
        "phase82_ruonia_normalized": "b2417ce39e64345aba0357d3e4b7aac536f8a6a6edbd33aef5aac83fbbc8ba17",
        "phase82_key_rate_normalized": "d4bd6064f943a008c149a566ee2408b3a21ffa700c2b09f234ef64d480a7b787",
        "phase82_external_matrix": "04bd613e850d1763026cbd6e19d7c38e0ff9c8ec33817ad400b4489cf36393ec",
        "phase82_coverage": "810c6c2136a24e1b766aadd5e36a3524801543cdbe527d17bf37e59e7090ba43",
        "phase82_staleness": "160a20e56e6548b5c67321707e5869bad82fa1118fbd38e53eb5905a1df217af",
        "phase82_blocker_register": "b0408a760ba688784ace0e76ee94f399f19b22ee078de9a9e461b10953316bf9",
        "phase82_gate_results": "dddf87a153722276effe27c88987c76ab5872ac64ef7b0151637e880daab241e",
    }
    phase82 = contract["accepted_phase8_2_input"]
    assert phase82["run_id"].endswith("20260716_v2")
    assert phase82["source_git_commit_sha"] == (
        "b512e0e9400ef150ecc1c0eee3954c56ab8c1dbc"
    )
    assert phase82["rejected_run_ids"] == [
        "phase8_2_external_data_pit_acceptance_matrix_20260715_v1"
    ]


def test_exact_matrix_inventory_and_E1_only_acceptance() -> None:
    inventory = _contract()["matrix_inventory"]
    assert tuple(inventory) == (
        "E0_FROZEN_PHASE7_2_CONTROL",
        "E1_M0_PLUS_EXTERNAL_FULL",
        "E2_M0_PLUS_POLICY_AND_MONEY_MARKET",
        "E3_M0_PLUS_RUONIA_ACTIVITY",
        "E4_EXTERNAL_ONLY",
    )
    assert inventory["E0_FROZEN_PHASE7_2_CONTROL"]["refit"] is False
    assert [
        name for name, value in inventory.items() if value["acceptance_eligible"]
    ] == ["E1_M0_PLUS_EXTERNAL_FULL"]
    assert inventory["E2_M0_PLUS_POLICY_AND_MONEY_MARKET"]["role"] == (
        "diagnostic_ablation"
    )
    assert inventory["E3_M0_PLUS_RUONIA_ACTIVITY"]["role"] == (
        "diagnostic_ablation"
    )
    assert inventory["E4_EXTERNAL_ONLY"]["role"] == "diagnostic_only"


def test_exact_eight_external_features_and_formulas() -> None:
    features = _contract()["external_feature_definitions"]
    assert list(features) == [
        "ext_key_rate_pct",
        "ext_ruonia_minus_key_rate_pp",
        "ext_ruonia_rate_range_pp",
        "ext_ruonia_rate_iqr_pp",
        "ext_log1p_key_rate_age_days",
        "ext_log1p_ruonia_transaction_volume_rub_bn",
        "ext_log1p_ruonia_transaction_count",
        "ext_log1p_ruonia_participant_count",
    ]
    assert features["ext_ruonia_rate_range_pp"]["formula"] == (
        "ruonia_maximum_rate_pct - ruonia_minimum_rate_pct"
    )
    assert features["ext_ruonia_rate_iqr_pp"]["formula"] == (
        "ruonia_percentile_75_rate_pct - ruonia_percentile_25_rate_pct"
    )
    assert features["ext_log1p_key_rate_age_days"]["formula"] == (
        "log1p(key_rate_age_calendar_days)"
    )
    assert features["ext_log1p_ruonia_transaction_volume_rub_bn"]["formula"] == (
        "log1p(ruonia_transaction_volume_rub_bn)"
    )


def test_exact_frozen_model_fold_and_preprocessing_protocol() -> None:
    protocol = _contract()["frozen_protocol"]
    assert protocol["splitter"] == {
        "type": "sklearn.model_selection.TimeSeriesSplit",
        "n_splits": 5,
        "test_size": 64,
        "gap": 0,
    }
    assert protocol["estimator"] == {
        "type": "sklearn.linear_model.LogisticRegression",
        "C": 1.0,
        "class_weight": "balanced",
        "solver": "lbfgs",
        "max_iter": 1000,
    }
    assert protocol["numeric_preprocessing"] == [
        "SimpleImputer(strategy=median)",
        "StandardScaler",
    ]
    assert protocol["categorical_preprocessing"] == [
        "SimpleImputer(strategy=most_frequent)",
        "OneHotEncoder(handle_unknown=ignore)",
    ]
    assert protocol["preprocessing_fit_scope"] == "each_training_fold_only"


def test_exact_absolute_thresholds_and_incremental_gates() -> None:
    contract = _contract()
    limits = contract["absolute_limits"]
    assert limits["B_recall"] == {
        "comparator": ">=",
        "threshold": 0.2458904109589041,
    }
    assert limits["multiclass_log_loss"] == {
        "comparator": "<",
        "threshold": 1.2796950500624311,
    }
    assert limits["mean_confidence_on_incorrect_predictions"] == {
        "comparator": "<=",
        "threshold": 0.7099371,
    }
    assert list(contract["incremental_gates"]) == [f"G{i}" for i in range(1, 13)]
    assert "- 0.01" in contract["incremental_gates"]["G6"]
    assert "0.70 times E0 gap" in contract["incremental_gates"]["G8"]


def test_exact_twelve_artifacts_and_cli() -> None:
    contract = _contract()
    assert contract["artifact_policy"]["exact_count"] == 12
    assert len(contract["declared_output_artifacts"]) == 12
    assert "validation_predictions_by_matrix.parquet" in contract[
        "declared_output_artifacts"
    ]
    assert contract["artifact_policy"]["model_file_allowed"] is False
    assert contract["required_cli_args"] == [
        "--modeling-dataset-path",
        "--dataset-manifest-path",
        "--feature-schema-path",
        "--m0-validation-predictions-path",
        "--phase8-2-input-identity-path",
        "--phase8-2-source-fetch-manifest-path",
        "--phase8-2-ruonia-normalized-path",
        "--phase8-2-key-rate-normalized-path",
        "--phase8-2-external-matrix-path",
        "--phase8-2-coverage-path",
        "--phase8-2-staleness-path",
        "--phase8-2-blocker-register-path",
        "--phase8-2-gate-results-path",
        "--experiment-contract-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ]


def test_no_search_threshold_calibration_promotion_broker_or_trading_authority() -> None:
    contract = _contract()
    protocol = contract["frozen_protocol"]
    assert protocol["feature_selection_search_allowed"] is False
    assert protocol["hyperparameter_search_allowed"] is False
    assert protocol["threshold_optimization_allowed"] is False
    assert protocol["calibration_allowed"] is False
    assert protocol["post_result_feature_modification_allowed"] is False
    authority = contract["authority_and_status"]
    assert authority["server_apply_allowed"] is False
    assert authority["real_runtime_allowed"] is False
    assert authority["model_serialization_allowed"] is False
    assert authority["model_promotion_allowed"] is False
    assert authority["strategy_promotion_allowed"] is False
    assert authority["live_prediction_allowed"] is False
    assert authority["broker_action_allowed"] is False
    assert authority["trading_allowed"] is False
