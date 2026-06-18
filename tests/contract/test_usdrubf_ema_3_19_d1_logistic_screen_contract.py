from __future__ import annotations

import json
from pathlib import Path

CONTRACT_PATH = Path("contracts/experiments/usdrubf_ema_3_19_d1_logistic_screen_v1.json")

EXPECTED_CLI_ARGS = [
    "--d1-ohlc-path",
    "--context-path",
    "--labels-path",
    "--output-dir",
    "--run-id",
]
EXPECTED_OUTPUTS = [
    "run_metadata.json",
    "m3_oos_predictions.csv",
    "m3_metrics.json",
    "m3_fold_metrics.csv",
    "m3_coefficients.csv",
    "m3_c_sensitivity.csv",
    "m3_quality_report.json",
]
EXPECTED_NUMERIC_FEATURES = [
    "ema_diff",
    "ema_diff_prev",
    "bars_since_prev_cross",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "rolling_vol_5d",
    "rolling_vol_20d",
]


def _load() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for value_item in value for item in _strings(value_item)]
    if isinstance(value, dict):
        return [item for value_item in value.values() for item in _strings(value_item)]
    return []


def test_experiment_contract_identity_and_required_cli_semantics() -> None:
    payload = _load()
    assert payload["experiment_id"] == "usdrubf_ema_3_19_d1_logistic_screen_v1"
    assert payload["schema_version"] == 1
    assert payload["producer"]["module"] == "src.moex_research.runners.usdrubf_ema_3_19_d1_logistic_screen"
    assert payload["required_cli_args"] == EXPECTED_CLI_ARGS

    input_artifacts = payload["input_artifacts"]
    assert [item["cli_arg"] for item in input_artifacts] == EXPECTED_CLI_ARGS[:3]
    assert all(item["contract_class"] == "cli_argument" for item in input_artifacts)
    assert all(item["sha256_required"] is True for item in input_artifacts)
    assert payload["input_path_semantics"]["all_inputs_explicit"] is True
    assert payload["input_path_semantics"]["glob_discovery_allowed"] is False
    assert payload["input_path_semantics"]["fallback_paths_allowed"] is False
    assert payload["input_path_semantics"]["mutable_aliases_rejected"] == [
        "latest",
        "current",
        "autodetect",
    ]


def test_output_contract_exactly_declares_seven_screening_artifacts() -> None:
    payload = _load()
    assert [item["filename"] for item in payload["output_artifacts"]] == EXPECTED_OUTPUTS
    assert sorted(payload["formats"]["json"] + payload["formats"]["csv"]) == sorted(EXPECTED_OUTPUTS)
    assert payload["artifact_write_policy"]["output_dir_arg"] == "--output-dir"
    assert payload["artifact_write_policy"]["all_outputs_below_output_dir"] is True
    assert payload["artifact_write_policy"]["declared_outputs_only"] is True
    assert payload["artifact_write_policy"]["stdout_only_result_allowed"] is False

    predictions = next(
        item for item in payload["output_artifacts"] if item["filename"] == "m3_oos_predictions.csv"
    )
    assert {
        "instrument_id",
        "end",
        "cross_dir",
        "target",
        "target_value",
        "probability",
        "fold",
    }.issubset(predictions["minimum_fields"])


def test_feature_target_and_validation_protocol_are_fixed() -> None:
    payload = _load()
    feature_contract = payload["feature_contract"]
    assert feature_contract["numeric_allowlist"] == EXPECTED_NUMERIC_FEATURES
    assert feature_contract["categorical_allowlist"] == ["cross_dir"]
    assert feature_contract["exact_allowlist_enforced"] is True
    assert feature_contract["preprocessing"]["numeric_imputer_fit_scope"] == "training_fold_only"
    assert feature_contract["preprocessing"]["standard_scaler_fit_scope"] == "training_fold_only"

    targets = payload["target_contract"]
    assert targets["primary"]["name"] == "allow_trade_h5_observed"
    assert targets["primary"]["stored_consistency_assertion_required"] is True
    assert targets["secondary"]["name"] == "positive_signed_ret_o2o_h1"
    assert targets["label_imputation_allowed"] is False
    assert targets["target_semantics_mixing_allowed"] is False

    validation = payload["validation_protocol"]
    assert validation["mode"] == "expanding_walk_forward"
    assert validation["minimum_initial_train_events_after_purge"] == 32
    assert validation["test_block_events"] == 8
    assert validation["purge_d1_sessions"] == 6
    assert validation["model"]["estimator"] == "sklearn.linear_model.LogisticRegression"
    assert validation["model"]["C"] == 1.0
    assert validation["model"]["class_weight"] == "balanced"
    assert validation["model"]["threshold"] == 0.5
    assert validation["threshold_tuning_allowed"] is False
    assert validation["c_sensitivity"]["values"] == [0.1, 1.0, 10.0]
    assert validation["c_sensitivity"]["descriptive_only"] is True


def test_screening_only_and_no_promotion_or_model_artifact_semantics() -> None:
    payload = _load()
    assert payload["status"] == {
        "result_status": "provisional_screening",
        "screening_only": True,
        "model_promotion_allowed": False,
        "strategy_package_ready": False,
        "runtime_ready": False,
        "production_or_trading_conclusion_allowed": False,
    }
    assert payload["no_model_artifact"] is True
    assert payload["no_promotion_artifact"] is True
    assert payload["no_strategy_package"] is True
    assert payload["no_runtime_consumption"] is True
    assert payload["no_broker_execution"] is True
    assert payload["artifact_write_policy"]["serialized_model_allowed"] is False
    assert payload["artifact_write_policy"]["promotion_artifact_allowed"] is False
    assert payload["screening_verdict"]["result_status"] == "provisional_screening"
    assert payload["screening_verdict"]["otherwise"] == "not_supported_or_hold"


def test_contract_contains_no_hardcoded_server_path() -> None:
    for value in _strings(_load()):
        assert not value.startswith("/")
        assert not value.startswith("~/")
        assert "/home/trader" not in value
        assert "moex_robot" not in value
