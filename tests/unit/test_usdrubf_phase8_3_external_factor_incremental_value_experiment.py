from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from moex_research.runners import usdrubf_phase8_3_external_factor_builder as builder
from moex_research.runners import (
    usdrubf_phase8_3_external_factor_incremental_value_experiment as experiment,
)


def _modeling_dataset() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=472)
    index = np.arange(472, dtype=float)
    frame = pd.DataFrame(
        {
            "target_trade_date": dates,
            "target_instrument_id": "forts.usdrubf",
            "target_phase_label": np.resize(np.asarray(["B", "S", "OUT"]), 472),
            "target_is_labeled": True,
            "target_source": "manual_phase_labels_v1",
        }
    )
    for offset, feature in enumerate(builder.M0_NUMERIC_FEATURES, 1):
        frame[feature] = index + offset + np.sin(index / (offset + 1))
    frame["lag1_ema_3_19_state"] = np.resize(
        np.asarray(["ema3_above_ema19", "ema3_below_ema19"]), 472
    )
    return frame


def _external_matrix() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=472)
    index = np.arange(472, dtype=float)
    return pd.DataFrame(
        {
            "target_trade_date": dates,
            "target_instrument_id": "forts.usdrubf",
            "key_rate_pct": 10.0 + index / 100.0,
            "ruonia_minus_key_rate_pp": np.sin(index / 20.0),
            "ruonia_minimum_rate_pct": 8.0 + index / 1000.0,
            "ruonia_percentile_25_rate_pct": 8.2 + index / 900.0,
            "ruonia_percentile_75_rate_pct": 8.8 + index / 800.0,
            "ruonia_maximum_rate_pct": 9.3 + index / 700.0,
            "key_rate_age_calendar_days": index % 181,
            "ruonia_transaction_volume_rub_bn": 100.0 + index,
            "ruonia_transaction_count": 1000.0 + index * 2,
            "ruonia_participant_count": 40.0 + (index % 100),
        }
    )


def _built() -> builder.ExternalFeatureBuildResult:
    return builder.build_external_feature_matrices(
        _modeling_dataset(), _external_matrix()
    )


def _phase82_documents() -> tuple[dict, dict, dict, dict]:
    identity = {
        "run_id": experiment.ACCEPTED_PHASE82_RUN_ID,
        "source_git_commit_sha": experiment.EXPECTED_SOURCE_COMMIT,
    }
    manifest = {
        "sources": [
            {"source_id": "cbr_ruonia_daily"},
            {"source_id": "cbr_key_rate_daily"},
        ]
    }
    blockers = {
        "accepted_sources": list(experiment.ACCEPTED_SOURCES),
        "blocked_sources": [
            {"source_id": source} for source in experiment.BLOCKED_SOURCES
        ],
    }
    gates = {f"G{i}_test": {"passed": True} for i in range(1, 10)}
    return identity, manifest, blockers, gates


def _request(tmp_path: Path) -> experiment.Phase83ExperimentRequest:
    suffixes = {
        "modeling_dataset": ".parquet",
        "dataset_manifest": ".json",
        "feature_schema": ".json",
        "m0_validation_predictions": ".parquet",
        "phase82_input_identity": ".json",
        "phase82_source_fetch_manifest": ".json",
        "phase82_ruonia_normalized": ".parquet",
        "phase82_key_rate_normalized": ".parquet",
        "phase82_external_matrix": ".parquet",
        "phase82_coverage": ".csv",
        "phase82_staleness": ".csv",
        "phase82_blocker_register": ".json",
        "phase82_gate_results": ".json",
        "experiment_contract": ".json",
    }
    paths = {}
    for name, suffix in suffixes.items():
        path = tmp_path / f"{name}{suffix}"
        path.write_bytes(name.encode("utf-8"))
        paths[name] = path
    return experiment.Phase83ExperimentRequest(
        modeling_dataset_path=paths["modeling_dataset"],
        dataset_manifest_path=paths["dataset_manifest"],
        feature_schema_path=paths["feature_schema"],
        m0_validation_predictions_path=paths["m0_validation_predictions"],
        phase82_input_identity_path=paths["phase82_input_identity"],
        phase82_source_fetch_manifest_path=paths["phase82_source_fetch_manifest"],
        phase82_ruonia_normalized_path=paths["phase82_ruonia_normalized"],
        phase82_key_rate_normalized_path=paths["phase82_key_rate_normalized"],
        phase82_external_matrix_path=paths["phase82_external_matrix"],
        phase82_coverage_path=paths["phase82_coverage"],
        phase82_staleness_path=paths["phase82_staleness"],
        phase82_blocker_register_path=paths["phase82_blocker_register"],
        phase82_gate_results_path=paths["phase82_gate_results"],
        experiment_contract_path=paths["experiment_contract"],
        output_dir=tmp_path / "output",
        run_id="synthetic_phase8_3",
        git_commit_sha="a" * 40,
    )


def _metric_payload(**overrides: object) -> dict:
    payload = {
        "validation_rows": 320,
        "accuracy": 0.42,
        "balanced_accuracy": 0.42,
        "macro_f1": 0.41,
        "weighted_f1": 0.41,
        "multiclass_log_loss": 0.98,
        "B_recall": 0.30,
        "S_to_OUT_rate": 0.36,
        "OUT_to_S_rate": 0.49,
        "mean_confidence_on_incorrect_predictions": 0.60,
        "zero_B_recall_fold_count": 0,
        "fold_macro_f1_range": 0.10,
        "fold_macro_f1_population_standard_deviation": 0.05,
        "minimum_fold_macro_f1": 0.30,
        "confidence_bucket": {
            "bucket_count": 10,
            "bucket_accuracy": 0.80,
            "bucket_mean_confidence": 0.90,
            "bucket_gap": 0.10,
            "status": "defined",
        },
    }
    payload.update(overrides)
    return payload


def _gate_inputs() -> tuple[dict, pd.DataFrame]:
    e0 = _metric_payload(
        macro_f1=0.40,
        multiclass_log_loss=1.00,
        S_to_OUT_rate=0.40,
        OUT_to_S_rate=0.50,
        confidence_bucket={
            "bucket_count": 10,
            "bucket_accuracy": 0.70,
            "bucket_mean_confidence": 0.90,
            "bucket_gap": 0.20,
            "status": "defined",
        },
    )
    e1 = _metric_payload()
    folds = pd.DataFrame(
        [
            {
                "matrix_id": "E0_FROZEN_PHASE7_2_CONTROL",
                "fold_id": fold,
                "macro_f1": 0.30,
            }
            for fold in range(1, 6)
        ]
        + [
            {
                "matrix_id": "E1_M0_PLUS_EXTERNAL_FULL",
                "fold_id": fold,
                "macro_f1": 0.32,
            }
            for fold in range(1, 6)
        ]
    )
    return {
        "E0_FROZEN_PHASE7_2_CONTROL": e0,
        "E1_M0_PLUS_EXTERNAL_FULL": e1,
    }, folds


def _evaluate_gates(aggregates: dict, folds: pd.DataFrame, **overrides: bool) -> dict:
    flags = {
        "immutable_hashes_verified": True,
        "phase82_identity_verified": True,
        "identity_verified": True,
        "feature_integrity_verified": True,
        "protocol_verified": True,
        "distribution_verified": True,
    }
    flags.update(overrides)
    return experiment.evaluate_gates(aggregates, folds, **flags)


def test_all_thirteen_immutable_data_hashes_are_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(tmp_path)
    reverse = {path: name for name, path in experiment._input_paths(request).items()}
    monkeypatch.setattr(
        experiment,
        "_sha256",
        lambda path: experiment.EXPECTED_INPUT_SHA256[reverse[path]],
    )
    observed = experiment.verify_immutable_hashes(request)
    assert len(observed) == 13
    assert observed == experiment.EXPECTED_INPUT_SHA256


def test_all_nine_phase82_artifacts_are_required(tmp_path: Path) -> None:
    paths = experiment._input_paths(_request(tmp_path))
    assert len([name for name in paths if name.startswith("phase82_")]) == 9
    assert set(name for name in experiment.EXPECTED_INPUT_SHA256 if name.startswith("phase82_")) == set(
        name for name in paths if name.startswith("phase82_")
    )


def test_rejected_phase82_v1_cannot_pass_declared_identity() -> None:
    identity, manifest, blockers, gates = _phase82_documents()
    identity["run_id"] = "phase8_2_external_data_pit_acceptance_matrix_20260715_v1"
    with pytest.raises(experiment.Phase83ExternalFactorExperimentError, match="rejected"):
        experiment.validate_phase82_acceptance_identity(
            identity, manifest, blockers, gates
        )


def test_phase82_G9_must_be_passed() -> None:
    identity, manifest, blockers, gates = _phase82_documents()
    gates["G9_test"]["passed"] = False
    with pytest.raises(experiment.Phase83ExternalFactorExperimentError, match="G1 through G9"):
        experiment.validate_phase82_acceptance_identity(
            identity, manifest, blockers, gates
        )


def test_exact_472_eligible_and_320_validation_identities() -> None:
    built = _built()
    folds = experiment.build_chronological_folds(built.eligible)
    expected = experiment._expected_validation_identities(built.eligible, folds)
    assert len(built.eligible) == 472
    assert len(expected) == 320


def test_five_chronological_folds_of_64_rows() -> None:
    eligible = _built().eligible
    folds = experiment.build_chronological_folds(eligible)
    assert len(folds) == 5
    assert [len(valid) for _, valid in folds] == [64] * 5
    dates = pd.to_datetime(eligible["target_trade_date"])
    assert all(dates.iloc[train].max() < dates.iloc[valid].min() for train, valid in folds)


def test_fixed_estimator_parameters() -> None:
    pipeline = experiment.build_candidate_pipeline(("x",), ())
    assert pipeline.named_steps["classifier"].get_params()["C"] == 1.0
    assert pipeline.named_steps["classifier"].get_params()["class_weight"] == "balanced"
    assert pipeline.named_steps["classifier"].get_params()["solver"] == "lbfgs"
    assert pipeline.named_steps["classifier"].get_params()["max_iter"] == 1000


def test_fixed_preprocessing() -> None:
    pipeline = experiment.build_candidate_pipeline(("x",), ("state",))
    preprocessor = pipeline.named_steps["preprocessor"]
    numeric = preprocessor.transformers[0][1]
    categorical = preprocessor.transformers[1][1]
    assert numeric.named_steps["imputer"].strategy == "median"
    assert isinstance(numeric.named_steps["scaler"], experiment.StandardScaler)
    assert categorical.named_steps["imputer"].strategy == "most_frequent"
    assert categorical.named_steps["encoder"].handle_unknown == "ignore"


def test_preprocessing_is_fitted_only_on_training_folds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = _built()
    folds = experiment.build_chronological_folds(built.eligible)
    fit_sizes: list[int] = []

    class StubPipeline:
        def __init__(self) -> None:
            self.named_steps = {
                "classifier": SimpleNamespace(classes_=np.asarray(experiment.CLASS_ORDER))
            }

        def fit(self, frame: pd.DataFrame, target: pd.Series) -> "StubPipeline":
            fit_sizes.append(len(frame))
            assert len(frame) == len(target)
            return self

        def predict(self, frame: pd.DataFrame) -> np.ndarray:
            return np.resize(np.asarray(experiment.CLASS_ORDER), len(frame))

        def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
            return np.tile(np.asarray([0.34, 0.33, 0.33]), (len(frame), 1))

    monkeypatch.setattr(
        experiment, "build_candidate_pipeline", lambda numeric, categorical: StubPipeline()
    )
    experiment._evaluate_candidate_matrix(
        "E2_M0_PLUS_POLICY_AND_MONEY_MARKET",
        built.matrices["E2_M0_PLUS_POLICY_AND_MONEY_MARKET"],
        built.eligible,
        folds,
    )
    assert fit_sizes == [152, 216, 280, 344, 408]


def test_E0_exact_prediction_order_is_preserved() -> None:
    built = _built()
    folds = experiment.build_chronological_folds(built.eligible)
    expected = experiment._expected_validation_identities(built.eligible, folds)
    predictions = expected.copy()
    predictions["candidate_y_pred"] = predictions["y_true"]
    predictions["probability_B"] = np.where(predictions["y_true"].eq("B"), 0.8, 0.1)
    predictions["probability_S"] = np.where(predictions["y_true"].eq("S"), 0.8, 0.1)
    predictions["probability_OUT"] = np.where(predictions["y_true"].eq("OUT"), 0.8, 0.1)
    observed = experiment.validate_m0_predictions(
        predictions, expected_fold_identities=expected
    )
    assert observed.loc[:, expected.columns].equals(expected)


def test_probability_columns_sum_to_one() -> None:
    raw = np.asarray([[0.2, 0.5, 0.3], [0.1, 0.2, 0.7]])
    aligned = experiment._align_probabilities(raw, ("S", "OUT", "B"))
    assert np.allclose(aligned.sum(axis=1), 1.0)
    assert np.allclose(aligned[0], [0.3, 0.2, 0.5])


def test_exact_metrics_are_calculated() -> None:
    y_true = np.asarray(["B", "S", "OUT"])
    y_pred = np.asarray(["B", "OUT", "OUT"])
    probabilities = np.asarray(
        [[0.8, 0.1, 0.1], [0.1, 0.2, 0.7], [0.1, 0.1, 0.8]]
    )
    metrics = experiment.calculate_metrics(y_true, y_pred, probabilities)
    assert metrics["validation_rows"] == 3
    assert metrics["accuracy"] == pytest.approx(2 / 3)
    assert metrics["balanced_accuracy"] == pytest.approx(2 / 3)
    assert metrics["B_recall"] == 1.0
    assert metrics["S_to_OUT_rate"] == 1.0
    assert metrics["OUT_to_S_rate"] == 0.0
    assert metrics["multiclass_log_loss"] == pytest.approx(
        -np.mean(np.log([0.8, 0.2, 0.8]))
    )


def test_E1_is_sole_acceptance_candidate() -> None:
    inventory = experiment._feature_inventory_payload()
    eligible = [name for name, value in inventory.items() if value["acceptance_eligible"]]
    assert eligible == ["E1_M0_PLUS_EXTERNAL_FULL"]


def test_E2_E3_E4_remain_diagnostic() -> None:
    inventory = experiment._feature_inventory_payload()
    for matrix_id in (
        "E2_M0_PLUS_POLICY_AND_MONEY_MARKET",
        "E3_M0_PLUS_RUONIA_ACTIVITY",
        "E4_EXTERNAL_ONLY",
    ):
        assert inventory[matrix_id]["acceptance_eligible"] is False
        assert inventory[matrix_id]["role"].startswith("diagnostic")


def test_log_loss_delta_gate_is_exact() -> None:
    aggregates, folds = _gate_inputs()
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["multiclass_log_loss"] = 0.99
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["macro_f1"] = 0.39
    gates = _evaluate_gates(aggregates, folds)
    assert gates["G6_probability_improvement_versus_E0"]["passed"] is True
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["multiclass_log_loss"] = 0.9900001
    assert _evaluate_gates(aggregates, folds)[
        "G6_probability_improvement_versus_E0"
    ]["passed"] is False


def test_confusion_gate_is_exact() -> None:
    aggregates, folds = _gate_inputs()
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["S_to_OUT_rate"] = 0.38
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["OUT_to_S_rate"] = 0.50
    assert _evaluate_gates(aggregates, folds)["G7_S_versus_OUT_objective"][
        "passed"
    ] is True
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["OUT_to_S_rate"] = 0.53
    assert _evaluate_gates(aggregates, folds)["G7_S_versus_OUT_objective"][
        "passed"
    ] is False


def test_calibration_bucket_gate_is_exact() -> None:
    aggregates, folds = _gate_inputs()
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["confidence_bucket"]["bucket_gap"] = 0.14
    assert _evaluate_gates(aggregates, folds)[
        "G8_high_confidence_calibration"
    ]["passed"] is True
    aggregates["E1_M0_PLUS_EXTERNAL_FULL"]["confidence_bucket"]["bucket_gap"] = 0.140001
    assert _evaluate_gates(aggregates, folds)[
        "G8_high_confidence_calibration"
    ]["passed"] is False


def test_fold_breadth_gate_is_exact() -> None:
    aggregates, folds = _gate_inputs()
    assert _evaluate_gates(aggregates, folds)["G9_fold_breadth"]["passed"] is True
    mask = folds["matrix_id"].eq("E1_M0_PLUS_EXTERNAL_FULL")
    folds.loc[mask, "macro_f1"] = [0.32, 0.32, 0.30, 0.30, 0.30]
    assert _evaluate_gates(aggregates, folds)["G9_fold_breadth"]["passed"] is False


def test_undefined_E0_bucket_fails_closed() -> None:
    aggregates, folds = _gate_inputs()
    aggregates["E0_FROZEN_PHASE7_2_CONTROL"]["confidence_bucket"] = {
        "bucket_count": 0,
        "bucket_gap": None,
    }
    assert _evaluate_gates(aggregates, folds)[
        "G8_high_confidence_calibration"
    ]["passed"] is False


def test_undefined_SMD_fails_closed() -> None:
    with pytest.raises(experiment.Phase83ExternalFactorExperimentError, match="insufficient"):
        experiment.external_feature_smd(
            pd.Series([np.nan, np.nan]), pd.Series([1.0])
        )


def test_constant_training_fold_external_feature_fails() -> None:
    with pytest.raises(experiment.Phase83ExternalFactorExperimentError, match="constant"):
        experiment.external_feature_smd(
            pd.Series([1.0, 1.0, 1.0]), pd.Series([1.0])
        )


def _artifact_payloads() -> dict[str, object]:
    payloads: dict[str, object] = {}
    for name in experiment.DECLARED_OUTPUT_ARTIFACTS:
        payloads[name] = {} if name.endswith(".json") else pd.DataFrame({"value": [1]})
    return payloads


def test_exactly_twelve_artifacts_are_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: Path(path).write_text("parquet", encoding="utf-8"),
    )
    output = tmp_path / "output"
    experiment._write_exact_artifacts(output, _artifact_payloads())
    assert sorted(path.name for path in output.iterdir()) == sorted(
        experiment.DECLARED_OUTPUT_ARTIFACTS
    )


def test_preexisting_output_directory_fails(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(experiment.Phase83ExternalFactorExperimentError, match="pre-exist"):
        experiment._write_exact_artifacts(output, _artifact_payloads())


def test_no_write_outside_output_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payloads = _artifact_payloads()
    escaped = ("../escape.json", *experiment.DECLARED_OUTPUT_ARTIFACTS[1:])
    payloads = {name: payloads.get(name, {}) for name in escaped}
    monkeypatch.setattr(experiment, "DECLARED_OUTPUT_ARTIFACTS", escaped)
    with pytest.raises(experiment.Phase83ExternalFactorExperimentError, match="outside"):
        experiment._write_exact_artifacts(tmp_path / "output", payloads)
    assert not (tmp_path / "escape.json").exists()


def test_no_model_serialization() -> None:
    source = inspect.getsource(experiment).lower()
    assert "joblib" not in source
    assert "pickle" not in source
    assert "model.pkl" not in source


def test_no_subprocess_call() -> None:
    source = inspect.getsource(experiment)
    assert "import subprocess" not in source
    assert "subprocess." not in source


def test_no_network_call() -> None:
    source = inspect.getsource(experiment)
    assert "import requests" not in source
    assert "import urllib" not in source
    assert "import socket" not in source
    assert "import httpx" not in source


def test_final_gate_fails_when_any_prior_gate_fails() -> None:
    aggregates, folds = _gate_inputs()
    gates = _evaluate_gates(aggregates, folds, distribution_verified=False)
    assert gates["G10_distribution_integrity"]["passed"] is False
    assert gates["G12_final_acceptance"]["passed"] is False
    assert "G10" in gates["G12_final_acceptance"]["failed_gates"]


def test_exact_runner_CLI_interface() -> None:
    parser = experiment.build_argument_parser()
    observed = tuple(
        action.option_strings[0]
        for action in parser._actions
        if action.option_strings and action.option_strings[0] != "-h"
    )
    assert observed == experiment.REQUIRED_CLI_ARGS
