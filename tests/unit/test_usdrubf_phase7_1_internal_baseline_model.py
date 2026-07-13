from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moex_research.runners import (  # noqa: E402
    usdrubf_phase7_1_internal_baseline_model as runner,
)


CANONICAL_READINESS = (
    ROOT
    / "contracts/validation/usdrubf_phase7_modeling_readiness_target_policy_v1.yaml"
)
FULL_SHA = "a" * 40


def _dataset_frame(
    *,
    shuffle: bool = False,
    last_fold_single_class: bool = False,
    first_train_missing_out: bool = False,
) -> pd.DataFrame:
    eligible_rows = 472
    dates = pd.bdate_range("2024-01-01", periods=eligible_rows)
    labels = np.asarray(
        [runner.CLASS_ORDER[index % 3] for index in range(eligible_rows)],
        dtype=object,
    )
    if last_fold_single_class:
        labels[-64:] = "B"
    if first_train_missing_out:
        labels[:152] = np.where(np.arange(152) % 2 == 0, "B", "S")

    frame = pd.DataFrame(
        {
            "target_phase_label": labels,
            "target_is_labeled": True,
            "target_source": runner.TARGET_SOURCE,
            "target_trade_date": dates.strftime("%Y-%m-%d"),
            "target_instrument_id": "forts.usdrubf",
            "prior_trade_date": pd.Series(dates).shift(1).dt.strftime("%Y-%m-%d"),
        }
    )
    base = np.arange(eligible_rows, dtype=float)
    for index, column in enumerate(runner.NUMERIC_FEATURES, start=1):
        frame[column] = 1.0 + base * index / 1000.0
    frame.loc[5, runner.NUMERIC_FEATURES[0]] = np.nan
    frame.loc[9, runner.NUMERIC_FEATURES[1]] = np.nan
    frame["lag1_ema_3_19_state"] = np.where(
        base % 3 == 0,
        "ema3_above_ema19",
        np.where(base % 3 == 1, "ema3_below_ema19", "ema3_equal_ema19"),
    )
    frame["unknown_extra_dataset_column"] = base

    excluded = frame.iloc[:3].copy()
    excluded["target_trade_date"] = ["2030-01-01", "2030-01-02", "2030-01-03"]
    excluded["target_source"] = [
        "unknown_source",
        runner.TARGET_SOURCE,
        runner.TARGET_SOURCE,
    ]
    excluded["target_phase_label"] = ["B", "UNLABELED", "UNKNOWN"]
    excluded["target_is_labeled"] = [True, True, True]
    frame = pd.concat([frame, excluded], ignore_index=True)

    if shuffle:
        frame = frame.sample(frac=1.0, random_state=17).reset_index(drop=True)
    return frame


def _manifest() -> dict[str, object]:
    return {
        "dataset_id": runner.DATASET_ID,
        "feature_schema_id": runner.FEATURE_SCHEMA_ID,
        "target_source": runner.TARGET_SOURCE,
        "target_columns": list(runner.PHASE6_TARGET_COLUMNS),
        "feature_columns": list(runner.PHASE6_FEATURE_COLUMNS),
    }


def _feature_schema() -> dict[str, object]:
    return {
        "schema_id": runner.FEATURE_SCHEMA_ID,
        "dataset_id": runner.DATASET_ID,
        "target_columns": list(runner.PHASE6_TARGET_COLUMNS),
        "feature_columns": list(runner.PHASE6_FEATURE_COLUMNS),
    }


def _write_inputs(
    tmp_path: Path,
    *,
    frame: pd.DataFrame | None = None,
) -> tuple[Path, Path, Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    modeling_dataset_path = tmp_path / "modeling_dataset.parquet"
    dataset_manifest_path = tmp_path / "manifest.json"
    feature_schema_path = tmp_path / "feature_schema.json"
    readiness_contract_path = tmp_path / "readiness.yaml"

    (frame if frame is not None else _dataset_frame()).to_parquet(
        modeling_dataset_path,
        index=False,
    )
    dataset_manifest_path.write_text(
        json.dumps(_manifest(), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    feature_schema_path.write_text(
        json.dumps(_feature_schema(), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    shutil.copyfile(CANONICAL_READINESS, readiness_contract_path)
    return (
        modeling_dataset_path,
        dataset_manifest_path,
        feature_schema_path,
        readiness_contract_path,
    )


def _request(
    tmp_path: Path,
    *,
    frame: pd.DataFrame | None = None,
    output_name: str = "out",
    run_id: str = "synthetic_phase71",
) -> runner.Phase71EvaluationRequest:
    paths = _write_inputs(tmp_path, frame=frame)
    return runner.Phase71EvaluationRequest(
        modeling_dataset_path=paths[0],
        dataset_manifest_path=paths[1],
        feature_schema_path=paths[2],
        readiness_contract_path=paths[3],
        output_dir=tmp_path / output_name,
        run_id=run_id,
        git_commit_sha=FULL_SHA,
    )


def _args_for_paths(
    paths: tuple[Path, Path, Path, Path],
    output_dir: Path,
    *,
    git_sha: str = FULL_SHA,
) -> argparse.Namespace:
    return argparse.Namespace(
        modeling_dataset_path=str(paths[0]),
        dataset_manifest_path=str(paths[1]),
        feature_schema_path=str(paths[2]),
        readiness_contract_path=str(paths[3]),
        output_dir=str(output_dir),
        run_id="synthetic_phase71",
        git_commit_sha=git_sha,
    )


def test_explicit_path_validation_rejects_empty_alias_glob_wrong_suffix_and_bad_sha(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(tmp_path)

    args = _args_for_paths(paths, tmp_path / "out")
    args.modeling_dataset_path = ""
    with pytest.raises(runner.Phase71InternalBaselineError, match="non-empty"):
        runner.request_from_args(args)

    latest_path = tmp_path / "latest.parquet"
    shutil.copyfile(paths[0], latest_path)
    args = _args_for_paths((latest_path, *paths[1:]), tmp_path / "out")
    with pytest.raises(runner.Phase71InternalBaselineError, match="mutable alias"):
        runner.request_from_args(args)

    args = _args_for_paths(paths, tmp_path / "out")
    args.modeling_dataset_path = str(tmp_path / "model*.parquet")
    with pytest.raises(runner.Phase71InternalBaselineError, match="glob"):
        runner.request_from_args(args)

    wrong_suffix = tmp_path / "modeling_dataset.csv"
    wrong_suffix.write_text("x\n", encoding="utf-8")
    args = _args_for_paths((wrong_suffix, *paths[1:]), tmp_path / "out")
    with pytest.raises(runner.Phase71InternalBaselineError, match="suffix"):
        runner.request_from_args(args)

    args = _args_for_paths(paths, tmp_path / "out", git_sha="abc")
    with pytest.raises(runner.Phase71InternalBaselineError, match="40 hexadecimal"):
        runner.request_from_args(args)


def test_duplicate_input_path_and_non_empty_output_dir_fail_closed(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(tmp_path)
    duplicate_request = runner.Phase71EvaluationRequest(
        modeling_dataset_path=paths[0],
        dataset_manifest_path=paths[1],
        feature_schema_path=paths[1],
        readiness_contract_path=paths[3],
        output_dir=tmp_path / "out",
        run_id="duplicate_path",
        git_commit_sha=FULL_SHA,
    )
    with pytest.raises(runner.Phase71InternalBaselineError, match="must be distinct"):
        runner._validate_distinct_input_paths(duplicate_request)

    output_dir = tmp_path / "non_empty"
    output_dir.mkdir()
    (output_dir / "stale.txt").write_text("stale", encoding="utf-8")
    with pytest.raises(runner.Phase71InternalBaselineError, match="non-empty"):
        runner.run_evaluation(
            runner.Phase71EvaluationRequest(
                modeling_dataset_path=paths[0],
                dataset_manifest_path=paths[1],
                feature_schema_path=paths[2],
                readiness_contract_path=paths[3],
                output_dir=output_dir,
                run_id="non_empty_output",
                git_commit_sha=FULL_SHA,
            )
        )


def test_dataset_manifest_and_feature_schema_identity_mismatches_fail_closed(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)

    manifest = _manifest()
    manifest["dataset_id"] = "wrong_dataset"
    request.dataset_manifest_path.write_text(json.dumps(manifest) + "\n")
    with pytest.raises(runner.Phase71InternalBaselineError, match="dataset_id mismatch"):
        runner.run_evaluation(request)

    request = _request(tmp_path / "manifest_mismatch")
    manifest = _manifest()
    manifest["target_source"] = "unknown_source"
    request.dataset_manifest_path.write_text(json.dumps(manifest) + "\n")
    with pytest.raises(runner.Phase71InternalBaselineError, match="target_source mismatch"):
        runner.run_evaluation(request)

    request = _request(tmp_path / "schema_mismatch")
    schema = _feature_schema()
    schema["schema_id"] = "wrong_schema"
    request.feature_schema_path.write_text(json.dumps(schema) + "\n")
    with pytest.raises(runner.Phase71InternalBaselineError, match="schema identity"):
        runner.run_evaluation(request)


def test_readiness_contract_sha_and_required_markers_fail_closed(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    request.readiness_contract_path.write_text("tampered\n", encoding="utf-8")
    with pytest.raises(runner.Phase71InternalBaselineError, match="SHA256 mismatch"):
        runner.run_evaluation(request)

    canonical = CANONICAL_READINESS.read_text(encoding="utf-8")
    altered = canonical.replace("multiclass_log_loss", "removed_probability_metric")
    marker_path = tmp_path / "marker_missing.yaml"
    marker_path.write_text(altered, encoding="utf-8")
    altered_hash = hashlib.sha256(marker_path.read_bytes()).hexdigest()
    with pytest.raises(
        runner.Phase71InternalBaselineError,
        match="required-marker mismatch",
    ):
        runner._validate_readiness_contract(
            marker_path,
            expected_sha256=altered_hash,
        )


def test_eligibility_excludes_unknown_unlabeled_and_unknown_class_then_sorts(
    tmp_path: Path,
) -> None:
    frame = _dataset_frame(shuffle=True)
    eligible = runner.prepare_eligible_rows(frame)

    assert len(eligible.index) == 472
    assert set(eligible["target_source"]) == {runner.TARGET_SOURCE}
    assert set(eligible["target_phase_label"]) == set(runner.CLASS_ORDER)
    assert eligible["target_is_labeled"].eq(True).all()
    assert eligible["target_trade_date"].is_monotonic_increasing
    assert list(eligible["target_trade_date"][:2]) == ["2024-01-01", "2024-01-02"]

    matrix = runner.build_model_matrix(eligible)
    assert tuple(matrix.columns) == runner.MODEL_FEATURES
    assert set(runner.EXCLUDED_TARGET_AND_METADATA_COLUMNS).isdisjoint(matrix.columns)
    assert "unknown_extra_dataset_column" not in matrix.columns


def test_duplicate_eligible_target_identity_is_rejected() -> None:
    frame = _dataset_frame()
    duplicate = frame.iloc[[10]].copy()
    duplicate["unknown_extra_dataset_column"] = -1
    frame = pd.concat([frame, duplicate], ignore_index=True)
    with pytest.raises(runner.Phase71InternalBaselineError, match="duplicate eligible"):
        runner.prepare_eligible_rows(frame)


def test_fixed_time_series_split_boundaries_and_training_classes() -> None:
    eligible = runner.prepare_eligible_rows(_dataset_frame(shuffle=True))
    folds = runner.build_chronological_folds(eligible)

    assert len(folds) == 5
    assert [(len(train), len(validation)) for train, validation in folds] == [
        (152, 64),
        (216, 64),
        (280, 64),
        (344, 64),
        (408, 64),
    ]
    for train_indices, validation_indices in folds:
        assert set(eligible.iloc[train_indices]["target_phase_label"]) == set(
            runner.CLASS_ORDER
        )
        assert (
            eligible.iloc[train_indices]["_target_trade_date_parsed"].max()
            < eligible.iloc[validation_indices]["_target_trade_date_parsed"].min()
        )


def test_training_fold_missing_class_fails_closed() -> None:
    eligible = runner.prepare_eligible_rows(
        _dataset_frame(first_train_missing_out=True)
    )
    with pytest.raises(runner.Phase71InternalBaselineError, match="lacks required class"):
        runner.build_chronological_folds(eligible)


def test_preprocessing_is_fitted_independently_on_training_folds_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_fit_rows: list[int] = []
    original_fit = StandardScaler.fit

    def recording_fit(self: StandardScaler, X: object, y: object = None, **kwargs: object):
        observed_fit_rows.append(len(X))  # type: ignore[arg-type]
        return original_fit(self, X, y, **kwargs)

    monkeypatch.setattr(StandardScaler, "fit", recording_fit)
    runner.run_evaluation(_request(tmp_path))

    assert observed_fit_rows == [152, 216, 280, 344, 408]
    assert 472 not in observed_fit_rows


def test_outputs_metrics_probabilities_and_baselines_match_fixed_contract(
    tmp_path: Path,
) -> None:
    result = runner.run_evaluation(_request(tmp_path))
    output_dir = result.output_dir

    assert result.fold_count == 5
    assert result.validation_row_count == 320
    assert sorted(path.name for path in output_dir.iterdir()) == sorted(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )
    assert not list(output_dir.glob("*.pkl"))
    assert not list(output_dir.glob("*.pickle"))
    assert not list(output_dir.glob("*.joblib"))

    predictions = pd.read_parquet(output_dir / "validation_predictions.parquet")
    assert len(predictions.index) == 320
    assert not predictions.duplicated(
        ["target_trade_date", "target_instrument_id"]
    ).any()
    assert set(predictions["y_true"]).issubset(set(runner.CLASS_ORDER))
    assert np.allclose(
        predictions[["probability_B", "probability_S", "probability_OUT"]].sum(
            axis=1
        ),
        1.0,
    )
    assert np.allclose(
        predictions[
            [
                "class_prior_probability_B",
                "class_prior_probability_S",
                "class_prior_probability_OUT",
            ]
        ].sum(axis=1),
        1.0,
    )

    fold_metrics = pd.read_csv(output_dir / "fold_metrics.csv")
    assert set(fold_metrics["model_or_baseline"]) == set(
        runner.MODEL_OR_BASELINE_ORDER
    )
    assert set(fold_metrics["class_order"]) == {"B|S|OUT"}
    assert set(runner.REQUIRED_COMMON_METRICS).issubset(
        {
            "eligible_validation_rows",
            "per_class_support",
            "confusion_matrix",
            "accuracy",
            "balanced_accuracy",
            "macro_f1",
            "weighted_f1",
            "per_class_precision",
            "per_class_recall",
            "per_class_f1",
        }
    )
    assert fold_metrics.loc[
        fold_metrics["model_or_baseline"] == "candidate_logistic_regression",
        runner.PROBABILITY_METRIC,
    ].notna().all()
    assert fold_metrics.loc[
        fold_metrics["model_or_baseline"] == "majority_class_train_only",
        runner.PROBABILITY_METRIC,
    ].isna().all()

    manifest = json.loads(
        (output_dir / "evaluation_manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["artifact_inventory"] == list(runner.DECLARED_OUTPUT_ARTIFACTS)
    assert manifest["class_order"] == list(runner.CLASS_ORDER)
    assert manifest["readiness_contract"]["sha256"] == runner.READINESS_CONTRACT_SHA256
    assert manifest["dataset"]["modeling_dataset_sha256"]
    assert manifest["dataset"]["dataset_manifest_sha256"]
    assert manifest["feature_schema"]["sha256"]
    assert manifest["side_effect_declaration"] == {
        "broker_or_trading_actions": False,
        "network_calls": False,
        "persistent_model_serialization": False,
        "provider_calls": False,
        "subprocess_calls": False,
        "undeclared_outputs": False,
        "writes_outside_output_dir": False,
    }

    baselines = json.loads(
        (output_dir / "baseline_metrics.json").read_text(encoding="utf-8")
    )
    assert baselines["majority_class_train_only"]["validation_information_used"] is False
    assert baselines["class_prior_train_only"]["validation_information_used"] is False
    assert (
        baselines["candidate_and_baselines_use_identical_validation_identities"]
        is True
    )


def test_zero_support_validation_class_is_explicitly_undefined(
    tmp_path: Path,
) -> None:
    request = _request(
        tmp_path,
        frame=_dataset_frame(last_fold_single_class=True),
    )
    runner.run_evaluation(request)
    fold_metrics = pd.read_csv(request.output_dir / "fold_metrics.csv")
    last_candidate = fold_metrics[
        (fold_metrics["fold_id"] == 5)
        & (
            fold_metrics["model_or_baseline"]
            == "candidate_logistic_regression"
        )
    ].iloc[0]
    support = json.loads(last_candidate["per_class_support_json"])
    precision = json.loads(last_candidate["per_class_precision_json"])
    recall = json.loads(last_candidate["per_class_recall_json"])
    f1 = json.loads(last_candidate["per_class_f1_json"])
    status = json.loads(last_candidate["per_class_metric_status_json"])

    assert support["OUT"] == 0
    assert precision["OUT"] is None
    assert recall["OUT"] is None
    assert f1["OUT"] is None
    assert status["OUT"] == "undefined_zero_support"


def test_repeated_synthetic_runs_are_byte_deterministic(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    request_one = runner.Phase71EvaluationRequest(
        *paths,
        output_dir=tmp_path / "out_one",
        run_id="deterministic",
        git_commit_sha=FULL_SHA,
    )
    request_two = runner.Phase71EvaluationRequest(
        *paths,
        output_dir=tmp_path / "out_two",
        run_id="deterministic",
        git_commit_sha=FULL_SHA,
    )
    runner.run_evaluation(request_one)
    runner.run_evaluation(request_two)

    for artifact in runner.DECLARED_OUTPUT_ARTIFACTS:
        assert (request_one.output_dir / artifact).read_bytes() == (
            request_two.output_dir / artifact
        ).read_bytes()


def test_repository_test_source_uses_only_tmp_synthetic_data() -> None:
    source = Path(__file__).read_text(encoding="utf-8")
    forbidden_server_root = "/home" + "/trader"
    forbidden_export_name = "futoi" + "_exports"
    forbidden_real_dataset_name = "phase6_internal_modeling_dataset" + ".parquet"
    assert forbidden_server_root not in source
    assert forbidden_export_name not in source
    assert forbidden_real_dataset_name not in source
