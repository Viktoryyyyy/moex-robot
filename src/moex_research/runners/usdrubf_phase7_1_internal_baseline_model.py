from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Iterable

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


EXPERIMENT_CONTRACT_ID: Final[str] = "usdrubf_phase7_1_internal_baseline_model_v1"
EXPERIMENT_CONTRACT_VERSION: Final[str] = "1.0"
DATASET_ID: Final[str] = "usdrubf_phase6_internal_modeling_dataset.v1"
FEATURE_SCHEMA_ID: Final[str] = "usdrubf_phase6_internal_factor_batches_v1"
READINESS_CONTRACT_ID: Final[str] = "usdrubf_phase7_modeling_readiness_target_policy_v1"
READINESS_CONTRACT_SHA256: Final[str] = (
    "d9a6c4bb712b872edb6c598e05fd080b88621842aa971272726539257d51178c"
)
TARGET_SOURCE: Final[str] = "manual_phase_labels_v1"
CLASS_ORDER: Final[tuple[str, ...]] = ("B", "S", "OUT")

REQUIRED_CLI_ARGS: Final[tuple[str, ...]] = (
    "--modeling-dataset-path",
    "--dataset-manifest-path",
    "--feature-schema-path",
    "--readiness-contract-path",
    "--output-dir",
    "--run-id",
    "--git-commit-sha",
)

EXCLUDED_TARGET_AND_METADATA_COLUMNS: Final[tuple[str, ...]] = (
    "target_phase_label",
    "target_is_labeled",
    "target_source",
    "target_trade_date",
    "target_instrument_id",
    "prior_trade_date",
)
NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "session_index",
    "days_since_prior_trade_date",
    "lag1_close_return_1d",
    "lag1_intraday_return",
    "rolling_past_return_mean",
    "rolling_past_return_std",
    "lag1_hl_range_pct",
    "rolling_past_hl_range_mean",
    "rolling_past_hl_range_std",
    "lag1_volume",
    "lag1_value",
    "lag1_num_trades",
    "rolling_past_volume_mean",
    "lag1_ema_3",
    "lag1_ema_19",
    "lag1_ema_3_19_spread",
)
CATEGORICAL_FEATURES: Final[tuple[str, ...]] = ("lag1_ema_3_19_state",)
MODEL_FEATURES: Final[tuple[str, ...]] = (*NUMERIC_FEATURES, *CATEGORICAL_FEATURES)
PHASE6_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "prior_trade_date",
    *NUMERIC_FEATURES,
    *CATEGORICAL_FEATURES,
)
PHASE6_TARGET_COLUMNS: Final[tuple[str, ...]] = (
    "target_phase_label",
    "target_is_labeled",
    "target_source",
    "target_trade_date",
    "target_instrument_id",
)

MODEL_CONSTRUCTOR: Final[dict[str, Any]] = {
    "C": 1.0,
    "class_weight": "balanced",
    "solver": "lbfgs",
    "max_iter": 1000,
}
SPLITTER_CONSTRUCTOR: Final[dict[str, int]] = {
    "n_splits": 5,
    "test_size": 64,
    "gap": 0,
}

MODEL_OR_BASELINE_ORDER: Final[tuple[str, ...]] = (
    "candidate_logistic_regression",
    "majority_class_train_only",
    "class_prior_train_only",
)
REQUIRED_COMMON_METRICS: Final[tuple[str, ...]] = (
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
)
PROBABILITY_METRIC: Final[str] = "multiclass_log_loss"
DECLARED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "evaluation_manifest.json",
    "fold_boundaries.csv",
    "fold_metrics.csv",
    "aggregate_metrics.json",
    "per_class_metrics.csv",
    "confusion_matrix.csv",
    "baseline_metrics.json",
    "validation_predictions.parquet",
)
REQUIRED_READINESS_MARKERS: Final[tuple[str, ...]] = (
    "usdrubf_phase7_modeling_readiness_target_policy_v1",
    "manual_phase_labels_v1",
    "supervised_classes",
    "chronological_walk_forward",
    "random_split_allowed: false",
    "shuffle_allowed: false",
    "majority_class_train_only",
    "class_prior_train_only",
    "multiclass_log_loss",
    "failure_invalidates_fold: true",
)

_ALIAS_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])",
    re.IGNORECASE,
)
_SHA_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-fA-F]{40}$")
_GLOB_CHARACTERS: Final[frozenset[str]] = frozenset("*?[]")


class Phase71InternalBaselineError(ValueError):
    """Raised when Phase 7.1 evaluation must fail closed."""


@dataclass(frozen=True)
class Phase71EvaluationRequest:
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    readiness_contract_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str


@dataclass(frozen=True)
class Phase71EvaluationResult:
    output_dir: Path
    artifact_names: tuple[str, ...]
    eligible_row_count: int
    validation_row_count: int
    fold_count: int


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_phase7_1_internal_baseline_model",
        description=(
            "Phase 7.1 internal-only LogisticRegression baseline evaluation for "
            "eligible manual B/S/OUT USDRUBF D1 rows."
        ),
    )
    parser.add_argument("--modeling-dataset-path", required=True)
    parser.add_argument("--dataset-manifest-path", required=True)
    parser.add_argument("--feature-schema-path", required=True)
    parser.add_argument("--readiness-contract-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--git-commit-sha", required=True)
    return parser


def request_from_args(args: argparse.Namespace) -> Phase71EvaluationRequest:
    modeling_dataset_path = _validate_explicit_path_argument(
        args.modeling_dataset_path,
        flag="--modeling-dataset-path",
        allowed_suffixes=(".parquet",),
    )
    dataset_manifest_path = _validate_explicit_path_argument(
        args.dataset_manifest_path,
        flag="--dataset-manifest-path",
        allowed_suffixes=(".json",),
    )
    feature_schema_path = _validate_explicit_path_argument(
        args.feature_schema_path,
        flag="--feature-schema-path",
        allowed_suffixes=(".json",),
    )
    readiness_contract_path = _validate_explicit_path_argument(
        args.readiness_contract_path,
        flag="--readiness-contract-path",
        allowed_suffixes=(".yaml", ".yml"),
    )
    output_dir = _validate_output_path_argument(args.output_dir)
    run_id = str(args.run_id).strip()
    if not run_id:
        raise Phase71InternalBaselineError("--run-id must be non-empty")
    git_commit_sha = str(args.git_commit_sha).strip()
    if not _SHA_PATTERN.fullmatch(git_commit_sha):
        raise Phase71InternalBaselineError(
            "--git-commit-sha must be exactly 40 hexadecimal characters"
        )

    request = Phase71EvaluationRequest(
        modeling_dataset_path=modeling_dataset_path,
        dataset_manifest_path=dataset_manifest_path,
        feature_schema_path=feature_schema_path,
        readiness_contract_path=readiness_contract_path,
        output_dir=output_dir,
        run_id=run_id,
        git_commit_sha=git_commit_sha.lower(),
    )
    _validate_distinct_input_paths(request)
    return request


def run_from_args(args: argparse.Namespace) -> Phase71EvaluationResult:
    return run_evaluation(request_from_args(args))


def run_evaluation(request: Phase71EvaluationRequest) -> Phase71EvaluationResult:
    _validate_request_paths(request)
    _assert_output_dir_ready(request.output_dir)

    dataset_sha256 = _sha256_file(request.modeling_dataset_path)
    manifest_sha256 = _sha256_file(request.dataset_manifest_path)
    feature_schema_sha256 = _sha256_file(request.feature_schema_path)
    readiness_sha256 = _validate_readiness_contract(request.readiness_contract_path)

    dataset_manifest = _read_json_object(request.dataset_manifest_path)
    feature_schema = _read_json_object(request.feature_schema_path)
    _validate_dataset_manifest(dataset_manifest)
    _validate_feature_schema(feature_schema)

    dataset = pd.read_parquet(request.modeling_dataset_path)
    eligible = prepare_eligible_rows(dataset)
    folds = build_chronological_folds(eligible)
    evaluation = evaluate_folds(eligible, folds)

    request.output_dir.mkdir(parents=True, exist_ok=True)
    paths = _output_paths(request.output_dir)
    _write_outputs(
        paths=paths,
        request=request,
        evaluation=evaluation,
        dataset_sha256=dataset_sha256,
        manifest_sha256=manifest_sha256,
        feature_schema_sha256=feature_schema_sha256,
        readiness_sha256=readiness_sha256,
    )
    _assert_exact_output_inventory(request.output_dir)

    return Phase71EvaluationResult(
        output_dir=request.output_dir,
        artifact_names=DECLARED_OUTPUT_ARTIFACTS,
        eligible_row_count=len(eligible.index),
        validation_row_count=len(evaluation["predictions"].index),
        fold_count=len(folds),
    )


def prepare_eligible_rows(dataset: pd.DataFrame) -> pd.DataFrame:
    required_columns = (
        *PHASE6_TARGET_COLUMNS,
        *PHASE6_FEATURE_COLUMNS,
    )
    missing = [column for column in required_columns if column not in dataset.columns]
    if missing:
        raise Phase71InternalBaselineError(
            "modeling dataset missing required columns: " + ", ".join(missing)
        )

    parsed_dates = pd.to_datetime(dataset["target_trade_date"], errors="coerce")
    instrument_text = dataset["target_instrument_id"].astype("string").str.strip()
    eligible_mask = (
        dataset["target_source"].eq(TARGET_SOURCE)
        & dataset["target_is_labeled"].eq(True)
        & dataset["target_phase_label"].isin(CLASS_ORDER)
        & parsed_dates.notna()
        & dataset["target_instrument_id"].notna()
        & instrument_text.ne("")
    )
    eligible = dataset.loc[eligible_mask, required_columns].copy()
    if eligible.empty:
        raise Phase71InternalBaselineError("no eligible supervised B/S/OUT rows")

    eligible["_target_trade_date_parsed"] = parsed_dates.loc[eligible.index]
    eligible["target_trade_date"] = eligible["_target_trade_date_parsed"].dt.strftime(
        "%Y-%m-%d"
    )
    eligible["target_instrument_id"] = instrument_text.loc[eligible.index].astype(str)
    eligible = eligible.sort_values(
        ["_target_trade_date_parsed", "target_instrument_id"],
        kind="mergesort",
    ).reset_index(drop=True)

    if eligible.duplicated(
        subset=["target_trade_date", "target_instrument_id"], keep=False
    ).any():
        raise Phase71InternalBaselineError(
            "duplicate eligible target identity: target_trade_date/target_instrument_id"
        )
    return eligible


def build_model_matrix(eligible: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in MODEL_FEATURES if column not in eligible.columns]
    if missing:
        raise Phase71InternalBaselineError(
            "eligible dataset missing approved model features: " + ", ".join(missing)
        )
    matrix = eligible.loc[:, MODEL_FEATURES].copy()
    if tuple(matrix.columns) != MODEL_FEATURES:
        raise Phase71InternalBaselineError("model matrix does not match frozen feature order")
    if set(matrix.columns) & set(EXCLUDED_TARGET_AND_METADATA_COLUMNS):
        raise Phase71InternalBaselineError(
            "target or metadata column entered the model matrix"
        )
    return matrix


def build_chronological_folds(
    eligible: pd.DataFrame,
) -> list[tuple[np.ndarray, np.ndarray]]:
    splitter = TimeSeriesSplit(**SPLITTER_CONSTRUCTOR)
    try:
        folds = [(train.copy(), validation.copy()) for train, validation in splitter.split(eligible)]
    except ValueError as exc:
        raise Phase71InternalBaselineError(
            "eligible rows cannot satisfy the fixed five-fold test_size=64 protocol"
        ) from exc

    seen_validation_rows: set[int] = set()
    for fold_id, (train_indices, validation_indices) in enumerate(folds, start=1):
        if len(validation_indices) != SPLITTER_CONSTRUCTOR["test_size"]:
            raise Phase71InternalBaselineError(
                f"fold {fold_id} validation row count is not 64"
            )
        overlap = seen_validation_rows.intersection(int(i) for i in validation_indices)
        if overlap:
            raise Phase71InternalBaselineError(
                "the same row appears in multiple validation folds"
            )
        seen_validation_rows.update(int(i) for i in validation_indices)

        train = eligible.iloc[train_indices]
        validation = eligible.iloc[validation_indices]
        max_train_date = train["_target_trade_date_parsed"].max()
        min_validation_date = validation["_target_trade_date_parsed"].min()
        if not max_train_date < min_validation_date:
            raise Phase71InternalBaselineError(
                f"fold {fold_id} violates strict chronological date separation"
            )
        train_support = _class_support(train["target_phase_label"])
        missing_classes = [label for label in CLASS_ORDER if train_support[label] == 0]
        if missing_classes:
            raise Phase71InternalBaselineError(
                f"fold {fold_id} training data lacks required class(es): "
                + ", ".join(missing_classes)
            )
    if len(folds) != SPLITTER_CONSTRUCTOR["n_splits"]:
        raise Phase71InternalBaselineError("splitter did not produce exactly five folds")
    return folds


def build_candidate_pipeline() -> Pipeline:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipeline, list(NUMERIC_FEATURES)),
            ("categorical", categorical_pipeline, list(CATEGORICAL_FEATURES)),
        ],
        remainder="drop",
    )
    classifier = LogisticRegression(**MODEL_CONSTRUCTOR)
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )


def evaluate_folds(
    eligible: pd.DataFrame,
    folds: list[tuple[np.ndarray, np.ndarray]],
) -> dict[str, Any]:
    model_matrix = build_model_matrix(eligible)
    fold_boundaries: list[dict[str, Any]] = []
    fold_metric_rows: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []

    for fold_id, (train_indices, validation_indices) in enumerate(folds, start=1):
        train = eligible.iloc[train_indices].copy()
        validation = eligible.iloc[validation_indices].copy()
        X_train = model_matrix.iloc[train_indices]
        X_validation = model_matrix.iloc[validation_indices]
        y_train = train["target_phase_label"].astype(str)
        y_validation = validation["target_phase_label"].astype(str)

        train_support = _class_support(y_train)
        validation_support = _class_support(y_validation)
        fold_boundaries.append(
            {
                "fold_id": fold_id,
                "train_start": train["target_trade_date"].iloc[0],
                "train_end": train["target_trade_date"].iloc[-1],
                "validation_start": validation["target_trade_date"].iloc[0],
                "validation_end": validation["target_trade_date"].iloc[-1],
                "eligible_train_rows": len(train.index),
                "eligible_validation_rows": len(validation.index),
                "train_support_B": train_support["B"],
                "train_support_S": train_support["S"],
                "train_support_OUT": train_support["OUT"],
                "validation_support_B": validation_support["B"],
                "validation_support_S": validation_support["S"],
                "validation_support_OUT": validation_support["OUT"],
            }
        )

        candidate = build_candidate_pipeline()
        candidate.fit(X_train, y_train)
        candidate_classifier = candidate.named_steps["classifier"]
        candidate_classes = tuple(str(label) for label in candidate_classifier.classes_)
        if set(candidate_classes) != set(CLASS_ORDER):
            raise Phase71InternalBaselineError(
                f"fold {fold_id} candidate fitted classes do not equal B/S/OUT"
            )
        candidate_predictions = candidate.predict(X_validation).astype(str)
        raw_probabilities = candidate.predict_proba(X_validation)
        candidate_probabilities = _align_probabilities(
            raw_probabilities,
            observed_classes=candidate_classes,
        )

        majority_label = _majority_label(train_support)
        majority_predictions = np.full(len(validation.index), majority_label, dtype=object)

        class_prior_probabilities = np.asarray(
            [train_support[label] / len(train.index) for label in CLASS_ORDER],
            dtype=float,
        )
        prior_matrix = np.tile(class_prior_probabilities, (len(validation.index), 1))
        class_prior_label = CLASS_ORDER[int(np.argmax(class_prior_probabilities))]
        class_prior_predictions = np.full(
            len(validation.index), class_prior_label, dtype=object
        )

        metric_inputs = (
            (
                "candidate_logistic_regression",
                candidate_predictions,
                candidate_probabilities,
            ),
            ("majority_class_train_only", majority_predictions, None),
            ("class_prior_train_only", class_prior_predictions, prior_matrix),
        )
        for model_or_baseline, predictions, probabilities in metric_inputs:
            metrics = calculate_metrics(
                y_validation.to_numpy(dtype=str),
                np.asarray(predictions, dtype=str),
                probabilities=probabilities,
            )
            fold_metric_rows.append(
                _flatten_fold_metrics(
                    fold_id=fold_id,
                    model_or_baseline=model_or_baseline,
                    metrics=metrics,
                )
            )

        prediction_frames.append(
            pd.DataFrame(
                {
                    "fold_id": fold_id,
                    "target_trade_date": validation["target_trade_date"].to_numpy(),
                    "target_instrument_id": validation[
                        "target_instrument_id"
                    ].to_numpy(),
                    "y_true": y_validation.to_numpy(dtype=str),
                    "candidate_y_pred": candidate_predictions,
                    "probability_B": candidate_probabilities[:, 0],
                    "probability_S": candidate_probabilities[:, 1],
                    "probability_OUT": candidate_probabilities[:, 2],
                    "majority_baseline_prediction": majority_predictions,
                    "class_prior_baseline_prediction": class_prior_predictions,
                    "class_prior_probability_B": prior_matrix[:, 0],
                    "class_prior_probability_S": prior_matrix[:, 1],
                    "class_prior_probability_OUT": prior_matrix[:, 2],
                }
            )
        )

    predictions = pd.concat(prediction_frames, ignore_index=True)
    expected_validation_rows = (
        SPLITTER_CONSTRUCTOR["n_splits"] * SPLITTER_CONSTRUCTOR["test_size"]
    )
    if len(predictions.index) != expected_validation_rows:
        raise Phase71InternalBaselineError(
            "aggregate validation row count does not equal fixed 320-row protocol"
        )
    if predictions.duplicated(
        subset=["target_trade_date", "target_instrument_id"], keep=False
    ).any():
        raise Phase71InternalBaselineError(
            "an eligible target identity appears in multiple validation folds"
        )

    aggregate = _aggregate_metrics_from_predictions(predictions)
    return {
        "fold_boundaries": pd.DataFrame(fold_boundaries),
        "fold_metrics": pd.DataFrame(fold_metric_rows),
        "predictions": predictions,
        "aggregate": aggregate,
    }


def calculate_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    probabilities: np.ndarray | None,
) -> dict[str, Any]:
    if len(y_true) != len(y_pred):
        raise Phase71InternalBaselineError("metric inputs use different validation rows")
    matrix = confusion_matrix(y_true, y_pred, labels=list(CLASS_ORDER))
    support = matrix.sum(axis=1)
    predicted = matrix.sum(axis=0)

    precision: dict[str, float | None] = {}
    recall: dict[str, float | None] = {}
    f1: dict[str, float | None] = {}
    defined: dict[str, str] = {}
    for index, label in enumerate(CLASS_ORDER):
        class_support = int(support[index])
        if class_support == 0:
            precision[label] = None
            recall[label] = None
            f1[label] = None
            defined[label] = "undefined_zero_support"
            continue
        true_positive = int(matrix[index, index])
        class_precision = (
            true_positive / int(predicted[index]) if int(predicted[index]) > 0 else 0.0
        )
        class_recall = true_positive / class_support
        class_f1 = (
            2.0 * class_precision * class_recall / (class_precision + class_recall)
            if class_precision + class_recall > 0
            else 0.0
        )
        precision[label] = float(class_precision)
        recall[label] = float(class_recall)
        f1[label] = float(class_f1)
        defined[label] = "defined"

    defined_recalls = [value for value in recall.values() if value is not None]
    defined_f1 = [value for value in f1.values() if value is not None]
    weighted_f1_numerator = sum(
        float(f1[label]) * int(support[index])
        for index, label in enumerate(CLASS_ORDER)
        if f1[label] is not None
    )

    metrics: dict[str, Any] = {
        "eligible_validation_rows": int(len(y_true)),
        "per_class_support": {
            label: int(support[index]) for index, label in enumerate(CLASS_ORDER)
        },
        "confusion_matrix": matrix.astype(int).tolist(),
        "accuracy": float(np.mean(y_true == y_pred)) if len(y_true) else None,
        "balanced_accuracy": (
            float(np.mean(defined_recalls)) if defined_recalls else None
        ),
        "macro_f1": float(np.mean(defined_f1)) if defined_f1 else None,
        "weighted_f1": (
            float(weighted_f1_numerator / len(y_true)) if len(y_true) else None
        ),
        "per_class_precision": precision,
        "per_class_recall": recall,
        "per_class_f1": f1,
        "per_class_metric_status": defined,
        "class_order": list(CLASS_ORDER),
        PROBABILITY_METRIC: None,
    }
    if probabilities is not None:
        probability_matrix = np.asarray(probabilities, dtype=float)
        if probability_matrix.shape != (len(y_true), len(CLASS_ORDER)):
            raise Phase71InternalBaselineError(
                "probability matrix does not use fixed B/S/OUT shape"
            )
        if not np.allclose(probability_matrix.sum(axis=1), 1.0):
            raise Phase71InternalBaselineError("probability rows must sum to one")
        metrics[PROBABILITY_METRIC] = _fixed_order_log_loss(
            y_true,
            probability_matrix,
        )
    return metrics


def _fixed_order_log_loss(
    y_true: np.ndarray,
    probability_matrix: np.ndarray,
) -> float:
    positions = {label: index for index, label in enumerate(CLASS_ORDER)}
    try:
        target_indices = np.asarray([positions[str(label)] for label in y_true], dtype=int)
    except KeyError as exc:
        raise Phase71InternalBaselineError(
            "log-loss target contains a class outside fixed B/S/OUT order"
        ) from exc
    epsilon = np.finfo(float).eps
    selected = probability_matrix[np.arange(len(target_indices)), target_indices]
    return float(-np.mean(np.log(np.clip(selected, epsilon, 1.0))))


def _aggregate_metrics_from_predictions(
    predictions: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    y_true = predictions["y_true"].to_numpy(dtype=str)
    return {
        "candidate_logistic_regression": calculate_metrics(
            y_true,
            predictions["candidate_y_pred"].to_numpy(dtype=str),
            probabilities=predictions[
                ["probability_B", "probability_S", "probability_OUT"]
            ].to_numpy(dtype=float),
        ),
        "majority_class_train_only": calculate_metrics(
            y_true,
            predictions["majority_baseline_prediction"].to_numpy(dtype=str),
            probabilities=None,
        ),
        "class_prior_train_only": calculate_metrics(
            y_true,
            predictions["class_prior_baseline_prediction"].to_numpy(dtype=str),
            probabilities=predictions[
                [
                    "class_prior_probability_B",
                    "class_prior_probability_S",
                    "class_prior_probability_OUT",
                ]
            ].to_numpy(dtype=float),
        ),
    }


def _write_outputs(
    *,
    paths: dict[str, Path],
    request: Phase71EvaluationRequest,
    evaluation: dict[str, Any],
    dataset_sha256: str,
    manifest_sha256: str,
    feature_schema_sha256: str,
    readiness_sha256: str,
) -> None:
    aggregate = evaluation["aggregate"]
    predictions = evaluation["predictions"]

    manifest = {
        "experiment_contract": {
            "contract_id": EXPERIMENT_CONTRACT_ID,
            "contract_version": EXPERIMENT_CONTRACT_VERSION,
        },
        "readiness_contract": {
            "contract_id": READINESS_CONTRACT_ID,
            "sha256": readiness_sha256,
        },
        "dataset": {
            "dataset_id": DATASET_ID,
            "modeling_dataset_sha256": dataset_sha256,
            "dataset_manifest_sha256": manifest_sha256,
        },
        "feature_schema": {
            "schema_id": FEATURE_SCHEMA_ID,
            "sha256": feature_schema_sha256,
        },
        "target_source": TARGET_SOURCE,
        "source_git_commit_sha": request.git_commit_sha,
        "run_id": request.run_id,
        "class_order": list(CLASS_ORDER),
        "numeric_features": list(NUMERIC_FEATURES),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "fold_count": SPLITTER_CONSTRUCTOR["n_splits"],
        "aggregate_validation_rows": len(predictions.index),
        "artifact_inventory": list(DECLARED_OUTPUT_ARTIFACTS),
        "side_effect_declaration": {
            "writes_outside_output_dir": False,
            "undeclared_outputs": False,
            "persistent_model_serialization": False,
            "network_calls": False,
            "subprocess_calls": False,
            "provider_calls": False,
            "broker_or_trading_actions": False,
        },
    }
    _write_json(paths["evaluation_manifest.json"], manifest)

    evaluation["fold_boundaries"].to_csv(paths["fold_boundaries.csv"], index=False)
    evaluation["fold_metrics"].to_csv(paths["fold_metrics.csv"], index=False)

    _write_json(
        paths["aggregate_metrics.json"],
        {
            "aggregation_method": "concatenate_disjoint_chronological_validation_folds",
            "aggregate_validation_rows": len(predictions.index),
            "class_order": list(CLASS_ORDER),
            "candidate_logistic_regression": aggregate[
                "candidate_logistic_regression"
            ],
        },
    )

    per_class_rows: list[dict[str, Any]] = []
    confusion_rows: list[dict[str, Any]] = []
    for model_or_baseline in MODEL_OR_BASELINE_ORDER:
        metrics = aggregate[model_or_baseline]
        for label in CLASS_ORDER:
            per_class_rows.append(
                {
                    "model_or_baseline": model_or_baseline,
                    "class": label,
                    "precision": metrics["per_class_precision"][label],
                    "recall": metrics["per_class_recall"][label],
                    "f1": metrics["per_class_f1"][label],
                    "support": metrics["per_class_support"][label],
                    "status": metrics["per_class_metric_status"][label],
                }
            )
        for true_index, true_label in enumerate(CLASS_ORDER):
            for predicted_index, predicted_label in enumerate(CLASS_ORDER):
                confusion_rows.append(
                    {
                        "model_or_baseline": model_or_baseline,
                        "true_class": true_label,
                        "predicted_class": predicted_label,
                        "true_class_order": true_index,
                        "predicted_class_order": predicted_index,
                        "count": metrics["confusion_matrix"][true_index][
                            predicted_index
                        ],
                    }
                )
    pd.DataFrame(per_class_rows).to_csv(paths["per_class_metrics.csv"], index=False)
    pd.DataFrame(confusion_rows).to_csv(paths["confusion_matrix.csv"], index=False)

    _write_json(
        paths["baseline_metrics.json"],
        {
            "class_order": list(CLASS_ORDER),
            "majority_class_train_only": {
                "definition": (
                    "Predict the most frequent eligible class observed in each "
                    "training fold."
                ),
                "validation_information_used": False,
                "recomputed_independently_per_fold": True,
                "aggregate_results": aggregate["majority_class_train_only"],
            },
            "class_prior_train_only": {
                "definition": (
                    "Use each training fold's B/S/OUT proportions as fixed "
                    "validation probabilities."
                ),
                "validation_information_used": False,
                "recomputed_independently_per_fold": True,
                "aggregate_results": aggregate["class_prior_train_only"],
            },
            "candidate_and_baselines_use_identical_validation_identities": True,
        },
    )
    predictions.to_parquet(paths["validation_predictions.parquet"], index=False)


def _flatten_fold_metrics(
    *,
    fold_id: int,
    model_or_baseline: str,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "fold_id": fold_id,
        "model_or_baseline": model_or_baseline,
        "class_order": "|".join(CLASS_ORDER),
        "eligible_validation_rows": metrics["eligible_validation_rows"],
        "accuracy": metrics["accuracy"],
        "balanced_accuracy": metrics["balanced_accuracy"],
        "macro_f1": metrics["macro_f1"],
        "weighted_f1": metrics["weighted_f1"],
        PROBABILITY_METRIC: metrics[PROBABILITY_METRIC],
        "per_class_support_json": _compact_json(metrics["per_class_support"]),
        "confusion_matrix_json": _compact_json(metrics["confusion_matrix"]),
        "per_class_precision_json": _compact_json(metrics["per_class_precision"]),
        "per_class_recall_json": _compact_json(metrics["per_class_recall"]),
        "per_class_f1_json": _compact_json(metrics["per_class_f1"]),
        "per_class_metric_status_json": _compact_json(
            metrics["per_class_metric_status"]
        ),
    }


def _validate_explicit_path_argument(
    raw_value: object,
    *,
    flag: str,
    allowed_suffixes: tuple[str, ...],
) -> Path:
    text = str(raw_value).strip()
    if not text:
        raise Phase71InternalBaselineError(f"{flag} must be non-empty")
    if any(character in text for character in _GLOB_CHARACTERS):
        raise Phase71InternalBaselineError(f"{flag} must not contain glob syntax")
    if _ALIAS_TOKEN_PATTERN.search(text):
        raise Phase71InternalBaselineError(
            f"{flag} must not use mutable alias latest/current/autodetect"
        )
    path = Path(text)
    if path.suffix.lower() not in allowed_suffixes:
        allowed = ", ".join(allowed_suffixes)
        raise Phase71InternalBaselineError(
            f"{flag} must identify an artifact with suffix: {allowed}"
        )
    if not path.exists() or not path.is_file():
        raise Phase71InternalBaselineError(
            f"{flag} must identify one explicit existing file"
        )
    return path


def _validate_output_path_argument(raw_value: object) -> Path:
    text = str(raw_value).strip()
    if not text:
        raise Phase71InternalBaselineError("--output-dir must be non-empty")
    if any(character in text for character in _GLOB_CHARACTERS):
        raise Phase71InternalBaselineError("--output-dir must not contain glob syntax")
    if _ALIAS_TOKEN_PATTERN.search(text):
        raise Phase71InternalBaselineError(
            "--output-dir must not use mutable alias latest/current/autodetect"
        )
    return Path(text)


def _validate_request_paths(request: Phase71EvaluationRequest) -> None:
    path_rules = (
        (request.modeling_dataset_path, "--modeling-dataset-path", (".parquet",)),
        (request.dataset_manifest_path, "--dataset-manifest-path", (".json",)),
        (request.feature_schema_path, "--feature-schema-path", (".json",)),
        (request.readiness_contract_path, "--readiness-contract-path", (".yaml", ".yml")),
    )
    for path, flag, suffixes in path_rules:
        _validate_explicit_path_argument(path, flag=flag, allowed_suffixes=suffixes)
    _validate_distinct_input_paths(request)
    if not request.run_id.strip():
        raise Phase71InternalBaselineError("--run-id must be non-empty")
    if not _SHA_PATTERN.fullmatch(request.git_commit_sha):
        raise Phase71InternalBaselineError(
            "--git-commit-sha must be exactly 40 hexadecimal characters"
        )


def _validate_distinct_input_paths(request: Phase71EvaluationRequest) -> None:
    paths = (
        request.modeling_dataset_path,
        request.dataset_manifest_path,
        request.feature_schema_path,
        request.readiness_contract_path,
    )
    resolved = [path.resolve() for path in paths]
    if len(set(resolved)) != len(resolved):
        raise Phase71InternalBaselineError(
            "the four input artifact paths must be distinct"
        )


def _assert_output_dir_ready(output_dir: Path) -> None:
    if not output_dir.exists():
        return
    if not output_dir.is_dir():
        raise Phase71InternalBaselineError("--output-dir must be absent or an empty directory")
    entries = list(output_dir.iterdir())
    if entries:
        raise Phase71InternalBaselineError(
            "--output-dir already exists and is non-empty"
        )


def _validate_dataset_manifest(payload: dict[str, Any]) -> None:
    if payload.get("dataset_id") != DATASET_ID:
        raise Phase71InternalBaselineError("dataset manifest dataset_id mismatch")
    if payload.get("feature_schema_id") != FEATURE_SCHEMA_ID:
        raise Phase71InternalBaselineError("dataset manifest feature_schema_id mismatch")
    if payload.get("target_source") != TARGET_SOURCE:
        raise Phase71InternalBaselineError("dataset manifest target_source mismatch")
    target_columns = tuple(payload.get("target_columns", ()))
    if target_columns != PHASE6_TARGET_COLUMNS:
        raise Phase71InternalBaselineError("dataset manifest target_columns mismatch")
    feature_columns = tuple(payload.get("feature_columns", ()))
    if feature_columns != PHASE6_FEATURE_COLUMNS:
        raise Phase71InternalBaselineError("dataset manifest feature_columns mismatch")


def _validate_feature_schema(payload: dict[str, Any]) -> None:
    if payload.get("schema_id") != FEATURE_SCHEMA_ID:
        raise Phase71InternalBaselineError("feature schema identity mismatch")
    if payload.get("dataset_id") != DATASET_ID:
        raise Phase71InternalBaselineError("feature schema dataset_id mismatch")
    if tuple(payload.get("target_columns", ())) != PHASE6_TARGET_COLUMNS:
        raise Phase71InternalBaselineError("feature schema target_columns mismatch")
    if tuple(payload.get("feature_columns", ())) != PHASE6_FEATURE_COLUMNS:
        raise Phase71InternalBaselineError("feature schema feature_columns mismatch")


def _validate_readiness_contract(
    path: Path,
    *,
    expected_sha256: str = READINESS_CONTRACT_SHA256,
) -> str:
    raw = path.read_bytes()
    observed_sha256 = hashlib.sha256(raw).hexdigest()
    if observed_sha256 != expected_sha256:
        raise Phase71InternalBaselineError("readiness contract SHA256 mismatch")
    text = raw.decode("utf-8")
    missing = [marker for marker in REQUIRED_READINESS_MARKERS if marker not in text]
    if missing:
        raise Phase71InternalBaselineError(
            "readiness contract required-marker mismatch: " + ", ".join(missing)
        )
    return observed_sha256


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase71InternalBaselineError(
            f"invalid JSON artifact: {path.as_posix()}"
        ) from exc
    if not isinstance(payload, dict):
        raise Phase71InternalBaselineError(
            f"JSON artifact must be an object: {path.as_posix()}"
        )
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _align_probabilities(
    probabilities: np.ndarray,
    *,
    observed_classes: tuple[str, ...],
) -> np.ndarray:
    output = np.zeros((probabilities.shape[0], len(CLASS_ORDER)), dtype=float)
    positions = {label: index for index, label in enumerate(observed_classes)}
    for output_index, label in enumerate(CLASS_ORDER):
        if label not in positions:
            raise Phase71InternalBaselineError(
                "candidate probability output lacks required class " + label
            )
        output[:, output_index] = probabilities[:, positions[label]]
    return output


def _class_support(labels: Iterable[str]) -> dict[str, int]:
    series = pd.Series(list(labels), dtype="string")
    counts = series.value_counts()
    return {label: int(counts.get(label, 0)) for label in CLASS_ORDER}


def _majority_label(support: dict[str, int]) -> str:
    return max(CLASS_ORDER, key=lambda label: (support[label], -CLASS_ORDER.index(label)))


def _output_paths(output_dir: Path) -> dict[str, Path]:
    paths = {name: output_dir / name for name in DECLARED_OUTPUT_ARTIFACTS}
    for path in paths.values():
        try:
            path.resolve().relative_to(output_dir.resolve())
        except ValueError as exc:
            raise Phase71InternalBaselineError(
                "declared output path escapes --output-dir"
            ) from exc
    return paths


def _assert_exact_output_inventory(output_dir: Path) -> None:
    observed = sorted(path.name for path in output_dir.iterdir() if path.is_file())
    expected = sorted(DECLARED_OUTPUT_ARTIFACTS)
    if observed != expected:
        raise Phase71InternalBaselineError(
            "output inventory does not equal the eight declared artifacts"
        )
    directories = [path for path in output_dir.iterdir() if path.is_dir()]
    if directories:
        raise Phase71InternalBaselineError("undeclared output directories are forbidden")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _compact_json(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if value is pd.NA:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    return value


def main() -> None:
    parser = build_argument_parser()
    run_from_args(parser.parse_args())


if __name__ == "__main__":
    main()
