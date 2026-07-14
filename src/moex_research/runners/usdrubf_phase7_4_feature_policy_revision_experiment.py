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

from .usdrubf_phase7_4_feature_policy_revision_builder import (
    IDENTITY_COLUMNS,
    MATRIX_CATEGORICAL_FEATURES,
    MATRIX_NUMERIC_FEATURES,
    M1_CATEGORICAL_FEATURES,
    M1_NUMERIC_FEATURES,
    NEW_NUMERIC_FEATURES,
    PHASE6_IDENTITY_AND_TARGET_COLUMNS,
    PHASE6_LEGACY_CATEGORICAL_COLUMNS,
    PHASE6_LEGACY_NUMERIC_COLUMNS,
    SOURCE_REQUIRED_COLUMNS,
    FeatureBuildResult,
    build_feature_matrices,
)

EXPERIMENT_CONTRACT_ID: Final[str] = "usdrubf_phase7_4_feature_policy_revision_experiment_v1"
EXPERIMENT_CONTRACT_VERSION: Final[str] = "1.0"
DATASET_ID: Final[str] = "usdrubf_phase6_internal_modeling_dataset.v1"
FEATURE_SCHEMA_ID: Final[str] = "usdrubf_phase6_internal_factor_batches_v1"
READINESS_CONTRACT_ID: Final[str] = "usdrubf_phase7_modeling_readiness_target_policy_v1"
TARGET_SOURCE: Final[str] = "manual_phase_labels_v1"
CLASS_ORDER: Final[tuple[str, ...]] = ("B", "S", "OUT")
PROBABILITY_COLUMNS: Final[tuple[str, ...]] = (
    "probability_B", "probability_S", "probability_OUT"
)
REQUIRED_CLI_ARGS: Final[tuple[str, ...]] = (
    "--source-panel-path", "--source-panel-manifest-path",
    "--modeling-dataset-path", "--dataset-manifest-path",
    "--feature-schema-path", "--readiness-contract-path",
    "--m0-validation-predictions-path", "--experiment-contract-path",
    "--output-dir", "--run-id", "--git-commit-sha",
)
SPLITTER_CONSTRUCTOR: Final[dict[str, int]] = {"n_splits": 5, "test_size": 64, "gap": 0}
MODEL_CONSTRUCTOR: Final[dict[str, Any]] = {
    "C": 1.0, "class_weight": "balanced", "solver": "lbfgs", "max_iter": 1000
}
DECLARED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json", "feature_matrix_inventory.json",
    "feature_nullness_by_matrix_and_fold.csv",
    "feature_shift_by_matrix_group_and_fold.csv", "fold_metrics_by_matrix.csv",
    "aggregate_metrics_by_matrix.json", "per_class_metrics_by_matrix.csv",
    "ablation_effects.csv", "gate_results.json",
)
MATRIX_ROLES: Final[dict[str, str]] = {
    "M0_FROZEN_PHASE7_2_CONTROL": "immutable_historical_control",
    "M1_REVISED_FULL": "sole_acceptance_candidate",
    "M2_MINUS_NORMALIZED_EMA_TREND": "diagnostic_only",
    "M3_MINUS_VOLATILITY_RANGE": "diagnostic_only",
    "M4_MINUS_VOLUME_ACTIVITY": "diagnostic_only",
    "M5_MINUS_LAGGED_INTERSESSION_GAP": "diagnostic_only",
}
REVISED_GROUPS: Final[dict[str, tuple[str, ...]]] = {
    "lagged_intersession_gap": ("lag1_intersession_gap_days",),
    "normalized_ema_trend": (
        "lag1_ema_3_19_spread_pct", "lag1_close_ema_19_distance_pct",
        "lag1_ema_3_slope_5_pct", "lag1_ema_19_slope_5_pct",
        "lag1_ema_3_19_spread_pct_change_5", "lag1_ema_3_19_state_run_length_log",
    ),
    "volatility_range": (
        "rolling_past_return_std_5", "rolling_past_return_std_20",
        "rolling_return_std_ratio_5_20", "lag1_hl_range_pct",
        "lag1_hl_range_to_prior20_mean", "rolling_hl_range_mean_ratio_5_20",
    ),
    "volume_activity": (
        "lag1_log_volume_rel_prior20", "lag1_log_num_trades_rel_prior20",
        "lag1_log_avg_trade_value_rel_prior20", "rolling_log_volume_mean_diff_5_20",
    ),
}
M0_GROUPS: Final[dict[str, tuple[str, ...]]] = {
    "lagged_intersession_gap": ("session_index", "days_since_prior_trade_date"),
    "normalized_ema_trend": ("lag1_ema_3", "lag1_ema_19", "lag1_ema_3_19_spread"),
    "volatility_range": (
        "rolling_past_return_std", "lag1_hl_range_pct",
        "rolling_past_hl_range_mean", "rolling_past_hl_range_std",
    ),
    "volume_activity": (
        "lag1_volume", "lag1_value", "lag1_num_trades", "rolling_past_volume_mean",
    ),
}
_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{40}$")
_GLOB_CHARS = frozenset("*?[]")


class Phase74FeaturePolicyExperimentError(ValueError):
    """Raised when the Phase 7.4 experiment must fail closed."""


@dataclass(frozen=True)
class Phase74ExperimentRequest:
    source_panel_path: Path
    source_panel_manifest_path: Path
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    readiness_contract_path: Path
    m0_validation_predictions_path: Path
    experiment_contract_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str


@dataclass(frozen=True)
class Phase74ExperimentResult:
    output_dir: Path
    artifact_names: tuple[str, ...]
    eligible_row_count: int
    validation_row_count: int
    fold_count: int
    final_gate_passed: bool


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_phase7_4_feature_policy_revision_experiment"
    )
    for flag in REQUIRED_CLI_ARGS:
        parser.add_argument(flag, required=True)
    return parser


def _explicit_path(raw: object, flag: str, suffixes: tuple[str, ...]) -> Path:
    text = str(raw).strip()
    if not text:
        raise Phase74FeaturePolicyExperimentError(f"{flag} must be non-empty")
    if any(char in text for char in _GLOB_CHARS):
        raise Phase74FeaturePolicyExperimentError(f"{flag} must not contain glob syntax")
    if _ALIAS_PATTERN.search(text):
        raise Phase74FeaturePolicyExperimentError(f"{flag} must not use mutable alias")
    path = Path(text)
    if path.suffix.lower() not in suffixes:
        raise Phase74FeaturePolicyExperimentError(f"{flag} suffix mismatch")
    if not path.exists() or not path.is_file():
        raise Phase74FeaturePolicyExperimentError(f"{flag} must identify one explicit existing file")
    return path


def _output_path(raw: object) -> Path:
    text = str(raw).strip()
    if not text or any(char in text for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(text):
        raise Phase74FeaturePolicyExperimentError("--output-dir must be explicit and immutable")
    return Path(text)


def request_from_args(args: argparse.Namespace) -> Phase74ExperimentRequest:
    request = Phase74ExperimentRequest(
        source_panel_path=_explicit_path(args.source_panel_path, "--source-panel-path", (".parquet",)),
        source_panel_manifest_path=_explicit_path(args.source_panel_manifest_path, "--source-panel-manifest-path", (".json",)),
        modeling_dataset_path=_explicit_path(args.modeling_dataset_path, "--modeling-dataset-path", (".parquet",)),
        dataset_manifest_path=_explicit_path(args.dataset_manifest_path, "--dataset-manifest-path", (".json",)),
        feature_schema_path=_explicit_path(args.feature_schema_path, "--feature-schema-path", (".json",)),
        readiness_contract_path=_explicit_path(args.readiness_contract_path, "--readiness-contract-path", (".yaml", ".yml")),
        m0_validation_predictions_path=_explicit_path(args.m0_validation_predictions_path, "--m0-validation-predictions-path", (".parquet",)),
        experiment_contract_path=_explicit_path(args.experiment_contract_path, "--experiment-contract-path", (".json",)),
        output_dir=_output_path(args.output_dir),
        run_id=str(args.run_id).strip(),
        git_commit_sha=str(args.git_commit_sha).strip().lower(),
    )
    if not request.run_id:
        raise Phase74FeaturePolicyExperimentError("--run-id must be non-empty")
    if not _SHA_PATTERN.fullmatch(request.git_commit_sha):
        raise Phase74FeaturePolicyExperimentError("--git-commit-sha must be 40 hexadecimal characters")
    resolved = [path.resolve() for path in _input_paths(request).values()]
    if len(set(resolved)) != 8:
        raise Phase74FeaturePolicyExperimentError("all eight file inputs must resolve to distinct paths")
    return request


def _input_paths(request: Phase74ExperimentRequest) -> dict[str, Path]:
    return {
        "source_panel": request.source_panel_path,
        "source_panel_manifest": request.source_panel_manifest_path,
        "modeling_dataset": request.modeling_dataset_path,
        "dataset_manifest": request.dataset_manifest_path,
        "feature_schema": request.feature_schema_path,
        "readiness_contract": request.readiness_contract_path,
        "m0_validation_predictions": request.m0_validation_predictions_path,
        "experiment_contract": request.experiment_contract_path,
    }


def _prepare_experiment_eligible_rows(dataset: pd.DataFrame) -> pd.DataFrame:
    required = (
        *PHASE6_IDENTITY_AND_TARGET_COLUMNS,
        *PHASE6_LEGACY_NUMERIC_COLUMNS,
        *PHASE6_LEGACY_CATEGORICAL_COLUMNS,
    )
    missing = [column for column in required if column not in dataset.columns]
    if missing:
        raise Phase74FeaturePolicyExperimentError("modeling dataset missing: " + ", ".join(missing))
    rows = dataset.loc[:, required].copy()
    dates = pd.to_datetime(rows["target_trade_date"], errors="coerce")
    instruments = rows["target_instrument_id"].astype("string").str.strip()
    eligible = (
        rows["target_source"].eq(TARGET_SOURCE)
        & rows["target_is_labeled"].eq(True)
        & rows["target_phase_label"].isin(CLASS_ORDER)
        & dates.notna() & instruments.notna() & instruments.ne("")
    )
    rows = rows.loc[eligible].copy()
    rows["_date"] = dates.loc[eligible]
    rows["target_instrument_id"] = instruments.loc[eligible].astype(str)
    rows = rows.sort_values(["_date", "target_instrument_id"], kind="mergesort").reset_index(drop=True)
    if rows.empty or rows.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase74FeaturePolicyExperimentError("eligible identity set is empty or duplicated")
    rows["target_trade_date"] = rows["_date"].dt.strftime("%Y-%m-%d")
    return rows


def build_chronological_folds(eligible: pd.DataFrame) -> list[tuple[np.ndarray, np.ndarray]]:
    try:
        folds = [(train.copy(), valid.copy()) for train, valid in TimeSeriesSplit(**SPLITTER_CONSTRUCTOR).split(eligible)]
    except ValueError as exc:
        raise Phase74FeaturePolicyExperimentError("eligible rows cannot satisfy frozen folds") from exc
    seen: set[int] = set()
    for fold_id, (train, valid) in enumerate(folds, 1):
        if len(valid) != 64 or seen.intersection(valid.tolist()):
            raise Phase74FeaturePolicyExperimentError("validation fold identity mismatch")
        seen.update(int(item) for item in valid)
        support = set(eligible.iloc[train]["target_phase_label"].astype(str))
        if support != set(CLASS_ORDER):
            raise Phase74FeaturePolicyExperimentError(f"fold {fold_id} training data lacks B S or OUT")
        if eligible.iloc[train]["_date"].max() >= eligible.iloc[valid]["_date"].min():
            raise Phase74FeaturePolicyExperimentError("fold violates chronology")
    if len(folds) != 5 or len(seen) != 320:
        raise Phase74FeaturePolicyExperimentError("frozen fold protocol must contain 320 identities")
    return folds


def _expected_validation_identities(
    eligible: pd.DataFrame,
    folds: list[tuple[np.ndarray, np.ndarray]],
) -> pd.DataFrame:
    frames = []
    for fold_id, (_, valid) in enumerate(folds, 1):
        frame = eligible.iloc[valid][["target_trade_date", "target_instrument_id", "target_phase_label"]].copy()
        frame.insert(0, "fold_id", fold_id)
        frame = frame.rename(columns={"target_phase_label": "y_true"})
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def validate_m0_predictions(
    predictions: pd.DataFrame,
    *,
    expected_fold_identities: pd.DataFrame,
) -> pd.DataFrame:
    required = (
        "fold_id", "target_trade_date", "target_instrument_id", "y_true",
        "candidate_y_pred", *PROBABILITY_COLUMNS,
    )
    missing = [column for column in required if column not in predictions.columns]
    if missing:
        raise Phase74FeaturePolicyExperimentError("M0 predictions missing: " + ", ".join(missing))
    observed = predictions.loc[:, required].copy().reset_index(drop=True)
    observed["target_trade_date"] = pd.to_datetime(observed["target_trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    expected = expected_fold_identities.reset_index(drop=True)
    if len(observed) != 320 or not observed.loc[:, expected.columns].equals(expected):
        raise Phase74FeaturePolicyExperimentError("M0 ordered fold identities or labels differ from frozen basis")
    probabilities = observed.loc[:, PROBABILITY_COLUMNS].to_numpy(float)
    if not np.isfinite(probabilities).all() or not np.allclose(probabilities.sum(axis=1), 1.0):
        raise Phase74FeaturePolicyExperimentError("M0 probabilities must be finite and sum to one")
    if not observed["candidate_y_pred"].isin(CLASS_ORDER).all():
        raise Phase74FeaturePolicyExperimentError("M0 prediction class mismatch")
    return observed


def build_candidate_pipeline(
    numeric_features: tuple[str, ...],
    categorical_features: tuple[str, ...],
) -> Pipeline:
    transformers: list[tuple[str, Pipeline, list[str]]] = []
    if numeric_features:
        transformers.append(("numeric", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]), list(numeric_features)))
    if categorical_features:
        transformers.append(("categorical", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore")),
        ]), list(categorical_features)))
    return Pipeline([
        ("preprocessor", ColumnTransformer(transformers=transformers, remainder="drop")),
        ("classifier", LogisticRegression(**MODEL_CONSTRUCTOR)),
    ])


def calculate_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    probabilities: np.ndarray,
) -> dict[str, Any]:
    matrix = confusion_matrix(y_true, y_pred, labels=list(CLASS_ORDER))
    support = matrix.sum(axis=1)
    predicted = matrix.sum(axis=0)
    precision: dict[str, float] = {}
    recall: dict[str, float] = {}
    f1: dict[str, float] = {}
    for index, label in enumerate(CLASS_ORDER):
        tp = int(matrix[index, index])
        precision[label] = tp / int(predicted[index]) if int(predicted[index]) else 0.0
        recall[label] = tp / int(support[index]) if int(support[index]) else 0.0
        total = precision[label] + recall[label]
        f1[label] = 2 * precision[label] * recall[label] / total if total else 0.0
    incorrect = y_true != y_pred
    confidence = probabilities.max(axis=1)
    return {
        "validation_rows": int(len(y_true)),
        "accuracy": float(np.mean(y_true == y_pred)),
        "balanced_accuracy": float(np.mean(list(recall.values()))),
        "macro_f1": float(np.mean(list(f1.values()))),
        "weighted_f1": float(sum(f1[label] * support[i] for i, label in enumerate(CLASS_ORDER)) / len(y_true)),
        "multiclass_log_loss": float(-np.mean(np.log(np.clip(
            probabilities[np.arange(len(y_true)), np.asarray([CLASS_ORDER.index(str(label)) for label in y_true])],
            np.finfo(float).eps, 1.0,
        )))),
        "B_recall": recall["B"],
        "S_to_OUT_rate": float(matrix[1, 2] / support[1]) if support[1] else np.nan,
        "OUT_to_S_rate": float(matrix[2, 1] / support[2]) if support[2] else np.nan,
        "mean_confidence_on_incorrect_predictions": float(np.mean(confidence[incorrect])) if incorrect.any() else np.nan,
        "zero_B_recall": bool(recall["B"] == 0.0),
        "per_class_precision": precision,
        "per_class_recall": recall,
        "per_class_f1": f1,
        "per_class_support": {label: int(support[i]) for i, label in enumerate(CLASS_ORDER)},
        "confusion_matrix": matrix.astype(int).tolist(),
    }


def confidence_bucket(predictions: pd.DataFrame) -> dict[str, Any]:
    probabilities = predictions.loc[:, PROBABILITY_COLUMNS].to_numpy(float)
    confidence = probabilities.max(axis=1)
    mask = (confidence >= 0.90) & (confidence <= 1.00)
    if not mask.any():
        return {"bucket_count": 0, "bucket_accuracy": None, "bucket_mean_confidence": None, "bucket_gap": None, "status": "undefined_empty"}
    correct = predictions["candidate_y_pred"].astype(str).to_numpy() == predictions["y_true"].astype(str).to_numpy()
    accuracy = float(np.mean(correct[mask]))
    mean_confidence = float(np.mean(confidence[mask]))
    gap = mean_confidence - accuracy
    return {"bucket_count": int(mask.sum()), "bucket_accuracy": accuracy, "bucket_mean_confidence": mean_confidence, "bucket_gap": gap, "status": "defined"}


def feature_smd(train: pd.Series, validation: pd.Series) -> float:
    train_values = pd.to_numeric(train, errors="coerce").to_numpy(float)
    validation_values = pd.to_numeric(validation, errors="coerce").to_numpy(float)
    train_values = train_values[np.isfinite(train_values)]
    validation_values = validation_values[np.isfinite(validation_values)]
    if len(train_values) < 2 or len(validation_values) < 1:
        raise Phase74FeaturePolicyExperimentError("required SMD has insufficient finite values")
    standard_deviation = float(np.std(train_values, ddof=1))
    if not np.isfinite(standard_deviation) or standard_deviation <= 0:
        raise Phase74FeaturePolicyExperimentError("required SMD training standard deviation is undefined")
    return abs(float(np.mean(validation_values) - np.mean(train_values))) / standard_deviation


def _align_probabilities(raw: np.ndarray, observed_classes: Iterable[str]) -> np.ndarray:
    classes = tuple(str(item) for item in observed_classes)
    positions = {label: index for index, label in enumerate(classes)}
    if set(classes) != set(CLASS_ORDER):
        raise Phase74FeaturePolicyExperimentError("fitted class set differs from B S OUT")
    return np.column_stack([raw[:, positions[label]] for label in CLASS_ORDER])


def _evaluate_matrix(
    matrix_id: str,
    matrix: pd.DataFrame,
    eligible: pd.DataFrame,
    folds: list[tuple[np.ndarray, np.ndarray]],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    numeric = MATRIX_NUMERIC_FEATURES[matrix_id]
    categorical = MATRIX_CATEGORICAL_FEATURES[matrix_id]
    fold_rows: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []
    null_rows: list[dict[str, Any]] = []
    for fold_id, (train_index, valid_index) in enumerate(folds, 1):
        train = matrix.iloc[train_index]
        valid = matrix.iloc[valid_index]
        for split, frame in (("train", train), ("validation", valid)):
            for feature in (*numeric, *categorical):
                null_count = int(frame[feature].isna().sum())
                null_rows.append({
                    "matrix_id": matrix_id, "fold_id": fold_id, "split": split,
                    "feature": feature, "row_count": len(frame), "null_count": null_count,
                    "null_rate": null_count / len(frame), "warmup_null_count": 0,
                    "warmup_null_rate": 0.0, "denominator_failure_count": 0,
                    "denominator_failure_rate": 0.0, "all_null": bool(frame[feature].isna().all()),
                })
            if split == "train" and any(frame[feature].isna().all() for feature in numeric):
                raise Phase74FeaturePolicyExperimentError("all-null numeric feature in training fold")
        pipeline = build_candidate_pipeline(numeric, categorical)
        feature_columns = [*numeric, *categorical]
        y_train = eligible.iloc[train_index]["target_phase_label"].astype(str)
        y_valid = eligible.iloc[valid_index]["target_phase_label"].astype(str).to_numpy()
        pipeline.fit(train.loc[:, feature_columns], y_train)
        predicted = pipeline.predict(valid.loc[:, feature_columns]).astype(str)
        classifier = pipeline.named_steps["classifier"]
        probabilities = _align_probabilities(
            pipeline.predict_proba(valid.loc[:, feature_columns]), classifier.classes_
        )
        metrics = calculate_metrics(y_valid, predicted, probabilities)
        fold_rows.append({
            "matrix_id": matrix_id, "matrix_role": MATRIX_ROLES[matrix_id],
            "fold_id": fold_id, **metrics,
            "acceptance_eligible": matrix_id == "M1_REVISED_FULL",
        })
        identity = eligible.iloc[valid_index][["target_trade_date", "target_instrument_id"]].reset_index(drop=True)
        identity.insert(0, "fold_id", fold_id)
        identity["y_true"] = y_valid
        identity["candidate_y_pred"] = predicted
        for column_index, column in enumerate(PROBABILITY_COLUMNS):
            identity[column] = probabilities[:, column_index]
        prediction_frames.append(identity)
    predictions = pd.concat(prediction_frames, ignore_index=True)
    aggregate = calculate_metrics(
        predictions["y_true"].to_numpy(str),
        predictions["candidate_y_pred"].to_numpy(str),
        predictions.loc[:, PROBABILITY_COLUMNS].to_numpy(float),
    )
    fold_macro = np.asarray([row["macro_f1"] for row in fold_rows], dtype=float)
    aggregate.update({
        "fold_macro_f1_range": float(fold_macro.max() - fold_macro.min()),
        "fold_macro_f1_population_standard_deviation": float(np.std(fold_macro, ddof=0)),
        "minimum_fold_macro_f1": float(fold_macro.min()),
        "zero_B_recall_fold_count": int(sum(bool(row["zero_B_recall"]) for row in fold_rows)),
        "confidence_bucket": confidence_bucket(predictions),
    })
    return pd.DataFrame(fold_rows), predictions, aggregate, null_rows


def _m0_metrics(m0: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = []
    for fold_id, group in m0.groupby("fold_id", sort=True):
        metrics = calculate_metrics(
            group["y_true"].to_numpy(str), group["candidate_y_pred"].to_numpy(str),
            group.loc[:, PROBABILITY_COLUMNS].to_numpy(float),
        )
        rows.append({"matrix_id": "M0_FROZEN_PHASE7_2_CONTROL", "matrix_role": MATRIX_ROLES["M0_FROZEN_PHASE7_2_CONTROL"], "fold_id": int(fold_id), **metrics, "acceptance_eligible": False})
    aggregate = calculate_metrics(
        m0["y_true"].to_numpy(str), m0["candidate_y_pred"].to_numpy(str),
        m0.loc[:, PROBABILITY_COLUMNS].to_numpy(float),
    )
    fold_macro = np.asarray([row["macro_f1"] for row in rows])
    aggregate.update({
        "fold_macro_f1_range": float(fold_macro.max() - fold_macro.min()),
        "fold_macro_f1_population_standard_deviation": float(np.std(fold_macro, ddof=0)),
        "minimum_fold_macro_f1": float(fold_macro.min()),
        "zero_B_recall_fold_count": int(sum(bool(row["zero_B_recall"]) for row in rows)),
        "confidence_bucket": confidence_bucket(m0),
    })
    bucket = aggregate["confidence_bucket"]
    if bucket["bucket_count"] == 0 or bucket["bucket_gap"] is None or not np.isfinite(bucket["bucket_gap"]) or bucket["bucket_gap"] <= 0:
        raise Phase74FeaturePolicyExperimentError("M0 highest-confidence bucket reference is empty undefined or nonpositive")
    return pd.DataFrame(rows), aggregate


def _shift_rows(
    eligible: pd.DataFrame,
    m1: pd.DataFrame,
    folds: list[tuple[np.ndarray, np.ndarray]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[float]] = {}
    for fold_id, (train, valid) in enumerate(folds, 1):
        for comparison, frame, groups in (
            ("revised_M1", m1, REVISED_GROUPS),
            ("mapped_M0", eligible, M0_GROUPS),
        ):
            for group, features in groups.items():
                values = [feature_smd(frame.iloc[train][feature], frame.iloc[valid][feature]) for feature in features]
                median = float(np.median(values))
                grouped.setdefault((comparison, group), []).append(median)
                rows.append({
                    "matrix_id": "M1_REVISED_FULL" if comparison == "revised_M1" else "M0_FROZEN_PHASE7_2_CONTROL",
                    "comparison_role": comparison, "group": group, "fold_id": fold_id,
                    "feature_count": len(features), "group_median_smd": median,
                    "group_max_smd": float(max(values)),
                })
    pooled = {(comparison, group): float(np.median(values)) for (comparison, group), values in grouped.items()}
    for row in rows:
        group = row["group"]
        row["pooled_group_median_smd"] = pooled[(row["comparison_role"], group)]
        row["mapped_M0_pooled_group_median_smd"] = pooled[("mapped_M0", group)]
        denominator = pooled[("mapped_M0", group)]
        if not np.isfinite(denominator) or denominator <= 0:
            raise Phase74FeaturePolicyExperimentError("mapped M0 pooled SMD is nonpositive or nonfinite")
        row["ratio_to_mapped_M0"] = pooled[("revised_M1", group)] / denominator if row["comparison_role"] == "revised_M1" else 1.0
        row["status"] = "defined"
    return rows


def _gate(name: str, observed: Any, comparator: str, threshold: Any, passed: bool, reason: str) -> dict[str, Any]:
    return {"gate": name, "observed_value": observed, "comparator": comparator, "threshold": threshold, "pass": bool(passed), "reason": reason}


def evaluate_gates(
    m0: dict[str, Any],
    m1: dict[str, Any],
    m0_folds: pd.DataFrame,
    m1_folds: pd.DataFrame,
    shift_rows: list[dict[str, Any]],
    identity_status: dict[str, Any],
) -> dict[str, Any]:
    success: dict[str, dict[str, Any]] = {}
    checks = {
        "S1": (m1["B_recall"], ">=", 0.2458904109589041, m1["B_recall"] >= 0.2458904109589041),
        "S2": (m1["zero_B_recall_fold_count"], "<=", 1, m1["zero_B_recall_fold_count"] <= 1),
        "S3": (m1["macro_f1"], ">=", 0.37439549155317, m1["macro_f1"] >= 0.37439549155317),
        "S4": (m1["balanced_accuracy"], ">=", 0.378390605558729, m1["balanced_accuracy"] >= 0.378390605558729),
        "S5": (m1["accuracy"], ">=", 0.38, m1["accuracy"] >= 0.38),
        "S6": (m1["multiclass_log_loss"], "<", 1.2796950500624311, m1["multiclass_log_loss"] < 1.2796950500624311),
        "S7": (m1["fold_macro_f1_range"], "<=", 0.3436553216604367, m1["fold_macro_f1_range"] <= 0.3436553216604367),
        "S8": (m1["fold_macro_f1_population_standard_deviation"], "<=", 0.11025708314162237, m1["fold_macro_f1_population_standard_deviation"] <= 0.11025708314162237),
        "S9": (m1["minimum_fold_macro_f1"], ">=", 0.0892156862745098, m1["minimum_fold_macro_f1"] >= 0.0892156862745098),
        "S10": (m1["S_to_OUT_rate"], "<=", 0.3774193548387097, m1["S_to_OUT_rate"] <= 0.3774193548387097),
        "S11": (m1["OUT_to_S_rate"], "<=", 0.49756097560975615, m1["OUT_to_S_rate"] <= 0.49756097560975615),
        "S12": (m1["mean_confidence_on_incorrect_predictions"], "<=", 0.7099371, np.isfinite(m1["mean_confidence_on_incorrect_predictions"]) and m1["mean_confidence_on_incorrect_predictions"] <= 0.7099371),
    }
    for name, (observed, comparator, threshold, passed) in checks.items():
        success[name] = _gate(name, observed, comparator, threshold, passed, "fixed accepted threshold")
    m0_gap = m0["confidence_bucket"]["bucket_gap"]
    m1_bucket = m1["confidence_bucket"]
    s13 = m1_bucket["bucket_count"] > 0 and m1_bucket["bucket_gap"] is not None and np.isfinite(m1_bucket["bucket_gap"]) and m1_bucket["bucket_gap"] <= 0.70 * m0_gap
    success["S13"] = _gate("S13", m1_bucket["bucket_gap"], "<=", 0.70 * m0_gap, s13, "inclusive 0.90-1.00 confidence bucket")
    deltas = m1_folds.sort_values("fold_id")["macro_f1"].to_numpy() - m0_folds.sort_values("fold_id")["macro_f1"].to_numpy()
    s14 = int(np.sum(deltas >= 0.01)) >= 3 and int(np.sum(deltas < -0.03)) <= 1
    success["S14"] = _gate("S14", {"improved": int(np.sum(deltas >= 0.01)), "degraded": int(np.sum(deltas < -0.03))}, "compound", {"improved_min": 3, "degraded_max": 1}, s14, "fixed fold breadth")
    s15 = m1["macro_f1"] > 0.12383375742154369 and m1["balanced_accuracy"] > 1 / 3 and m1["multiclass_log_loss"] < 1.2796950500624311
    success["S15"] = _gate("S15", {key: m1[key] for key in ("macro_f1", "balanced_accuracy", "multiclass_log_loss")}, "compound", "fixed class-prior comparison", s15, "fixed baseline thresholds")
    ratios = {row["group"]: row["ratio_to_mapped_M0"] for row in shift_rows if row["comparison_role"] == "revised_M1"}
    s16 = len(ratios) == 4 and sum(value <= 0.80 for value in ratios.values()) >= 3 and all(value <= 1.10 for value in ratios.values())
    success["S16"] = _gate("S16", ratios, "compound", {"three_of_four": 0.80, "none_above": 1.10}, s16, "mapped feature shift")
    s17 = all(bool(value) for value in identity_status.values())
    success["S17"] = _gate("S17", identity_status, "all", True, s17, "identity and missingness invariants")
    success["S18"] = _gate("S18", None, "all", "S1-S17", all(success[f"S{i}"]["pass"] for i in range(1, 18)), "M1 sole final gate")

    failure: dict[str, dict[str, Any]] = {}
    conditions = {
        "F1": m1["B_recall"] <= 0.1458904109589041,
        "F2": m1["zero_B_recall_fold_count"] >= 2,
        "F4": m1["multiclass_log_loss"] >= 1.2796950500624311,
        "F5": int(np.sum(deltas >= 0.01)) < 3 or m1["fold_macro_f1_range"] > 0.3866122368679913,
        "F6": not identity_status.get("zero_eligible_row_loss", False) or not identity_status.get("warmup_rates_within_limit", False),
        "F7": not identity_status.get("denominator_rates_within_limit", False) or not identity_status.get("no_all_null_training_fold_feature", False),
        "F8": False,
        "F9": not all(success[f"S{i}"]["pass"] for i in range(1, 18)),
        "F10": m0_gap is None or not np.isfinite(m0_gap) or m0_gap <= 0,
        "F11": not s13,
    }
    if any(m0[key] <= 0 or not np.isfinite(m0[key]) for key in ("S_to_OUT_rate", "OUT_to_S_rate")):
        confusion_failure = True
    else:
        relative_improvements = [
            (m0[key] - m1[key]) / m0[key]
            for key in ("S_to_OUT_rate", "OUT_to_S_rate")
        ]
        neither_improves = all(value < 0.05 for value in relative_improvements)
        either_worsens = any(
            m1[key] - m0[key] >= 0.03
            for key in ("S_to_OUT_rate", "OUT_to_S_rate")
        )
        confusion_failure = neither_improves or either_worsens
    conditions["F3"] = confusion_failure
    for index in range(1, 12):
        name = f"F{index}"
        triggered = bool(conditions[name])
        failure[name] = _gate(name, triggered, "is", True, not triggered, "failure criterion must not trigger")
    return {
        "success_criteria": success,
        "failure_criteria": failure,
        "M1_final_gate_passed": bool(success["S18"]["pass"]),
        "M0_reference_status": "immutable_historical_control",
        "M2_M5_status": "diagnostic_only_not_eligible_for_acceptance",
        "no_promotion_declaration": True,
    }


def _json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase74FeaturePolicyExperimentError(f"invalid JSON: {path.name}") from exc
    if not isinstance(payload, dict):
        raise Phase74FeaturePolicyExperimentError(f"JSON must be an object: {path.name}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, float) and not np.isfinite(value):
        return None
    if value is pd.NA:
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _assert_output_ready(path: Path) -> None:
    if path.exists() and (not path.is_dir() or any(path.iterdir())):
        raise Phase74FeaturePolicyExperimentError("--output-dir must be absent or empty")


def _baseline_metrics(eligible: pd.DataFrame, folds: list[tuple[np.ndarray, np.ndarray]]) -> dict[str, Any]:
    majority_frames = []
    prior_frames = []
    for fold_id, (train, valid) in enumerate(folds, 1):
        y_train = eligible.iloc[train]["target_phase_label"].astype(str)
        y_valid = eligible.iloc[valid]["target_phase_label"].astype(str).to_numpy()
        counts = y_train.value_counts()
        majority = max(CLASS_ORDER, key=lambda label: (int(counts.get(label, 0)), -CLASS_ORDER.index(label)))
        priors = np.asarray([counts.get(label, 0) / len(train) for label in CLASS_ORDER], float)
        majority_prob = np.zeros((len(valid), 3), float)
        majority_prob[:, CLASS_ORDER.index(majority)] = 1.0
        prior_prob = np.tile(priors, (len(valid), 1))
        majority_frames.append(calculate_metrics(y_valid, np.full(len(valid), majority), majority_prob))
        prior_frames.append(calculate_metrics(y_valid, np.full(len(valid), CLASS_ORDER[int(np.argmax(priors))]), prior_prob))
    return {"majority_class_train_only": majority_frames, "class_prior_train_only": prior_frames}


def run_experiment(request: Phase74ExperimentRequest) -> Phase74ExperimentResult:
    _assert_output_ready(request.output_dir)
    source_manifest = _json(request.source_panel_manifest_path)
    dataset_manifest = _json(request.dataset_manifest_path)
    feature_schema = _json(request.feature_schema_path)
    experiment_contract = _json(request.experiment_contract_path)
    readiness_text = request.readiness_contract_path.read_text(encoding="utf-8")
    if tuple(source_manifest.get("required_columns", ())) != SOURCE_REQUIRED_COLUMNS:
        raise Phase74FeaturePolicyExperimentError("source panel manifest column declaration mismatch")
    declared_instruments = source_manifest.get("instruments")
    if not isinstance(declared_instruments, list) or not declared_instruments:
        raise Phase74FeaturePolicyExperimentError("source panel manifest instrument declaration mismatch")
    if (
        dataset_manifest.get("dataset_id") != DATASET_ID
        or dataset_manifest.get("feature_schema_id") != FEATURE_SCHEMA_ID
        or dataset_manifest.get("target_source") != TARGET_SOURCE
        or feature_schema.get("schema_id") != FEATURE_SCHEMA_ID
        or feature_schema.get("dataset_id") != DATASET_ID
    ):
        raise Phase74FeaturePolicyExperimentError("dataset or feature schema identity mismatch")
    identity = experiment_contract.get("experiment_identity", {})
    if identity.get("contract_id") != EXPERIMENT_CONTRACT_ID or identity.get("contract_version") != EXPERIMENT_CONTRACT_VERSION:
        raise Phase74FeaturePolicyExperimentError("experiment contract identity mismatch")
    if READINESS_CONTRACT_ID not in readiness_text or TARGET_SOURCE not in readiness_text:
        raise Phase74FeaturePolicyExperimentError("readiness contract identity mismatch")

    source = pd.read_parquet(request.source_panel_path)
    observed_instruments = sorted(source["instrument_id"].astype(str).str.strip().unique().tolist()) if "instrument_id" in source else []
    if observed_instruments != sorted(str(item).strip() for item in declared_instruments):
        raise Phase74FeaturePolicyExperimentError("source panel instrument declaration mismatch")
    dataset = pd.read_parquet(request.modeling_dataset_path)
    eligible = _prepare_experiment_eligible_rows(dataset)
    folds = build_chronological_folds(eligible)
    identities = _expected_validation_identities(eligible, folds)
    m0 = validate_m0_predictions(pd.read_parquet(request.m0_validation_predictions_path), expected_fold_identities=identities)
    m0_folds, m0_aggregate = _m0_metrics(m0)
    built: FeatureBuildResult = build_feature_matrices(source, dataset)
    if not built.ordered_identities.equals(eligible.loc[:, IDENTITY_COLUMNS]):
        raise Phase74FeaturePolicyExperimentError("builder eligible identity order differs from frozen Phase 6 basis")

    all_fold_frames = [m0_folds]
    aggregate: dict[str, Any] = {"M0_FROZEN_PHASE7_2_CONTROL": m0_aggregate}
    null_rows: list[dict[str, Any]] = []
    predictions: dict[str, pd.DataFrame] = {}
    for matrix_id, matrix in built.matrices.items():
        fold_frame, prediction_frame, metrics, matrix_null_rows = _evaluate_matrix(matrix_id, matrix, eligible, folds)
        all_fold_frames.append(fold_frame)
        aggregate[matrix_id] = metrics
        predictions[matrix_id] = prediction_frame
        null_rows.extend(matrix_null_rows)
    diagnostic = built.diagnostics
    for row in null_rows:
        if row["feature"] not in NEW_NUMERIC_FEATURES:
            continue
        train_index, valid_index = folds[int(row["fold_id"]) - 1]
        selected_index = train_index if row["split"] == "train" else valid_index
        selected = diagnostic.loc[
            diagnostic["feature"].eq(row["feature"])
            & diagnostic["row_index"].isin(selected_index)
        ]
        warmup_count = int(selected["warmup_null"].sum())
        denominator_count = int(selected["denominator_failure"].sum())
        nonwarmup_count = int((~selected["warmup_null"]).sum())
        row["warmup_null_count"] = warmup_count
        row["warmup_null_rate"] = warmup_count / len(selected) if len(selected) else 0.0
        row["denominator_failure_count"] = denominator_count
        row["denominator_failure_rate"] = denominator_count / nonwarmup_count if nonwarmup_count else 0.0
    shift_rows = _shift_rows(eligible, built.matrices["M1_REVISED_FULL"], folds)

    warmup_rates = diagnostic.groupby("feature")["warmup_null"].mean()
    nonwarmup = diagnostic.loc[~diagnostic["warmup_null"]]
    denominator_rates = nonwarmup.groupby("feature")["denominator_failure"].mean()
    identity_status = {
        "zero_eligible_row_loss": len(built.ordered_identities) == len(eligible),
        "same_eligible_identities_as_M0_basis": built.ordered_identities.equals(eligible.loc[:, IDENTITY_COLUMNS]),
        "same_320_validation_identities_as_M0": predictions["M1_REVISED_FULL"].loc[:, ["fold_id", *IDENTITY_COLUMNS, "y_true"]].equals(identities),
        "no_all_null_training_fold_feature": not any(bool(row["all_null"]) for row in null_rows if row["split"] == "train"),
        "warmup_rates_within_limit": bool((warmup_rates <= 0.05).all()),
        "denominator_rates_within_limit": bool((denominator_rates <= 0.01).all()),
    }
    gates = evaluate_gates(
        m0_aggregate, aggregate["M1_REVISED_FULL"], m0_folds,
        pd.concat(all_fold_frames[1:2], ignore_index=True), shift_rows, identity_status,
    )
    baseline = _baseline_metrics(eligible, folds)

    request.output_dir.mkdir(parents=True, exist_ok=True)
    paths = {name: request.output_dir / name for name in DECLARED_OUTPUT_ARTIFACTS}
    _write_json(paths["input_identity_verification.json"], {
        "run_id": request.run_id, "source_git_commit_sha": request.git_commit_sha,
        "inputs": {name: {"path": str(path), "sha256": _sha256(path)} for name, path in _input_paths(request).items()},
        "eligible_identity_count": len(eligible), "validation_identity_count": len(identities),
        "M0_identity_comparison_result": True, "fold_identity_comparison_result": True,
        "side_effect_declaration": {"writes_outside_output_dir": False, "network_calls": False, "subprocess_calls": False, "model_serialization": False},
    })
    inventory: dict[str, Any] = {}
    for matrix_id in MATRIX_ROLES:
        numeric_features = tuple(MATRIX_NUMERIC_FEATURES.get(matrix_id, PHASE6_LEGACY_NUMERIC_COLUMNS))
        categorical_features = tuple(MATRIX_CATEGORICAL_FEATURES.get(matrix_id, PHASE6_LEGACY_CATEGORICAL_COLUMNS))
        inventory[matrix_id] = {
            "role": MATRIX_ROLES[matrix_id],
            "numeric_features": list(numeric_features),
            "categorical_features": list(categorical_features),
            "numeric_count": len(numeric_features),
            "categorical_count": len(categorical_features),
            "total_pre_encoding": len(numeric_features) + len(categorical_features),
            "acceptance_eligible": matrix_id == "M1_REVISED_FULL",
            "removed_from_M1": [] if matrix_id in {"M0_FROZEN_PHASE7_2_CONTROL", "M1_REVISED_FULL"} else [
                feature for feature in (*M1_NUMERIC_FEATURES, *M1_CATEGORICAL_FEATURES)
                if feature not in (*numeric_features, *categorical_features)
            ],
            "promotion_allowed": False,
        }
    _write_json(paths["feature_matrix_inventory.json"], inventory)
    pd.DataFrame(null_rows).to_csv(paths["feature_nullness_by_matrix_and_fold.csv"], index=False)
    pd.DataFrame(shift_rows).to_csv(paths["feature_shift_by_matrix_group_and_fold.csv"], index=False)
    fold_metrics = pd.concat(all_fold_frames, ignore_index=True)
    metric_columns = [
        "matrix_id", "matrix_role", "fold_id", "validation_rows", "accuracy",
        "balanced_accuracy", "macro_f1", "weighted_f1", "multiclass_log_loss",
        "B_recall", "S_to_OUT_rate", "OUT_to_S_rate",
        "mean_confidence_on_incorrect_predictions", "zero_B_recall", "acceptance_eligible",
    ]
    fold_metrics.loc[:, metric_columns].to_csv(paths["fold_metrics_by_matrix.csv"], index=False)
    _write_json(paths["aggregate_metrics_by_matrix.json"], {
        "fixed_class_order": list(CLASS_ORDER), "matrices": aggregate, "baselines": baseline
    })
    per_class_rows = []
    for matrix_id, metrics in aggregate.items():
        for label in CLASS_ORDER:
            per_class_rows.append({
                "matrix_id": matrix_id, "class": label,
                "precision": metrics["per_class_precision"][label],
                "recall": metrics["per_class_recall"][label],
                "f1": metrics["per_class_f1"][label],
                "support": metrics["per_class_support"][label], "status": "defined",
            })
    pd.DataFrame(per_class_rows).to_csv(paths["per_class_metrics_by_matrix.csv"], index=False)
    ablation_rows = []
    for matrix_id in tuple(MATRIX_NUMERIC_FEATURES)[1:]:
        for metric in ("accuracy", "balanced_accuracy", "macro_f1", "multiclass_log_loss"):
            value = aggregate[matrix_id][metric]
            reference = aggregate["M1_REVISED_FULL"][metric]
            ablation_rows.append({
                "ablation_matrix": matrix_id, "reference_matrix": "M1_REVISED_FULL",
                "metric": metric, "ablation_value": value, "M1_value": reference,
                "absolute_delta": value - reference, "diagnostic_only": True,
                "promotion_allowed": False,
            })
    pd.DataFrame(ablation_rows).to_csv(paths["ablation_effects.csv"], index=False)
    _write_json(paths["gate_results.json"], gates)
    observed = sorted(path.name for path in request.output_dir.iterdir() if path.is_file())
    if observed != sorted(DECLARED_OUTPUT_ARTIFACTS) or any(path.is_dir() for path in request.output_dir.iterdir()):
        raise Phase74FeaturePolicyExperimentError("output inventory differs from exact nine artifacts")
    return Phase74ExperimentResult(
        output_dir=request.output_dir, artifact_names=DECLARED_OUTPUT_ARTIFACTS,
        eligible_row_count=len(eligible), validation_row_count=len(identities),
        fold_count=len(folds), final_gate_passed=bool(gates["M1_final_gate_passed"]),
    )


def run_from_args(args: argparse.Namespace) -> Phase74ExperimentResult:
    return run_experiment(request_from_args(args))


def main() -> None:
    run_from_args(build_argument_parser().parse_args())


if __name__ == "__main__":
    main()
