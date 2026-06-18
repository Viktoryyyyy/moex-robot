from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

EXPERIMENT_ID = "usdrubf_ema_3_19_d1_logistic_screen_v1"
PRODUCER = "src.moex_research.runners.usdrubf_ema_3_19_d1_logistic_screen"
RESULT_STATUS = "provisional_screening"

KEY_COLUMNS = ("instrument_id", "end", "cross_dir")
NUMERIC_FEATURES = (
    "ema_diff",
    "ema_diff_prev",
    "bars_since_prev_cross",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "rolling_vol_5d",
    "rolling_vol_20d",
)
CATEGORICAL_FEATURES = ("cross_dir",)
MODEL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

PRIMARY_TARGET = "allow_trade_h5_observed"
SECONDARY_TARGET = "positive_signed_ret_o2o_h1"
TARGET_ORDER = (PRIMARY_TARGET, SECONDARY_TARGET)
TARGET_ROLES = {PRIMARY_TARGET: "primary", SECONDARY_TARGET: "secondary"}

MINIMUM_PRIMARY_ELIGIBLE_ROWS = 50
MINIMUM_INITIAL_TRAIN_EVENTS = 32
TEST_BLOCK_EVENTS = 8
PURGE_D1_SESSIONS = 6
PRIMARY_C = 1.0
C_SENSITIVITY_VALUES = (0.1, 1.0, 10.0)
CLASSIFICATION_THRESHOLD = 0.5

OUTPUT_RUN_METADATA = "run_metadata.json"
OUTPUT_PREDICTIONS = "m3_oos_predictions.csv"
OUTPUT_METRICS = "m3_metrics.json"
OUTPUT_FOLD_METRICS = "m3_fold_metrics.csv"
OUTPUT_COEFFICIENTS = "m3_coefficients.csv"
OUTPUT_C_SENSITIVITY = "m3_c_sensitivity.csv"
OUTPUT_QUALITY_REPORT = "m3_quality_report.json"
DECLARED_OUTPUT_FILES = (
    OUTPUT_RUN_METADATA,
    OUTPUT_PREDICTIONS,
    OUTPUT_METRICS,
    OUTPUT_FOLD_METRICS,
    OUTPUT_COEFFICIENTS,
    OUTPUT_C_SENSITIVITY,
    OUTPUT_QUALITY_REPORT,
)

SOURCE_PATH_ALIAS_TOKENS = ("latest", "current", "autodetect")
_GLOB_CHARACTERS = frozenset("*?[]")
_FILENAME_STEM_TOKEN_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+")
_VALID_CROSS_DIRECTIONS = {"cross_up", "cross_down"}
_LABEL_LIKE_PREFIXES = ("signed_ret_", "allow_trade_", "max_adverse_", "max_favorable_")
_FUTURE_CONTEXT_MARKERS = (
    "d+1",
    "d_plus_1",
    "entry_open",
    "exit_open",
    "outcome",
    "signed_ret_o2o",
    "allow_trade",
    "max_adverse",
    "max_favorable",
)
_ALLOWED_CONTEXT_NON_MODEL_COLUMNS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "ema3",
    "ema19",
    "known_by_when",
}
_ALLOWED_CONTEXT_COLUMNS = set(KEY_COLUMNS) | set(MODEL_FEATURES) | _ALLOWED_CONTEXT_NON_MODEL_COLUMNS
_REQUIRED_LABEL_COLUMNS = {
    *KEY_COLUMNS,
    "signed_ret_o2o_h1",
    "signed_ret_o2o_h5",
    "allow_trade_h5",
}


@dataclass(frozen=True)
class WalkForwardFold:
    fold: int
    train_positions: tuple[int, ...]
    test_positions: tuple[int, ...]
    first_test_session_index: int


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the research-only USDRUBF D1 EMA(3/19) logistic OOS screen.",
    )
    parser.add_argument("--d1-ohlc-path", required=True)
    parser.add_argument("--context-path", required=True)
    parser.add_argument("--labels-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    return parser


def _path_alias_tokens(path_value: str) -> list[str]:
    components = [
        component.lower()
        for component in re.split(r"[\\/]+", path_value)
        if component and component not in {".", ".."}
    ]
    filename_stem = Path(components[-1]).stem if components else ""
    stem_tokens = {
        token.lower()
        for token in _FILENAME_STEM_TOKEN_SPLIT_RE.split(filename_stem)
        if token
    }
    component_tokens = set(components)
    return [
        token
        for token in SOURCE_PATH_ALIAS_TOKENS
        if token in component_tokens or token in stem_tokens
    ]


def _input_path_alias_tokens(raw_path: str) -> list[str]:
    candidates = [raw_path]
    try:
        resolved = str(Path(raw_path).expanduser().resolve(strict=False))
    except (OSError, RuntimeError):
        resolved = str(Path(raw_path).expanduser().absolute())
    if resolved != raw_path:
        candidates.append(resolved)

    found: list[str] = []
    for candidate in candidates:
        for token in _path_alias_tokens(candidate):
            if token not in found:
                found.append(token)
    return found


def _validate_explicit_input_path(
    *,
    raw_value: str,
    argument_name: str,
    parser: argparse.ArgumentParser,
) -> Path:
    raw_path = raw_value.strip()
    if not raw_path:
        parser.error(f"{argument_name} must be non-empty")
    if any(character in raw_path for character in _GLOB_CHARACTERS):
        parser.error(f"{argument_name} must reference one explicit file and must not contain glob syntax")
    aliases = _input_path_alias_tokens(raw_path)
    if aliases:
        parser.error(
            f"{argument_name} must not use mutable alias token(s): " + ", ".join(aliases)
        )

    path = Path(raw_path)
    if not path.exists():
        parser.error(f"{argument_name} must reference an existing file")
    if not path.is_file():
        parser.error(f"{argument_name} must reference a file")
    return path


def _validate_cli_args(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> tuple[Path, Path, Path, Path, str]:
    d1_path = _validate_explicit_input_path(
        raw_value=str(args.d1_ohlc_path),
        argument_name="--d1-ohlc-path",
        parser=parser,
    )
    context_path = _validate_explicit_input_path(
        raw_value=str(args.context_path),
        argument_name="--context-path",
        parser=parser,
    )
    labels_path = _validate_explicit_input_path(
        raw_value=str(args.labels_path),
        argument_name="--labels-path",
        parser=parser,
    )

    raw_output_dir = str(args.output_dir).strip()
    if not raw_output_dir:
        parser.error("--output-dir must be non-empty")
    output_dir = Path(raw_output_dir)
    if output_dir.exists() and not output_dir.is_dir():
        parser.error("--output-dir must reference a directory")
    if output_dir.exists() and any(output_dir.iterdir()):
        parser.error("--output-dir must be absent or empty to prevent stale evidence")

    run_id = str(args.run_id).strip()
    if not run_id:
        parser.error("--run-id must be non-empty")
    return d1_path, context_path, labels_path, output_dir, run_id


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_safe(value: Any) -> Any:
    if value is pd.NA:
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False, lineterminator="\n", float_format="%.12g")


def _read_csv(path: Path, *, artifact_name: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - pandas exception variants are version-specific
        raise ValueError(f"failed to read {artifact_name} CSV: {exc}") from exc


def _normalize_event_keys(frame: pd.DataFrame, *, artifact_name: str) -> pd.DataFrame:
    missing = [column for column in KEY_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"{artifact_name} missing key columns: " + ", ".join(missing))

    work = frame.copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != "usdrubf").any():
        raise ValueError(f"{artifact_name} instrument_id values must all equal 'usdrubf'")
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    if work["end"].isna().any():
        raise ValueError(f"{artifact_name} contains invalid end timestamps")
    work["cross_dir"] = work["cross_dir"].astype(str)
    invalid_directions = sorted(set(work["cross_dir"]) - _VALID_CROSS_DIRECTIONS)
    if invalid_directions:
        raise ValueError(
            f"{artifact_name} contains unsupported cross_dir values: "
            + ", ".join(invalid_directions)
        )
    if work.duplicated(list(KEY_COLUMNS)).any():
        raise ValueError(f"{artifact_name} contains duplicate event keys")
    if not work["end"].is_monotonic_increasing:
        raise ValueError(f"{artifact_name} event ordering must be chronological by end")
    return work.reset_index(drop=True)


def _normalize_d1_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"instrument_id", "end"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("D1 OHLC artifact missing columns: " + ", ".join(missing))

    work = frame.copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != "usdrubf").any():
        raise ValueError("D1 OHLC instrument_id values must all equal 'usdrubf'")
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    if work["end"].isna().any():
        raise ValueError("D1 OHLC artifact contains invalid end timestamps")
    if work.duplicated(["instrument_id", "end"]).any():
        raise ValueError("D1 OHLC artifact contains duplicate session keys")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("D1 OHLC sessions must be chronological by end")
    if work.empty:
        raise ValueError("D1 OHLC artifact has zero rows")
    work["_session_index"] = np.arange(len(work), dtype=int)
    return work.reset_index(drop=True)


def _validate_and_normalize_context(frame: pd.DataFrame) -> pd.DataFrame:
    work = _normalize_event_keys(frame, artifact_name="context artifact")

    missing_features = [column for column in MODEL_FEATURES if column not in work.columns]
    if missing_features:
        raise ValueError("context artifact missing exact model feature(s): " + ", ".join(missing_features))

    forbidden_columns = sorted(
        column
        for column in work.columns
        if column == "year"
        or column.startswith(_LABEL_LIKE_PREFIXES)
        or any(marker in column.lower() for marker in _FUTURE_CONTEXT_MARKERS)
    )
    if forbidden_columns:
        raise ValueError("context artifact contains forbidden feature/outcome column(s): " + ", ".join(forbidden_columns))

    extra_columns = sorted(set(work.columns) - _ALLOWED_CONTEXT_COLUMNS)
    if extra_columns:
        raise ValueError("context artifact contains undeclared columns: " + ", ".join(extra_columns))

    for column in NUMERIC_FEATURES:
        original_non_null = work[column].notna()
        numeric = pd.to_numeric(work[column], errors="coerce")
        if (original_non_null & numeric.isna()).any():
            raise ValueError(f"context feature {column} contains non-numeric values")
        work[column] = numeric
        if not work[column].notna().any():
            raise ValueError(f"context feature {column} has no observed numeric values")

    return work


def _validate_and_normalize_labels(frame: pd.DataFrame) -> pd.DataFrame:
    work = _normalize_event_keys(frame, artifact_name="labels artifact")
    missing = sorted(_REQUIRED_LABEL_COLUMNS - set(work.columns))
    if missing:
        raise ValueError("labels artifact missing columns: " + ", ".join(missing))
    for column in ("signed_ret_o2o_h1", "signed_ret_o2o_h5", "allow_trade_h5"):
        original_non_null = work[column].notna()
        numeric = pd.to_numeric(work[column], errors="coerce")
        if (original_non_null & numeric.isna()).any():
            raise ValueError(f"labels column {column} contains non-numeric values")
        work[column] = numeric
    return work


def _join_context_and_labels(context: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    joined = context.merge(
        labels,
        on=list(KEY_COLUMNS),
        how="outer",
        sort=False,
        validate="one_to_one",
        indicator=True,
        suffixes=("", "_label"),
    )
    unmatched = joined.loc[joined["_merge"] != "both", [*KEY_COLUMNS, "_merge"]]
    if not unmatched.empty:
        left_only = int((unmatched["_merge"] == "left_only").sum())
        right_only = int((unmatched["_merge"] == "right_only").sum())
        raise ValueError(
            "context/labels event keys do not match one-to-one "
            f"(context_only={left_only}, labels_only={right_only})"
        )
    joined = joined.drop(columns=["_merge"])
    if not joined["end"].is_monotonic_increasing:
        raise ValueError("joined event ordering must be chronological by end")
    return joined.reset_index(drop=True)


def _attach_session_indices(joined: pd.DataFrame, d1: pd.DataFrame) -> pd.DataFrame:
    session_lookup = d1[["instrument_id", "end", "_session_index"]]
    work = joined.merge(
        session_lookup,
        on=["instrument_id", "end"],
        how="left",
        sort=False,
        validate="many_to_one",
    )
    if work["_session_index"].isna().any():
        raise ValueError("one or more event end timestamps are absent from D1 OHLC sessions")
    work["_session_index"] = work["_session_index"].astype(int)
    if not work["_session_index"].is_monotonic_increasing:
        raise ValueError("event session indices must be chronological")
    return work.reset_index(drop=True)


def _build_target_frames(joined: pd.DataFrame) -> dict[str, pd.DataFrame]:
    primary = joined.loc[joined["signed_ret_o2o_h5"].notna()].copy()
    primary["target_value"] = (primary["signed_ret_o2o_h5"] > 0.0).astype(int)
    stored = primary["allow_trade_h5"]
    if stored.isna().any():
        raise ValueError("allow_trade_h5 is missing for an observed H5 outcome")
    if (~stored.isin([0, 1])).any():
        raise ValueError("allow_trade_h5 must contain only 0/1 values for observed H5 outcomes")
    if not np.array_equal(stored.astype(int).to_numpy(), primary["target_value"].to_numpy()):
        raise ValueError("stored allow_trade_h5 is inconsistent with signed_ret_o2o_h5 > 0")

    secondary = joined.loc[joined["signed_ret_o2o_h1"].notna()].copy()
    secondary["target_value"] = (secondary["signed_ret_o2o_h1"] > 0.0).astype(int)

    frames = {
        PRIMARY_TARGET: primary.reset_index(drop=True),
        SECONDARY_TARGET: secondary.reset_index(drop=True),
    }
    if len(primary) < MINIMUM_PRIMARY_ELIGIBLE_ROWS:
        raise ValueError(
            f"eligible primary target has {len(primary)} rows; minimum is {MINIMUM_PRIMARY_ELIGIBLE_ROWS}"
        )
    for target_name, frame in frames.items():
        if frame["target_value"].nunique(dropna=False) < 2:
            raise ValueError(f"target {target_name} contains only one class")
    return frames


def load_and_prepare_inputs(
    *,
    d1_ohlc_path: Path,
    context_path: Path,
    labels_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    d1 = _normalize_d1_frame(_read_csv(d1_ohlc_path, artifact_name="D1 OHLC artifact"))
    context = _validate_and_normalize_context(
        _read_csv(context_path, artifact_name="context artifact")
    )
    labels = _validate_and_normalize_labels(
        _read_csv(labels_path, artifact_name="labels artifact")
    )
    joined = _attach_session_indices(_join_context_and_labels(context, labels), d1)
    targets = _build_target_frames(joined)
    return d1, context, labels, targets


def build_walk_forward_folds(frame: pd.DataFrame) -> list[WalkForwardFold]:
    if len(frame) <= MINIMUM_INITIAL_TRAIN_EVENTS:
        raise ValueError("target does not contain enough rows for walk-forward OOS evaluation")

    initial_test_position = MINIMUM_INITIAL_TRAIN_EVENTS
    while initial_test_position < len(frame):
        first_test_session = int(frame.iloc[initial_test_position]["_session_index"])
        train_positions = tuple(
            position
            for position in range(initial_test_position)
            if int(frame.iloc[position]["_session_index"]) + PURGE_D1_SESSIONS
            < first_test_session
        )
        train_classes = frame.iloc[list(train_positions)]["target_value"].nunique() if train_positions else 0
        if len(train_positions) >= MINIMUM_INITIAL_TRAIN_EVENTS and train_classes == 2:
            break
        initial_test_position += 1
    else:
        raise ValueError("no valid initial walk-forward fold remains after the six-session purge")

    folds: list[WalkForwardFold] = []
    fold_number = 1
    test_start = initial_test_position
    while test_start < len(frame):
        test_stop = min(test_start + TEST_BLOCK_EVENTS, len(frame))
        test_positions = tuple(range(test_start, test_stop))
        first_test_session = int(frame.iloc[test_start]["_session_index"])
        train_positions = tuple(
            position
            for position in range(test_start)
            if int(frame.iloc[position]["_session_index"]) + PURGE_D1_SESSIONS
            < first_test_session
        )
        if len(train_positions) < MINIMUM_INITIAL_TRAIN_EVENTS:
            raise ValueError("walk-forward training fold violates minimum train event count after purge")
        if frame.iloc[list(train_positions)]["target_value"].nunique() < 2:
            raise ValueError("walk-forward training fold contains only one target class")
        folds.append(
            WalkForwardFold(
                fold=fold_number,
                train_positions=train_positions,
                test_positions=test_positions,
                first_test_session_index=first_test_session,
            )
        )
        fold_number += 1
        test_start = test_stop

    if not folds:
        raise ValueError("walk-forward protocol produced zero OOS folds")
    return folds


def build_model_pipeline(*, c_value: float) -> Pipeline:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipeline, list(NUMERIC_FEATURES)),
            ("categorical", OneHotEncoder(handle_unknown="error"), list(CATEGORICAL_FEATURES)),
        ],
        remainder="drop",
    )
    model = LogisticRegression(
        C=float(c_value),
        class_weight="balanced",
        solver="liblinear",
        max_iter=1000,
        random_state=0,
    )
    return Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])


def _fit_fold_model(train_frame: pd.DataFrame, *, c_value: float) -> Pipeline:
    pipeline = build_model_pipeline(c_value=c_value)
    pipeline.fit(train_frame[list(MODEL_FEATURES)], train_frame["target_value"].astype(int))
    return pipeline


def _positive_probability(pipeline: Pipeline, frame: pd.DataFrame) -> np.ndarray:
    probabilities = pipeline.predict_proba(frame[list(MODEL_FEATURES)])
    classes = list(pipeline.named_steps["model"].classes_)
    if 1 not in classes:
        raise ValueError("fitted model does not expose the positive class")
    return probabilities[:, classes.index(1)]


def _coefficient_rows(
    pipeline: Pipeline,
    *,
    target_name: str,
    fold: int,
    c_value: float,
) -> list[dict[str, Any]]:
    preprocessor = pipeline.named_steps["preprocessor"]
    model = pipeline.named_steps["model"]
    feature_names = [str(name) for name in preprocessor.get_feature_names_out()]
    coefficients = model.coef_[0]
    if len(feature_names) != len(coefficients):
        raise ValueError("coefficient count does not match transformed feature count")

    rows = [
        {
            "target": target_name,
            "target_role": TARGET_ROLES[target_name],
            "fold": fold,
            "c": float(c_value),
            "coefficient_scope": "fold_training_only",
            "feature": feature_name,
            "coefficient": float(coefficient),
        }
        for feature_name, coefficient in zip(feature_names, coefficients, strict=True)
    ]
    rows.append(
        {
            "target": target_name,
            "target_role": TARGET_ROLES[target_name],
            "fold": fold,
            "c": float(c_value),
            "coefficient_scope": "fold_training_only",
            "feature": "__intercept__",
            "coefficient": float(model.intercept_[0]),
        }
    )
    return rows


def _metric_bundle(predictions: pd.DataFrame) -> dict[str, Any]:
    metric_names = (
        "positive_rate",
        "roc_auc",
        "pr_auc",
        "brier_score",
        "accuracy",
        "balanced_accuracy",
        "precision",
        "recall",
        "f1",
        "prevalence_baseline_brier",
        "majority_baseline_accuracy",
        "model_minus_baseline_brier",
    )
    if predictions.empty:
        return {
            "sample_count": 0,
            **{metric: None for metric in metric_names},
            "undefined_metric_reasons": {metric: "split has zero OOS samples" for metric in metric_names},
        }

    y_true = predictions["target_value"].astype(int).to_numpy()
    probability = predictions["probability"].astype(float).to_numpy()
    predicted = (probability >= CLASSIFICATION_THRESHOLD).astype(int)
    baseline_probability = predictions["baseline_probability"].astype(float).to_numpy()
    baseline_class = predictions["baseline_class"].astype(int).to_numpy()

    model_brier = float(brier_score_loss(y_true, probability))
    baseline_brier = float(brier_score_loss(y_true, baseline_probability))
    single_class = np.unique(y_true).size < 2
    result: dict[str, Any] = {
        "sample_count": int(len(predictions)),
        "positive_rate": float(np.mean(y_true)),
        "brier_score": model_brier,
        "accuracy": float(accuracy_score(y_true, predicted)),
        "balanced_accuracy": (
            None if single_class else float(balanced_accuracy_score(y_true, predicted))
        ),
        "precision": float(precision_score(y_true, predicted, zero_division=0)),
        "recall": float(recall_score(y_true, predicted, zero_division=0)),
        "f1": float(f1_score(y_true, predicted, zero_division=0)),
        "prevalence_baseline_brier": baseline_brier,
        "majority_baseline_accuracy": float(accuracy_score(y_true, baseline_class)),
        "model_minus_baseline_brier": model_brier - baseline_brier,
        "undefined_metric_reasons": {},
    }
    if single_class:
        reason = "split contains one target class"
        result["roc_auc"] = None
        result["pr_auc"] = None
        result["undefined_metric_reasons"] = {
            "roc_auc": reason,
            "pr_auc": reason,
            "balanced_accuracy": reason,
        }
    else:
        result["roc_auc"] = float(roc_auc_score(y_true, probability))
        result["pr_auc"] = float(average_precision_score(y_true, probability))
    return result


def _evaluate_target(
    frame: pd.DataFrame,
    *,
    target_name: str,
    c_value: float,
    collect_coefficients: bool,
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    prediction_rows: list[dict[str, Any]] = []
    fold_metric_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []

    for fold in build_walk_forward_folds(frame):
        train = frame.iloc[list(fold.train_positions)].copy()
        test = frame.iloc[list(fold.test_positions)].copy()
        pipeline = _fit_fold_model(train, c_value=c_value)
        probability = _positive_probability(pipeline, test)
        prevalence = float(train["target_value"].mean())
        majority_class = 1 if prevalence > 0.5 else 0

        for row_offset, (_, event) in enumerate(test.iterrows()):
            prediction_rows.append(
                {
                    "instrument_id": str(event["instrument_id"]),
                    "end": pd.Timestamp(event["end"]).isoformat(),
                    "cross_dir": str(event["cross_dir"]),
                    "target": target_name,
                    "target_role": TARGET_ROLES[target_name],
                    "target_value": int(event["target_value"]),
                    "probability": float(probability[row_offset]),
                    "predicted_class": int(probability[row_offset] >= CLASSIFICATION_THRESHOLD),
                    "fold": int(fold.fold),
                    "baseline_probability": prevalence,
                    "baseline_class": majority_class,
                    "signed_ret_o2o_h5": (
                        None
                        if pd.isna(event.get("signed_ret_o2o_h5"))
                        else float(event["signed_ret_o2o_h5"])
                    ),
                }
            )

        fold_predictions = pd.DataFrame(prediction_rows[-len(test) :])
        metrics = _metric_bundle(fold_predictions)
        fold_metric_rows.append(
            {
                "target": target_name,
                "target_role": TARGET_ROLES[target_name],
                "fold": int(fold.fold),
                "c": float(c_value),
                "train_count": int(len(train)),
                "test_count": int(len(test)),
                "train_start": pd.Timestamp(train.iloc[0]["end"]).isoformat(),
                "train_end": pd.Timestamp(train.iloc[-1]["end"]).isoformat(),
                "test_start": pd.Timestamp(test.iloc[0]["end"]).isoformat(),
                "test_end": pd.Timestamp(test.iloc[-1]["end"]).isoformat(),
                "first_test_session_index": int(fold.first_test_session_index),
                "max_train_label_completion_session_index": int(
                    (train["_session_index"] + PURGE_D1_SESSIONS).max()
                ),
                "purge_d1_sessions": PURGE_D1_SESSIONS,
                "train_prevalence": prevalence,
                "majority_class": majority_class,
                **{key: value for key, value in metrics.items() if key != "undefined_metric_reasons"},
                "roc_auc_reason": metrics["undefined_metric_reasons"].get("roc_auc"),
                "pr_auc_reason": metrics["undefined_metric_reasons"].get("pr_auc"),
                "balanced_accuracy_reason": metrics["undefined_metric_reasons"].get(
                    "balanced_accuracy"
                ),
            }
        )
        if collect_coefficients:
            coefficient_rows.extend(
                _coefficient_rows(
                    pipeline,
                    target_name=target_name,
                    fold=fold.fold,
                    c_value=c_value,
                )
            )

    predictions = pd.DataFrame(prediction_rows)
    return predictions, fold_metric_rows, coefficient_rows


def _split_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    return {
        "total": _metric_bundle(predictions),
        "cross_up": _metric_bundle(predictions.loc[predictions["cross_dir"] == "cross_up"]),
        "cross_down": _metric_bundle(predictions.loc[predictions["cross_dir"] == "cross_down"]),
    }


def _primary_decision_diagnostics(predictions: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    h5 = pd.to_numeric(predictions["signed_ret_o2o_h5"], errors="coerce")
    if h5.isna().any():
        raise ValueError("primary OOS predictions contain unavailable H5 outcomes")
    retained_mask = predictions["probability"] >= CLASSIFICATION_THRESHOLD
    retained_h5 = h5.loc[retained_mask]
    all_mean = float(h5.mean())
    retained_mean = None if retained_h5.empty else float(retained_h5.mean())
    diagnostics = {
        "all_eligible_oos_mean_signed_ret_o2o_h5": all_mean,
        "retained_mean_signed_ret_o2o_h5": retained_mean,
        "retained_event_count": int(retained_mask.sum()),
        "retained_event_share": float(retained_mask.mean()),
        "classification_threshold": CLASSIFICATION_THRESHOLD,
    }

    fold_rows: list[dict[str, Any]] = []
    positive_uplift_folds = 0
    for fold_number, fold_predictions in predictions.groupby("fold", sort=True):
        fold_h5 = pd.to_numeric(fold_predictions["signed_ret_o2o_h5"], errors="coerce")
        fold_retained = fold_predictions["probability"] >= CLASSIFICATION_THRESHOLD
        fold_all_mean = float(fold_h5.mean())
        fold_retained_mean = (
            None if not fold_retained.any() else float(fold_h5.loc[fold_retained].mean())
        )
        uplift = None if fold_retained_mean is None else fold_retained_mean - fold_all_mean
        if uplift is not None and uplift > 0.0:
            positive_uplift_folds += 1
        fold_rows.append(
            {
                "fold": int(fold_number),
                "oos_count": int(len(fold_predictions)),
                "retained_count": int(fold_retained.sum()),
                "all_mean_signed_ret_o2o_h5": fold_all_mean,
                "retained_mean_signed_ret_o2o_h5": fold_retained_mean,
                "retained_minus_all_mean": uplift,
            }
        )

    distribution = {
        "folds": fold_rows,
        "positive_uplift_fold_count": positive_uplift_folds,
        "uplift_not_supplied_by_single_fold": positive_uplift_folds >= 2,
        "mechanical_rule": "at least two OOS folds must have retained-minus-all H5 mean greater than zero",
    }
    return diagnostics, distribution


def _screening_verdict(
    total_metrics: dict[str, Any],
    diagnostics: dict[str, Any],
    fold_distribution: dict[str, Any],
) -> dict[str, Any]:
    retained_mean = diagnostics["retained_mean_signed_ret_o2o_h5"]
    conditions = {
        "primary_oos_roc_auc_gt_0_55": (
            total_metrics["roc_auc"] is not None and total_metrics["roc_auc"] > 0.55
        ),
        "primary_brier_better_than_fold_prevalence_baseline": (
            total_metrics["brier_score"] < total_metrics["prevalence_baseline_brier"]
        ),
        "retained_h5_mean_exceeds_all_oos_h5_mean": (
            retained_mean is not None
            and retained_mean > diagnostics["all_eligible_oos_mean_signed_ret_o2o_h5"]
        ),
        "uplift_not_supplied_by_single_fold": bool(
            fold_distribution["uplift_not_supplied_by_single_fold"]
        ),
    }
    supported = all(conditions.values())
    return {
        "result_status": RESULT_STATUS,
        "screening_support": (
            "supported_for_next_model_phase" if supported else "not_supported_or_hold"
        ),
        "conditions": conditions,
        "model_promotion_allowed": False,
        "strategy_or_runtime_conclusion_allowed": False,
        "caution": (
            "Small-sample research screen. The canonical M2.1 packet contains approximately "
            "63 observed H5 targets; no production, model-promotion, strategy-package, or runtime conclusion is allowed."
        ),
    }


def _build_metrics_payload(
    predictions_by_target: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    targets: dict[str, Any] = {}
    primary_diagnostics: dict[str, Any] | None = None
    primary_distribution: dict[str, Any] | None = None

    for target_name in TARGET_ORDER:
        predictions = predictions_by_target[target_name]
        split_metrics = _split_metrics(predictions)
        target_payload: dict[str, Any] = {
            "target": target_name,
            "target_role": TARGET_ROLES[target_name],
            "oos_metrics": split_metrics,
        }
        if target_name == PRIMARY_TARGET:
            primary_diagnostics, primary_distribution = _primary_decision_diagnostics(predictions)
            target_payload["decision_diagnostics"] = primary_diagnostics
            target_payload["fold_uplift_distribution"] = primary_distribution
        else:
            target_payload["decision_diagnostics"] = None
            target_payload["decision_diagnostics_reason"] = (
                "H5 retention diagnostics are reserved for the primary H5 target to avoid mixing target semantics."
            )
        targets[target_name] = target_payload

    if primary_diagnostics is None or primary_distribution is None:  # pragma: no cover
        raise AssertionError("primary target diagnostics were not created")
    verdict = _screening_verdict(
        targets[PRIMARY_TARGET]["oos_metrics"]["total"],
        primary_diagnostics,
        primary_distribution,
    )
    targets[PRIMARY_TARGET]["screening_verdict"] = verdict
    return {
        "experiment_id": EXPERIMENT_ID,
        "result_status": RESULT_STATUS,
        "threshold": CLASSIFICATION_THRESHOLD,
        "targets": targets,
        "screening_verdict": verdict,
    }


def _build_c_sensitivity(target_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for target_name in TARGET_ORDER:
        for c_value in C_SENSITIVITY_VALUES:
            predictions, _, _ = _evaluate_target(
                target_frames[target_name],
                target_name=target_name,
                c_value=c_value,
                collect_coefficients=False,
            )
            metrics = _metric_bundle(predictions)
            rows.append(
                {
                    "target": target_name,
                    "target_role": TARGET_ROLES[target_name],
                    "c": float(c_value),
                    "descriptive_only": True,
                    "selection_or_promotion_allowed": False,
                    **{key: value for key, value in metrics.items() if key != "undefined_metric_reasons"},
                    "roc_auc_reason": metrics["undefined_metric_reasons"].get("roc_auc"),
                    "pr_auc_reason": metrics["undefined_metric_reasons"].get("pr_auc"),
                    "balanced_accuracy_reason": metrics["undefined_metric_reasons"].get(
                        "balanced_accuracy"
                    ),
                }
            )
    return pd.DataFrame(rows)


def _class_counts(frame: pd.DataFrame) -> dict[str, int]:
    counts = frame["target_value"].value_counts().to_dict()
    return {"0": int(counts.get(0, 0)), "1": int(counts.get(1, 0))}


def _build_run_metadata(
    *,
    run_id: str,
    input_records: list[dict[str, Any]],
    target_frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "producer": PRODUCER,
        "run_id": run_id,
        "result_status": RESULT_STATUS,
        "screening_only": True,
        "input_artifacts": input_records,
        "output_contract": {
            "root_cli_argument": "--output-dir",
            "declared_files": list(DECLARED_OUTPUT_FILES),
            "no_external_writes": True,
        },
        "feature_contract": {
            "numeric_features": list(NUMERIC_FEATURES),
            "categorical_features": list(CATEGORICAL_FEATURES),
            "exact_allowlist_enforced": True,
            "fold_local_median_imputation": True,
            "fold_local_standard_scaling": True,
            "categorical_encoding_inside_pipeline": True,
        },
        "target_contract": {
            PRIMARY_TARGET: {
                "role": "primary",
                "eligibility": "signed_ret_o2o_h5 is non-null",
                "recomputation": "signed_ret_o2o_h5 > 0",
                "stored_consistency_assertion": "allow_trade_h5",
                "eligible_rows": int(len(target_frames[PRIMARY_TARGET])),
                "class_counts": _class_counts(target_frames[PRIMARY_TARGET]),
            },
            SECONDARY_TARGET: {
                "role": "secondary",
                "eligibility": "signed_ret_o2o_h1 is non-null",
                "recomputation": "signed_ret_o2o_h1 > 0",
                "eligible_rows": int(len(target_frames[SECONDARY_TARGET])),
                "class_counts": _class_counts(target_frames[SECONDARY_TARGET]),
            },
        },
        "validation_protocol": {
            "mode": "expanding_walk_forward",
            "chronological": True,
            "shuffle": False,
            "random_split": False,
            "minimum_initial_train_events_after_purge": MINIMUM_INITIAL_TRAIN_EVENTS,
            "test_block_events": TEST_BLOCK_EVENTS,
            "final_partial_test_block_allowed": True,
            "purge_d1_sessions": PURGE_D1_SESSIONS,
            "purge_rule": "training event session index + 6 must be less than first test event session index",
            "primary_c": PRIMARY_C,
            "class_weight": "balanced",
            "threshold": CLASSIFICATION_THRESHOLD,
            "threshold_tuning": False,
            "c_sensitivity": list(C_SENSITIVITY_VALUES),
            "c_sensitivity_descriptive_only": True,
        },
        "no_model_artifact": True,
        "no_model_promotion": True,
        "no_strategy_package": True,
        "no_runtime": True,
    }


def _build_quality_report(
    *,
    d1: pd.DataFrame,
    context: pd.DataFrame,
    labels: pd.DataFrame,
    target_frames: dict[str, pd.DataFrame],
    predictions_by_target: dict[str, pd.DataFrame],
    fold_metrics: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "result_status": RESULT_STATUS,
        "row_counts": {
            "d1_ohlc": int(len(d1)),
            "context": int(len(context)),
            "labels": int(len(labels)),
            "primary_eligible": int(len(target_frames[PRIMARY_TARGET])),
            "secondary_eligible": int(len(target_frames[SECONDARY_TARGET])),
            "primary_oos": int(len(predictions_by_target[PRIMARY_TARGET])),
            "secondary_oos": int(len(predictions_by_target[SECONDARY_TARGET])),
        },
        "class_counts": {
            target_name: _class_counts(target_frames[target_name]) for target_name in TARGET_ORDER
        },
        "join_checks": {
            "key_columns": list(KEY_COLUMNS),
            "context_unique": True,
            "labels_unique": True,
            "one_to_one_match": True,
            "chronological": True,
        },
        "leakage_checks": {
            "model_feature_columns": list(MODEL_FEATURES),
            "forbidden_model_features_absent": True,
            "label_like_context_columns_absent": True,
            "future_outcome_context_columns_absent": True,
            "preprocessing_fitted_per_training_fold": True,
            "baseline_prevalence_fitted_per_training_fold": True,
            "six_session_purge_enforced": bool(
                (
                    fold_metrics["max_train_label_completion_session_index"]
                    < fold_metrics["first_test_session_index"]
                ).all()
            ),
        },
        "output_checks": {
            "declared_files": list(DECLARED_OUTPUT_FILES),
            "model_artifact_emitted": False,
            "promotion_artifact_emitted": False,
        },
        "screening_only": True,
    }


def run_logistic_screen(
    *,
    d1_ohlc_path: Path,
    context_path: Path,
    labels_path: Path,
    output_dir: Path,
    run_id: str,
) -> dict[str, Path]:
    input_paths = {
        "d1_ohlc": d1_ohlc_path,
        "context": context_path,
        "labels": labels_path,
    }
    input_records = [
        {
            "artifact_role": role,
            "path": str(path),
            "sha256": _sha256_file(path),
        }
        for role, path in input_paths.items()
    ]

    d1, context, labels, target_frames = load_and_prepare_inputs(
        d1_ohlc_path=d1_ohlc_path,
        context_path=context_path,
        labels_path=labels_path,
    )
    row_counts = {
        "d1_ohlc": len(d1),
        "context": len(context),
        "labels": len(labels),
    }
    for record in input_records:
        record["row_count"] = int(row_counts[record["artifact_role"]])

    predictions_by_target: dict[str, pd.DataFrame] = {}
    fold_metric_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    for target_name in TARGET_ORDER:
        predictions, target_fold_metrics, target_coefficients = _evaluate_target(
            target_frames[target_name],
            target_name=target_name,
            c_value=PRIMARY_C,
            collect_coefficients=True,
        )
        predictions_by_target[target_name] = predictions
        fold_metric_rows.extend(target_fold_metrics)
        coefficient_rows.extend(target_coefficients)

    predictions = pd.concat(
        [predictions_by_target[target_name] for target_name in TARGET_ORDER],
        ignore_index=True,
    )
    fold_metrics = pd.DataFrame(fold_metric_rows)
    coefficients = pd.DataFrame(coefficient_rows)
    c_sensitivity = _build_c_sensitivity(target_frames)
    metrics_payload = _build_metrics_payload(predictions_by_target)
    run_metadata = _build_run_metadata(
        run_id=run_id,
        input_records=input_records,
        target_frames=target_frames,
    )
    quality_report = _build_quality_report(
        d1=d1,
        context=context,
        labels=labels,
        target_frames=target_frames,
        predictions_by_target=predictions_by_target,
        fold_metrics=fold_metrics,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {filename: output_dir / filename for filename in DECLARED_OUTPUT_FILES}
    _write_json(output_paths[OUTPUT_RUN_METADATA], run_metadata)
    _write_csv(output_paths[OUTPUT_PREDICTIONS], predictions)
    _write_json(output_paths[OUTPUT_METRICS], metrics_payload)
    _write_csv(output_paths[OUTPUT_FOLD_METRICS], fold_metrics)
    _write_csv(output_paths[OUTPUT_COEFFICIENTS], coefficients)
    _write_csv(output_paths[OUTPUT_C_SENSITIVITY], c_sensitivity)
    _write_json(output_paths[OUTPUT_QUALITY_REPORT], quality_report)
    return output_paths


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    d1_path, context_path, labels_path, output_dir, run_id = _validate_cli_args(args, parser)
    run_logistic_screen(
        d1_ohlc_path=d1_path,
        context_path=context_path,
        labels_path=labels_path,
        output_dir=output_dir,
        run_id=run_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
