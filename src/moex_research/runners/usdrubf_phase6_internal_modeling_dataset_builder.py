from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import pandas as pd

from moex_research.labels import usdrubf_d1_manual_phase_labels as manual_labels

DATASET_ID: Final[str] = "usdrubf_phase6_internal_modeling_dataset.v1"
FEATURE_SCHEMA_ID: Final[str] = "usdrubf_phase6_internal_factor_batches_v1"
TARGET_SOURCE: Final[str] = "manual_phase_labels_v1"
PHASE_ORDER: Final[tuple[str, ...]] = ("B", "S", "OUT")
UNLABELED_PHASE: Final[str] = "UNLABELED"
REQUIRED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "modeling_dataset.parquet",
    "manifest.json",
    "feature_schema.json",
    "dataset_preview.csv",
    "target_distribution.csv",
)
SAFETY_GATES: Final[dict[str, str]] = {
    "internal_d1_only": "--internal-d1-only",
    "no_external_data": "--no-external-data",
    "no_model_fitting": "--no-model-fitting",
    "no_prediction": "--no-prediction",
    "no_trading": "--no-trading",
    "no_overwrite": "--no-overwrite",
}
REQUIRED_PANEL_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date",
    "instrument_id",
    "open",
    "high",
    "low",
    "close",
)
OPTIONAL_NUMERIC_COLUMNS: Final[tuple[str, ...]] = (
    "volume",
    "value",
    "num_trades",
)
FORBIDDEN_INPUT_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "phase_label",
        "B",
        "S",
        "OUT",
        "target",
        "y",
        "future_return",
        "source_interval_id",
        "phase_remaining_sessions",
        "next_regime_if_current_ends",
        "transition_exit_day",
        "boundary_distance",
        "future_phase",
        "future_volatility",
        "label_annotation",
        "annotator",
        "label_availability_ts",
    }
)
TARGET_COLUMNS: Final[tuple[str, ...]] = (
    "target_phase_label",
    "target_is_labeled",
    "target_source",
    "target_trade_date",
    "target_instrument_id",
)
FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "prior_trade_date",
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
    "lag1_ema_3_19_state",
)


class Phase6DatasetBuilderError(ValueError):
    """Raised when the Phase 6 internal-only dataset builder must fail closed."""


@dataclass(frozen=True)
class Phase6DatasetBuildRequest:
    panel_path: Path
    panel_manifest_path: Path
    label_contract_path: Path
    output_dir: Path
    run_id: str
    internal_d1_only: bool
    no_external_data: bool
    no_model_fitting: bool
    no_prediction: bool
    no_trading: bool
    no_overwrite: bool


@dataclass(frozen=True)
class Phase6DatasetBuildResult:
    output_dir: Path
    artifact_names: tuple[str, ...]
    row_count: int
    labeled_row_count: int


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_phase6_internal_modeling_dataset_builder",
        description="Internal-only Phase 6 modeling dataset builder for USDRUBF D1 factor batches.",
    )
    parser.add_argument("--panel-path", required=True)
    parser.add_argument("--panel-manifest-path", required=True)
    parser.add_argument("--label-contract-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)

    parser.add_argument("--internal-d1-only", action="store_true")
    parser.add_argument("--no-external-data", action="store_true")
    parser.add_argument("--no-model-fitting", action="store_true")
    parser.add_argument("--no-prediction", action="store_true")
    parser.add_argument("--no-trading", action="store_true")
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        required=True,
        help="Required guard. Existing or non-empty output directories are refused.",
    )
    return parser


def request_from_args(args: argparse.Namespace) -> Phase6DatasetBuildRequest:
    return Phase6DatasetBuildRequest(
        panel_path=Path(args.panel_path),
        panel_manifest_path=Path(args.panel_manifest_path),
        label_contract_path=Path(args.label_contract_path),
        output_dir=Path(args.output_dir),
        run_id=str(args.run_id),
        internal_d1_only=bool(args.internal_d1_only),
        no_external_data=bool(args.no_external_data),
        no_model_fitting=bool(args.no_model_fitting),
        no_prediction=bool(args.no_prediction),
        no_trading=bool(args.no_trading),
        no_overwrite=bool(args.no_overwrite),
    )


def build_dataset_from_args(args: argparse.Namespace) -> Phase6DatasetBuildResult:
    return build_modeling_dataset(request_from_args(args))


def build_modeling_dataset(request: Phase6DatasetBuildRequest) -> Phase6DatasetBuildResult:
    _assert_required_safety_gates(request)
    _assert_output_dir_ready_for_new_run(request.output_dir)

    panel = pd.read_parquet(request.panel_path)
    panel_manifest = _read_json(request.panel_manifest_path)
    label_contract = _read_json(request.label_contract_path)

    dataset = build_modeling_dataset_frame(
        panel=panel,
        label_contract=label_contract,
    )
    _assert_safe_output_columns(dataset)

    request.output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = _output_paths(request.output_dir)
    if any(path.exists() for path in output_paths.values()):
        raise Phase6DatasetBuilderError(
            "--no-overwrite is set and one or more output artifacts already exist"
        )

    dataset.to_parquet(output_paths["modeling_dataset.parquet"], index=False)
    _write_json(output_paths["feature_schema.json"], _build_feature_schema())
    _write_dataset_preview(dataset, output_paths["dataset_preview.csv"])
    _write_target_distribution(dataset, output_paths["target_distribution.csv"])
    _write_json(
        output_paths["manifest.json"],
        _build_manifest(
            dataset=dataset,
            request=request,
            panel_manifest=panel_manifest,
            label_contract=label_contract,
            output_paths=output_paths,
        ),
    )

    return Phase6DatasetBuildResult(
        output_dir=request.output_dir,
        artifact_names=REQUIRED_OUTPUT_ARTIFACTS,
        row_count=int(len(dataset.index)),
        labeled_row_count=int(dataset["target_is_labeled"].sum()),
    )


def build_modeling_dataset_frame(
    *,
    panel: pd.DataFrame,
    label_contract: dict[str, Any],
) -> pd.DataFrame:
    _validate_label_contract(label_contract)
    prepared_panel = _prepare_internal_d1_panel(panel)
    diagnostic_panel = _add_past_only_diagnostics(prepared_panel)
    targets = _build_target_frame(diagnostic_panel["trade_date"], diagnostic_panel["instrument_id"])
    dataset = pd.concat([targets, _build_feature_frame(diagnostic_panel)], axis=1)
    return dataset.loc[:, [*TARGET_COLUMNS, *FEATURE_COLUMNS]].copy()


def _assert_required_safety_gates(request: Phase6DatasetBuildRequest) -> None:
    missing = [
        flag_name
        for attribute, flag_name in SAFETY_GATES.items()
        if not bool(getattr(request, attribute, False))
    ]
    if missing:
        raise Phase6DatasetBuilderError(
            "Missing required safety gate(s): " + ", ".join(missing)
        )


def _assert_output_dir_ready_for_new_run(output_dir: Path) -> None:
    if not output_dir.exists():
        return
    if not output_dir.is_dir():
        raise Phase6DatasetBuilderError(
            "--output-dir must be a directory path when it already exists: "
            + output_dir.as_posix()
        )
    existing_entries = sorted(path.name for path in output_dir.iterdir())
    if existing_entries:
        raise Phase6DatasetBuilderError(
            "--output-dir already exists and is non-empty; refusing to reuse it: "
            + output_dir.as_posix()
        )


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise Phase6DatasetBuilderError(f"JSON artifact must be an object: {path.as_posix()}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _output_paths(output_dir: Path) -> dict[str, Path]:
    return {artifact: output_dir / artifact for artifact in REQUIRED_OUTPUT_ARTIFACTS}


def _validate_label_contract(label_contract: dict[str, Any]) -> None:
    allowed_labels = tuple(label_contract.get("allowed_labels", ()))
    if allowed_labels != PHASE_ORDER:
        raise Phase6DatasetBuilderError("manual label contract allowed_labels must be B/S/OUT")
    provenance = label_contract.get("provenance", {})
    if isinstance(provenance, dict) and not provenance.get("manual_hypothesis_label"):
        raise Phase6DatasetBuilderError("manual label contract must identify manual hypothesis labels")


def _prepare_internal_d1_panel(panel: pd.DataFrame) -> pd.DataFrame:
    missing_columns = [column for column in REQUIRED_PANEL_COLUMNS if column not in panel.columns]
    if missing_columns:
        raise Phase6DatasetBuilderError(
            "internal D1 panel missing required columns: " + ", ".join(missing_columns)
        )

    forbidden_columns = _find_forbidden_input_panel_columns(panel)
    if forbidden_columns:
        raise Phase6DatasetBuilderError(
            "input panel must not contain target-like, label, future, or manual phase metadata columns: "
            + ", ".join(forbidden_columns)
        )

    prepared = panel.copy()
    parsed_trade_dates = pd.to_datetime(prepared["trade_date"], errors="coerce")
    if parsed_trade_dates.isna().any():
        raise Phase6DatasetBuilderError("trade_date column contains unparsable values")
    prepared["trade_date"] = parsed_trade_dates.dt.strftime("%Y-%m-%d")

    for column in ("open", "high", "low", "close", *OPTIONAL_NUMERIC_COLUMNS):
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    if prepared.loc[:, ["open", "high", "low", "close"]].isna().any().any():
        raise Phase6DatasetBuilderError("internal D1 panel contains non-numeric OHLC values")
    if (prepared["high"] < prepared["low"]).any():
        raise Phase6DatasetBuilderError("internal D1 panel contains high lower than low")

    prepared = prepared.sort_values(["trade_date", "instrument_id"]).reset_index(drop=True)
    if prepared.duplicated(subset=["trade_date", "instrument_id"]).any():
        raise Phase6DatasetBuilderError(
            "internal D1 panel must contain one row per target trade_date/instrument_id"
        )
    return prepared


def _find_forbidden_input_panel_columns(panel: pd.DataFrame) -> list[str]:
    forbidden = set(FORBIDDEN_INPUT_PANEL_COLUMNS) | set(manual_labels.NON_RUNTIME_FIELDS)
    return sorted(str(column) for column in panel.columns if str(column) in forbidden)


def _add_past_only_diagnostics(panel: pd.DataFrame) -> pd.DataFrame:
    diagnostic = panel.copy()
    previous_close = diagnostic["close"].shift(1)
    diagnostic["_close_return_1d"] = _safe_ratio(
        diagnostic["close"] - previous_close, previous_close
    )
    diagnostic["_intraday_return"] = _safe_ratio(
        diagnostic["close"] - diagnostic["open"], diagnostic["open"]
    )
    diagnostic["_hl_range_pct"] = _safe_ratio(
        diagnostic["high"] - diagnostic["low"], diagnostic["close"]
    )
    diagnostic["_ema_3"] = diagnostic["close"].ewm(span=3, adjust=False).mean()
    diagnostic["_ema_19"] = diagnostic["close"].ewm(span=19, adjust=False).mean()
    diagnostic["_ema_3_19_spread"] = diagnostic["_ema_3"] - diagnostic["_ema_19"]
    diagnostic["_ema_3_19_state"] = diagnostic["_ema_3_19_spread"].map(
        _ema_state_from_spread
    )
    return diagnostic


def _build_target_frame(
    trade_dates: pd.Series,
    instrument_ids: pd.Series,
) -> pd.DataFrame:
    label_rows = manual_labels.materialize_phase_label_dicts(trade_dates.tolist())
    manual_labels.assert_single_primary_label_per_session(label_rows)

    label_map = {
        str(row["session_date"]): str(row["phase_label"])
        for row in label_rows
        if row.get("phase_label") in PHASE_ORDER
    }

    target = pd.DataFrame(
        {
            "target_trade_date": trade_dates.astype(str),
            "target_instrument_id": instrument_ids.astype(str),
        }
    )
    target["target_phase_label"] = target["target_trade_date"].map(label_map)
    target["target_is_labeled"] = target["target_phase_label"].notna()
    target["target_source"] = TARGET_SOURCE
    return target.loc[:, list(TARGET_COLUMNS)]


def _build_feature_frame(diagnostic: pd.DataFrame) -> pd.DataFrame:
    trade_dates = pd.to_datetime(diagnostic["trade_date"])
    features = pd.DataFrame(index=diagnostic.index)
    features["prior_trade_date"] = diagnostic["trade_date"].shift(1)
    features["session_index"] = range(len(diagnostic.index))
    features["days_since_prior_trade_date"] = trade_dates.diff().dt.days

    features["lag1_close_return_1d"] = diagnostic["_close_return_1d"].shift(1)
    features["lag1_intraday_return"] = diagnostic["_intraday_return"].shift(1)
    features["rolling_past_return_mean"] = diagnostic["_close_return_1d"].shift(1).rolling(
        window=5, min_periods=1
    ).mean()
    features["rolling_past_return_std"] = diagnostic["_close_return_1d"].shift(1).rolling(
        window=5, min_periods=2
    ).std()

    features["lag1_hl_range_pct"] = diagnostic["_hl_range_pct"].shift(1)
    features["rolling_past_hl_range_mean"] = diagnostic["_hl_range_pct"].shift(1).rolling(
        window=5, min_periods=1
    ).mean()
    features["rolling_past_hl_range_std"] = diagnostic["_hl_range_pct"].shift(1).rolling(
        window=5, min_periods=2
    ).std()

    for column in OPTIONAL_NUMERIC_COLUMNS:
        if column in diagnostic.columns:
            features[f"lag1_{column}"] = diagnostic[column].shift(1)
        else:
            features[f"lag1_{column}"] = pd.NA
    if "volume" in diagnostic.columns:
        features["rolling_past_volume_mean"] = diagnostic["volume"].shift(1).rolling(
            window=5, min_periods=1
        ).mean()
    else:
        features["rolling_past_volume_mean"] = pd.NA

    features["lag1_ema_3"] = diagnostic["_ema_3"].shift(1)
    features["lag1_ema_19"] = diagnostic["_ema_19"].shift(1)
    features["lag1_ema_3_19_spread"] = diagnostic["_ema_3_19_spread"].shift(1)
    features["lag1_ema_3_19_state"] = diagnostic["_ema_3_19_state"].shift(1)
    return features.loc[:, list(FEATURE_COLUMNS)]


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.mask(denominator == 0)
    return numerator / denominator


def _ema_state_from_spread(value: object) -> str | None:
    if pd.isna(value):
        return None
    numeric = float(value)
    if numeric > 0:
        return "ema3_above_ema19"
    if numeric < 0:
        return "ema3_below_ema19"
    return "ema3_equal_ema19"


def _assert_safe_output_columns(dataset: pd.DataFrame) -> None:
    forbidden = _find_forbidden_input_panel_columns(dataset)
    if forbidden:
        raise Phase6DatasetBuilderError(
            "modeling dataset leaked forbidden feature or manual metadata columns: "
            + ", ".join(forbidden)
        )
    missing = [column for column in (*TARGET_COLUMNS, *FEATURE_COLUMNS) if column not in dataset.columns]
    if missing:
        raise Phase6DatasetBuilderError("modeling dataset missing required columns: " + ", ".join(missing))


def _build_feature_schema() -> dict[str, Any]:
    return {
        "schema_id": FEATURE_SCHEMA_ID,
        "dataset_id": DATASET_ID,
        "target_columns": list(TARGET_COLUMNS),
        "feature_columns": list(FEATURE_COLUMNS),
        "feature_batches": {
            "internal_price_return": {
                "columns": [
                    "lag1_close_return_1d",
                    "lag1_intraday_return",
                    "rolling_past_return_mean",
                    "rolling_past_return_std",
                ],
                "past_only": True,
            },
            "internal_volatility_range": {
                "columns": [
                    "lag1_hl_range_pct",
                    "rolling_past_hl_range_mean",
                    "rolling_past_hl_range_std",
                ],
                "past_only": True,
            },
            "internal_volume_liquidity": {
                "columns": [
                    "lag1_volume",
                    "lag1_value",
                    "lag1_num_trades",
                    "rolling_past_volume_mean",
                ],
                "past_only": True,
            },
            "ema_3_19_baseline_context": {
                "columns": [
                    "lag1_ema_3",
                    "lag1_ema_19",
                    "lag1_ema_3_19_spread",
                    "lag1_ema_3_19_state",
                ],
                "past_only": True,
                "modeling_role": "diagnostic_baseline_context_only",
            },
            "session_index_context": {
                "columns": [
                    "target_trade_date",
                    "prior_trade_date",
                    "session_index",
                    "days_since_prior_trade_date",
                ],
                "phase_boundary_derived": False,
            },
        },
        "forbidden_feature_columns": sorted(FORBIDDEN_INPUT_PANEL_COLUMNS),
        "leakage_guards": {
            "labels_are_targets_only": True,
            "manual_phase_metadata_features_allowed": False,
            "same_day_target_ohlcv_features_allowed": False,
            "external_factors_included": False,
            "model_fitting_performed": False,
            "prediction_performed": False,
            "trading_signal_created": False,
        },
    }


def _write_dataset_preview(dataset: pd.DataFrame, path: Path, row_limit: int = 20) -> None:
    dataset.head(row_limit).to_csv(path, index=False, float_format="%.10g")


def _write_target_distribution(dataset: pd.DataFrame, path: Path) -> None:
    distribution = (
        dataset.assign(
            target_phase_label=dataset["target_phase_label"].fillna(UNLABELED_PHASE)
        )
        .groupby(["target_phase_label", "target_is_labeled"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["target_is_labeled", "target_phase_label"], ascending=[False, True])
    )
    distribution.to_csv(path, index=False)


def _build_manifest(
    *,
    dataset: pd.DataFrame,
    request: Phase6DatasetBuildRequest,
    panel_manifest: dict[str, Any],
    label_contract: dict[str, Any],
    output_paths: dict[str, Path],
) -> dict[str, Any]:
    return {
        "dataset_id": DATASET_ID,
        "run_id": request.run_id,
        "runner_scope": "internal_d1_only_factor_batches_modeling_dataset_builder",
        "input_artifacts": {
            "panel_path": request.panel_path.as_posix(),
            "panel_manifest_path": request.panel_manifest_path.as_posix(),
            "label_contract_path": request.label_contract_path.as_posix(),
        },
        "input_panel_manifest_run_id": panel_manifest.get(
            "run_id", panel_manifest.get("manifest_run_id")
        ),
        "label_contract_id": label_contract.get("contract_id"),
        "target_source": TARGET_SOURCE,
        "row_count": int(len(dataset.index)),
        "labeled_row_count": int(dataset["target_is_labeled"].sum()),
        "unlabeled_row_count": int((~dataset["target_is_labeled"]).sum()),
        "target_distribution": {
            str(label): int(count)
            for label, count in dataset["target_phase_label"]
            .fillna(UNLABELED_PHASE)
            .value_counts()
            .sort_index()
            .items()
        },
        "output_artifacts": list(REQUIRED_OUTPUT_ARTIFACTS),
        "output_paths": {
            name: path.as_posix()
            for name, path in sorted(output_paths.items())
        },
        "feature_schema_id": FEATURE_SCHEMA_ID,
        "target_columns": list(TARGET_COLUMNS),
        "feature_columns": list(FEATURE_COLUMNS),
        "leakage_guard_summary": {
            "labels_are_targets_only": True,
            "label_metadata_features_written": False,
            "same_day_target_ohlcv_feature_columns_written": False,
            "external_factors_included": False,
            "phase_boundary_metadata_features_written": False,
        },
        "side_effect_summary": {
            "network_or_provider_api_calls": False,
            "external_data_ingestion": False,
            "model_fitting": False,
            "prediction": False,
            "trading_or_broker_actions": False,
            "writes": [path.as_posix() for path in output_paths.values()],
        },
    }


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if pd.isna(value):
        return None
    return value


def main() -> None:
    parser = build_argument_parser()
    build_dataset_from_args(parser.parse_args())


if __name__ == "__main__":
    main()
