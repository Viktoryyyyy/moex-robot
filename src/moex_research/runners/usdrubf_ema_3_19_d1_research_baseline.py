from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.moex_features.daily.usdrubf_d1_ohlc_from_5m import materialize_d1_ohlc_from_5m
from src.moex_features.daily.usdrubf_d1_ema_3_19_cross_context import (
    materialize_feature_frame as materialize_cross_context_frame,
)
from src.moex_research.labels.usdrubf_d1_ema_3_19_cross_labels import materialize_label_frame


EXPERIMENT_ID = "usdrubf_ema_3_19_d1_research_baseline_v1"
PRODUCER = "src.moex_research.runners.usdrubf_ema_3_19_d1_research_baseline"
D_CLOSE_KNOWN_BY_WHEN = "D close after finalized D1 bar"
EARLIEST_LABEL_OUTCOME_ANCHOR = "D+1 open"
YEAR_SPLIT_MIN_ROWS = 5

OUTPUT_RUN_METADATA = "run_metadata.json"
OUTPUT_D1_OHLC = "usdrubf_d1_ohlc.csv"
OUTPUT_CROSS_CONTEXT = "usdrubf_d1_ema_3_19_cross_context.csv"
OUTPUT_CROSS_LABELS = "usdrubf_d1_ema_3_19_cross_labels.csv"
OUTPUT_RAW_BASELINE_SUMMARY = "usdrubf_ema_3_19_raw_baseline_summary.csv"
OUTPUT_QUALITY_REPORT = "usdrubf_ema_3_19_quality_report.json"

DECLARED_OUTPUT_FILES = (
    OUTPUT_RUN_METADATA,
    OUTPUT_D1_OHLC,
    OUTPUT_CROSS_CONTEXT,
    OUTPUT_CROSS_LABELS,
    OUTPUT_RAW_BASELINE_SUMMARY,
    OUTPUT_QUALITY_REPORT,
)

FEATURE_CONTEXT_OUTPUTS = (
    OUTPUT_D1_OHLC,
    OUTPUT_CROSS_CONTEXT,
)
LABEL_OUTPUTS = (OUTPUT_CROSS_LABELS,)

LABEL_LIKE_PREFIXES = (
    "signed_ret_",
    "allow_trade_",
    "max_adverse_",
    "max_favorable_",
)

FUTURE_CONTEXT_MARKERS = (
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize the USDRUBF D1 EMA(3/19) research baseline artifact pack.",
    )
    parser.add_argument("--source-dataset-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    return parser


def _validate_cli_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> tuple[Path, Path, str]:
    source_dataset_path = Path(args.source_dataset_path)
    output_dir = Path(args.output_dir)
    run_id = str(args.run_id).strip()

    if not str(args.source_dataset_path).strip():
        parser.error("--source-dataset-path must be non-empty")
    if not source_dataset_path.exists():
        parser.error("--source-dataset-path must reference an existing file")
    if not source_dataset_path.is_file():
        parser.error("--source-dataset-path must reference a file")
    if not str(args.output_dir).strip():
        parser.error("--output-dir must be non-empty")
    if output_dir.exists() and not output_dir.is_dir():
        parser.error("--output-dir must reference a directory")
    if not run_id:
        parser.error("--run-id must be non-empty")

    return source_dataset_path, output_dir, run_id


def _declared_output_paths(output_dir: Path) -> dict[str, Path]:
    root = output_dir.resolve()
    paths = {filename: root / filename for filename in DECLARED_OUTPUT_FILES}
    for path in paths.values():
        resolved = path.resolve()
        if resolved.parent != root:
            raise ValueError("declared output path escaped output directory")
    return paths


def _json_safe(value: Any) -> Any:
    if value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and pd.isna(value):
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
    frame.to_csv(path, index=False)


def _add_known_by_when(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "known_by_when" not in out.columns:
        out["known_by_when"] = D_CLOSE_KNOWN_BY_WHEN
    return out


def _label_like_columns(frame: pd.DataFrame) -> list[str]:
    return sorted(
        column
        for column in frame.columns
        if column.startswith(LABEL_LIKE_PREFIXES)
    )


def _future_context_columns(frame: pd.DataFrame) -> list[str]:
    columns: list[str] = []
    for column in frame.columns:
        lowered = str(column).lower()
        if any(marker in lowered for marker in FUTURE_CONTEXT_MARKERS):
            columns.append(str(column))
    return sorted(columns)


def _assert_no_feature_label_leakage(*, d1_ohlc: pd.DataFrame, cross_context: pd.DataFrame) -> dict[str, Any]:
    d1_label_columns = _label_like_columns(d1_ohlc)
    context_label_columns = _label_like_columns(cross_context)
    d1_future_columns = _future_context_columns(d1_ohlc)
    context_future_columns = _future_context_columns(cross_context)

    if d1_label_columns or context_label_columns:
        raise ValueError("feature/context artifacts contain label-like columns")
    if d1_future_columns or context_future_columns:
        raise ValueError("feature/context artifacts contain future outcome columns")

    return {
        "feature_context_label_like_columns": {
            OUTPUT_D1_OHLC: d1_label_columns,
            OUTPUT_CROSS_CONTEXT: context_label_columns,
        },
        "feature_context_future_outcome_columns": {
            OUTPUT_D1_OHLC: d1_future_columns,
            OUTPUT_CROSS_CONTEXT: context_future_columns,
        },
        "no_d_plus_1_values_in_feature_context_rows": True,
        "labels_kept_research_only": True,
    }


def _numeric_summary(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    prefix = column
    if series.empty:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_mean": None,
            f"{prefix}_std": None,
            f"{prefix}_min": None,
            f"{prefix}_p25": None,
            f"{prefix}_median": None,
            f"{prefix}_p75": None,
            f"{prefix}_max": None,
        }

    return {
        f"{prefix}_count": int(series.count()),
        f"{prefix}_mean": float(series.mean()),
        f"{prefix}_std": None if len(series) < 2 else float(series.std()),
        f"{prefix}_min": float(series.min()),
        f"{prefix}_p25": float(series.quantile(0.25)),
        f"{prefix}_median": float(series.median()),
        f"{prefix}_p75": float(series.quantile(0.75)),
        f"{prefix}_max": float(series.max()),
    }


def _summary_row(frame: pd.DataFrame, *, group_type: str, group_value: str) -> dict[str, Any]:
    row: dict[str, Any] = {
        "experiment_id": EXPERIMENT_ID,
        "group_type": group_type,
        "group_value": group_value,
        "row_count": int(len(frame)),
        "event_count": int(len(frame)),
        "cross_up_count": int((frame["cross_dir"] == "cross_up").sum()) if "cross_dir" in frame else 0,
        "cross_down_count": int((frame["cross_dir"] == "cross_down").sum()) if "cross_dir" in frame else 0,
    }
    for column in ("signed_ret_o2o_h1", "signed_ret_o2o_h2", "signed_ret_o2o_h5", "max_adverse_excursion_h5"):
        row.update(_numeric_summary(frame, column))

    allow_trade = pd.to_numeric(frame["allow_trade_h5"], errors="coerce").dropna()
    row["allow_trade_h5_count"] = int(allow_trade.count())
    row["allow_trade_h5_rate"] = None if allow_trade.empty else float(allow_trade.mean())
    return row


def build_raw_baseline_summary(labels: pd.DataFrame) -> pd.DataFrame:
    if labels.empty:
        rows = [_summary_row(labels, group_type="total", group_value="all")]
        return pd.DataFrame(rows)

    work = labels.copy()
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    rows = [_summary_row(work, group_type="total", group_value="all")]

    for cross_dir, group in work.groupby("cross_dir", sort=True):
        rows.append(_summary_row(group, group_type="cross_dir", group_value=str(cross_dir)))

    work["year"] = work["end"].dt.year
    for year, group in work.groupby("year", sort=True):
        if len(group) >= YEAR_SPLIT_MIN_ROWS:
            rows.append(_summary_row(group, group_type="year", group_value=str(int(year))))

    return pd.DataFrame(rows)


def _build_quality_report(
    *,
    run_id: str,
    source_dataset_path: Path,
    output_dir: Path,
    d1_ohlc: pd.DataFrame,
    cross_context: pd.DataFrame,
    labels: pd.DataFrame,
    summary: pd.DataFrame,
    leakage_checks: dict[str, Any],
) -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "source_dataset_path": str(source_dataset_path),
        "output_dir": str(output_dir),
        "declared_outputs": list(DECLARED_OUTPUT_FILES),
        "artifact_groups": {
            "feature_context_artifacts": [
                {
                    "filename": OUTPUT_D1_OHLC,
                    "artifact_role": "feature_context",
                    "row_count": int(len(d1_ohlc)),
                    "known_by_when": D_CLOSE_KNOWN_BY_WHEN,
                    "contains_label_columns": False,
                    "contains_future_outcome_columns": False,
                },
                {
                    "filename": OUTPUT_CROSS_CONTEXT,
                    "artifact_role": "feature_context",
                    "row_count": int(len(cross_context)),
                    "event_contract": "one row per finalized D1 EMA(3/19) crossover",
                    "known_by_when": D_CLOSE_KNOWN_BY_WHEN,
                    "contains_label_columns": False,
                    "contains_future_outcome_columns": False,
                },
            ],
            "label_artifacts": [
                {
                    "filename": OUTPUT_CROSS_LABELS,
                    "artifact_role": "research_label",
                    "row_count": int(len(labels)),
                    "research_only": True,
                    "earliest_outcome_anchor": EARLIEST_LABEL_OUTCOME_ANCHOR,
                    "labels_must_not_enter_feature_rows": True,
                }
            ],
            "summary_artifacts": [
                {
                    "filename": OUTPUT_RAW_BASELINE_SUMMARY,
                    "artifact_role": "raw_research_summary",
                    "row_count": int(len(summary)),
                    "model_training": False,
                    "threshold_artifact": False,
                }
            ],
        },
        "row_counts": {
            "d1_ohlc": int(len(d1_ohlc)),
            "cross_context": int(len(cross_context)),
            "cross_labels": int(len(labels)),
            "raw_baseline_summary": int(len(summary)),
        },
        "time_semantics": {
            "event_day_symbol": "D",
            "d_close_known_by_when": D_CLOSE_KNOWN_BY_WHEN,
            "earliest_label_outcome_anchor": EARLIEST_LABEL_OUTCOME_ANCHOR,
            "feature_context_uses_d_plus_1_values": False,
        },
        "leakage_checks": leakage_checks,
    }


def _build_run_metadata(
    *,
    run_id: str,
    source_dataset_path: Path,
    output_dir: Path,
    run_started_at_utc: str,
    run_completed_at_utc: str,
    d1_ohlc: pd.DataFrame,
    cross_context: pd.DataFrame,
    labels: pd.DataFrame,
    summary: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "producer": PRODUCER,
        "run_id": run_id,
        "run_started_at_utc": run_started_at_utc,
        "run_completed_at_utc": run_completed_at_utc,
        "source_dataset_path": str(source_dataset_path),
        "output_dir": str(output_dir),
        "input_contracts": [
            "contracts/datasets/research_usdrubf_5m_full_history.json",
            "contracts/features/usdrubf_d1_ohlc_from_5m.json",
            "contracts/features/usdrubf_d1_ema_3_19_cross_context.json",
            "contracts/labels/usdrubf_d1_ema_3_19_cross_labels.json",
        ],
        "output_artifacts": list(DECLARED_OUTPUT_FILES),
        "row_counts": {
            OUTPUT_D1_OHLC: int(len(d1_ohlc)),
            OUTPUT_CROSS_CONTEXT: int(len(cross_context)),
            OUTPUT_CROSS_LABELS: int(len(labels)),
            OUTPUT_RAW_BASELINE_SUMMARY: int(len(summary)),
        },
        "no_runtime_consumption": True,
        "no_broker_execution": True,
        "model_training": False,
        "threshold_artifact_created": False,
    }


def run_research_baseline(*, source_dataset_path: Path, output_dir: Path, run_id: str) -> dict[str, Path]:
    run_started_at_utc = _utc_now_iso()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = _declared_output_paths(output_dir)

    d1_ohlc = materialize_d1_ohlc_from_5m(dataset_artifact_path=source_dataset_path)
    cross_context = materialize_cross_context_frame(d1_ohlc_frame=d1_ohlc)
    labels = materialize_label_frame(event_frame=cross_context, d1_ohlc_frame=d1_ohlc)

    d1_ohlc = _add_known_by_when(d1_ohlc)
    cross_context = _add_known_by_when(cross_context)

    leakage_checks = _assert_no_feature_label_leakage(d1_ohlc=d1_ohlc, cross_context=cross_context)
    summary = build_raw_baseline_summary(labels)
    run_completed_at_utc = _utc_now_iso()

    quality_report = _build_quality_report(
        run_id=run_id,
        source_dataset_path=source_dataset_path,
        output_dir=output_dir,
        d1_ohlc=d1_ohlc,
        cross_context=cross_context,
        labels=labels,
        summary=summary,
        leakage_checks=leakage_checks,
    )
    run_metadata = _build_run_metadata(
        run_id=run_id,
        source_dataset_path=source_dataset_path,
        output_dir=output_dir,
        run_started_at_utc=run_started_at_utc,
        run_completed_at_utc=run_completed_at_utc,
        d1_ohlc=d1_ohlc,
        cross_context=cross_context,
        labels=labels,
        summary=summary,
    )

    _write_json(output_paths[OUTPUT_RUN_METADATA], run_metadata)
    _write_csv(output_paths[OUTPUT_D1_OHLC], d1_ohlc)
    _write_csv(output_paths[OUTPUT_CROSS_CONTEXT], cross_context)
    _write_csv(output_paths[OUTPUT_CROSS_LABELS], labels)
    _write_csv(output_paths[OUTPUT_RAW_BASELINE_SUMMARY], summary)
    _write_json(output_paths[OUTPUT_QUALITY_REPORT], quality_report)

    return output_paths


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    source_dataset_path, output_dir, run_id = _validate_cli_args(args, parser)
    run_research_baseline(
        source_dataset_path=source_dataset_path,
        output_dir=output_dir,
        run_id=run_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
