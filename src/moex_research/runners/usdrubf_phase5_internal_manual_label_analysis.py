from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Iterable

import pandas as pd

from moex_research.labels import usdrubf_d1_manual_phase_labels as manual_labels

PHASE_ORDER: Final[tuple[str, ...]] = ("B", "S", "OUT")
UNLABELED_PHASE: Final[str] = "UNLABELED"
REQUIRED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "manifest.json",
    "analysis_report.md",
    "phase_summary.csv",
    "transition_counts.csv",
    "boundary_window_summary.csv",
    "joined_panel_preview.csv",
)
SAFETY_GATES: Final[dict[str, str]] = {
    "internal_d1_only": "--internal-d1-only",
    "no_external_data": "--no-external-data",
    "no_model_fitting": "--no-model-fitting",
    "no_prediction": "--no-prediction",
    "no_trading": "--no-trading",
}
REQUIRED_PANEL_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date",
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
DESCRIPTIVE_NUMERIC_COLUMNS: Final[tuple[str, ...]] = (
    "open",
    "high",
    "low",
    "close",
    "close_return_1d",
    "intraday_return",
    "hl_range_pct",
    "ema_3",
    "ema_19",
    "ema_3_19_spread",
    "volume",
    "value",
    "num_trades",
)


class Phase5RunnerError(ValueError):
    """Raised when the Phase 5 internal-only runner must fail closed."""


@dataclass(frozen=True)
class Phase5RunResult:
    output_dir: Path
    artifact_names: tuple[str, ...]
    row_count: int
    labeled_row_count: int


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_phase5_internal_manual_label_analysis",
        description="Internal-only Phase 5 descriptive analysis runner for USDRUBF D1 manual labels.",
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
    return parser


def run_analysis_from_args(args: argparse.Namespace) -> Phase5RunResult:
    _assert_required_safety_gates(args)

    panel_path = Path(args.panel_path)
    panel_manifest_path = Path(args.panel_manifest_path)
    label_contract_path = Path(args.label_contract_path)
    output_dir = Path(args.output_dir)

    panel = pd.read_parquet(panel_path)
    panel_manifest = _read_json(panel_manifest_path)
    label_contract = _read_json(label_contract_path)

    return run_analysis(
        panel=panel,
        panel_manifest=panel_manifest,
        label_contract=label_contract,
        panel_path=panel_path,
        panel_manifest_path=panel_manifest_path,
        label_contract_path=label_contract_path,
        output_dir=output_dir,
        run_id=str(args.run_id),
    )


def run_analysis(
    *,
    panel: pd.DataFrame,
    panel_manifest: dict[str, Any],
    label_contract: dict[str, Any],
    panel_path: Path,
    panel_manifest_path: Path,
    label_contract_path: Path,
    output_dir: Path,
    run_id: str,
) -> Phase5RunResult:
    _validate_label_contract(label_contract)
    prepared_panel = _prepare_internal_d1_panel(panel)
    joined_panel = _join_manual_labels(prepared_panel)
    joined_panel = _add_descriptive_context(joined_panel)

    phase_summary = _build_phase_summary(joined_panel)
    transition_counts = _build_transition_counts(joined_panel)
    boundary_summary = _build_boundary_window_summary(joined_panel)
    preview = _build_joined_panel_preview(joined_panel)

    output_dir.mkdir(parents=True, exist_ok=True)

    phase_summary.to_csv(output_dir / "phase_summary.csv", index=False, float_format="%.10g")
    transition_counts.to_csv(
        output_dir / "transition_counts.csv", index=False, float_format="%.10g"
    )
    boundary_summary.to_csv(
        output_dir / "boundary_window_summary.csv", index=False, float_format="%.10g"
    )
    preview.to_csv(output_dir / "joined_panel_preview.csv", index=False, float_format="%.10g")

    report = _build_analysis_report(
        joined_panel=joined_panel,
        phase_summary=phase_summary,
        transition_counts=transition_counts,
        boundary_summary=boundary_summary,
        panel_manifest=panel_manifest,
        label_contract=label_contract,
        run_id=run_id,
    )
    (output_dir / "analysis_report.md").write_text(report, encoding="utf-8")

    manifest = _build_output_manifest(
        joined_panel=joined_panel,
        panel_manifest=panel_manifest,
        label_contract=label_contract,
        panel_path=panel_path,
        panel_manifest_path=panel_manifest_path,
        label_contract_path=label_contract_path,
        run_id=run_id,
    )
    _write_json(output_dir / "manifest.json", manifest)

    return Phase5RunResult(
        output_dir=output_dir,
        artifact_names=REQUIRED_OUTPUT_ARTIFACTS,
        row_count=int(len(joined_panel.index)),
        labeled_row_count=int(joined_panel["phase_label"].notna().sum()),
    )


def _assert_required_safety_gates(args: argparse.Namespace) -> None:
    missing = [
        flag_name
        for attribute, flag_name in SAFETY_GATES.items()
        if not bool(getattr(args, attribute, False))
    ]
    if missing:
        raise Phase5RunnerError(
            "Missing required safety gate(s): " + ", ".join(missing)
        )


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise Phase5RunnerError(f"JSON artifact must be an object: {path.as_posix()}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _validate_label_contract(label_contract: dict[str, Any]) -> None:
    allowed_labels = tuple(label_contract.get("allowed_labels", ()))
    if allowed_labels != PHASE_ORDER:
        raise Phase5RunnerError("manual label contract allowed_labels must be B/S/OUT")
    provenance = label_contract.get("provenance", {})
    if isinstance(provenance, dict) and not provenance.get("manual_hypothesis_label"):
        raise Phase5RunnerError("manual label contract must identify manual hypothesis labels")


def _prepare_internal_d1_panel(panel: pd.DataFrame) -> pd.DataFrame:
    missing_columns = [column for column in REQUIRED_PANEL_COLUMNS if column not in panel.columns]
    if missing_columns:
        raise Phase5RunnerError(
            "internal D1 panel missing required columns: " + ", ".join(missing_columns)
        )

    leaked_label_columns = sorted(set(panel.columns).intersection(manual_labels.NON_RUNTIME_FIELDS))
    if leaked_label_columns:
        raise Phase5RunnerError(
            "input panel must not already contain manual label or target columns: "
            + ", ".join(leaked_label_columns)
        )

    prepared = panel.copy()
    parsed_trade_dates = pd.to_datetime(prepared["trade_date"], errors="coerce")
    if parsed_trade_dates.isna().any():
        raise Phase5RunnerError("trade_date column contains unparsable values")
    prepared["trade_date"] = parsed_trade_dates.dt.strftime("%Y-%m-%d")

    for column in ("open", "high", "low", "close", *OPTIONAL_NUMERIC_COLUMNS):
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    if prepared.loc[:, ["open", "high", "low", "close"]].isna().any().any():
        raise Phase5RunnerError("internal D1 panel contains non-numeric OHLC values")

    prepared = prepared.sort_values("trade_date").reset_index(drop=True)
    if prepared["trade_date"].duplicated().any():
        raise Phase5RunnerError("internal D1 panel must contain one row per trade_date")
    return prepared


def _join_manual_labels(panel: pd.DataFrame) -> pd.DataFrame:
    label_rows = manual_labels.materialize_phase_label_dicts(panel["trade_date"].tolist())
    manual_labels.assert_single_primary_label_per_session(label_rows)

    label_frame = pd.DataFrame(label_rows)
    if label_frame.empty:
        label_frame = pd.DataFrame(
            columns=[
                "session_date",
                "phase_label",
                "phase_label_meaning",
                "source_interval_id",
                "interval_start_date",
                "interval_end_date",
                "transition_exit_day",
                "phase_remaining_sessions",
                "current_regime_ends_within_1d",
                "current_regime_ends_within_3d",
                "current_regime_ends_within_5d",
                "next_regime_if_current_ends",
            ]
        )

    label_frame = label_frame.rename(columns={"session_date": "trade_date"})
    joined = panel.merge(label_frame, on="trade_date", how="left", validate="one_to_one")
    joined["phase_label_analysis"] = joined["phase_label"].fillna(UNLABELED_PHASE)
    return joined.sort_values("trade_date").reset_index(drop=True)


def _add_descriptive_context(joined: pd.DataFrame) -> pd.DataFrame:
    enriched = joined.copy()
    previous_close = enriched["close"].shift(1)
    enriched["close_return_1d"] = _safe_ratio(enriched["close"] - previous_close, previous_close)
    enriched["intraday_return"] = _safe_ratio(enriched["close"] - enriched["open"], enriched["open"])
    enriched["hl_range_pct"] = _safe_ratio(enriched["high"] - enriched["low"], enriched["close"])

    enriched["ema_3"] = enriched["close"].ewm(span=3, adjust=False).mean()
    enriched["ema_19"] = enriched["close"].ewm(span=19, adjust=False).mean()
    enriched["ema_3_19_spread"] = enriched["ema_3"] - enriched["ema_19"]
    enriched["ema_3_19_state"] = enriched["ema_3_19_spread"].map(_ema_state_from_spread)
    return enriched


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.mask(denominator == 0)
    return numerator / denominator


def _ema_state_from_spread(value: object) -> str | None:
    if pd.isna(value):
        return None
    numeric = float(value)
    if numeric > 0:
        return "B_context"
    if numeric < 0:
        return "S_context"
    return "OUT_context"


def _build_phase_summary(joined: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for phase in (*PHASE_ORDER, UNLABELED_PHASE):
        frame = joined.loc[joined["phase_label_analysis"] == phase]
        if frame.empty:
            continue

        row: dict[str, object] = {
            "phase": phase,
            "row_count": int(len(frame.index)),
            "min_trade_date": str(frame["trade_date"].min()),
            "max_trade_date": str(frame["trade_date"].max()),
        }
        for column in DESCRIPTIVE_NUMERIC_COLUMNS:
            if column not in frame.columns:
                continue
            row[f"{column}_mean"] = _rounded_mean(frame[column])
            row[f"{column}_std"] = _rounded_std(frame[column])
            row[f"{column}_min"] = _rounded_min(frame[column])
            row[f"{column}_max"] = _rounded_max(frame[column])
        rows.append(row)
    return pd.DataFrame(rows)


def _build_transition_counts(joined: pd.DataFrame) -> pd.DataFrame:
    counts = {(from_phase, to_phase): 0 for from_phase in PHASE_ORDER for to_phase in PHASE_ORDER}
    labeled = joined.dropna(subset=["phase_label"]).sort_values("trade_date").reset_index(drop=True)

    previous_phase: str | None = None
    for phase in labeled["phase_label"].astype(str):
        if previous_phase is not None and phase != previous_phase:
            counts[(previous_phase, phase)] = counts.get((previous_phase, phase), 0) + 1
        previous_phase = phase

    return pd.DataFrame(
        [
            {
                "from_phase": from_phase,
                "to_phase": to_phase,
                "transition_count": counts[(from_phase, to_phase)],
            }
            for from_phase in PHASE_ORDER
            for to_phase in PHASE_ORDER
        ]
    )


def _build_boundary_window_summary(joined: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    labeled = joined.dropna(subset=["phase_label"]).copy()
    if labeled.empty:
        return pd.DataFrame(columns=_boundary_summary_columns())

    labeled["previous_phase_label"] = labeled["phase_label"].shift(1)
    boundaries = labeled.loc[
        labeled["previous_phase_label"].notna()
        & (labeled["previous_phase_label"] != labeled["phase_label"])
    ]

    records: list[dict[str, object]] = []
    for boundary_number, boundary in enumerate(boundaries.itertuples(index=True), start=1):
        boundary_position = int(boundary.Index)
        for offset in range(-window, window + 1):
            position = boundary_position + offset
            if position < 0 or position >= len(joined.index):
                continue
            row = joined.iloc[position]
            records.append(
                {
                    "boundary_id": boundary_number,
                    "boundary_date": boundary.trade_date,
                    "previous_phase": boundary.previous_phase_label,
                    "new_phase": boundary.phase_label,
                    "offset": offset,
                    "trade_date": row["trade_date"],
                    "row_phase": row.get("phase_label"),
                    "close_return_1d": row.get("close_return_1d"),
                    "intraday_return": row.get("intraday_return"),
                    "hl_range_pct": row.get("hl_range_pct"),
                    "volume": row.get("volume") if "volume" in joined.columns else None,
                }
            )

    if not records:
        return pd.DataFrame(columns=_boundary_summary_columns())

    detail = pd.DataFrame(records)
    summary = (
        detail.groupby("offset", dropna=False)
        .agg(
            boundary_count=("boundary_id", "nunique"),
            row_count=("trade_date", "count"),
            mean_close_return_1d=("close_return_1d", "mean"),
            mean_intraday_return=("intraday_return", "mean"),
            mean_hl_range_pct=("hl_range_pct", "mean"),
            mean_volume=("volume", "mean"),
        )
        .reset_index()
        .sort_values("offset")
    )
    return summary.loc[:, _boundary_summary_columns()]


def _boundary_summary_columns() -> list[str]:
    return [
        "offset",
        "boundary_count",
        "row_count",
        "mean_close_return_1d",
        "mean_intraday_return",
        "mean_hl_range_pct",
        "mean_volume",
    ]


def _build_joined_panel_preview(joined: pd.DataFrame, row_limit: int = 20) -> pd.DataFrame:
    preferred_columns = [
        "trade_date",
        "instrument_id",
        "secid",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_return_1d",
        "intraday_return",
        "hl_range_pct",
        "ema_3",
        "ema_19",
        "ema_3_19_spread",
        "ema_3_19_state",
        "phase_label",
        "phase_label_meaning",
        "source_interval_id",
        "transition_exit_day",
        "phase_remaining_sessions",
        "next_regime_if_current_ends",
    ]
    columns = [column for column in preferred_columns if column in joined.columns]
    return joined.loc[:, columns].head(row_limit).copy()


def _build_analysis_report(
    *,
    joined_panel: pd.DataFrame,
    phase_summary: pd.DataFrame,
    transition_counts: pd.DataFrame,
    boundary_summary: pd.DataFrame,
    panel_manifest: dict[str, Any],
    label_contract: dict[str, Any],
    run_id: str,
) -> str:
    labeled_row_count = int(joined_panel["phase_label"].notna().sum())
    unlabeled_row_count = int(joined_panel["phase_label"].isna().sum())
    date_min = str(joined_panel["trade_date"].min()) if not joined_panel.empty else "none"
    date_max = str(joined_panel["trade_date"].max()) if not joined_panel.empty else "none"

    lines = [
        "# Phase 5 internal manual label analysis report",
        "",
        f"Run ID: `{run_id}`",
        "",
        "Manual labels are manual research labels and are not EMA-derived.",
        "EMA 3/19 is baseline/context only, not label source.",
        "EMA 3/19 baseline context is computed only from the internal D1 close series.",
        "No external data ingestion, no model fitting, no prediction, and no trading or broker action is performed.",
        "",
        "## Input coverage",
        "",
        f"- Panel rows: {len(joined_panel.index)}",
        f"- Labeled rows: {labeled_row_count}",
        f"- Unlabeled rows: {unlabeled_row_count}",
        f"- Panel date coverage: {date_min} .. {date_max}",
        f"- Panel manifest run_id: `{panel_manifest.get('run_id', panel_manifest.get('manifest_run_id', 'not_declared'))}`",
        f"- Label contract: `{label_contract.get('contract_id', 'not_declared')}`",
        "",
        "## Row counts and date coverage by phase",
        "",
        _markdown_table(phase_summary.loc[:, ["phase", "row_count", "min_trade_date", "max_trade_date"]]),
        "",
        "## Transition counts B/S/OUT",
        "",
        _markdown_table(transition_counts),
        "",
        "## Boundary window summary",
        "",
        _markdown_table(boundary_summary),
        "",
        "## Output artifacts",
        "",
    ]

    lines.extend(f"- `{artifact}`" for artifact in REQUIRED_OUTPUT_ARTIFACTS)
    lines.extend(
        [
            "",
            "Optional `joined_panel.parquet` is not produced by default.",
            "",
        ]
    )
    return "\n".join(lines)


def _build_output_manifest(
    *,
    joined_panel: pd.DataFrame,
    panel_manifest: dict[str, Any],
    label_contract: dict[str, Any],
    panel_path: Path,
    panel_manifest_path: Path,
    label_contract_path: Path,
    run_id: str,
) -> dict[str, Any]:
    return {
        "schema_version": "usdrubf_phase5_internal_manual_label_analysis_runner.v1",
        "run_id": run_id,
        "runner_scope": "internal_d1_panel_manual_label_descriptive_analysis_only",
        "input_artifacts": {
            "panel_path": panel_path.as_posix(),
            "panel_manifest_path": panel_manifest_path.as_posix(),
            "label_contract_path": label_contract_path.as_posix(),
        },
        "input_panel_manifest_run_id": panel_manifest.get(
            "run_id", panel_manifest.get("manifest_run_id")
        ),
        "label_contract_id": label_contract.get("contract_id"),
        "row_count": int(len(joined_panel.index)),
        "labeled_row_count": int(joined_panel["phase_label"].notna().sum()),
        "unlabeled_row_count": int(joined_panel["phase_label"].isna().sum()),
        "phase_counts": {
            phase: int(count)
            for phase, count in joined_panel["phase_label_analysis"].value_counts()
            .sort_index()
            .items()
        },
        "output_artifacts": list(REQUIRED_OUTPUT_ARTIFACTS),
        "optional_joined_panel_parquet_written": False,
        "manual_label_statement": "manual labels are the label source and are not EMA-derived",
        "ema_3_19_statement": "EMA 3/19 is baseline/context only, not label source",
        "side_effect_summary": {
            "external_data_ingestion": False,
            "network_or_provider_api_calls": False,
            "model_fitting": False,
            "prediction": False,
            "trading_or_broker_actions": False,
        },
    }


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"

    columns = list(frame.columns)
    rows = [columns]
    rows.append(["---" for _ in columns])
    for _, row in frame.iterrows():
        rows.append([_format_markdown_cell(row[column]) for column in columns])
    return "\n".join("| " + " | ".join(values) + " |" for values in rows)


def _format_markdown_cell(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _rounded_mean(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return None if numeric.empty else round(float(numeric.mean()), 10)


def _rounded_std(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return None if len(numeric.index) <= 1 else round(float(numeric.std()), 10)


def _rounded_min(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return None if numeric.empty else round(float(numeric.min()), 10)


def _rounded_max(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return None if numeric.empty else round(float(numeric.max()), 10)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]

    missing = False
    if not isinstance(value, (str, bytes)):
        try:
            missing_check = pd.isna(value)
            missing = bool(missing_check) if isinstance(missing_check, bool) else False
        except (TypeError, ValueError):
            missing = False
    if missing:
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return value
    return value


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    run_analysis_from_args(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
