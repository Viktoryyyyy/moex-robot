from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
import json
from pathlib import Path
import tempfile
from typing import Final

import pandas as pd

PANEL_ID: Final[str] = "usdrubf_phase2_d1_panel.v1"
PANEL_SCHEMA_VERSION: Final[str] = "usdrubf_phase2_d1_panel.v1"
DEFAULT_INSTRUMENT_ID: Final[str] = "forts.usdrubf"
DEFAULT_SECID: Final[str] = "USDRUBF"
RAW_5M_RELATIVE_ROOT: Final[tuple[str, ...]] = ("forts", "raw_5m", "tradestats")
OUTPUT_RELATIVE_ROOT: Final[tuple[str, ...]] = (
    "research",
    "ema_3_19_ai",
    PANEL_ID,
)
RAW_PART_FILENAME: Final[str] = "part.parquet"
OUTPUT_FILENAME: Final[str] = "part.parquet"
MANIFEST_FILENAME: Final[str] = "manifest.json"

_PRICE_COLUMNS: Final[tuple[str, ...]] = ("open", "high", "low", "close")
_LABEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "phase_label",
        "B",
        "S",
        "OUT",
        "target",
        "y",
        "future_return",
    }
)
_OPTIONAL_SUM_COLUMNS: Final[tuple[str, ...]] = ("volume", "value", "num_trades")


class D1PanelBuilderError(ValueError):
    """Raised when the controlled D1 panel builder must fail closed."""


@dataclass(frozen=True)
class D1PanelBuildRequest:
    data_root: Path
    instrument_id: str
    secid: str
    start_date: date
    end_date: date
    run_id: str
    no_overwrite: bool
    input_root: Path | None = None
    output_path: Path | None = None
    manifest_path: Path | None = None


@dataclass(frozen=True)
class D1PanelBuildResult:
    output_path: Path
    manifest_path: Path
    row_count: int
    input_partitions: tuple[Path, ...]


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_data.futures.usdrubf_phase2_d1_panel_builder",
        description="Controlled builder for usdrubf_phase2_d1_panel.v1.",
    )
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        required=True,
        help="Required guard. Existing output parquet or manifest paths are refused.",
    )
    parser.add_argument("--input-root", default=None)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--manifest-path", default=None)
    return parser


def request_from_args(args: argparse.Namespace) -> D1PanelBuildRequest:
    return D1PanelBuildRequest(
        data_root=Path(args.data_root),
        instrument_id=_require_safe_path_token(args.instrument_id, "instrument_id"),
        secid=_require_safe_path_token(args.secid, "secid"),
        start_date=_parse_iso_date(args.start_date, "start_date"),
        end_date=_parse_iso_date(args.end_date, "end_date"),
        run_id=_require_safe_path_token(args.run_id, "run_id"),
        no_overwrite=bool(args.no_overwrite),
        input_root=Path(args.input_root) if args.input_root else None,
        output_path=Path(args.output_path) if args.output_path else None,
        manifest_path=Path(args.manifest_path) if args.manifest_path else None,
    )


def build_panel(request: D1PanelBuildRequest) -> D1PanelBuildResult:
    if not request.no_overwrite:
        raise D1PanelBuilderError("--no-overwrite is required for this builder")
    if request.start_date > request.end_date:
        raise D1PanelBuilderError("start-date must be <= end-date")

    data_root = _resolved(request.data_root)
    default_input_root = _resolved(data_root.joinpath(*RAW_5M_RELATIVE_ROOT))
    input_root = _resolved(request.input_root or default_input_root)
    _require_path_under(input_root, default_input_root, "input_root")

    output_path = _resolve_output_path(request, data_root)
    manifest_path = _resolve_manifest_path(request, data_root)
    approved_output_root = _approved_output_root(data_root)
    _require_path_under(output_path, approved_output_root, "output_path")
    _require_path_under(manifest_path, approved_output_root, "manifest_path")

    if output_path.exists() or manifest_path.exists():
        raise D1PanelBuilderError("target output exists and --no-overwrite is set")

    input_partitions = _discover_input_partitions(
        input_root=input_root,
        instrument_id=request.instrument_id,
        secid=request.secid,
        start_date=request.start_date,
        end_date=request.end_date,
    )
    if not input_partitions:
        raise D1PanelBuilderError("no raw 5m partitions found in requested date range")

    panel = _build_panel_frame(input_partitions, request)
    _reject_label_columns(panel)
    _write_outputs(panel, request, output_path, manifest_path, input_partitions)

    return D1PanelBuildResult(
        output_path=output_path,
        manifest_path=manifest_path,
        row_count=int(len(panel.index)),
        input_partitions=tuple(input_partitions),
    )


def _resolve_output_path(request: D1PanelBuildRequest, data_root: Path) -> Path:
    if request.output_path is not None:
        return _resolved(request.output_path)
    return _resolved(
        _approved_output_root(data_root)
        / f"instrument_id={request.instrument_id}"
        / f"run_id={request.run_id}"
        / OUTPUT_FILENAME
    )


def _resolve_manifest_path(request: D1PanelBuildRequest, data_root: Path) -> Path:
    if request.manifest_path is not None:
        return _resolved(request.manifest_path)
    return _resolved(
        _approved_output_root(data_root)
        / f"instrument_id={request.instrument_id}"
        / f"run_id={request.run_id}"
        / MANIFEST_FILENAME
    )


def _approved_output_root(data_root: Path) -> Path:
    return _resolved(data_root.joinpath(*OUTPUT_RELATIVE_ROOT))


def _discover_input_partitions(
    *,
    input_root: Path,
    instrument_id: str,
    secid: str,
    start_date: date,
    end_date: date,
) -> list[Path]:
    partitions: list[Path] = []
    for trade_date in _date_range(start_date, end_date):
        partition = (
            input_root
            / f"trade_date={trade_date.isoformat()}"
            / f"instrument_id={instrument_id}"
            / f"secid={secid}"
            / RAW_PART_FILENAME
        )
        if partition.exists():
            _require_path_under(_resolved(partition), input_root, "input_partition")
            partitions.append(_resolved(partition))
    return partitions


def _build_panel_frame(partitions: list[Path], request: D1PanelBuildRequest) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for partition in partitions:
        frame = pd.read_parquet(partition)
        trade_date = _trade_date_from_partition(partition)
        rows.append(
            _build_single_trade_date_row(
                frame=frame,
                trade_date=trade_date,
                partition=partition,
                request=request,
                partition_count=len(partitions),
            )
        )

    panel = pd.DataFrame(rows).sort_values(["trade_date", "instrument_id", "secid"])
    if panel.empty:
        raise D1PanelBuilderError("D1 panel build produced zero rows")
    return panel.reset_index(drop=True)


def _build_single_trade_date_row(
    *,
    frame: pd.DataFrame,
    trade_date: str,
    partition: Path,
    request: D1PanelBuildRequest,
    partition_count: int,
) -> dict[str, object]:
    if frame.empty:
        raise D1PanelBuilderError(f"raw 5m partition is empty: {partition.as_posix()}")

    timestamp_column = _timestamp_column(frame)
    required = [timestamp_column, *_PRICE_COLUMNS]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise D1PanelBuilderError("raw 5m partition missing required columns: " + ", ".join(missing))

    work = frame.copy()
    _validate_identity_column(work, "trade_date", trade_date, required=False)
    _validate_identity_column(work, "instrument_id", request.instrument_id, required=False)
    _validate_identity_column(work, "secid", request.secid, required=False)

    timestamps = pd.to_datetime(work[timestamp_column], errors="coerce")
    if timestamps.isna().any():
        raise D1PanelBuilderError(f"raw 5m partition has invalid timestamps: {partition.as_posix()}")

    for column in _PRICE_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    if work.loc[:, list(_PRICE_COLUMNS)].isna().any().any():
        raise D1PanelBuilderError("raw 5m partition has non-numeric OHLC values")
    if (work["high"] < work["low"]).any():
        raise D1PanelBuilderError("raw 5m partition has high lower than low")

    order = timestamps.sort_values().index
    ordered = work.loc[order].reset_index(drop=True)
    ordered_ts = timestamps.loc[order].reset_index(drop=True)

    row: dict[str, object] = {
        "trade_date": trade_date,
        "instrument_id": request.instrument_id,
        "secid": request.secid,
        "open": float(ordered.iloc[0]["open"]),
        "high": float(ordered["high"].max()),
        "low": float(ordered["low"].min()),
        "close": float(ordered.iloc[-1]["close"]),
        "first_ts": ordered_ts.iloc[0].isoformat(),
        "last_ts": ordered_ts.iloc[-1].isoformat(),
        "source_raw_5m_partition_count": int(partition_count),
        "panel_schema_version": PANEL_SCHEMA_VERSION,
        "build_run_id": request.run_id,
    }

    for column in _OPTIONAL_SUM_COLUMNS:
        if column in ordered.columns:
            numeric = pd.to_numeric(ordered[column], errors="coerce")
            row[column] = None if numeric.isna().all() else float(numeric.sum(skipna=True))

    return row


def _write_outputs(
    panel: pd.DataFrame,
    request: D1PanelBuildRequest,
    output_path: Path,
    manifest_path: Path,
    input_partitions: list[Path],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() or manifest_path.exists():
        raise D1PanelBuilderError("target output exists and --no-overwrite is set")

    panel.to_parquet(output_path, index=False)
    manifest = _manifest_values(panel, request, output_path, manifest_path, input_partitions)
    _write_json_atomic(manifest_path, manifest)


def _manifest_values(
    panel: pd.DataFrame,
    request: D1PanelBuildRequest,
    output_path: Path,
    manifest_path: Path,
    input_partitions: list[Path],
) -> dict[str, object]:
    return {
        "panel_id": PANEL_ID,
        "panel_schema_version": PANEL_SCHEMA_VERSION,
        "instrument_id": request.instrument_id,
        "secid": request.secid,
        "run_id": request.run_id,
        "requested_date_range": {
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
        },
        "built_date_range": {
            "min_trade_date": str(panel["trade_date"].min()),
            "max_trade_date": str(panel["trade_date"].max()),
        },
        "row_count": int(len(panel.index)),
        "input_partition_count": int(len(input_partitions)),
        "input_partitions": [path.as_posix() for path in input_partitions],
        "output_path": output_path.as_posix(),
        "manifest_path": manifest_path.as_posix(),
        "side_effect_summary": {
            "network_calls": False,
            "subprocess_calls": False,
            "model_fitting": False,
            "prediction": False,
            "trading_or_broker_actions": False,
            "writes": [output_path.as_posix(), manifest_path.as_posix()],
        },
        "labels_included_as_feature_columns": False,
    }


def _write_json_atomic(path: Path, values: dict[str, object]) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_path = Path(handle.name)
    temporary_path.replace(path)


def _timestamp_column(frame: pd.DataFrame) -> str:
    if "ts" in frame.columns:
        return "ts"
    if "end" in frame.columns:
        return "end"
    raise D1PanelBuilderError("raw 5m partition requires ts or end timestamp column")


def _validate_identity_column(
    frame: pd.DataFrame,
    column: str,
    expected_value: str,
    *,
    required: bool,
) -> None:
    if column not in frame.columns:
        if required:
            raise D1PanelBuilderError(f"raw 5m partition missing required identity column: {column}")
        return
    actual = frame[column].astype(str).str.strip()
    if not (actual == expected_value).all():
        raise D1PanelBuilderError(f"raw 5m partition {column} values do not match requested value")


def _reject_label_columns(frame: pd.DataFrame) -> None:
    leaked = sorted(set(frame.columns).intersection(_LABEL_COLUMNS))
    if leaked:
        raise D1PanelBuilderError("label columns are forbidden in D1 panel output: " + ", ".join(leaked))


def _trade_date_from_partition(partition: Path) -> str:
    for parent in partition.parents:
        name = parent.name
        if name.startswith("trade_date="):
            return _parse_iso_date(name.split("=", 1)[1], "trade_date").isoformat()
    raise D1PanelBuilderError("raw partition path does not contain trade_date=YYYY-MM-DD")


def _date_range(start_date: date, end_date: date) -> list[date]:
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise D1PanelBuilderError(f"{field_name} must be an explicit YYYY-MM-DD date") from exc


def _require_safe_path_token(value: str, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise D1PanelBuilderError(f"{field_name} is required")
    if text in {".", ".."} or "/" in text or "\\" in text:
        raise D1PanelBuilderError(f"{field_name} must be a single safe path token")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    if any(character not in allowed for character in text):
        raise D1PanelBuilderError(f"{field_name} contains unsupported characters")
    return text


def _resolved(path: Path) -> Path:
    return path.expanduser().resolve()


def _require_path_under(path: Path, root: Path, field_name: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise D1PanelBuilderError(f"{field_name} must stay under {root.as_posix()}") from exc


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    build_panel(request_from_args(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
