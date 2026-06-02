from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Final

import pandas as pd

from .contract_io import FuturesContractIoError, expand_contract_path, load_futures_data_lake_contract_package, reject_dynamic_markers
from .manifest import validate_refresh_manifest_values
from .quality import validate_quality_report_rows
from .schemas import EXPECTED_DATASET_CONTRACT_IDS, FuturesDatasetContract


TARGET_DATASET_ID: Final[str] = "futures_raw_5m"
TARGET_CONTRACT_ID: Final[str] = "futures_raw_5m.v1"
TARGET_TRADE_DATE: Final[str] = "2026-06-02"
TARGET_FAMILY: Final[str] = "Si"
TARGET_SECID: Final[str] = "SiM6"
MANIFEST_DATASET_ID: Final[str] = "futures_data_refresh_manifest"
QUALITY_DATASET_ID: Final[str] = "futures_quality_report"
BLOCKED_NO_SOURCE_STATUS: Final[str] = "blocked_no_source_artifact"
VALIDATION_FAILED_STATUS: Final[str] = "failed_validation"
SUCCEEDED_STATUS: Final[str] = "succeeded"

RAW_5M_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date",
    "ts",
    "session_date",
    "secid",
    "family",
    "board",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
    "num_trades",
    "source",
    "ingest_ts",
)

_OHLC_COLUMNS: Final[tuple[str, ...]] = ("open", "high", "low", "close")


class FuturesRaw5mMaterializationError(ValueError):
    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class Raw5mMaterializationRequest:
    repo_root: Path
    dataset_id: str
    contract_id: str
    trade_date: str
    family: str
    secid: str
    source_path: Path
    run_id: str


@dataclass(frozen=True)
class Raw5mMaterializationPaths:
    partition_path: Path
    manifest_path: Path
    quality_report_path: Path


@dataclass(frozen=True)
class Raw5mMaterializationResult:
    status: str
    rows: int
    partition_path: Path
    manifest_path: Path
    quality_report_path: Path
    quality_status: str


def _fail(message: str, status: str = VALIDATION_FAILED_STATUS) -> None:
    raise FuturesRaw5mMaterializationError(status, message)


def _as_materialization_error(exc: Exception) -> FuturesRaw5mMaterializationError:
    if isinstance(exc, FuturesRaw5mMaterializationError):
        return exc
    return FuturesRaw5mMaterializationError(VALIDATION_FAILED_STATUS, str(exc))


def _require_text(value: str | None, field_name: str) -> str:
    try:
        return reject_dynamic_markers(value or "", field_name)
    except FuturesContractIoError as exc:
        raise FuturesRaw5mMaterializationError(VALIDATION_FAILED_STATUS, str(exc)) from exc


def _require_run_id(value: str | None) -> str:
    run_id = _require_text(value, "run_id")
    if run_id.startswith(("/", "\\")) or "/" in run_id or "\\" in run_id or run_id in (".", ".."):
        _fail("run_id must be a single safe path token")
    return run_id


def _require_source_path(value: str | None) -> Path:
    if value is None or not str(value).strip():
        _fail("source_path is required", BLOCKED_NO_SOURCE_STATUS)
    source_text = _require_text(value, "source_path")
    source_path = Path(source_text)
    if not source_path.exists() or not source_path.is_file():
        _fail("source_path does not exist", BLOCKED_NO_SOURCE_STATUS)
    return source_path


def _require_exact_target(dataset_id: str, contract_id: str, trade_date: str, family: str, secid: str) -> None:
    if dataset_id != TARGET_DATASET_ID:
        _fail("unsupported dataset_id")
    if contract_id != TARGET_CONTRACT_ID:
        _fail("contract_id does not match controlled materialization package")
    if trade_date != TARGET_TRADE_DATE:
        _fail("trade_date does not match controlled target partition")
    if family != TARGET_FAMILY:
        _fail("family does not match controlled target partition")
    if secid != TARGET_SECID:
        _fail("secid does not match controlled target partition")


def _contract_for(package_contracts: Mapping[str, FuturesDatasetContract], dataset_id: str, contract_id: str) -> FuturesDatasetContract:
    contract = package_contracts.get(dataset_id)
    if contract is None:
        _fail("unsupported dataset_id")
    if contract.contract_id != contract_id:
        _fail("contract_id does not match dataset_id")
    return contract


def _env_root(env: Mapping[str, str] | None) -> str:
    active_env = os.environ if env is None else env
    root = active_env.get("MOEX_DATA_ROOT")
    if not root or not root.strip():
        _fail("MOEX_DATA_ROOT is required")
    return root


def build_materialization_request(
    repo_root: str | Path,
    dataset_id: str,
    contract_id: str,
    trade_date: str,
    family: str,
    secid: str,
    source_path: str | None,
    run_id: str,
) -> Raw5mMaterializationRequest:
    checked_dataset_id = _require_text(dataset_id, "dataset_id")
    checked_contract_id = _require_text(contract_id, "contract_id")
    checked_trade_date = _require_text(trade_date, "trade_date")
    checked_family = _require_text(family, "family")
    checked_secid = _require_text(secid, "secid")
    _require_exact_target(checked_dataset_id, checked_contract_id, checked_trade_date, checked_family, checked_secid)
    return Raw5mMaterializationRequest(
        repo_root=Path(repo_root),
        dataset_id=checked_dataset_id,
        contract_id=checked_contract_id,
        trade_date=checked_trade_date,
        family=checked_family,
        secid=checked_secid,
        source_path=_require_source_path(source_path),
        run_id=_require_run_id(run_id),
    )


def materialization_target_paths(
    repo_root: str | Path,
    dataset_id: str,
    contract_id: str,
    trade_date: str,
    family: str,
    secid: str,
    run_id: str,
    env: Mapping[str, str] | None = None,
) -> Raw5mMaterializationPaths:
    checked_dataset_id = _require_text(dataset_id, "dataset_id")
    checked_contract_id = _require_text(contract_id, "contract_id")
    checked_trade_date = _require_text(trade_date, "trade_date")
    checked_family = _require_text(family, "family")
    checked_secid = _require_text(secid, "secid")
    checked_run_id = _require_run_id(run_id)
    _require_exact_target(checked_dataset_id, checked_contract_id, checked_trade_date, checked_family, checked_secid)

    package = load_futures_data_lake_contract_package(repo_root)
    root = _env_root(env)
    raw_contract = _contract_for(package.contracts_by_dataset_id, checked_dataset_id, checked_contract_id)
    manifest_contract = _contract_for(
        package.contracts_by_dataset_id,
        MANIFEST_DATASET_ID,
        "futures_data_refresh_manifest.v1",
    )
    quality_contract = _contract_for(package.contracts_by_dataset_id, QUALITY_DATASET_ID, "futures_quality_report.v1")

    try:
        partition_path = expand_contract_path(
            raw_contract.path_pattern,
            root,
            {"YYYY-MM-DD": checked_trade_date, "FAMILY": checked_family, "SECID": checked_secid},
        )
        manifest_path = expand_contract_path(
            manifest_contract.path_pattern,
            root,
            {"YYYY-MM-DD": checked_trade_date, "RUN_ID": checked_run_id},
        )
        quality_report_path = expand_contract_path(
            quality_contract.path_pattern,
            root,
            {"YYYY-MM-DD": checked_trade_date, "RUN_ID": checked_run_id},
        )
    except FuturesContractIoError as exc:
        raise FuturesRaw5mMaterializationError(VALIDATION_FAILED_STATUS, str(exc)) from exc
    return Raw5mMaterializationPaths(
        partition_path=partition_path,
        manifest_path=manifest_path,
        quality_report_path=quality_report_path,
    )


def _load_source_table(source_path: Path) -> pd.DataFrame:
    suffix = source_path.suffix.casefold()
    if suffix == ".parquet":
        return pd.read_parquet(source_path)
    if suffix == ".csv":
        return pd.read_csv(source_path)
    _fail("source_path must point to a .parquet or .csv file")


def _timestamp_strings(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.isna().any():
        _fail("ts contains invalid timestamp values")
    return timestamps


def _gap_count(timestamps: pd.Series) -> int:
    ordered = timestamps.sort_values()
    deltas = ordered.diff().dropna()
    return int((deltas > pd.Timedelta(minutes=5)).sum())


def _validate_source_table(df: pd.DataFrame, trade_date: str, family: str, secid: str) -> tuple[pd.DataFrame, dict[str, object]]:
    missing = tuple(column for column in RAW_5M_REQUIRED_COLUMNS if column not in df.columns)
    if missing:
        _fail("source table is missing required raw 5m columns")
    if df.empty:
        _fail("source table is empty")

    output = df.loc[:, RAW_5M_REQUIRED_COLUMNS].copy()
    row_count = int(len(output.index))
    if not (output["trade_date"].astype(str) == trade_date).all():
        _fail("source trade_date values do not match target partition")
    if not (output["family"].astype(str) == family).all():
        _fail("source family values do not match target partition")
    if not (output["secid"].astype(str) == secid).all():
        _fail("source secid values do not match target partition")

    null_ohlc_count = int(output.loc[:, _OHLC_COLUMNS].isna().any(axis=1).sum())
    if null_ohlc_count:
        _fail("source table contains null OHLC values")
    for column in _OHLC_COLUMNS:
        output[column] = pd.to_numeric(output[column], errors="coerce")
    if output.loc[:, _OHLC_COLUMNS].isna().any(axis=1).sum():
        _fail("source table contains non-numeric OHLC values")

    invalid_ohlc_count = int((output["high"] < output["low"]).sum())
    if invalid_ohlc_count:
        _fail("source table contains high lower than low")
    inside_range_invalid = (
        (output["open"] < output["low"])
        | (output["open"] > output["high"])
        | (output["close"] < output["low"])
        | (output["close"] > output["high"])
    )
    if bool(inside_range_invalid.any()):
        _fail("source table contains open or close outside high-low range")

    duplicate_key_count = int(output.duplicated(subset=["ts", "secid"]).sum())
    if duplicate_key_count:
        _fail("source table contains duplicate ts/secid keys")

    timestamps = _timestamp_strings(output["ts"])
    for _, group_index in output.groupby("secid", sort=False).groups.items():
        group_ts = timestamps.loc[group_index]
        if not bool(group_ts.is_monotonic_increasing):
            _fail("source table contains non-monotonic ts by secid")

    metrics = {
        "rows": row_count,
        "min_ts": str(timestamps.min()),
        "max_ts": str(timestamps.max()),
        "duplicate_key_count": duplicate_key_count,
        "gap_count": _gap_count(timestamps),
        "null_ohlc_count": null_ohlc_count,
        "invalid_ohlc_count": invalid_ohlc_count,
    }
    return output, metrics


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_name = handle.name
    Path(temporary_name).replace(path)


def _write_parquet_atomic(path: Path, df: pd.DataFrame, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{run_id}.tmp")
    try:
        df.to_parquet(temporary_path, index=False)
        temporary_path.replace(path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _quality_row(request: Raw5mMaterializationRequest, metrics: Mapping[str, object]) -> dict[str, object]:
    return {
        "run_id": request.run_id,
        "dataset_id": request.dataset_id,
        "family": request.family,
        "secid": request.secid,
        "trade_date": request.trade_date,
        "rows": int(metrics["rows"]),
        "min_ts": str(metrics["min_ts"]),
        "max_ts": str(metrics["max_ts"]),
        "duplicate_key_count": int(metrics["duplicate_key_count"]),
        "gap_count": int(metrics["gap_count"]),
        "null_ohlc_count": int(metrics["null_ohlc_count"]),
        "invalid_ohlc_count": int(metrics["invalid_ohlc_count"]),
        "futoi_missing_count": 0,
        "calendar_status": "not_checked",
        "quality_status": "pass",
        "notes": "single_partition_minimal_materialization",
    }


def _manifest(request: Raw5mMaterializationRequest, paths: Raw5mMaterializationPaths) -> dict[str, object]:
    now = _utc_now()
    values = {
        "run_id": request.run_id,
        "run_date": request.trade_date,
        "requested_from": request.trade_date,
        "requested_till": request.trade_date,
        "family_scope": [request.family],
        "dataset_contract_refs": list(EXPECTED_DATASET_CONTRACT_IDS),
        "partitions_written": [paths.partition_path.as_posix()],
        "partitions_skipped": [],
        "quality_report_ref": paths.quality_report_path.as_posix(),
        "refresh_status": SUCCEEDED_STATUS,
        "started_at": now,
        "finished_at": now,
    }
    validate_refresh_manifest_values(values)
    return values


def materialize_single_raw_5m_partition(
    repo_root: str | Path,
    dataset_id: str,
    contract_id: str,
    trade_date: str,
    family: str,
    secid: str,
    source_path: str | None,
    run_id: str,
    env: Mapping[str, str] | None = None,
) -> Raw5mMaterializationResult:
    try:
        request = build_materialization_request(
            repo_root=repo_root,
            dataset_id=dataset_id,
            contract_id=contract_id,
            trade_date=trade_date,
            family=family,
            secid=secid,
            source_path=source_path,
            run_id=run_id,
        )
        paths = materialization_target_paths(
            repo_root=request.repo_root,
            dataset_id=request.dataset_id,
            contract_id=request.contract_id,
            trade_date=request.trade_date,
            family=request.family,
            secid=request.secid,
            run_id=request.run_id,
            env=env,
        )
        source_table = _load_source_table(request.source_path)
        output_table, metrics = _validate_source_table(source_table, request.trade_date, request.family, request.secid)
        quality_row = _quality_row(request, metrics)
        validate_quality_report_rows([quality_row])
        quality_report = {"run_id": request.run_id, "rows": [quality_row]}
        manifest = _manifest(request, paths)

        _write_parquet_atomic(paths.partition_path, output_table, request.run_id)
        _write_json_atomic(paths.quality_report_path, quality_report)
        _write_json_atomic(paths.manifest_path, manifest)
        return Raw5mMaterializationResult(
            status=SUCCEEDED_STATUS,
            rows=int(metrics["rows"]),
            partition_path=paths.partition_path,
            manifest_path=paths.manifest_path,
            quality_report_path=paths.quality_report_path,
            quality_status="pass",
        )
    except Exception as exc:
        raise _as_materialization_error(exc) from exc


def _result_payload(result: Raw5mMaterializationResult) -> dict[str, object]:
    return {
        "status": result.status,
        "rows": result.rows,
        "partition_path": result.partition_path.as_posix(),
        "manifest_path": result.manifest_path.as_posix(),
        "quality_report_path": result.quality_report_path.as_posix(),
        "quality_status": result.quality_status,
    }


def _error_payload(error: FuturesRaw5mMaterializationError) -> dict[str, object]:
    return {"status": error.status, "message": error.message}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize one controlled futures_raw_5m partition")
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--family", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--source-path", default=None)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = materialize_single_raw_5m_partition(
            repo_root=args.repo_root,
            dataset_id=args.dataset_id,
            contract_id=args.contract_id,
            trade_date=args.trade_date,
            family=args.family,
            secid=args.secid,
            source_path=args.source_path,
            run_id=args.run_id,
        )
    except FuturesRaw5mMaterializationError as exc:
        print(json.dumps(_error_payload(exc), ensure_ascii=False, sort_keys=True))
        if exc.status == BLOCKED_NO_SOURCE_STATUS:
            return 2
        return 1
    print(json.dumps(_result_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
