from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import json
import os
import tempfile
from typing import Final

import pandas as pd

from .contract_io import FuturesContractIoError, expand_contract_path, load_futures_data_lake_contract_package, reject_dynamic_markers
from .schemas import FuturesDatasetContract


RAW_5M_DATASET_ID: Final[str] = "futures_raw_5m"
RAW_5M_CONTRACT_ID: Final[str] = "futures_raw_5m.v1"
D1_DATASET_ID: Final[str] = "futures_derived_d1"
D1_CONTRACT_ID: Final[str] = "futures_derived_d1.v1"
TARGET_FAMILY: Final[str] = "Si"
TARGET_SECID: Final[str] = "SiM6"
TARGET_SERIES_TYPE: Final[str] = "native"
APPROVED_TRADE_DATES: Final[tuple[str, ...]] = ("2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05")
SUCCEEDED_STATUS: Final[str] = "succeeded"
VALIDATION_FAILED_STATUS: Final[str] = "failed_validation"
MANIFEST_QUALITY_LINKAGE_STATUS: Final[str] = "explicitly_reported_no_dedicated_d1_manifest_quality_contract_in_scope"

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
D1_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date",
    "symbol",
    "family",
    "series_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
    "num_trades",
    "source_schema_version",
    "build_ts",
)
OHLC_COLUMNS: Final[tuple[str, ...]] = ("open", "high", "low", "close")


class FuturesD1ReadinessError(ValueError):
    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class FuturesD1ReadinessPaths:
    input_partition_paths: tuple[Path, ...]
    output_partition_path: Path


@dataclass(frozen=True)
class FuturesD1ReadinessResult:
    status: str
    rows: int
    output_partition_path: Path
    trade_dates: tuple[str, ...]
    symbols: tuple[str, ...]
    rows_per_trade_date: dict[str, int]
    input_partition_paths: tuple[Path, ...]
    manifest_quality_linkage_status: str


def _fail(message: str, status: str = VALIDATION_FAILED_STATUS) -> None:
    raise FuturesD1ReadinessError(status, message)


def _require_text(value: str | None, field_name: str) -> str:
    try:
        return reject_dynamic_markers(value or "", field_name)
    except FuturesContractIoError as exc:
        raise FuturesD1ReadinessError(VALIDATION_FAILED_STATUS, str(exc)) from exc


def _require_trade_date(value: str) -> str:
    text = _require_text(value, "trade_date")
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        _fail("trade_date must be an explicit YYYY-MM-DD date")
    try:
        return pd.Timestamp(text).date().isoformat()
    except Exception as exc:
        raise FuturesD1ReadinessError(VALIDATION_FAILED_STATUS, "trade_date must be an explicit YYYY-MM-DD date") from exc


def _require_trade_dates(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        _fail("trade_dates must be an explicit sequence")
    checked = tuple(_require_trade_date(str(value)) for value in values)
    if checked != APPROVED_TRADE_DATES:
        _fail("trade_dates must exactly match the approved controlled D1 readiness slice")
    if len(set(checked)) != len(checked):
        _fail("trade_dates must not contain duplicates")
    return checked


def _require_target_scope(family: str, secid: str, series_type: str) -> tuple[str, str, str]:
    checked_family = _require_text(family, "family")
    checked_secid = _require_text(secid, "secid")
    checked_series_type = _require_text(series_type, "series_type")
    if checked_family != TARGET_FAMILY:
        _fail("family does not match the controlled D1 readiness slice")
    if checked_secid != TARGET_SECID:
        _fail("secid does not match the controlled D1 readiness slice")
    if checked_series_type != TARGET_SERIES_TYPE:
        _fail("series_type does not match the controlled D1 readiness slice")
    return checked_family, checked_secid, checked_series_type


def _env_root(env: Mapping[str, str] | None) -> str:
    active_env = os.environ if env is None else env
    root = str(active_env.get("MOEX_DATA_ROOT", "")).strip()
    if not root:
        _fail("MOEX_DATA_ROOT is required")
    return root


def _contract_for(package_contracts: Mapping[str, FuturesDatasetContract], dataset_id: str, contract_id: str) -> FuturesDatasetContract:
    contract = package_contracts.get(dataset_id)
    if contract is None:
        _fail("unsupported dataset_id")
    if contract.contract_id != contract_id:
        _fail("contract_id does not match dataset_id")
    return contract


def d1_readiness_paths(
    repo_root: str | Path,
    trade_dates: Sequence[str],
    family: str,
    secid: str,
    series_type: str,
    env: Mapping[str, str] | None = None,
) -> FuturesD1ReadinessPaths:
    checked_dates = _require_trade_dates(trade_dates)
    checked_family, checked_secid, checked_series_type = _require_target_scope(family, secid, series_type)
    package = load_futures_data_lake_contract_package(repo_root)
    root = _env_root(env)
    raw_contract = _contract_for(package.contracts_by_dataset_id, RAW_5M_DATASET_ID, RAW_5M_CONTRACT_ID)
    d1_contract = _contract_for(package.contracts_by_dataset_id, D1_DATASET_ID, D1_CONTRACT_ID)
    try:
        input_paths = tuple(
            expand_contract_path(
                raw_contract.path_pattern,
                root,
                {"YYYY-MM-DD": trade_date, "FAMILY": checked_family, "SECID": checked_secid},
            )
            for trade_date in checked_dates
        )
        output_path = expand_contract_path(
            d1_contract.path_pattern,
            root,
            {"SERIES_TYPE": checked_series_type, "FAMILY": checked_family},
        )
    except FuturesContractIoError as exc:
        raise FuturesD1ReadinessError(VALIDATION_FAILED_STATUS, str(exc)) from exc
    return FuturesD1ReadinessPaths(input_partition_paths=input_paths, output_partition_path=output_path)


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        _fail("required raw 5m input partition does not exist")
    return pd.read_parquet(path)


def _timestamp_series(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if bool(timestamps.isna().any()):
        _fail("raw 5m partition contains invalid ts values")
    return timestamps


def _validate_raw_partition(df: pd.DataFrame, trade_date: str, family: str, secid: str) -> tuple[pd.DataFrame, dict[str, object]]:
    missing = tuple(column for column in RAW_5M_REQUIRED_COLUMNS if column not in df.columns)
    if missing:
        _fail("raw 5m partition is missing required columns")
    if df.empty:
        _fail("raw 5m partition is empty")
    work = df.loc[:, RAW_5M_REQUIRED_COLUMNS].copy()
    if set(work["trade_date"].astype(str)) != {trade_date}:
        _fail("raw 5m partition trade_date values do not match the requested approved date")
    if set(work["family"].astype(str)) != {family}:
        _fail("raw 5m partition family values do not match the requested family")
    if set(work["secid"].astype(str)) != {secid}:
        _fail("raw 5m partition secid values do not match the requested secid")
    for column in OHLC_COLUMNS + ("volume", "value", "num_trades"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    if bool(work.loc[:, OHLC_COLUMNS].isna().any(axis=1).any()):
        _fail("raw 5m partition contains null or non-numeric OHLC values")
    if bool((work["high"] < work["low"]).any()):
        _fail("raw 5m partition contains high lower than low")
    if bool(((work["open"] < work["low"]) | (work["open"] > work["high"]) | (work["close"] < work["low"]) | (work["close"] > work["high"])).any()):
        _fail("raw 5m partition contains open or close outside high-low range")
    timestamps = _timestamp_series(work["ts"])
    work["_ts"] = timestamps
    work = work.sort_values(["_ts", "secid"], kind="mergesort").reset_index(drop=True)
    if int(work.duplicated(subset=["_ts", "secid"]).sum()):
        _fail("raw 5m partition contains duplicate ts/secid keys")
    if not bool(work["_ts"].is_monotonic_increasing):
        _fail("raw 5m partition contains non-monotonic ts values")
    return work, {
        "rows": int(len(work.index)),
        "min_ts": str(work["_ts"].min()),
        "max_ts": str(work["_ts"].max()),
        "input_build_ts": str(work["ingest_ts"].astype(str).max()),
    }


def _sum_or_na(values: pd.Series) -> object:
    result = values.sum(min_count=1)
    if pd.isna(result):
        return pd.NA
    return result


def _aggregate_day(work: pd.DataFrame, metrics: Mapping[str, object], source_path: Path, family: str, secid: str, series_type: str) -> dict[str, object]:
    return {
        "trade_date": str(work["trade_date"].iloc[0]),
        "symbol": secid,
        "secid": secid,
        "family": family,
        "series_type": series_type,
        "open": work["open"].iloc[0],
        "high": work["high"].max(),
        "low": work["low"].min(),
        "close": work["close"].iloc[-1],
        "volume": _sum_or_na(work["volume"]),
        "value": _sum_or_na(work["value"]),
        "num_trades": _sum_or_na(work["num_trades"]),
        "source_schema_version": RAW_5M_CONTRACT_ID,
        "build_ts": str(metrics["input_build_ts"]),
        "input_partition_path": source_path.as_posix(),
        "input_rows": int(metrics["rows"]),
        "input_min_ts": str(metrics["min_ts"]),
        "input_max_ts": str(metrics["max_ts"]),
        "input_manifest_quality_linkage_status": MANIFEST_QUALITY_LINKAGE_STATUS,
    }


def _validate_d1_output(df: pd.DataFrame, trade_dates: tuple[str, ...], family: str, secid: str, series_type: str) -> None:
    missing = tuple(column for column in D1_REQUIRED_COLUMNS if column not in df.columns)
    if missing:
        _fail("derived D1 output is missing required contract columns")
    if int(len(df.index)) != len(trade_dates):
        _fail("derived D1 output must contain one row per approved trade_date")
    if tuple(df["trade_date"].astype(str).tolist()) != trade_dates:
        _fail("derived D1 output trade_date coverage does not match approved dates")
    if set(df["family"].astype(str)) != {family}:
        _fail("derived D1 output family coverage does not match approved family")
    if set(df["symbol"].astype(str)) != {secid}:
        _fail("derived D1 output symbol coverage does not match approved secid")
    if "secid" in df.columns and set(df["secid"].astype(str)) != {secid}:
        _fail("derived D1 output secid coverage does not match approved secid")
    if set(df["series_type"].astype(str)) != {series_type}:
        _fail("derived D1 output series_type coverage does not match approved series_type")
    if int(df.duplicated(subset=["trade_date", "symbol", "series_type"]).sum()):
        _fail("derived D1 output contains duplicate trade_date/symbol/series_type keys")
    for column in OHLC_COLUMNS:
        numeric = pd.to_numeric(df[column], errors="coerce")
        if bool(numeric.isna().any()):
            _fail("derived D1 output contains null or non-numeric OHLC values")
    if bool((df["high"] < df["low"]).any()):
        _fail("derived D1 output contains high lower than low")


def _write_parquet_atomic(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".tmp") as handle:
        temporary_name = handle.name
    temporary_path = Path(temporary_name)
    try:
        df.to_parquet(temporary_path, index=False)
        temporary_path.replace(path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def derive_d1_readiness_from_raw_5m_partitions(
    repo_root: str | Path,
    trade_dates: Sequence[str],
    family: str,
    secid: str,
    series_type: str,
    env: Mapping[str, str] | None = None,
) -> FuturesD1ReadinessResult:
    checked_dates = _require_trade_dates(trade_dates)
    checked_family, checked_secid, checked_series_type = _require_target_scope(family, secid, series_type)
    paths = d1_readiness_paths(repo_root, checked_dates, checked_family, checked_secid, checked_series_type, env)
    rows: list[dict[str, object]] = []
    for trade_date, input_path in zip(checked_dates, paths.input_partition_paths):
        raw_frame = _read_parquet(input_path)
        checked_frame, metrics = _validate_raw_partition(raw_frame, trade_date, checked_family, checked_secid)
        rows.append(_aggregate_day(checked_frame, metrics, input_path, checked_family, checked_secid, checked_series_type))
    output = pd.DataFrame(rows).sort_values(["trade_date", "symbol", "series_type"], kind="mergesort").reset_index(drop=True)
    _validate_d1_output(output, checked_dates, checked_family, checked_secid, checked_series_type)
    _write_parquet_atomic(paths.output_partition_path, output)
    rows_per_trade_date = {trade_date: int(count) for trade_date, count in output.groupby("trade_date").size().items()}
    return FuturesD1ReadinessResult(
        status=SUCCEEDED_STATUS,
        rows=int(len(output.index)),
        output_partition_path=paths.output_partition_path,
        trade_dates=tuple(output["trade_date"].astype(str).tolist()),
        symbols=tuple(sorted(set(output["symbol"].astype(str)))),
        rows_per_trade_date=rows_per_trade_date,
        input_partition_paths=paths.input_partition_paths,
        manifest_quality_linkage_status=MANIFEST_QUALITY_LINKAGE_STATUS,
    )


def _result_payload(result: FuturesD1ReadinessResult) -> dict[str, object]:
    return {
        "status": result.status,
        "rows": result.rows,
        "output_partition_path": result.output_partition_path.as_posix(),
        "trade_dates": list(result.trade_dates),
        "symbols": list(result.symbols),
        "rows_per_trade_date": result.rows_per_trade_date,
        "input_partition_paths": [path.as_posix() for path in result.input_partition_paths],
        "manifest_quality_linkage_status": result.manifest_quality_linkage_status,
    }


def _error_payload(error: FuturesD1ReadinessError) -> dict[str, object]:
    return {"status": error.status, "message": error.message}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Derive one controlled futures_derived_d1 readiness artifact from accepted raw 5m partitions")
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--trade-date", action="append", required=True)
    parser.add_argument("--family", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--series-type", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = derive_d1_readiness_from_raw_5m_partitions(
            repo_root=args.repo_root,
            trade_dates=tuple(args.trade_date),
            family=args.family,
            secid=args.secid,
            series_type=args.series_type,
        )
    except FuturesD1ReadinessError as exc:
        print(json.dumps(_error_payload(exc), ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(_result_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
