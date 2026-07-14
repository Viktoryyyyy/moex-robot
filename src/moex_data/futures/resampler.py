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
D1_TIMEFRAME: Final[str] = "1D"
APPROVED_TRADE_DATES: Final[tuple[str, ...]] = ("2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05")
SUCCEEDED_STATUS: Final[str] = "succeeded"
VALIDATION_FAILED_STATUS: Final[str] = "failed_validation"
MANIFEST_QUALITY_LINKAGE_STATUS: Final[str] = "explicitly_reported_no_dedicated_d1_manifest_quality_contract_in_scope"

RAW_5M_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "instrument_id",
    "trade_date",
    "ts",
    "session_date",
    "secid",
    "board",
    "market",
    "engine",
    "source_id",
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
    "instrument_id",
    "canonical_symbol",
    "timeframe",
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
class FuturesD1ReadinessIdentity:
    instrument_id: str
    source_id: str
    secid: str
    board: str | None
    market: str | None
    engine: str | None
    canonical_symbol: str
    family: str | None
    series_type: str


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


def _optional_text(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _coalesce_required(primary: str | None, fallback: str | None, field_name: str) -> str:
    return _require_text(primary or fallback, field_name)


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


def _require_identity(
    *,
    instrument_id: str | None,
    source_id: str | None,
    secid: str | None,
    canonical_symbol: str | None,
    family: str | None,
    board: str | None,
    market: str | None,
    engine: str | None,
    series_type: str | None,
) -> FuturesD1ReadinessIdentity:
    checked_family = _optional_text(family, "family")
    checked_secid = _require_text(secid, "secid")
    return FuturesD1ReadinessIdentity(
        instrument_id=_coalesce_required(instrument_id, checked_family, "instrument_id"),
        source_id=_coalesce_required(source_id, checked_secid, "source_id"),
        secid=checked_secid,
        board=_optional_text(board, "board"),
        market=_optional_text(market, "market"),
        engine=_optional_text(engine, "engine"),
        canonical_symbol=_coalesce_required(canonical_symbol, checked_secid, "canonical_symbol"),
        family=checked_family,
        series_type=_require_text(series_type or "native", "series_type"),
    )


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
    family: str | None = None,
    secid: str | None = None,
    series_type: str | None = "native",
    env: Mapping[str, str] | None = None,
    *,
    instrument_id: str | None = None,
    source_id: str | None = None,
    canonical_symbol: str | None = None,
    board: str | None = None,
    market: str | None = None,
    engine: str | None = None,
) -> FuturesD1ReadinessPaths:
    checked_dates = _require_trade_dates(trade_dates)
    identity = _require_identity(
        instrument_id=instrument_id,
        source_id=source_id,
        secid=secid,
        canonical_symbol=canonical_symbol,
        family=family,
        board=board,
        market=market,
        engine=engine,
        series_type=series_type,
    )
    package = load_futures_data_lake_contract_package(repo_root)
    root = _env_root(env)
    raw_contract = _contract_for(package.contracts_by_dataset_id, RAW_5M_DATASET_ID, RAW_5M_CONTRACT_ID)
    d1_contract = _contract_for(package.contracts_by_dataset_id, D1_DATASET_ID, D1_CONTRACT_ID)
    try:
        input_paths = tuple(
            expand_contract_path(
                raw_contract.path_pattern,
                root,
                {"YYYY-MM-DD": trade_date, "INSTRUMENT_ID": identity.instrument_id, "SOURCE_ID": identity.source_id},
            )
            for trade_date in checked_dates
        )
        output_path = expand_contract_path(
            d1_contract.path_pattern,
            root,
            {"INSTRUMENT_ID": identity.instrument_id},
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


def _validate_raw_partition(df: pd.DataFrame, trade_date: str, identity: FuturesD1ReadinessIdentity) -> tuple[pd.DataFrame, dict[str, object]]:
    missing = tuple(column for column in RAW_5M_REQUIRED_COLUMNS if column not in df.columns)
    if missing:
        _fail("raw 5m partition is missing required columns")
    if df.empty:
        _fail("raw 5m partition is empty")
    work = df.loc[:, RAW_5M_REQUIRED_COLUMNS].copy()
    if set(work["trade_date"].astype(str)) != {trade_date}:
        _fail("raw 5m partition trade_date values do not match the requested approved date")
    if set(work["instrument_id"].astype(str)) != {identity.instrument_id}:
        _fail("raw 5m partition instrument_id values do not match the requested instrument_id")
    if set(work["source_id"].astype(str)) != {identity.source_id}:
        _fail("raw 5m partition source_id values do not match the requested source_id")
    if set(work["secid"].astype(str)) != {identity.secid}:
        _fail("raw 5m partition secid values do not match the requested secid")
    if identity.board is not None and set(work["board"].astype(str)) != {identity.board}:
        _fail("raw 5m partition board values do not match the requested board")
    if identity.market is not None and set(work["market"].astype(str)) != {identity.market}:
        _fail("raw 5m partition market values do not match the requested market")
    if identity.engine is not None and set(work["engine"].astype(str)) != {identity.engine}:
        _fail("raw 5m partition engine values do not match the requested engine")
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
    work = work.sort_values(["_ts", "instrument_id", "source_id"], kind="mergesort").reset_index(drop=True)
    if int(work.duplicated(subset=["_ts", "instrument_id", "source_id"]).sum()):
        _fail("raw 5m partition contains duplicate ts/instrument_id/source_id keys")
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


def _aggregate_day(work: pd.DataFrame, metrics: Mapping[str, object], source_path: Path, identity: FuturesD1ReadinessIdentity) -> dict[str, object]:
    return {
        "trade_date": str(work["trade_date"].iloc[0]),
        "instrument_id": identity.instrument_id,
        "canonical_symbol": identity.canonical_symbol,
        "timeframe": D1_TIMEFRAME,
        "secid": identity.secid,
        "source_id": identity.source_id,
        "series_type": identity.series_type,
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


def _validate_d1_output(df: pd.DataFrame, trade_dates: tuple[str, ...], identity: FuturesD1ReadinessIdentity) -> None:
    missing = tuple(column for column in D1_REQUIRED_COLUMNS if column not in df.columns)
    if missing:
        _fail("derived D1 output is missing required contract columns")
    if int(len(df.index)) != len(trade_dates):
        _fail("derived D1 output must contain one row per approved trade_date")
    if tuple(df["trade_date"].astype(str).tolist()) != trade_dates:
        _fail("derived D1 output trade_date coverage does not match approved dates")
    if set(df["instrument_id"].astype(str)) != {identity.instrument_id}:
        _fail("derived D1 output instrument_id coverage does not match requested instrument_id")
    if set(df["canonical_symbol"].astype(str)) != {identity.canonical_symbol}:
        _fail("derived D1 output canonical_symbol coverage does not match requested canonical_symbol")
    if set(df["timeframe"].astype(str)) != {D1_TIMEFRAME}:
        _fail("derived D1 output timeframe coverage does not match D1")
    if int(df.duplicated(subset=["trade_date", "instrument_id", "timeframe"]).sum()):
        _fail("derived D1 output contains duplicate trade_date/instrument_id/timeframe keys")
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
    family: str | None = None,
    secid: str | None = None,
    series_type: str | None = "native",
    env: Mapping[str, str] | None = None,
    *,
    instrument_id: str | None = None,
    source_id: str | None = None,
    canonical_symbol: str | None = None,
    board: str | None = None,
    market: str | None = None,
    engine: str | None = None,
) -> FuturesD1ReadinessResult:
    checked_dates = _require_trade_dates(trade_dates)
    identity = _require_identity(
        instrument_id=instrument_id,
        source_id=source_id,
        secid=secid,
        canonical_symbol=canonical_symbol,
        family=family,
        board=board,
        market=market,
        engine=engine,
        series_type=series_type,
    )
    paths = d1_readiness_paths(
        repo_root,
        checked_dates,
        family=identity.family,
        secid=identity.secid,
        series_type=identity.series_type,
        env=env,
        instrument_id=identity.instrument_id,
        source_id=identity.source_id,
        canonical_symbol=identity.canonical_symbol,
        board=identity.board,
        market=identity.market,
        engine=identity.engine,
    )
    rows: list[dict[str, object]] = []
    for trade_date, input_path in zip(checked_dates, paths.input_partition_paths):
        raw_frame = _read_parquet(input_path)
        checked_frame, metrics = _validate_raw_partition(raw_frame, trade_date, identity)
        rows.append(_aggregate_day(checked_frame, metrics, input_path, identity))
    output = pd.DataFrame(rows).sort_values(["trade_date", "instrument_id", "timeframe"], kind="mergesort").reset_index(drop=True)
    _validate_d1_output(output, checked_dates, identity)
    _write_parquet_atomic(paths.output_partition_path, output)
    rows_per_trade_date = {trade_date: int(count) for trade_date, count in output.groupby("trade_date").size().items()}
    return FuturesD1ReadinessResult(
        status=SUCCEEDED_STATUS,
        rows=int(len(output.index)),
        output_partition_path=paths.output_partition_path,
        trade_dates=tuple(output["trade_date"].astype(str).tolist()),
        symbols=tuple(sorted(set(output["canonical_symbol"].astype(str)))),
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
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--canonical-symbol", required=True)
    parser.add_argument("--board", default=None)
    parser.add_argument("--market", default=None)
    parser.add_argument("--engine", default=None)
    parser.add_argument("--series-type", default="native")
    parser.add_argument("--family", default=None)
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
            instrument_id=args.instrument_id,
            source_id=args.source_id,
            canonical_symbol=args.canonical_symbol,
            board=args.board,
            market=args.market,
            engine=args.engine,
        )
    except FuturesD1ReadinessError as exc:
        print(json.dumps(_error_payload(exc), ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(_result_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
