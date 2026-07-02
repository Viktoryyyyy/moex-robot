from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Final

import pandas as pd
import requests

from .contract_io import FuturesContractIoError, expand_contract_path, load_futures_data_lake_contract_package, reject_dynamic_markers
from .manifest import validate_refresh_manifest_values
from .quality import validate_quality_report_rows
from .schemas import EXPECTED_DATASET_CONTRACT_IDS, FuturesDatasetContract

TARGET_DATASET_ID: Final[str] = "futures_raw_5m"
TARGET_CONTRACT_ID: Final[str] = "futures_raw_5m.v1"
TARGET_TRADE_DATE: Final[str] = "2026-06-02"
TARGET_FAMILY: Final[str] = "Si"
TARGET_SECID: Final[str] = "SiM6"
TARGET_MARKET: Final[str] = "FORTS"
TARGET_BOARD: Final[str] = "RFUD"
TARGET_GRANULARITY: Final[str] = "5m"
TARGET_SERIES_TYPE: Final[str] = "native"
SOURCE_CANDIDATE_APIM_TRADESTATS: Final[str] = "MOEX_ALGOPACK_FO_TRADESTATS"
SOURCE_ENDPOINT_APIM_FO_TRADESTATS: Final[str] = "/iss/datashop/algopack/fo/tradestats.json"
DEFAULT_APIM_BASE_URL: Final[str] = "https://apim.moex.com"
MANIFEST_DATASET_ID: Final[str] = "futures_data_refresh_manifest"
QUALITY_DATASET_ID: Final[str] = "futures_quality_report"
BLOCKED_NO_SOURCE_STATUS: Final[str] = "blocked_no_source_artifact"
VALIDATION_FAILED_STATUS: Final[str] = "failed_validation"
SUCCEEDED_STATUS: Final[str] = "succeeded"

RAW_5M_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "instrument_id", "trade_date", "ts", "session_date", "secid", "board", "market", "engine", "source_id",
    "open", "high", "low", "close", "volume", "value", "num_trades", "source", "ingest_ts",
)
_OHLC_COLUMNS: Final[tuple[str, ...]] = ("open", "high", "low", "close")


class FuturesRaw5mMaterializationError(ValueError):
    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class Raw5mMaterializationRequest:
    repo_root: str | Path
    dataset_id: str
    contract_id: str
    trade_date: str
    family: str | None = None
    secid: str | None = None
    source_path: Path | None = None
    run_id: str | None = None
    source_candidate: str | None = None
    source_endpoint: str | None = None
    market: str | None = None
    board: str | None = None
    series_type: str | None = None
    granularity: str | None = None
    instrument_id: str | None = None
    source_id: str | None = None
    engine: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.repo_root, Path):
            object.__setattr__(self, "repo_root", Path(self.repo_root))
        if self.instrument_id is None:
            object.__setattr__(self, "instrument_id", self.family or self.secid)
        if self.source_id is None:
            object.__setattr__(self, "source_id", self.secid)
        if self.engine is None:
            object.__setattr__(self, "engine", "legacy")


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
    source_candidate: str
    source_endpoint: str


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


def _optional_text(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _require_trade_date(value: str) -> str:
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        _fail("trade_date must be an explicit YYYY-MM-DD date")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise FuturesRaw5mMaterializationError(VALIDATION_FAILED_STATUS, "trade_date must be an explicit YYYY-MM-DD date") from exc


def _require_run_id(value: str | None) -> str:
    run_id = _require_text(value, "run_id")
    if run_id.startswith(("/", "\\")) or "/" in run_id or "\\" in run_id or run_id in (".", ".."):
        _fail("run_id must be a single safe path token")
    return run_id


def _optional_source_path(value: str | None) -> Path | None:
    if value is None or not str(value).strip():
        return None
    source_path = Path(_require_text(value, "source_path"))
    if not source_path.exists() or not source_path.is_file():
        _fail("source_path does not exist", BLOCKED_NO_SOURCE_STATUS)
    return source_path


def _contract_for(package_contracts: Mapping[str, FuturesDatasetContract], dataset_id: str, contract_id: str) -> FuturesDatasetContract:
    contract = package_contracts.get(dataset_id)
    if contract is None:
        _fail("unsupported dataset_id")
    if contract.contract_id != contract_id:
        _fail("contract_id does not match dataset_id")
    return contract


def _env_root(env: Mapping[str, str] | None) -> str:
    root = (os.environ if env is None else env).get("MOEX_DATA_ROOT")
    if not root or not root.strip():
        _fail("MOEX_DATA_ROOT is required")
    return root


def _coalesce_required(primary: str | None, fallback: str | None, field_name: str) -> str:
    return _require_text(primary or fallback, field_name)


def _require_source_contract(source_candidate: str | None, source_endpoint: str | None, market: str | None, board: str | None, engine: str | None, series_type: str | None, granularity: str | None) -> tuple[str, str, str, str, str, str, str]:
    candidate = _require_text(source_candidate, "source_candidate")
    endpoint = _require_text(source_endpoint, "source_endpoint")
    checked_market = _require_text(market, "market").upper()
    checked_board = _require_text(board, "board").upper()
    checked_engine = _require_text(engine, "engine") if engine is not None else "legacy"
    checked_series_type = _require_text(series_type, "series_type")
    checked_granularity = _require_text(granularity, "granularity")
    if candidate != SOURCE_CANDIDATE_APIM_TRADESTATS:
        _fail("source_candidate is not the declared native 5m source")
    if endpoint != SOURCE_ENDPOINT_APIM_FO_TRADESTATS:
        _fail("source_endpoint does not match the declared APIM tradestats source")
    if checked_series_type != TARGET_SERIES_TYPE:
        _fail("series_type does not match raw 5m native materialization")
    if checked_granularity != TARGET_GRANULARITY:
        _fail("granularity does not match raw 5m materialization")
    return candidate, endpoint, checked_market, checked_board, checked_engine, checked_series_type, checked_granularity


def _require_exact_target(dataset_id: str, contract_id: str, trade_date: str) -> None:
    if dataset_id != TARGET_DATASET_ID:
        _fail("unsupported dataset_id")
    if contract_id != TARGET_CONTRACT_ID:
        _fail("contract_id does not match controlled materialization package")
    _require_trade_date(trade_date)


def build_materialization_request(repo_root: str | Path, dataset_id: str, contract_id: str, trade_date: str, family: str | None = None, secid: str | None = None, source_path: str | None = None, run_id: str | None = None, *, instrument_id: str | None = None, source_id: str | None = None, source_candidate: str | None = None, source_endpoint: str | None = None, market: str | None = None, board: str | None = None, engine: str | None = None, series_type: str | None = None, granularity: str | None = None) -> Raw5mMaterializationRequest:
    dataset = _require_text(dataset_id, "dataset_id")
    contract = _require_text(contract_id, "contract_id")
    checked_trade_date = _require_trade_date(_require_text(trade_date, "trade_date"))
    _require_exact_target(dataset, contract, checked_trade_date)
    checked_family = _optional_text(family, "family")
    checked_secid = _require_text(secid, "secid")
    checked_instrument_id = _coalesce_required(instrument_id, checked_family, "instrument_id")
    checked_source_id = _coalesce_required(source_id, checked_secid, "source_id")
    candidate, endpoint, checked_market, checked_board, checked_engine, checked_series_type, checked_granularity = _require_source_contract(source_candidate, source_endpoint, market, board, engine, series_type, granularity)
    return Raw5mMaterializationRequest(Path(repo_root), dataset, contract, checked_trade_date, checked_family, checked_secid, _optional_source_path(source_path), _require_run_id(run_id), candidate, endpoint, checked_market, checked_board, checked_series_type, checked_granularity, checked_instrument_id, checked_source_id, checked_engine)


def materialization_target_paths(repo_root: str | Path, dataset_id: str, contract_id: str, trade_date: str, family: str | None = None, secid: str | None = None, run_id: str | None = None, env: Mapping[str, str] | None = None, *, instrument_id: str | None = None, source_id: str | None = None) -> Raw5mMaterializationPaths:
    dataset = _require_text(dataset_id, "dataset_id")
    contract = _require_text(contract_id, "contract_id")
    checked_trade_date = _require_trade_date(_require_text(trade_date, "trade_date"))
    checked_family = _optional_text(family, "family")
    checked_secid = _optional_text(secid, "secid")
    checked_instrument_id = _coalesce_required(instrument_id, checked_family, "instrument_id")
    checked_source_id = _coalesce_required(source_id, checked_secid, "source_id")
    checked_run_id = _require_run_id(run_id)
    _require_exact_target(dataset, contract, checked_trade_date)
    package = load_futures_data_lake_contract_package(repo_root)
    root = _env_root(env)
    raw_contract = _contract_for(package.contracts_by_dataset_id, dataset, contract)
    manifest_contract = _contract_for(package.contracts_by_dataset_id, MANIFEST_DATASET_ID, "futures_data_refresh_manifest.v1")
    quality_contract = _contract_for(package.contracts_by_dataset_id, QUALITY_DATASET_ID, "futures_quality_report.v1")
    placeholders = {"YYYY-MM-DD": checked_trade_date, "FAMILY": checked_family, "SECID": checked_secid, "INSTRUMENT_ID": checked_instrument_id, "SOURCE_ID": checked_source_id, "RUN_ID": checked_run_id}
    try:
        return Raw5mMaterializationPaths(
            expand_contract_path(raw_contract.path_pattern, root, placeholders),
            expand_contract_path(manifest_contract.path_pattern, root, placeholders),
            expand_contract_path(quality_contract.path_pattern, root, placeholders),
        )
    except FuturesContractIoError as exc:
        raise FuturesRaw5mMaterializationError(VALIDATION_FAILED_STATUS, str(exc)) from exc


def _load_source_table(source_path: Path) -> pd.DataFrame:
    if source_path.suffix.casefold() == ".parquet":
        return pd.read_parquet(source_path)
    if source_path.suffix.casefold() == ".csv":
        return pd.read_csv(source_path)
    _fail("source_path must point to a .parquet or .csv file")


def _timestamp_strings(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.isna().any():
        _fail("ts contains invalid timestamp values")
    return timestamps


def _gap_count(timestamps: pd.Series) -> int:
    return int((timestamps.sort_values().diff().dropna() > pd.Timedelta(minutes=5)).sum())


def _canonical_column(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    by_upper = {str(column).upper(): column for column in df.columns}
    for candidate in candidates:
        if candidate.upper() in by_upper:
            return by_upper[candidate.upper()]
    return None


def _parse_trade_date(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return pd.to_datetime(text).date().isoformat()
    except Exception:
        return text[:10] if len(text) >= 10 else None


def _combine_ts(frame: pd.DataFrame, date_col: str, time_col: str | None) -> pd.Series:
    dates = frame[date_col].map(_parse_trade_date)
    if time_col is None:
        return pd.to_datetime(dates, errors="coerce")
    values = []
    for date_value, time_value in zip(dates.tolist(), frame[time_col].astype(str).str.strip().tolist()):
        if not date_value or not time_value or time_value.lower() == "nan":
            values.append(None)
        elif len(time_value) >= 10 and "-" in time_value[:10]:
            values.append(time_value)
        else:
            values.append(date_value + " " + time_value)
    return pd.to_datetime(pd.Series(values), errors="coerce")


def _block_to_frame(data: Mapping[str, object]) -> pd.DataFrame:
    for raw in (data.get("data"), data.get("tradestats"), *data.values()):
        if isinstance(raw, Mapping):
            columns = raw.get("columns") or []
            rows = raw.get("data") or []
            if isinstance(columns, list) and isinstance(rows, list) and columns:
                return pd.DataFrame(rows, columns=columns)
    return pd.DataFrame()


def _auth_headers(env: Mapping[str, str] | None) -> dict[str, str]:
    user_agent = str((os.environ if env is None else env).get("MOEX_UA", "moex_bot_controlled_raw_5m_materialization/1.0")).strip()
    return {"User-Agent": user_agent or "moex_bot_controlled_raw_5m_materialization/1.0"}


def _apim_base_url(apim_base_url: str | None, env: Mapping[str, str] | None) -> str:
    value = str(apim_base_url or (os.environ if env is None else env).get("MOEX_API_URL", DEFAULT_APIM_BASE_URL)).strip()
    if not value:
        _fail("MOEX_API_URL is required for declared APIM source")
    return value.rstrip("/")


def _source_url(base_url: str, endpoint: str) -> str:
    return base_url.rstrip("/") + "/" + endpoint.lstrip("/")


def _fetch_apim_tradestats_frame(request: Raw5mMaterializationRequest, timeout: float, apim_base_url: str | None, env: Mapping[str, str] | None) -> tuple[pd.DataFrame, str]:
    url = _source_url(_apim_base_url(apim_base_url, env), str(request.source_endpoint))
    params = {"date": request.trade_date, "from": request.trade_date, "till": request.trade_date, "secid": request.secid, "start": 0, "iss.meta": "off", "iss.only": "tradestats"}
    response = requests.get(url, params=params, headers=_auth_headers(env), timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, Mapping):
        _fail("APIM tradestats response JSON root is not an object")
    frame = _block_to_frame(data)
    if frame.empty:
        _fail("APIM tradestats response returned no rows")
    return frame, str(getattr(response, "url", url))


def _normalize_apim_tradestats(frame: pd.DataFrame, request: Raw5mMaterializationRequest, source_url: str, ingest_ts: str) -> pd.DataFrame:
    date_col = _canonical_column(frame, ("tradedate", "date"))
    time_col = _canonical_column(frame, ("tradetime", "time", "moment"))
    secid_col = _canonical_column(frame, ("secid",))
    open_col = _canonical_column(frame, ("pr_open", "open"))
    high_col = _canonical_column(frame, ("pr_high", "high"))
    low_col = _canonical_column(frame, ("pr_low", "low"))
    close_col = _canonical_column(frame, ("pr_close", "close"))
    volume_col = _canonical_column(frame, ("vol", "volume", "qty"))
    value_col = _canonical_column(frame, ("val", "value", "turnover"))
    trades_col = _canonical_column(frame, ("trades", "num_trades", "numtrades"))
    missing = tuple(name for name, column in {"trade_date": date_col, "source_secid": secid_col, "open": open_col, "high": high_col, "low": low_col, "close": close_col, "volume": volume_col}.items() if column is None)
    if missing:
        _fail("APIM tradestats response missing required columns: " + ",".join(missing))
    assert date_col and secid_col and open_col and high_col and low_col and close_col and volume_col
    work = frame.loc[frame[secid_col].astype(str).str.strip().str.upper() == str(request.secid).upper()].copy().reset_index(drop=True)
    if work.empty:
        _fail("APIM tradestats response contains no rows for requested secid")
    work["_parsed_trade_date"] = work[date_col].map(_parse_trade_date)
    work = work.loc[work["_parsed_trade_date"] == request.trade_date].copy().reset_index(drop=True)
    if work.empty:
        _fail("APIM tradestats response contains no rows for requested secid/date")
    output = pd.DataFrame(index=work.index)
    output["instrument_id"] = request.instrument_id
    output["trade_date"] = work["_parsed_trade_date"]
    output["ts"] = _combine_ts(work, date_col, time_col)
    output["session_date"] = output["trade_date"]
    output["secid"] = request.secid
    output["board"] = request.board
    output["market"] = request.market
    output["engine"] = request.engine
    output["source_id"] = request.source_id
    output["open"] = pd.to_numeric(work[open_col], errors="coerce")
    output["high"] = pd.to_numeric(work[high_col], errors="coerce")
    output["low"] = pd.to_numeric(work[low_col], errors="coerce")
    output["close"] = pd.to_numeric(work[close_col], errors="coerce")
    output["volume"] = pd.to_numeric(work[volume_col], errors="coerce")
    output["value"] = pd.to_numeric(work[value_col], errors="coerce") if value_col else pd.NA
    output["num_trades"] = pd.to_numeric(work[trades_col], errors="coerce") if trades_col else pd.NA
    output["source"] = request.source_candidate
    output["ingest_ts"] = ingest_ts
    if request.family is not None:
        output["family"] = request.family
    return output.sort_values(["ts", "instrument_id", "source_id"]).reset_index(drop=True)


def _source_table_for_request(request: Raw5mMaterializationRequest, timeout: float, apim_base_url: str | None, env: Mapping[str, str] | None) -> tuple[pd.DataFrame, dict[str, object]]:
    if request.source_path is not None:
        return _load_source_table(request.source_path), {"source_fetch_mode": "declared_source_path", "source_path": request.source_path.as_posix(), "source_candidate": request.source_candidate, "source_endpoint": request.source_endpoint}
    frame, url = _fetch_apim_tradestats_frame(request, timeout, apim_base_url, env)
    return _normalize_apim_tradestats(frame, request, url, _utc_now()), {"source_fetch_mode": "declared_apim_tradestats", "source_candidate": request.source_candidate, "source_endpoint": request.source_endpoint, "source_endpoint_url": url}


def _column_missing_or_empty(frame: pd.DataFrame, column: str) -> bool:
    if column not in frame.columns:
        return True
    text = frame[column].astype(str).str.strip().str.lower()
    return bool(frame[column].isna().all() or text.isin(("", "none", "nan", "<na>")).all())


def _with_canonical_identity_columns(df: pd.DataFrame, request: Raw5mMaterializationRequest) -> pd.DataFrame:
    output = df.copy()
    for column, value in {"instrument_id": request.instrument_id, "source_id": request.source_id, "market": request.market, "engine": request.engine, "board": request.board, "secid": request.secid}.items():
        if _column_missing_or_empty(output, column):
            output[column] = value
    if "family" not in output.columns and request.family is not None:
        output["family"] = request.family
    return output


def _request_from_legacy_args(trade_date: str, family: str, secid: str) -> Raw5mMaterializationRequest:
    return Raw5mMaterializationRequest(Path.cwd(), TARGET_DATASET_ID, TARGET_CONTRACT_ID, trade_date, family, secid, None, "legacy_validation", SOURCE_CANDIDATE_APIM_TRADESTATS, SOURCE_ENDPOINT_APIM_FO_TRADESTATS, TARGET_MARKET, TARGET_BOARD, TARGET_SERIES_TYPE, TARGET_GRANULARITY)


def _validate_source_table(df: pd.DataFrame, request_or_trade_date: Raw5mMaterializationRequest | str, family: str | None = None, secid: str | None = None) -> tuple[pd.DataFrame, dict[str, object]]:
    request = request_or_trade_date if isinstance(request_or_trade_date, Raw5mMaterializationRequest) else _request_from_legacy_args(str(request_or_trade_date), str(family), str(secid))
    work = _with_canonical_identity_columns(df, request)
    missing = tuple(column for column in RAW_5M_REQUIRED_COLUMNS if column not in work.columns)
    if missing:
        _fail("source table is missing required raw 5m columns")
    if work.empty:
        _fail("source table is empty")
    output = work.loc[:, RAW_5M_REQUIRED_COLUMNS].copy()
    if not (output["trade_date"].astype(str) == request.trade_date).all():
        _fail("source trade_date values do not match target partition")
    if not (output["instrument_id"].astype(str) == str(request.instrument_id)).all():
        _fail("source instrument_id values do not match target partition")
    if not (output["source_id"].astype(str) == str(request.source_id)).all():
        _fail("source source_id values do not match target partition")
    if not (output["secid"].astype(str) == str(request.secid)).all():
        _fail("source secid values do not match target partition")
    for column in _OHLC_COLUMNS:
        output[column] = pd.to_numeric(output[column], errors="coerce")
    null_ohlc_count = int(output.loc[:, _OHLC_COLUMNS].isna().any(axis=1).sum())
    if null_ohlc_count:
        _fail("source table contains null OHLC values")
    invalid_ohlc_count = int((output["high"] < output["low"]).sum())
    if invalid_ohlc_count:
        _fail("source table contains high lower than low")
    if bool(((output["open"] < output["low"]) | (output["open"] > output["high"]) | (output["close"] < output["low"]) | (output["close"] > output["high"])).any()):
        _fail("source table contains open or close outside high-low range")
    duplicate_key_count = int(output.duplicated(subset=["instrument_id", "ts", "source_id"]).sum())
    if duplicate_key_count:
        _fail("source table contains duplicate instrument_id/ts/source_id keys")
    timestamps = _timestamp_strings(output["ts"])
    for _, group_index in output.groupby(["instrument_id", "source_id"], sort=False).groups.items():
        if not bool(timestamps.loc[group_index].is_monotonic_increasing):
            _fail("source table contains non-monotonic ts by instrument_id/source_id")
    metrics = {"rows": int(len(output.index)), "min_ts": str(timestamps.min()), "max_ts": str(timestamps.max()), "duplicate_key_count": duplicate_key_count, "gap_count": _gap_count(timestamps), "null_ohlc_count": null_ohlc_count, "invalid_ohlc_count": invalid_ohlc_count}
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
    temporary_path = path.with_name("." + path.name + "." + run_id + ".tmp")
    try:
        df.to_parquet(temporary_path, index=False)
        temporary_path.replace(path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _quality_family(request: Raw5mMaterializationRequest) -> str:
    return request.family or str(request.instrument_id)


def _quality_row(request: Raw5mMaterializationRequest, metrics: Mapping[str, object]) -> dict[str, object]:
    return {"run_id": request.run_id, "dataset_id": request.dataset_id, "family": _quality_family(request), "secid": request.secid, "trade_date": request.trade_date, "rows": int(metrics["rows"]), "min_ts": str(metrics["min_ts"]), "max_ts": str(metrics["max_ts"]), "duplicate_key_count": int(metrics["duplicate_key_count"]), "gap_count": int(metrics["gap_count"]), "null_ohlc_count": int(metrics["null_ohlc_count"]), "invalid_ohlc_count": int(metrics["invalid_ohlc_count"]), "futoi_missing_count": 0, "calendar_status": "not_checked", "quality_status": "pass", "notes": "single_partition_apim_tradestats_materialization"}


def _source_contract_manifest(request: Raw5mMaterializationRequest, source_info: Mapping[str, object]) -> dict[str, object]:
    return {"source_candidate": request.source_candidate, "source_endpoint": request.source_endpoint, "source_endpoint_url": source_info.get("source_endpoint_url"), "source_fetch_mode": source_info.get("source_fetch_mode"), "source_path": source_info.get("source_path"), "instrument_id": request.instrument_id, "source_id": request.source_id, "market": request.market, "board": request.board, "engine": request.engine, "family": request.family, "secid": request.secid, "trade_date": request.trade_date, "granularity": request.granularity, "series_type": request.series_type, "failure_semantics": {"empty_response": "fail_closed", "schema_mismatch": "fail_closed", "missing_required_contract_input": "fail_closed", "implicit_fallback": "forbidden", "iss_candles_masking": "forbidden"}}


def _manifest(request: Raw5mMaterializationRequest, paths: Raw5mMaterializationPaths, source_info: Mapping[str, object]) -> dict[str, object]:
    now = _utc_now()
    values = {"run_id": request.run_id, "run_date": request.trade_date, "requested_from": request.trade_date, "requested_till": request.trade_date, "instrument_scope": [request.instrument_id], "family_scope": [_quality_family(request)], "dataset_contract_refs": list(EXPECTED_DATASET_CONTRACT_IDS), "partitions_written": [paths.partition_path.as_posix()], "partitions_skipped": [], "quality_report_ref": paths.quality_report_path.as_posix(), "refresh_status": SUCCEEDED_STATUS, "started_at": now, "finished_at": now, "source_contract": _source_contract_manifest(request, source_info)}
    validate_refresh_manifest_values(values)
    return values


def materialize_single_raw_5m_partition(repo_root: str | Path, dataset_id: str, contract_id: str, trade_date: str, family: str | None = None, secid: str | None = None, source_path: str | None = None, run_id: str | None = None, env: Mapping[str, str] | None = None, *, instrument_id: str | None = None, source_id: str | None = None, source_candidate: str | None = None, source_endpoint: str | None = None, market: str | None = None, board: str | None = None, engine: str | None = None, series_type: str | None = None, granularity: str | None = None, timeout: float = 60.0, apim_base_url: str | None = None) -> Raw5mMaterializationResult:
    try:
        request = build_materialization_request(repo_root, dataset_id, contract_id, trade_date, family, secid, source_path, run_id, instrument_id=instrument_id, source_id=source_id, source_candidate=source_candidate, source_endpoint=source_endpoint, market=market, board=board, engine=engine, series_type=series_type, granularity=granularity)
        paths = materialization_target_paths(request.repo_root, request.dataset_id, request.contract_id, request.trade_date, request.family, request.secid, request.run_id, env, instrument_id=request.instrument_id, source_id=request.source_id)
        source_table, source_info = _source_table_for_request(request, timeout, apim_base_url, env)
        output_table, metrics = _validate_source_table(source_table, request)
        quality_row = _quality_row(request, metrics)
        validate_quality_report_rows([quality_row])
        quality_report = {"run_id": request.run_id, "rows": [quality_row]}
        manifest = _manifest(request, paths, source_info)
        _write_parquet_atomic(paths.partition_path, output_table, str(request.run_id))
        _write_json_atomic(paths.quality_report_path, quality_report)
        _write_json_atomic(paths.manifest_path, manifest)
        return Raw5mMaterializationResult(SUCCEEDED_STATUS, int(metrics["rows"]), paths.partition_path, paths.manifest_path, paths.quality_report_path, "pass", str(request.source_candidate), str(request.source_endpoint))
    except Exception as exc:
        raise _as_materialization_error(exc) from exc


def _result_payload(result: Raw5mMaterializationResult) -> dict[str, object]:
    return {"status": result.status, "rows": result.rows, "partition_path": result.partition_path.as_posix(), "manifest_path": result.manifest_path.as_posix(), "quality_report_path": result.quality_report_path.as_posix(), "quality_status": result.quality_status, "source_candidate": result.source_candidate, "source_endpoint": result.source_endpoint}


def _error_payload(error: FuturesRaw5mMaterializationError) -> dict[str, object]:
    return {"status": error.status, "message": error.message}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize one controlled futures_raw_5m partition")
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--board", required=True)
    parser.add_argument("--engine", required=True)
    parser.add_argument("--series-type", required=True)
    parser.add_argument("--granularity", required=True)
    parser.add_argument("--family", default=None)
    parser.add_argument("--source-candidate", required=True)
    parser.add_argument("--source-endpoint", required=True)
    parser.add_argument("--source-path", default=None)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = materialize_single_raw_5m_partition(repo_root=args.repo_root, dataset_id=args.dataset_id, contract_id=args.contract_id, trade_date=args.trade_date, family=args.family, secid=args.secid, source_path=args.source_path, run_id=args.run_id, instrument_id=args.instrument_id, source_id=args.source_id, source_candidate=args.source_candidate, source_endpoint=args.source_endpoint, market=args.market, board=args.board, engine=args.engine, series_type=args.series_type, granularity=args.granularity, timeout=args.timeout, apim_base_url=args.apim_base_url)
    except FuturesRaw5mMaterializationError as exc:
        print(json.dumps(_error_payload(exc), ensure_ascii=False, sort_keys=True))
        return 2 if exc.status == BLOCKED_NO_SOURCE_STATUS else 1
    print(json.dumps(_result_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
