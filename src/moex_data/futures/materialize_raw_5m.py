from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
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
TARGET_SERIES_TYPE: Final[str] = "native"
TARGET_GRANULARITY: Final[str] = "5m"
SOURCE_CANDIDATE_APIM_TRADESTATS: Final[str] = "MOEX_ALGOPACK_FO_TRADESTATS"
SOURCE_ENDPOINT_APIM_FO_TRADESTATS: Final[str] = "/iss/datashop/algopack/fo/tradestats.json"
DEFAULT_APIM_BASE_URL: Final[str] = "https://apim.moex.com"
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
    source_path: Path | None
    run_id: str
    source_candidate: str
    source_endpoint: str
    market: str
    board: str
    series_type: str
    granularity: str


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


def _require_run_id(value: str | None) -> str:
    run_id = _require_text(value, "run_id")
    if run_id.startswith(("/", "\\")) or "/" in run_id or "\\" in run_id or run_id in (".", ".."):
        _fail("run_id must be a single safe path token")
    return run_id


def _optional_source_path(value: str | None) -> Path | None:
    if value is None or not str(value).strip():
        return None
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


def _require_source_contract(
    source_candidate: str | None,
    source_endpoint: str | None,
    market: str | None,
    board: str | None,
    series_type: str | None,
    granularity: str | None,
) -> tuple[str, str, str, str, str, str]:
    checked_candidate = _require_text(source_candidate, "source_candidate")
    checked_endpoint = _require_text(source_endpoint, "source_endpoint")
    checked_market = _require_text(market, "market").upper()
    checked_board = _require_text(board, "board").upper()
    checked_series_type = _require_text(series_type, "series_type")
    checked_granularity = _require_text(granularity, "granularity")
    if checked_candidate != SOURCE_CANDIDATE_APIM_TRADESTATS:
        _fail("source_candidate is not the declared FORTS native 5m source")
    if checked_endpoint != SOURCE_ENDPOINT_APIM_FO_TRADESTATS:
        _fail("source_endpoint does not match the declared APIM tradestats source")
    if checked_market != TARGET_MARKET:
        _fail("market does not match controlled target partition")
    if checked_board != TARGET_BOARD:
        _fail("board does not match controlled target partition")
    if checked_series_type != TARGET_SERIES_TYPE:
        _fail("series_type does not match controlled target partition")
    if checked_granularity != TARGET_GRANULARITY:
        _fail("granularity does not match controlled target partition")
    return checked_candidate, checked_endpoint, checked_market, checked_board, checked_series_type, checked_granularity


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
    *,
    source_candidate: str | None = None,
    source_endpoint: str | None = None,
    market: str | None = None,
    board: str | None = None,
    series_type: str | None = None,
    granularity: str | None = None,
) -> Raw5mMaterializationRequest:
    checked_dataset_id = _require_text(dataset_id, "dataset_id")
    checked_contract_id = _require_text(contract_id, "contract_id")
    checked_trade_date = _require_text(trade_date, "trade_date")
    checked_family = _require_text(family, "family")
    checked_secid = _require_text(secid, "secid")
    _require_exact_target(checked_dataset_id, checked_contract_id, checked_trade_date, checked_family, checked_secid)
    checked_candidate, checked_endpoint, checked_market, checked_board, checked_series_type, checked_granularity = _require_source_contract(
        source_candidate=source_candidate,
        source_endpoint=source_endpoint,
        market=market,
        board=board,
        series_type=series_type,
        granularity=granularity,
    )
    return Raw5mMaterializationRequest(
        repo_root=Path(repo_root),
        dataset_id=checked_dataset_id,
        contract_id=checked_contract_id,
        trade_date=checked_trade_date,
        family=checked_family,
        secid=checked_secid,
        source_path=_optional_source_path(source_path),
        run_id=_require_run_id(run_id),
        source_candidate=checked_candidate,
        source_endpoint=checked_endpoint,
        market=checked_market,
        board=checked_board,
        series_type=checked_series_type,
        granularity=checked_granularity,
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


def _canonical_column(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    by_upper = {str(column).upper(): column for column in df.columns}
    for candidate in candidates:
        found = by_upper.get(candidate.upper())
        if found is not None:
            return found
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
    values: list[str | None] = []
    for date_value, time_value in zip(dates.tolist(), frame[time_col].astype(str).str.strip().tolist()):
        if not date_value or not time_value or time_value.lower() == "nan":
            values.append(None)
        elif len(time_value) >= 10 and "-" in time_value[:10]:
            values.append(time_value)
        else:
            values.append(date_value + " " + time_value)
    return pd.to_datetime(pd.Series(values), errors="coerce")


def _block_to_frame(data: Mapping[str, object]) -> pd.DataFrame:
    for block in ("data", "tradestats"):
        raw = data.get(block)
        if isinstance(raw, Mapping):
            columns = raw.get("columns") or []
            rows = raw.get("data") or []
            if isinstance(columns, list) and isinstance(rows, list) and columns:
                return pd.DataFrame(rows, columns=columns)
    for raw in data.values():
        if isinstance(raw, Mapping):
            columns = raw.get("columns") or []
            rows = raw.get("data") or []
            if isinstance(columns, list) and isinstance(rows, list) and columns:
                return pd.DataFrame(rows, columns=columns)
    return pd.DataFrame()


def _auth_headers(env: Mapping[str, str] | None) -> dict[str, str]:
    active_env = os.environ if env is None else env
    user_agent = str(active_env.get("MOEX_UA", "moex_bot_controlled_raw_5m_materialization/1.0")).strip()
    headers = {"User-Agent": user_agent or "moex_bot_controlled_raw_5m_materialization/1.0"}
    token = str(active_env.get("MOEX_API_KEY", "")).strip()
    if token:
        headers["Authorization"] = "Bearer " + token
    return headers


def _apim_base_url(apim_base_url: str | None, env: Mapping[str, str] | None) -> str:
    active_env = os.environ if env is None else env
    value = str(apim_base_url or active_env.get("MOEX_API_URL", DEFAULT_APIM_BASE_URL)).strip()
    if not value:
        _fail("MOEX_API_URL is required for declared APIM source")
    return value.rstrip("/")


def _source_url(base_url: str, endpoint: str) -> str:
    return base_url.rstrip("/") + "/" + endpoint.lstrip("/")


def _fetch_apim_tradestats_frame(request: Raw5mMaterializationRequest, timeout: float, apim_base_url: str | None, env: Mapping[str, str] | None) -> tuple[pd.DataFrame, str]:
    base_url = _apim_base_url(apim_base_url, env)
    url = _source_url(base_url, request.source_endpoint)
    params = {"secid": request.secid, "from": request.trade_date, "till": request.trade_date, "iss.meta": "off"}
    response = requests.get(url, params=params, headers=_auth_headers(env), timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, Mapping):
        _fail("APIM tradestats response JSON root is not an object")
    frame = _block_to_frame(data)
    source_url = str(getattr(response, "url", url))
    if frame.empty:
        _fail("APIM tradestats response returned no rows")
    return frame, source_url


def _normalize_apim_tradestats(frame: pd.DataFrame, request: Raw5mMaterializationRequest, source_url: str, ingest_ts: str) -> pd.DataFrame:
    date_col = _canonical_column(frame, ("tradedate", "TRADEDATE", "date", "DATE"))
    time_col = _canonical_column(frame, ("tradetime", "TRADETIME", "time", "TIME", "moment", "MOMENT"))
    source_secid_col = _canonical_column(frame, ("secid", "SECID"))
    open_col = _canonical_column(frame, ("pr_open", "PR_OPEN", "open", "OPEN"))
    high_col = _canonical_column(frame, ("pr_high", "PR_HIGH", "high", "HIGH"))
    low_col = _canonical_column(frame, ("pr_low", "PR_LOW", "low", "LOW"))
    close_col = _canonical_column(frame, ("pr_close", "PR_CLOSE", "close", "CLOSE"))
    volume_col = _canonical_column(frame, ("vol", "VOL", "volume", "VOLUME", "qty", "QTY"))
    value_col = _canonical_column(frame, ("val", "VAL", "value", "VALUE", "turnover", "TURNOVER"))
    trades_col = _canonical_column(frame, ("trades", "TRADES", "num_trades", "NUM_TRADES", "numtrades", "NUMTRADES"))
    required = {
        "trade_date": date_col,
        "source_secid": source_secid_col,
        "open": open_col,
        "high": high_col,
        "low": low_col,
        "close": close_col,
        "volume": volume_col,
    }
    missing = tuple(name for name, column in required.items() if column is None)
    if missing:
        _fail("APIM tradestats response missing required columns: " + ",".join(missing))
    assert date_col is not None
    assert source_secid_col is not None
    assert open_col is not None
    assert high_col is not None
    assert low_col is not None
    assert close_col is not None
    assert volume_col is not None
    requested_secid = request.secid.upper()
    identity = frame[source_secid_col].astype(str).str.strip().str.upper()
    work = frame.loc[identity == requested_secid].copy().reset_index(drop=True)
    if work.empty:
        _fail("APIM tradestats response contains no rows for requested secid")
    output = pd.DataFrame()
    output["trade_date"] = work[date_col].map(_parse_trade_date)
    output["ts"] = _combine_ts(work, date_col, time_col)
    output["session_date"] = output["trade_date"]
    output["secid"] = request.secid
    output["family"] = request.family
    output["board"] = request.board
    output["open"] = pd.to_numeric(work[open_col], errors="coerce")
    output["high"] = pd.to_numeric(work[high_col], errors="coerce")
    output["low"] = pd.to_numeric(work[low_col], errors="coerce")
    output["close"] = pd.to_numeric(work[close_col], errors="coerce")
    output["volume"] = pd.to_numeric(work[volume_col], errors="coerce")
    output["value"] = pd.to_numeric(work[value_col], errors="coerce") if value_col else pd.NA
    output["num_trades"] = pd.to_numeric(work[trades_col], errors="coerce") if trades_col else pd.NA
    output["source"] = request.source_candidate
    output["source_endpoint_url"] = source_url
    output["ingest_ts"] = ingest_ts
    return output.sort_values(["ts", "secid"]).reset_index(drop=True)


def _source_table_for_request(request: Raw5mMaterializationRequest, timeout: float, apim_base_url: str | None, env: Mapping[str, str] | None) -> tuple[pd.DataFrame, dict[str, object]]:
    if request.source_path is not None:
        source_table = _load_source_table(request.source_path)
        return source_table, {
            "source_fetch_mode": "declared_source_path",
            "source_path": request.source_path.as_posix(),
            "source_candidate": request.source_candidate,
            "source_endpoint": request.source_endpoint,
        }
    frame, source_url = _fetch_apim_tradestats_frame(request, timeout=timeout, apim_base_url=apim_base_url, env=env)
    normalized = _normalize_apim_tradestats(frame, request, source_url, _utc_now())
    return normalized, {
        "source_fetch_mode": "declared_apim_tradestats",
        "source_candidate": request.source_candidate,
        "source_endpoint": request.source_endpoint,
        "source_endpoint_url": source_url,
    }


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
        "notes": "single_partition_apim_tradestats_materialization",
    }


def _source_contract_manifest(request: Raw5mMaterializationRequest, source_info: Mapping[str, object]) -> dict[str, object]:
    return {
        "source_candidate": request.source_candidate,
        "source_endpoint": request.source_endpoint,
        "source_endpoint_url": source_info.get("source_endpoint_url"),
        "source_fetch_mode": source_info.get("source_fetch_mode"),
        "source_path": source_info.get("source_path"),
        "market": request.market,
        "board": request.board,
        "family": request.family,
        "secid": request.secid,
        "trade_date": request.trade_date,
        "granularity": request.granularity,
        "series_type": request.series_type,
        "failure_semantics": {
            "empty_response": "fail_closed",
            "schema_mismatch": "fail_closed",
            "missing_required_contract_input": "fail_closed",
            "implicit_fallback": "forbidden",
            "iss_candles_masking": "forbidden",
        },
    }


def _manifest(request: Raw5mMaterializationRequest, paths: Raw5mMaterializationPaths, source_info: Mapping[str, object]) -> dict[str, object]:
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
        "source_contract": _source_contract_manifest(request, source_info),
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
    *,
    source_candidate: str | None = None,
    source_endpoint: str | None = None,
    market: str | None = None,
    board: str | None = None,
    series_type: str | None = None,
    granularity: str | None = None,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
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
            source_candidate=source_candidate,
            source_endpoint=source_endpoint,
            market=market,
            board=board,
            series_type=series_type,
            granularity=granularity,
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
        source_table, source_info = _source_table_for_request(request, timeout=timeout, apim_base_url=apim_base_url, env=env)
        output_table, metrics = _validate_source_table(source_table, request.trade_date, request.family, request.secid)
        quality_row = _quality_row(request, metrics)
        validate_quality_report_rows([quality_row])
        quality_report = {"run_id": request.run_id, "rows": [quality_row]}
        manifest = _manifest(request, paths, source_info)

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
            source_candidate=request.source_candidate,
            source_endpoint=request.source_endpoint,
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
        "source_candidate": result.source_candidate,
        "source_endpoint": result.source_endpoint,
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
    parser.add_argument("--market", required=True)
    parser.add_argument("--board", required=True)
    parser.add_argument("--series-type", required=True)
    parser.add_argument("--granularity", required=True)
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
        result = materialize_single_raw_5m_partition(
            repo_root=args.repo_root,
            dataset_id=args.dataset_id,
            contract_id=args.contract_id,
            trade_date=args.trade_date,
            family=args.family,
            secid=args.secid,
            source_path=args.source_path,
            run_id=args.run_id,
            source_candidate=args.source_candidate,
            source_endpoint=args.source_endpoint,
            market=args.market,
            board=args.board,
            series_type=args.series_type,
            granularity=args.granularity,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
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
