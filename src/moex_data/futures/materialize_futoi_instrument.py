from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

import pandas as pd

from . import algopack_availability_probe as availability
from . import futoi_raw_loader as legacy

DATASET_ID: Final[str] = "futures_futoi_raw"
SOURCE_ID: Final[str] = "moex_algopack_futoi"
SOURCE_CONTRACT_REF: Final[str] = "contracts/sources/futures/moex_algopack_futoi.v1.yaml"
RAW_CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_raw.v1.yaml"
QUALITY_CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_quality_report.v1.yaml"
MANIFEST_CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_refresh_manifest.v1.yaml"
REGISTRY_PATH: Final[str] = "configs/instruments/forts_instrument_registry.v1.yaml"
PRODUCER_ID: Final[str] = "moex_data.futures.materialize_futoi_instrument.v1"
SOURCE_RECORD_KEY_FIELDS: Final[tuple[str, ...]] = (
    "trade_date",
    "sess_id",
    "seqnum",
    "secid",
    "clgroup",
)
POSITION_FIELDS: Final[tuple[str, ...]] = (
    "pos",
    "pos_long",
    "pos_short",
    "pos_long_num",
    "pos_short_num",
)
RETRYABLE_HTTP_STATUS: Final[frozenset[int]] = frozenset({401, 429, 500, 502, 503, 504})
MAX_FETCH_ATTEMPTS: Final[int] = 3


class FutoiMaterializationError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiMaterializationError(message)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(field_name + " must be an explicit safe token")
    return text


def _require_date(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise FutoiMaterializationError(field_name + " must be YYYY-MM-DD") from exc


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    return Path(value)


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        _fail("env_file does not exist")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _parse_scalar(value: str) -> object:
    text = value.strip().strip('"').strip("'")
    if text == "true":
        return True
    if text == "false":
        return False
    return text


def _registry_binding(registry_path: str | Path, instrument_id: str) -> dict[str, object]:
    wanted = _require_token(instrument_id, "instrument_id")
    lines = Path(registry_path).read_text(encoding="utf-8").splitlines()
    current: dict[str, object] | None = None
    in_supplementary = False
    in_futoi = False
    for raw in lines:
        stripped = raw.strip()
        if raw.startswith("  - instrument_id:"):
            if current and current.get("instrument_id") == wanted:
                break
            current = {"instrument_id": _parse_scalar(raw.split(":", 1)[1])}
            in_supplementary = False
            in_futoi = False
            continue
        if current is None:
            continue
        if raw.startswith("    ") and not raw.startswith("      ") and ":" in stripped:
            key, value = stripped.split(":", 1)
            if key == "supplementary_sources":
                in_supplementary = True
                in_futoi = False
                continue
            current[key] = _parse_scalar(value)
            in_supplementary = False
            in_futoi = False
            continue
        if in_supplementary and raw.startswith("      futures_futoi_raw:"):
            in_futoi = True
            continue
        if in_futoi and raw.startswith("        ") and not raw.startswith("          ") and ":" in stripped:
            key, value = stripped.split(":", 1)
            current["futoi." + key] = _parse_scalar(value)
            continue
    if current and current.get("instrument_id") == wanted:
        required = (
            "canonical_symbol",
            "secid",
            "board",
            "market",
            "engine",
            "futoi.source_id",
            "futoi.ticker",
            "futoi.availability_status",
            "futoi.probe_status",
            "futoi.enabled_for_materialization",
        )
        missing = [field for field in required if field not in current]
        if missing:
            _fail("registry FUTOI binding missing fields: " + ",".join(missing))
        return current
    _fail("instrument_id not found in FORTS registry")


def _partition_path(trade_date: str, instrument_id: str, source_id: str) -> Path:
    return (
        _data_root()
        / "market"
        / "supplementary"
        / ("dataset_id=" + DATASET_ID)
        / ("instrument_id=" + instrument_id)
        / ("trade_date=" + trade_date)
        / ("source=" + source_id)
        / "part.parquet"
    )


def _quality_path(trade_date: str, run_id: str) -> Path:
    return _data_root() / "state" / "quality" / ("dataset_id=" + DATASET_ID) / ("run_date=" + trade_date) / ("run_id=" + run_id) / "quality_report.json"


def _manifest_path(trade_date: str, run_id: str) -> Path:
    return _data_root() / "state" / "refresh" / ("dataset_id=" + DATASET_ID) / ("run_date=" + trade_date) / ("run_id=" + run_id) / "manifest.json"


def accepted_pointer_path(instrument_id: str) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temporary_name = handle.name
    Path(temporary_name).replace(path)


def _write_parquet_atomic(path: Path, frame: pd.DataFrame, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name("." + path.name + "." + run_id + ".tmp")
    try:
        frame.to_parquet(temp_path, index=False)
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _fetch_exact(ticker: str, trade_date: str, timeout: float, apim_base_url: str | None) -> tuple[pd.DataFrame, str]:
    token = str(os.environ.get("MOEX_API_KEY", "")).strip()
    if not token:
        _fail("MOEX_API_KEY is required for FUTOI APIM")
    base_url = str(apim_base_url or os.environ.get("MOEX_API_URL", availability.DEFAULT_APIM_BASE_URL)).strip().rstrip("/")
    if not base_url:
        _fail("MOEX_API_URL is required")
    path = "/iss/analyticalproducts/futoi/securities/" + ticker.lower() + ".json"
    frame: pd.DataFrame | None = None
    for attempt in range(MAX_FETCH_ATTEMPTS):
        try:
            frame = availability.fetch_paged_frame(
                base_url,
                path,
                {"from": trade_date, "till": trade_date, "latest": 0},
                "futoi",
                timeout,
                True,
            )
            break
        except Exception as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            if status_code not in RETRYABLE_HTTP_STATUS or attempt + 1 >= MAX_FETCH_ATTEMPTS:
                raise
            time.sleep(0.5 * (attempt + 1))
    if frame is None or frame.empty:
        _fail("FUTOI APIM exact source returned no rows")
    normalized_columns = [str(column).strip().lower() for column in frame.columns]
    if len(normalized_columns) != len(set(normalized_columns)):
        _fail("FUTOI APIM schema contains duplicate columns after case normalization")
    frame = frame.copy()
    frame.columns = normalized_columns
    columns = set(normalized_columns)
    if "error_message" in columns:
        _fail("FUTOI APIM returned ERROR_MESSAGE instead of data")
    required = {
        "sess_id",
        "seqnum",
        "tradedate",
        "tradetime",
        "ticker",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "systime",
    }
    if not required.issubset(columns):
        _fail("FUTOI APIM schema mismatch")
    return frame, availability.url_join(base_url, path)


def _validate_raw_source_rows(frame: pd.DataFrame, trade_date: str, ticker: str) -> pd.DataFrame:
    column_by_name = {str(column).strip().lower(): column for column in frame.columns}
    required = ("tradedate", "tradetime", "ticker", "clgroup", "systime")
    missing = [field for field in required if field not in column_by_name]
    if missing:
        _fail("FUTOI raw source missing required fields: " + ",".join(missing))

    result = frame.copy()
    source_trade_dates = result[column_by_name["tradedate"]].astype("string").str.strip()
    parsed_trade_dates = pd.to_datetime(source_trade_dates, errors="coerce")
    if bool(parsed_trade_dates.isna().any()):
        _fail("FUTOI raw source contains invalid tradedate")
    exact_trade_dates = parsed_trade_dates.dt.date.astype(str)
    if not bool(exact_trade_dates.eq(trade_date).all()):
        _fail("FUTOI raw source contains rows outside explicit trade_date")

    source_trade_times = result[column_by_name["tradetime"]].astype("string").str.strip()
    reference_ts = pd.to_datetime(source_trade_dates + " " + source_trade_times, errors="coerce")
    if bool(reference_ts.isna().any()):
        _fail("FUTOI raw source contains invalid tradedate/tradetime reference timestamp")

    publication_ts = pd.to_datetime(result[column_by_name["systime"]], errors="coerce")
    if bool(publication_ts.isna().any()):
        _fail("FUTOI raw source contains invalid systime publication timestamp")
    if bool((publication_ts < reference_ts).any()):
        _fail("FUTOI raw source publication systime precedes reference timestamp")

    source_tickers = result[column_by_name["ticker"]].astype("string").str.strip()
    if bool(source_tickers.isna().any()) or bool(source_tickers.eq("").any()):
        _fail("FUTOI raw source contains missing ticker identity")
    if not bool(source_tickers.str.lower().eq(ticker.lower()).all()):
        _fail("FUTOI raw source ticker does not match explicit registry ticker")

    source_groups = result[column_by_name["clgroup"]].astype("string").str.upper().str.strip()
    if bool(source_groups.isna().any()) or bool(source_groups.eq("").any()):
        _fail("FUTOI raw source contains missing clgroup")
    if not bool(source_groups.isin({"FIZ", "YUR"}).all()):
        _fail("FUTOI raw source contains unsupported clgroup")

    return result.reset_index(drop=True)


def _coerce_source_identifier(value: object, field: str) -> int:
    if value is None or isinstance(value, bool) or value.__class__.__name__ == "bool_" or pd.isna(value):
        _fail("FUTOI source contains invalid required source identifier: " + field)
    text = str(value).strip()
    if not text:
        _fail("FUTOI source contains invalid required source identifier: " + field)
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError):
        _fail("FUTOI source contains invalid required source identifier: " + field)
    if not number.is_finite() or number != number.to_integral_value():
        _fail("FUTOI source contains invalid required source identifier: " + field)
    return int(number)


def _validate_required_source_identifiers(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for field in ("sess_id", "seqnum"):
        if field not in result.columns:
            _fail("FUTOI source missing required source identifier: " + field)
        result[field] = [_coerce_source_identifier(value, field) for value in result[field].tolist()]
    return result.reset_index(drop=True)


def _enforce_publication_timestamp(frame: pd.DataFrame) -> pd.DataFrame:
    if "systime" not in frame.columns:
        _fail("normalized FUTOI source missing systime publication timestamp")
    if "moment" not in frame.columns:
        _fail("normalized FUTOI source missing source reference moment")
    publication_ts = pd.to_datetime(frame["systime"], errors="coerce")
    reference_ts = pd.to_datetime(frame["moment"], errors="coerce")
    if bool(publication_ts.isna().any()):
        _fail("normalized FUTOI source contains invalid systime publication timestamp")
    if bool(reference_ts.isna().any()):
        _fail("normalized FUTOI source contains invalid source reference moment")
    result = frame.copy()
    result["ts"] = reference_ts
    result["moment"] = reference_ts
    result["systime"] = publication_ts
    trade_dates = result["trade_date"].astype(str)
    reference_dates = reference_ts.dt.date.astype(str)
    if not bool(trade_dates.eq(reference_dates).all()):
        _fail("FUTOI source reference moment date does not match trade_date")
    if bool((publication_ts < reference_ts).any()):
        _fail("FUTOI publication systime precedes source reference moment")
    return result.reset_index(drop=True)


def _deduplicate_exact_source_duplicates(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if frame.empty:
        return frame.copy(), 0
    missing_key = [field for field in SOURCE_RECORD_KEY_FIELDS if field not in frame.columns]
    if missing_key:
        _fail("FUTOI source record key missing fields: " + ",".join(missing_key))
    duplicate_mask = frame.duplicated(subset=list(SOURCE_RECORD_KEY_FIELDS), keep=False)
    if not bool(duplicate_mask.any()):
        return frame.copy().reset_index(drop=True), 0

    duplicate_rows = frame.loc[duplicate_mask].copy()
    comparison_fields = [
        field
        for field in ("source_ticker", "moment", "systime", *POSITION_FIELDS)
        if field in duplicate_rows.columns
    ]
    for _, group in duplicate_rows.groupby(list(SOURCE_RECORD_KEY_FIELDS), dropna=False, sort=False):
        if len(group) < 2:
            continue
        for field in comparison_fields:
            first = group[field].iloc[0]
            equal = group[field].isna() if pd.isna(first) else group[field].eq(first)
            if not bool(equal.all()):
                _fail("conflicting duplicate FUTOI source record")

    work = frame.copy()
    before = len(work)
    work = work.drop_duplicates(subset=list(SOURCE_RECORD_KEY_FIELDS), keep="last")
    dropped = before - len(work)
    return work.reset_index(drop=True), int(dropped)


def _quality_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {
            "rows": 0,
            "duplicate_key_count": 0,
            "null_required_count": 0,
            "invalid_position_count": 0,
        }
    required = [
        "trade_date",
        "ts",
        "moment",
        "systime",
        "sess_id",
        "seqnum",
        "secid",
        "clgroup",
        *POSITION_FIELDS,
    ]
    missing = [field for field in required if field not in frame.columns]
    if missing:
        _fail("normalized FUTOI source missing quality fields: " + ",".join(missing))
    duplicates = int(frame.duplicated(subset=list(SOURCE_RECORD_KEY_FIELDS)).sum())
    null_required = int(frame[required].isna().any(axis=1).sum())
    invalid = (
        (frame["pos_long"] < 0)
        | (frame["pos_short"] > 0)
        | (frame["pos_long_num"] < 0)
        | (frame["pos_short_num"] < 0)
    )
    return {
        "rows": int(len(frame)),
        "duplicate_key_count": duplicates,
        "null_required_count": null_required,
        "invalid_position_count": int(invalid.fillna(True).sum()),
    }


def _quality(frame: pd.DataFrame, binding: Mapping[str, object], trade_date: str, run_id: str) -> dict[str, object]:
    counts = _quality_counts(frame)
    availability_status = str(binding["futoi.availability_status"])
    probe_status = str(binding["futoi.probe_status"])
    failures: list[str] = []
    if int(counts.get("rows") or 0) <= 0:
        failures.append("row_count_zero")
    for field in ("duplicate_key_count", "null_required_count", "invalid_position_count"):
        if int(counts.get(field) or 0) != 0:
            failures.append(field + "_nonzero")
    if availability_status != "available":
        failures.append("availability_status_not_available")
    if probe_status != "completed":
        failures.append("probe_status_not_completed")
    status = "pass" if not failures else "fail"
    return {
        "run_id": run_id,
        "dataset_id": DATASET_ID,
        "instrument_id": str(binding["instrument_id"]),
        "source_id": str(binding["futoi.source_id"]),
        "secid": str(binding["secid"]),
        "futoi_ticker": str(binding["futoi.ticker"]),
        "board": str(binding["board"]),
        "market": str(binding["market"]),
        "engine": str(binding["engine"]),
        "trade_date": trade_date,
        "quality_status": status,
        "row_count": int(counts.get("rows") or 0),
        "duplicate_key_count": int(counts.get("duplicate_key_count") or 0),
        "null_required_count": int(counts.get("null_required_count") or 0),
        "invalid_position_count": int(counts.get("invalid_position_count") or 0),
        "availability_status": availability_status,
        "probe_status": probe_status,
        "timestamp_semantics": "ts=source_reference_moment;systime=source_publication_metadata",
        "source_record_key_fields": list(SOURCE_RECORD_KEY_FIELDS),
        "failure_reasons": failures,
        "quality_contract_ref": QUALITY_CONTRACT_REF,
    }


def materialize_futoi_partition(
    *,
    trade_date: str,
    instrument_id: str,
    run_id: str,
    registry_path: str | Path = REGISTRY_PATH,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
    require_enabled: bool = False,
) -> dict[str, object]:
    checked_date = _require_date(trade_date, "trade_date")
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_run_id = _require_token(run_id, "run_id")
    binding = _registry_binding(registry_path, checked_instrument)
    source_id = _require_token(binding["futoi.source_id"], "futoi.source_id")
    ticker = _require_token(binding["futoi.ticker"], "futoi.ticker")
    if source_id != SOURCE_ID:
        _fail("registry FUTOI source_id does not match canonical source contract")
    if require_enabled and binding["futoi.enabled_for_materialization"] is not True:
        _fail("registry FUTOI materialization is not enabled")
    if str(binding["futoi.availability_status"]) != "available" or str(binding["futoi.probe_status"]) != "completed":
        _fail("registry FUTOI APIM availability evidence is not completed/available")

    ingest_ts = _utc_now()
    source_frame, source_url = _fetch_exact(ticker, checked_date, timeout, apim_base_url)
    source_frame = _validate_required_source_identifiers(source_frame)
    source_frame = _validate_raw_source_rows(source_frame, checked_date, ticker)
    normalized, meta = legacy.normalize_futoi(
        source_frame,
        str(binding["secid"]),
        str(binding["canonical_symbol"]),
        str(binding["board"]),
        source_url,
        ticker,
        ingest_ts,
        False,
        "not_checked_stage2_pilot",
    )
    if meta.get("error"):
        _fail(str(meta.get("error")))
    normalized = normalized.loc[normalized["trade_date"].astype(str) == checked_date].copy().reset_index(drop=True)
    if normalized.empty:
        _fail("normalized FUTOI source contains no rows for explicit trade_date")
    normalized["instrument_id"] = checked_instrument
    normalized["source_id"] = source_id
    normalized["market"] = str(binding["market"])
    normalized["engine"] = str(binding["engine"])
    normalized["availability_ts_utc"] = ingest_ts
    normalized = _validate_required_source_identifiers(normalized)
    normalized = _enforce_publication_timestamp(normalized)
    normalized, exact_duplicate_rows_dropped = _deduplicate_exact_source_duplicates(normalized)
    normalized = normalized.sort_values(["ts", "sess_id", "seqnum", "clgroup"]).reset_index(drop=True)

    quality = _quality(normalized, binding, checked_date, checked_run_id)
    quality["exact_duplicate_rows_dropped"] = exact_duplicate_rows_dropped
    partition_path = _partition_path(checked_date, checked_instrument, source_id)
    quality_path = _quality_path(checked_date, checked_run_id)
    manifest_path = _manifest_path(checked_date, checked_run_id)
    pointer_path = accepted_pointer_path(checked_instrument)
    manifest = {
        "run_id": checked_run_id,
        "run_date": checked_date,
        "dataset_id": DATASET_ID,
        "instrument_scope": [checked_instrument],
        "source_scope": [source_id],
        "requested_from": checked_date,
        "requested_till": checked_date,
        "partitions_written": [partition_path.as_posix()] if quality["quality_status"] == "pass" else [],
        "partitions_skipped": [],
        "quality_report_ref": quality_path.as_posix(),
        "accepted_manifest_ref": "${MOEX_DATA_ROOT}/state/datasets/dataset_id=" + DATASET_ID + "/instrument_id=" + checked_instrument + "/current_accepted_manifest.json",
        "refresh_status": "succeeded" if quality["quality_status"] == "pass" else "failed",
        "source_contract": {
            "source_id": source_id,
            "source_contract_ref": SOURCE_CONTRACT_REF,
            "raw_contract_ref": RAW_CONTRACT_REF,
            "futoi_ticker": ticker,
            "source_endpoint_url": source_url,
            "transport": "authenticated_apim",
            "timestamp_semantics": "ts=source_reference_moment;systime=source_publication_metadata",
            "source_record_key_fields": list(SOURCE_RECORD_KEY_FIELDS),
        },
        "producer": PRODUCER_ID,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "accepted_pointer_path_preview": pointer_path.as_posix(),
        "exact_duplicate_rows_dropped": exact_duplicate_rows_dropped,
    }

    if quality["quality_status"] != "pass":
        _write_json_atomic(quality_path, quality)
        _write_json_atomic(manifest_path, manifest)
        _fail("FUTOI quality failed: " + ",".join(quality["failure_reasons"]))
    _write_parquet_atomic(partition_path, normalized, checked_run_id)
    _write_json_atomic(quality_path, quality)
    _write_json_atomic(manifest_path, manifest)
    return {
        "status": "succeeded",
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "source_id": source_id,
        "secid": str(binding["secid"]),
        "futoi_ticker": ticker,
        "trade_date": checked_date,
        "row_count": int(quality["row_count"]),
        "quality_status": quality["quality_status"],
        "storage_partition_path": partition_path.as_posix(),
        "quality_report_reference": quality_path.as_posix(),
        "manifest_reference": manifest_path.as_posix(),
        "accepted_manifest_pointer_reference": None,
        "source_contract_ref": SOURCE_CONTRACT_REF,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "timestamp_semantics": "source_reference_moment",
        "source_record_key_fields": list(SOURCE_RECORD_KEY_FIELDS),
        "exact_duplicate_rows_dropped": exact_duplicate_rows_dropped,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize one canonical FUTOI supplementary partition by registry identity.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--registry-path", default=REGISTRY_PATH)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    parser.add_argument("--require-enabled", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        payload = materialize_futoi_partition(
            trade_date=args.trade_date,
            instrument_id=args.instrument_id,
            run_id=args.run_id,
            registry_path=args.registry_path,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
            require_enabled=args.require_enabled,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
