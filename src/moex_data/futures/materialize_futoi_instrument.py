from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
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
    frame = availability.fetch_paged_frame(
        base_url,
        path,
        {"from": trade_date, "till": trade_date, "latest": 0},
        "futoi",
        timeout,
        True,
    )
    if frame.empty:
        _fail("FUTOI APIM exact source returned no rows")
    columns = {str(column).strip().lower() for column in frame.columns}
    if "error_message" in columns:
        _fail("FUTOI APIM returned ERROR_MESSAGE instead of data")
    required = {"clgroup", "pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num"}
    timestamp_ok = "moment" in columns or {"tradedate", "tradetime"}.issubset(columns)
    if not required.issubset(columns) or not timestamp_ok:
        _fail("FUTOI APIM schema mismatch")
    return frame, availability.url_join(base_url, path)


def _quality(frame: pd.DataFrame, binding: Mapping[str, object], trade_date: str, run_id: str) -> dict[str, object]:
    counts = legacy.quality_counts(frame, None)
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
    normalized = normalized.sort_values(["ts", "clgroup"]).reset_index(drop=True)

    quality = _quality(normalized, binding, checked_date, checked_run_id)
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
        },
        "producer": PRODUCER_ID,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "accepted_pointer_path_preview": pointer_path.as_posix(),
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
