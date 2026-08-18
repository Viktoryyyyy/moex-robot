from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

from . import accepted_manifest
from . import materialize_forts_raw_5m_instrument as materializer
from .manifest import validate_refresh_manifest_values
from .quality import validate_quality_report_rows
from .schemas import EXPECTED_DATASET_CONTRACT_IDS

DATASET_ID: Final[str] = materializer.DATASET_ID
SOURCE_ID: Final[str] = materializer.SOURCE_ID
SOURCE_CONTRACT_REF: Final[str] = materializer.SOURCE_CONTRACT_REF
PRODUCER_ID: Final[str] = "moex_data.futures.backfill_forts_raw_5m_instrument.v2"
REGISTRY_PATH: Final[str] = "configs/instruments/forts_instrument_registry.v1.yaml"


@dataclass(frozen=True)
class BackfillSummary:
    payload: dict[str, object]
    manifest_path: Path
    quality_report_path: Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _require_token(value: str | None, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        raise ValueError(field_name + " must be an explicit safe token")
    return text


def _require_date(value: str | None, field_name: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(field_name + " must be YYYY-MM-DD") from exc


def _date_range(date_start: str, date_end: str, max_dates: int | None = None) -> list[str]:
    start = date.fromisoformat(_require_date(date_start, "date_start"))
    end = date.fromisoformat(_require_date(date_end, "date_end"))
    if start > end:
        raise ValueError("date_start must be <= date_end")
    result: list[str] = []
    current = start
    while current <= end:
        result.append(current.isoformat())
        if max_dates is not None and len(result) >= max_dates:
            break
        current += timedelta(days=1)
    return result


def load_env_file(path: str | None) -> None:
    materializer.load_env_file(path)


def _parse_scalar(value: str) -> object:
    text = value.strip().strip('"').strip("'")
    if text == "true":
        return True
    if text == "false":
        return False
    return text


def _registry_entries(text: str) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    in_instruments = False
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped == "instruments:":
            in_instruments = True
            continue
        if in_instruments and not raw_line.startswith(" ") and not stripped.startswith("-"):
            break
        if in_instruments and raw_line.startswith("  - "):
            if current:
                entries.append(current)
            current = {}
            payload = stripped[2:].strip()
            if ":" in payload:
                key, value = payload.split(":", 1)
                current[key.strip()] = _parse_scalar(value)
            continue
        if in_instruments and current is not None and raw_line.startswith("    ") and not raw_line.startswith("      ") and ":" in stripped:
            key, value = stripped.split(":", 1)
            current[key.strip()] = _parse_scalar(value)
    if current:
        entries.append(current)
    return entries


def registry_allows_instrument(
    registry_path: str | Path,
    instrument_id: str,
    secid: str,
    source_id: str = SOURCE_ID,
) -> bool:
    text = Path(registry_path).read_text(encoding="utf-8")
    instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    checked_source_id = _require_token(source_id, "source_id")
    if "family_partition_key_allowed: false" not in text:
        return False
    for entry in _registry_entries(text):
        if entry.get("instrument_id") != instrument or entry.get("secid") != checked_secid:
            continue
        entry_source_id = entry.get("source_id")
        if entry_source_id is not None and entry_source_id != checked_source_id:
            continue
        if entry.get("enabled_for_raw_5m_materialization") is True:
            return True
    return False


def _empty_source_error(error: str) -> bool:
    lowered = error.lower()
    return (
        "returned no rows" in lowered
        or "contains no rows" in lowered
        or "source table is empty" in lowered
        or "response contains no rows" in lowered
    )


def _partition_version(run_id: str, trade_date: str, instrument_id: str, secid: str) -> str:
    safe_instrument = instrument_id.replace(".", "_").replace("-", "_")
    return run_id + "_partition_" + trade_date.replace("-", "") + "_" + safe_instrument + "_" + secid


def _load_quality_row(path: str | Path, run_id: str) -> dict[str, object]:
    values = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = list(values.get("rows") or []) if isinstance(values, Mapping) else []
    if len(rows) != 1 or not isinstance(rows[0], Mapping):
        raise ValueError("partition quality report must contain exactly one quality row")
    row = dict(rows[0])
    row["run_id"] = run_id
    return row


def _compatibility_quality_row(
    *,
    run_id: str,
    trade_date: str,
    instrument_id: str,
    secid: str,
    source_id: str,
    payload: Mapping[str, object],
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "dataset_id": DATASET_ID,
        "instrument_id": instrument_id,
        "source_id": source_id,
        "secid": secid,
        "board": "RFUD",
        "market": "forts",
        "engine": "futures",
        "trade_date": trade_date,
        "rows": int(payload.get("row_count") or 0),
        "duplicate_key_count": 0,
        "gap_count": 0,
        "null_ohlc_count": 0,
        "invalid_ohlc_count": 0,
        "futoi_missing_count": 0,
        "calendar_status": "compatibility_runner_not_checked",
        "quality_status": "pass",
    }


def _accepted_manifest_ref(instrument_id: str) -> str:
    return (
        "${MOEX_DATA_ROOT}/state/datasets/dataset_id="
        + DATASET_ID
        + "/instrument_id="
        + instrument_id
        + "/current_accepted_manifest.json"
    )


def backfill_range(
    *,
    date_start: str,
    date_end: str,
    instrument_id: str,
    secid: str,
    artifact_version: str,
    source_id: str = SOURCE_ID,
    registry_path: str | Path = REGISTRY_PATH,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
    max_dates: int | None = None,
    create_accepted_pointer: bool = False,
    runner: Callable[..., object] | None = None,
) -> BackfillSummary:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    checked_source_id = _require_token(source_id, "source_id")
    checked_version = _require_token(artifact_version, "artifact_version")
    if checked_source_id != SOURCE_ID:
        raise ValueError("source_id does not match canonical FORTS tradestats source contract")
    if not registry_allows_instrument(registry_path, checked_instrument, checked_secid, checked_source_id):
        raise ValueError("instrument/source identity is not enabled by registry")

    requested_dates = _date_range(date_start, date_end, max_dates=max_dates)
    if not requested_dates:
        raise ValueError("requested date range is empty")
    top_paths = materializer.target_paths(requested_dates[-1], checked_instrument, checked_secid, checked_version, checked_source_id)
    started_at = _utc_now()
    successes: list[dict[str, object]] = []
    skipped_empty_dates: list[str] = []
    failures: list[dict[str, str]] = []
    quality_rows: list[dict[str, object]] = []
    active_runner = materializer.materialize_instrument_partition if runner is None else runner

    for trade_date in requested_dates:
        partition_version = _partition_version(checked_version, trade_date, checked_instrument, checked_secid)
        try:
            if runner is None:
                result = active_runner(
                    trade_date=trade_date,
                    instrument_id=checked_instrument,
                    secid=checked_secid,
                    source_id=checked_source_id,
                    artifact_version=partition_version,
                    timeout=timeout,
                    apim_base_url=apim_base_url,
                )
            else:
                result = active_runner(
                    trade_date=trade_date,
                    instrument_id=checked_instrument,
                    secid=checked_secid,
                    artifact_version=partition_version,
                    timeout=timeout,
                    apim_base_url=apim_base_url,
                )
            payload = dict(result.payload)
            successes.append(payload)
            quality_ref = payload.get("quality_report_reference")
            if quality_ref:
                quality_rows.append(_load_quality_row(str(quality_ref), checked_version))
            else:
                quality_rows.append(
                    _compatibility_quality_row(
                        run_id=checked_version,
                        trade_date=trade_date,
                        instrument_id=checked_instrument,
                        secid=checked_secid,
                        source_id=checked_source_id,
                        payload=payload,
                    )
                )
        except Exception as exc:
            message = str(exc)
            if _empty_source_error(message):
                skipped_empty_dates.append(trade_date)
            else:
                failures.append({"trade_date": trade_date, "error": message})

    if quality_rows:
        validate_quality_report_rows(quality_rows)
    refresh_status = "succeeded" if quality_rows and not failures else ("partial" if quality_rows else "failed")
    quality_status = "pass" if quality_rows and not failures else "fail"
    finished_at = _utc_now()
    manifest_values: dict[str, object] = {
        "run_id": checked_version,
        "run_date": requested_dates[-1],
        "requested_from": requested_dates[0],
        "requested_till": requested_dates[-1],
        "instrument_scope": [checked_instrument],
        "source_scope": [checked_source_id],
        "dataset_contract_refs": list(EXPECTED_DATASET_CONTRACT_IDS),
        "partitions_written": [str(item["storage_partition_path"]) for item in successes],
        "partitions_skipped": skipped_empty_dates,
        "quality_report_ref": top_paths.quality_report_path.as_posix(),
        "accepted_manifest_ref": _accepted_manifest_ref(checked_instrument),
        "refresh_status": refresh_status,
        "started_at": started_at,
        "finished_at": finished_at,
        "source_contract": {
            "source_id": checked_source_id,
            "source_contract_ref": SOURCE_CONTRACT_REF,
            "secid": checked_secid,
            "board": "RFUD",
            "market": "forts",
            "engine": "futures",
        },
        "failed_dates": failures,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
    }
    validate_refresh_manifest_values(manifest_values)

    quality_report = {
        "run_id": checked_version,
        "quality_status": quality_status,
        "rows": quality_rows,
        "failed_dates": failures,
    }
    materializer.core._write_json_atomic(top_paths.quality_report_path, quality_report)
    materializer.core._write_json_atomic(top_paths.manifest_path, manifest_values)

    pointer_path: str | None = None
    if create_accepted_pointer:
        if refresh_status != "succeeded" or quality_status != "pass":
            raise ValueError("accepted pointer cannot be created unless full backfill quality passes")
        pointer = accepted_manifest.write_accepted_manifest_pointer(
            env=None,
            dataset_id=DATASET_ID,
            instrument_id=checked_instrument,
            manifest_ref=top_paths.manifest_path.as_posix(),
            manifest_values=manifest_values,
            quality_rows=quality_rows,
        )
        pointer_path = pointer.accepted_manifest_path.as_posix()

    payload = dict(manifest_values)
    payload.update(
        {
            "status": refresh_status,
            "dataset_id": DATASET_ID,
            "source_id": checked_source_id,
            "quality_status": quality_status,
            "legacy_quality_status": "passed" if quality_status == "pass" else "failed",
            "row_count": sum(int(item["row_count"]) for item in successes),
            "partition_count": len(successes),
            "instrument_id_scope": [checked_instrument],
            "secid_scope": [checked_secid],
            "skipped_empty_source_dates": skipped_empty_dates,
            "manifest_reference": top_paths.manifest_path.as_posix(),
            "quality_report_reference": top_paths.quality_report_path.as_posix(),
            "accepted_manifest_pointer_reference": pointer_path,
        }
    )
    return BackfillSummary(payload=payload, manifest_path=top_paths.manifest_path, quality_report_path=top_paths.quality_report_path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill canonical FORTS raw 5m tradestats by explicit registry identity.")
    parser.add_argument("--date-start", required=True)
    parser.add_argument("--date-end", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--source-id", default=SOURCE_ID)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--registry-path", default=REGISTRY_PATH)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    parser.add_argument("--max-dates", type=int, default=None)
    parser.add_argument("--create-accepted-pointer", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        summary = backfill_range(
            date_start=args.date_start,
            date_end=args.date_end,
            instrument_id=args.instrument_id,
            secid=args.secid,
            source_id=args.source_id,
            artifact_version=args.artifact_version,
            registry_path=args.registry_path,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
            max_dates=args.max_dates,
            create_accepted_pointer=args.create_accepted_pointer,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(summary.payload, ensure_ascii=False, sort_keys=True))
    return 0 if summary.payload["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
