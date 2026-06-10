from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import tempfile
from typing import Final

from . import materialize_forts_raw_5m_instrument as materializer

ARTIFACT_ID: Final[str] = "dataset.forts.raw_5m.tradestats.v1"
SOURCE_ARTIFACT_ID: Final[str] = "external.apim.fo.tradestats.v1"
PRODUCER_ID: Final[str] = "moex_data.futures.backfill_forts_raw_5m_instrument.v1"
REGISTRY_PATH: Final[str] = "configs/instruments/forts_instrument_registry.v1.yaml"


@dataclass(frozen=True)
class BackfillPaths:
    manifest_path: Path
    quality_report_path: Path


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
        current = current + timedelta(days=1)
    return result


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        raise ValueError("MOEX_DATA_ROOT is required")
    return Path(value)


def load_env_file(path: str | None) -> None:
    materializer.load_env_file(path)


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_name = handle.name
    Path(temporary_name).replace(path)


def build_backfill_paths(artifact_version: str) -> BackfillPaths:
    checked_version = _require_token(artifact_version, "artifact_version")
    root = _data_root()
    base = ("artifact_id=" + ARTIFACT_ID, "artifact_version=" + checked_version)
    return BackfillPaths(
        manifest_path=root / "manifests" / base[0] / base[1] / "manifest.json",
        quality_report_path=root / "quality_reports" / base[0] / base[1] / "quality_report.json",
    )


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
        if not stripped:
            continue
        if stripped == "instruments:":
            in_instruments = True
            continue
        if in_instruments and not raw_line.startswith(" ") and not stripped.startswith("-"):
            break
        if in_instruments and stripped.startswith("- "):
            if current:
                entries.append(current)
            current = {}
            payload = stripped[2:].strip()
            if ":" in payload:
                key, value = payload.split(":", 1)
                current[key.strip()] = _parse_scalar(value)
            continue
        if in_instruments and current is not None and ":" in stripped:
            key, value = stripped.split(":", 1)
            current[key.strip()] = _parse_scalar(value)
    if current:
        entries.append(current)
    return entries


def registry_allows_instrument(registry_path: str | Path, instrument_id: str, secid: str) -> bool:
    text = Path(registry_path).read_text(encoding="utf-8")
    instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    if "family_partition_key_allowed: false" not in text:
        return False
    for entry in _registry_entries(text):
        if (
            entry.get("instrument_id") == instrument
            and entry.get("secid") == checked_secid
            and entry.get("enabled_for_raw_5m_materialization") is True
        ):
            return True
    return False


def _empty_source_error(error: str) -> bool:
    lowered = error.lower()
    return "no rows" in lowered or "source table is empty" in lowered


def _partition_version(run_id: str, trade_date: str, instrument_id: str, secid: str) -> str:
    safe_instrument = instrument_id.replace(".", "_").replace("-", "_")
    return run_id + "_partition_" + trade_date.replace("-", "") + "_" + safe_instrument + "_" + secid


def _manifest_payload(
    artifact_version: str,
    instrument_id: str,
    secid: str,
    requested_dates: Sequence[str],
    successes: Sequence[Mapping[str, object]],
    skipped_empty_dates: Sequence[str],
    failures: Sequence[Mapping[str, object]],
    build_started_at: str,
    build_finished_at: str,
    quality_report_path: Path,
) -> dict[str, object]:
    row_count = sum(int(item["row_count"]) for item in successes)
    valid_dates = [str(item["data_start"]) for item in successes]
    partition_hashes = {str(item["storage_partition_path"]): str(item["content_hash"]) for item in successes}
    return {
        "artifact_id": ARTIFACT_ID,
        "artifact_class": "raw_native_dataset_backfill_manifest",
        "artifact_version": artifact_version,
        "schema_version": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "producer": PRODUCER_ID,
        "deterministic_builder_config_version": PRODUCER_ID,
        "registry_reference": REGISTRY_PATH,
        "input_references": [SOURCE_ARTIFACT_ID, REGISTRY_PATH],
        "instrument_id_scope": [instrument_id],
        "secid_scope": [secid],
        "requested_data_start": requested_dates[0] if requested_dates else None,
        "requested_data_end": requested_dates[-1] if requested_dates else None,
        "data_start": min(valid_dates) if valid_dates else None,
        "data_end": max(valid_dates) if valid_dates else None,
        "last_valid_trade_date": max(valid_dates) if valid_dates else None,
        "row_count": row_count,
        "partition_count": len(successes),
        "requested_date_count": len(requested_dates),
        "skipped_empty_source_dates": list(skipped_empty_dates),
        "failed_dates": list(failures),
        "calendar_contract": "moex_iss_futures_calendar",
        "session_binding": "explicit_trade_date_session",
        "storage_pattern": materializer.STORAGE_PATTERN,
        "partition_hashes": partition_hashes,
        "quality_report_reference": quality_report_path.as_posix(),
        "build_started_at": build_started_at,
        "build_finished_at": build_finished_at,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
    }


def _quality_payload(artifact_version: str, manifest: Mapping[str, object]) -> dict[str, object]:
    failures = list(manifest["failed_dates"])
    status = "passed" if not failures and int(manifest["partition_count"]) > 0 else "failed"
    return {
        "artifact_id": "reports.data_asset.quality.v1",
        "artifact_class": "quality_report",
        "artifact_version": artifact_version,
        "schema_version": "reports.data_asset.quality.v1",
        "target_artifact_id": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "deterministic_builder_config_version": PRODUCER_ID,
        "quality_status": status,
        "instrument_id_scope": manifest["instrument_id_scope"],
        "secid_scope": manifest["secid_scope"],
        "data_start": manifest["data_start"],
        "data_end": manifest["data_end"],
        "last_valid_trade_date": manifest["last_valid_trade_date"],
        "row_count": manifest["row_count"],
        "partition_count": manifest["partition_count"],
        "skipped_empty_source_dates_count": len(list(manifest["skipped_empty_source_dates"])),
        "failed_dates_count": len(failures),
        "failure_reasons": failures,
        "checked_at": _utc_now(),
    }


def backfill_range(
    *,
    date_start: str,
    date_end: str,
    instrument_id: str,
    secid: str,
    artifact_version: str,
    registry_path: str | Path = REGISTRY_PATH,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
    max_dates: int | None = None,
    runner: Callable[..., object] = materializer.materialize_instrument_partition,
) -> BackfillSummary:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    checked_version = _require_token(artifact_version, "artifact_version")
    if not registry_allows_instrument(registry_path, checked_instrument, checked_secid):
        raise ValueError("instrument is not enabled by registry")
    requested_dates = _date_range(date_start, date_end, max_dates=max_dates)
    paths = build_backfill_paths(checked_version)
    build_started_at = _utc_now()
    successes: list[Mapping[str, object]] = []
    skipped_empty_dates: list[str] = []
    failures: list[Mapping[str, object]] = []
    for trade_date in requested_dates:
        try:
            result = runner(
                trade_date=trade_date,
                instrument_id=checked_instrument,
                secid=checked_secid,
                artifact_version=_partition_version(checked_version, trade_date, checked_instrument, checked_secid),
                timeout=timeout,
                apim_base_url=apim_base_url,
            )
            successes.append(result.payload)
        except Exception as exc:
            message = str(exc)
            if _empty_source_error(message):
                skipped_empty_dates.append(trade_date)
            else:
                failures.append({"trade_date": trade_date, "error": message})
    build_finished_at = _utc_now()
    manifest = _manifest_payload(
        checked_version,
        checked_instrument,
        checked_secid,
        requested_dates,
        successes,
        skipped_empty_dates,
        failures,
        build_started_at,
        build_finished_at,
        paths.quality_report_path,
    )
    quality = _quality_payload(checked_version, manifest)
    _write_json_atomic(paths.quality_report_path, quality)
    _write_json_atomic(paths.manifest_path, manifest)
    payload = dict(manifest)
    payload["status"] = "succeeded" if quality["quality_status"] == "passed" else "failed"
    payload["manifest_reference"] = paths.manifest_path.as_posix()
    payload["quality_report_reference"] = paths.quality_report_path.as_posix()
    payload["quality_status"] = quality["quality_status"]
    return BackfillSummary(payload=payload, manifest_path=paths.manifest_path, quality_report_path=paths.quality_report_path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill FORTS raw 5m tradestats by explicit instrument registry identity.")
    parser.add_argument("--date-start", required=True)
    parser.add_argument("--date-end", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--registry-path", default=REGISTRY_PATH)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    parser.add_argument("--max-dates", type=int, default=None)
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
            artifact_version=args.artifact_version,
            registry_path=args.registry_path,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
            max_dates=args.max_dates,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(summary.payload, ensure_ascii=False, sort_keys=True))
    return 0 if summary.payload["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
