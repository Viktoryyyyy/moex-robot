from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

from . import materialize_forts_raw_5m_instrument as materializer

ARTIFACT_ID: Final[str] = "dataset.forts.raw_5m.tradestats.v1"
SOURCE_ARTIFACT_ID: Final[str] = "external.apim.fo.tradestats.v1"
PRODUCER_ID: Final[str] = "moex_data.futures.refresh_forts_raw_5m_incremental.v1"
REGISTRY_PATH: Final[str] = "configs/instruments/forts_instrument_registry.v1.yaml"
DEFAULT_MOEX_ISS_BASE_URL: Final[str] = "https://iss.moex.com"


@dataclass(frozen=True)
class RefreshPaths:
    manifest_path: Path
    quality_report_path: Path


@dataclass(frozen=True)
class RefreshSummary:
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


def _coerce_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(_require_date(value, field_name))
    raise ValueError(field_name + " must be date, datetime, or YYYY-MM-DD")


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


def build_refresh_paths(artifact_version: str) -> RefreshPaths:
    checked_version = _require_token(artifact_version, "artifact_version")
    root = _data_root()
    base = ("artifact_id=" + ARTIFACT_ID, "artifact_version=" + checked_version)
    return RefreshPaths(
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


def _leading_spaces(value: str) -> int:
    return len(value) - len(value.lstrip(" "))


def _registry_entries(text: str) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    in_instruments = False
    instruments_indent = 0
    item_field_indent: int | None = None
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = _leading_spaces(raw_line)
        if stripped == "instruments:":
            in_instruments = True
            instruments_indent = indent
            continue
        if not in_instruments:
            continue
        if indent <= instruments_indent and not stripped.startswith("-"):
            break
        if stripped.startswith("- "):
            if current:
                entries.append(current)
            current = {}
            item_field_indent = indent + 2
            payload = stripped[2:].strip()
            if ":" in payload:
                key, value = payload.split(":", 1)
                current[key.strip()] = _parse_scalar(value)
            continue
        if current is not None and item_field_indent is not None and indent == item_field_indent and ":" in stripped:
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


def _load_json(path: str | Path, field_name: str) -> dict[str, object]:
    checked_path = Path(path)
    if not checked_path.exists():
        raise ValueError(field_name + " does not exist")
    loaded = json.loads(checked_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(field_name + " must contain a JSON object")
    return loaded


def _require_base_manifest(base_manifest: Mapping[str, object], instrument_id: str, secid: str) -> str:
    if base_manifest.get("artifact_id") != ARTIFACT_ID:
        raise ValueError("base_manifest artifact_id mismatch")
    if base_manifest.get("source_artifact_id") != SOURCE_ARTIFACT_ID:
        raise ValueError("base_manifest source_artifact_id mismatch")
    instrument_scope = list(base_manifest.get("instrument_id_scope") or [])
    secid_scope = list(base_manifest.get("secid_scope") or [])
    if instrument_scope != [instrument_id] or secid_scope != [secid]:
        raise ValueError("base_manifest instrument/secid scope mismatch")
    last_valid = base_manifest.get("last_valid_trade_date")
    return _require_date(str(last_valid or ""), "base_manifest.last_valid_trade_date")


def _calendar_row_date(row: Mapping[str, object]) -> date:
    value = row.get("trade_date")
    if value is None:
        value = row.get("date")
    return _coerce_date(value, "calendar trade_date")


def _calendar_row_is_trading(row: Mapping[str, object]) -> bool:
    value = row.get("is_trading_day")
    if value is None:
        value = row.get("futures_")
    if value is None:
        value = row.get("futures")
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return value == 1
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "t", "yes"):
            return True
        if text in ("0", "false", "f", "no", ""):
            return False
    raise ValueError("futures calendar row must contain boolean is_trading_day or futures_ 0/1 status")


def _calendar_map(rows: Sequence[Mapping[str, object]]) -> dict[date, bool]:
    calendar: dict[date, bool] = {}
    for row in rows:
        trade_date = _calendar_row_date(row)
        if trade_date in calendar:
            raise ValueError("duplicate futures calendar date")
        calendar[trade_date] = _calendar_row_is_trading(row)
    if not calendar:
        raise ValueError("futures calendar is empty")
    return calendar


def _next_cursor_start(cursor_table: object) -> int | None:
    if not isinstance(cursor_table, Mapping):
        return None
    columns = cursor_table.get("columns")
    data = cursor_table.get("data")
    if not isinstance(columns, list) or not isinstance(data, list) or not data:
        return None
    first = data[0]
    if not isinstance(first, list):
        return None
    values = dict(zip([str(item).lower() for item in columns], first, strict=False))
    try:
        index = int(values.get("index", 0))
        total = int(values.get("total", 0))
        page_size = int(values.get("pagesize", 0))
    except (TypeError, ValueError):
        return None
    next_start = index + page_size
    if page_size <= 0 or next_start >= total:
        return None
    return next_start


def fetch_futures_calendar_rows(
    date_start: str,
    date_end: str,
    *,
    timeout: float = 30.0,
    calendar_base_url: str | None = None,
) -> list[dict[str, object]]:
    import requests

    start_date = _coerce_date(date_start, "date_start")
    end_date = _coerce_date(date_end, "date_end")
    if start_date > end_date:
        raise ValueError("date_start must be <= date_end")
    base_url = (calendar_base_url or DEFAULT_MOEX_ISS_BASE_URL).rstrip("/")
    endpoint = base_url + "/iss/calendars.json"
    rows: list[dict[str, object]] = []
    for year in range(start_date.year, end_date.year + 1):
        year_start = max(start_date, date(year, 1, 1))
        year_end = min(end_date, date(year, 12, 31))
        cursor_start = 0
        while True:
            params = {
                "from": year_start.isoformat(),
                "till": year_end.isoformat(),
                "iss.only": "off_days",
                "show_all_days": "1",
                "start": str(cursor_start),
            }
            response = requests.get(endpoint, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            table = payload.get("off_days")
            if not isinstance(table, Mapping):
                raise ValueError("MOEX calendar response missing off_days table")
            columns = table.get("columns")
            data = table.get("data")
            if not isinstance(columns, list) or not isinstance(data, list):
                raise ValueError("MOEX calendar off_days table has invalid shape")
            for item in data:
                if isinstance(item, list):
                    rows.append(dict(zip([str(column) for column in columns], item, strict=False)))
            next_start = _next_cursor_start(payload.get("off_days.cursor"))
            if next_start is None or next_start <= cursor_start:
                break
            cursor_start = next_start
    return rows


def _require_calendar_date(calendar: Mapping[date, bool], value: date) -> bool:
    if value not in calendar:
        raise ValueError("futures calendar missing date " + value.isoformat())
    return bool(calendar[value])


def _previous_trading_day_on_or_before(calendar: Mapping[date, bool], value: date) -> date:
    lower = min(calendar)
    current = value
    while current >= lower:
        if _require_calendar_date(calendar, current):
            return current
        current = current - timedelta(days=1)
    raise ValueError("no completed futures trading day in calendar range")


def _next_trading_day_after(calendar: Mapping[date, bool], value: date, end_date: date) -> date | None:
    current = value + timedelta(days=1)
    while current <= end_date:
        if _require_calendar_date(calendar, current):
            return current
        current = current + timedelta(days=1)
    return None


def _trading_days_between(calendar: Mapping[date, bool], start_date: date, end_date: date) -> list[str]:
    result: list[str] = []
    current = start_date
    while current <= end_date:
        if _require_calendar_date(calendar, current):
            result.append(current.isoformat())
        current = current + timedelta(days=1)
    return result


def _partition_version(run_id: str, trade_date: str, instrument_id: str, secid: str) -> str:
    safe_instrument = instrument_id.replace(".", "_").replace("-", "_")
    return run_id + "_partition_" + trade_date.replace("-", "") + "_" + safe_instrument + "_" + secid


def _result_payload(result: object) -> Mapping[str, object]:
    payload = getattr(result, "payload", result)
    if not isinstance(payload, Mapping):
        raise ValueError("partition runner returned invalid payload")
    return payload


def _build_manifest(
    *,
    artifact_version: str,
    base_manifest: Mapping[str, object],
    base_manifest_path: Path,
    instrument_id: str,
    secid: str,
    last_completed_valid_trading_day: date,
    incremental_start: str | None,
    requested_dates: Sequence[str],
    successes: Sequence[Mapping[str, object]],
    failures: Sequence[Mapping[str, object]],
    build_started_at: str,
    build_finished_at: str,
    quality_report_path: Path,
) -> dict[str, object]:
    base_hashes = base_manifest.get("partition_hashes") or {}
    if not isinstance(base_hashes, Mapping):
        raise ValueError("base_manifest partition_hashes must be an object")
    partition_hashes = {str(key): str(value) for key, value in base_hashes.items()}
    for item in successes:
        partition_hashes[str(item["storage_partition_path"])] = str(item["content_hash"])
    base_row_count = int(base_manifest.get("row_count") or 0)
    base_partition_count = int(base_manifest.get("partition_count") or 0)
    added_row_count = sum(int(item["row_count"]) for item in successes)
    success_dates = [str(item["data_start"]) for item in successes]
    base_last = str(base_manifest["last_valid_trade_date"])
    cumulative_last = max([base_last] + success_dates) if success_dates else base_last
    status = "failed" if failures else ("no_op" if not requested_dates else "succeeded")
    return {
        "artifact_id": ARTIFACT_ID,
        "artifact_class": "raw_native_dataset_incremental_refresh_manifest",
        "artifact_version": artifact_version,
        "schema_version": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "producer": PRODUCER_ID,
        "deterministic_builder_config_version": PRODUCER_ID,
        "base_manifest_reference": base_manifest_path.as_posix(),
        "registry_reference": REGISTRY_PATH,
        "input_references": [base_manifest_path.as_posix(), SOURCE_ARTIFACT_ID, REGISTRY_PATH],
        "instrument_id_scope": [instrument_id],
        "secid_scope": [secid],
        "requested_data_start": base_manifest.get("requested_data_start"),
        "requested_data_end": last_completed_valid_trading_day.isoformat(),
        "base_last_valid_trade_date": base_last,
        "last_completed_valid_trading_day": last_completed_valid_trading_day.isoformat(),
        "incremental_start": incremental_start,
        "incremental_requested_dates": list(requested_dates),
        "incremental_requested_date_count": len(requested_dates),
        "incremental_succeeded_dates": success_dates,
        "incremental_failed_dates": list(failures),
        "failed_dates": list(failures),
        "data_start": base_manifest.get("data_start"),
        "data_end": cumulative_last,
        "last_valid_trade_date": cumulative_last,
        "row_count": base_row_count + added_row_count,
        "partition_count": base_partition_count + len(successes),
        "added_row_count": added_row_count,
        "added_partition_count": len(successes),
        "calendar_contract": "moex_iss_futures_calendar",
        "session_binding": "explicit_trade_date_session",
        "storage_pattern": materializer.STORAGE_PATTERN,
        "partition_hashes": partition_hashes,
        "quality_report_reference": quality_report_path.as_posix(),
        "refresh_status": status,
        "build_started_at": build_started_at,
        "build_finished_at": build_finished_at,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "preserved_existing_partitions": True,
    }


def _quality_payload(artifact_version: str, manifest: Mapping[str, object]) -> dict[str, object]:
    failures = list(manifest["failed_dates"])
    status = "failed" if failures else "passed"
    return {
        "artifact_id": "reports.data_asset.quality.v1",
        "artifact_class": "quality_report",
        "artifact_version": artifact_version,
        "schema_version": "reports.data_asset.quality.v1",
        "target_artifact_id": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "deterministic_builder_config_version": PRODUCER_ID,
        "quality_status": status,
        "refresh_status": manifest["refresh_status"],
        "instrument_id_scope": manifest["instrument_id_scope"],
        "secid_scope": manifest["secid_scope"],
        "data_start": manifest["data_start"],
        "data_end": manifest["data_end"],
        "last_valid_trade_date": manifest["last_valid_trade_date"],
        "last_completed_valid_trading_day": manifest["last_completed_valid_trading_day"],
        "row_count": manifest["row_count"],
        "partition_count": manifest["partition_count"],
        "added_row_count": manifest["added_row_count"],
        "added_partition_count": manifest["added_partition_count"],
        "failed_dates_count": len(failures),
        "failure_reasons": failures,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "checked_at": _utc_now(),
    }


def refresh_incremental(
    *,
    instrument_id: str,
    secid: str,
    base_manifest: str | Path,
    artifact_version: str,
    registry_path: str | Path,
    as_of_date: str | None = None,
    date_end: str | None = None,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
    calendar_base_url: str | None = None,
    calendar_rows: Sequence[Mapping[str, object]] | None = None,
    calendar_loader: Callable[..., Sequence[Mapping[str, object]]] = fetch_futures_calendar_rows,
    runner: Callable[..., object] = materializer.materialize_instrument_partition,
) -> RefreshSummary:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_secid = _require_token(secid, "secid")
    checked_version = _require_token(artifact_version, "artifact_version")
    if as_of_date and date_end:
        raise ValueError("use either as_of_date or date_end, not both")
    if not registry_allows_instrument(registry_path, checked_instrument, checked_secid):
        raise ValueError("instrument is not enabled by registry")
    base_manifest_path = Path(base_manifest)
    base_payload = _load_json(base_manifest_path, "base_manifest")
    base_last_text = _require_base_manifest(base_payload, checked_instrument, checked_secid)
    base_last = _coerce_date(base_last_text, "base_manifest.last_valid_trade_date")
    if date_end:
        upper_bound = _coerce_date(date_end, "date_end")
    else:
        effective_as_of = _coerce_date(as_of_date, "as_of_date") if as_of_date else datetime.now(timezone.utc).date()
        upper_bound = effective_as_of - timedelta(days=1)
    calendar_start = min(base_last, upper_bound)
    calendar_end = max(base_last, upper_bound)
    rows = list(calendar_rows) if calendar_rows is not None else list(
        calendar_loader(
            calendar_start.isoformat(),
            calendar_end.isoformat(),
            timeout=timeout,
            calendar_base_url=calendar_base_url,
        )
    )
    calendar = _calendar_map(rows)
    if upper_bound < min(calendar):
        raise ValueError("futures calendar does not cover requested upper bound")
    last_completed = _previous_trading_day_on_or_before(calendar, upper_bound)
    if last_completed <= base_last:
        requested_dates: list[str] = []
        incremental_start = None
    else:
        next_start = _next_trading_day_after(calendar, base_last, last_completed)
        incremental_start = next_start.isoformat() if next_start is not None else None
        requested_dates = [] if next_start is None else _trading_days_between(calendar, next_start, last_completed)
    paths = build_refresh_paths(checked_version)
    build_started_at = _utc_now()
    successes: list[Mapping[str, object]] = []
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
            successes.append(_result_payload(result))
        except Exception as exc:
            failures.append({"trade_date": trade_date, "error": str(exc)})
    build_finished_at = _utc_now()
    manifest = _build_manifest(
        artifact_version=checked_version,
        base_manifest=base_payload,
        base_manifest_path=base_manifest_path,
        instrument_id=checked_instrument,
        secid=checked_secid,
        last_completed_valid_trading_day=last_completed,
        incremental_start=incremental_start,
        requested_dates=requested_dates,
        successes=successes,
        failures=failures,
        build_started_at=build_started_at,
        build_finished_at=build_finished_at,
        quality_report_path=paths.quality_report_path,
    )
    quality = _quality_payload(checked_version, manifest)
    _write_json_atomic(paths.quality_report_path, quality)
    _write_json_atomic(paths.manifest_path, manifest)
    payload = dict(manifest)
    payload["status"] = manifest["refresh_status"]
    payload["quality_status"] = quality["quality_status"]
    payload["manifest_reference"] = paths.manifest_path.as_posix()
    payload["quality_report_reference"] = paths.quality_report_path.as_posix()
    return RefreshSummary(payload=payload, manifest_path=paths.manifest_path, quality_report_path=paths.quality_report_path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally refresh FORTS raw 5m tradestats from an explicit base manifest.")
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--base-manifest", required=True)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--registry-path", required=True)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--date-end", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    parser.add_argument("--calendar-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        summary = refresh_incremental(
            instrument_id=args.instrument_id,
            secid=args.secid,
            base_manifest=args.base_manifest,
            artifact_version=args.artifact_version,
            registry_path=args.registry_path,
            as_of_date=args.as_of_date,
            date_end=args.date_end,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
            calendar_base_url=args.calendar_base_url,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(summary.payload, ensure_ascii=False, sort_keys=True))
    return 1 if summary.payload["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
