from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

from . import refresh_forts_raw_5m_incremental as base_refresh

ARTIFACT_ID: Final[str] = base_refresh.ARTIFACT_ID
SOURCE_ARTIFACT_ID: Final[str] = base_refresh.SOURCE_ARTIFACT_ID
CALENDAR_SOURCE_ARTIFACT_ID: Final[str] = "external.moex.iss.futures_calendar.v1"
CALENDAR_CONTRACT_ID: Final[str] = "moex_iss_futures_calendar.off_days.v1"
OBSERVED_SOURCE_CALENDAR_CONTRACT_ID: Final[str] = "observed_source_calendar_fallback.v1"
CALENDAR_ENDPOINT_BINDING_STATUS: Final[str] = "moex_iss_calendar_endpoint"
OBSERVED_SOURCE_CALENDAR_BINDING_STATUS: Final[str] = "observed_source_calendar_fallback"
CALENDAR_ENV_NAME: Final[str] = "MOEX_CALENDAR_BASE_URL"
CALENDAR_ENDPOINT_PATH: Final[str] = "/iss/calendars.json"
DEFAULT_MOEX_CALENDAR_BASE_URL: Final[str] = "https://iss.moex.com"
CALENDAR_MODE: Final[str] = "calendar"
OBSERVED_SOURCE_MODE: Final[str] = "observed-source"
CALENDAR_ENDPOINT_UNRESOLVED_NOTE: Final[str] = (
    "MOEX calendar endpoint unresolved for server scheduler; observed APIM source rows define incremental candidate acceptance"
)
EMPTY_SOURCE_ERROR_MARKERS: Final[tuple[str, ...]] = (
    "returned no rows",
    "contains no rows",
    "source table is empty",
)
POINTER_ARTIFACT_ID: Final[str] = "state.dataset.forts.raw_5m.tradestats.current_accepted_manifest.v1"
PRODUCER_ID: Final[str] = "moex_data.futures.refresh_forts_raw_5m_incremental_pointer.v1"
REGISTRY_PATH: Final[str] = base_refresh.REGISTRY_PATH


@dataclass(frozen=True)
class RefreshSummary:
    payload: dict[str, object]
    manifest_path: Path
    quality_report_path: Path
    accepted_manifest_pointer_path: Path


def load_env_file(path: str | None) -> None:
    base_refresh.load_env_file(path)


def build_accepted_manifest_pointer_path() -> Path:
    return base_refresh._data_root() / "state" / "datasets" / ("artifact_id=" + ARTIFACT_ID) / "current_accepted_manifest.json"


def _safe_reference(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(field_name + " is required")
    if any(marker in text for marker in ("*", "?", "[", "]", "{", "}", "$(", "`")):
        raise ValueError(field_name + " must be an explicit path reference without glob/autodetect markers")
    return text


def _load_pointer(pointer_path: Path, instrument_id: str, secid: str) -> dict[str, object]:
    payload = base_refresh._load_json(pointer_path, "accepted_manifest_pointer")
    if payload.get("artifact_id") != POINTER_ARTIFACT_ID:
        raise ValueError("accepted_manifest_pointer artifact_id mismatch")
    if payload.get("target_artifact_id") != ARTIFACT_ID:
        raise ValueError("accepted_manifest_pointer target_artifact_id mismatch")
    if payload.get("quality_status") != "passed":
        raise ValueError("accepted_manifest_pointer quality_status must be passed")
    if list(payload.get("instrument_id_scope") or []) != [instrument_id]:
        raise ValueError("accepted_manifest_pointer instrument_id_scope mismatch")
    if list(payload.get("secid_scope") or []) != [secid]:
        raise ValueError("accepted_manifest_pointer secid_scope mismatch")
    if payload.get("latest_autodetect_used") is not False:
        raise ValueError("accepted_manifest_pointer latest_autodetect_used must be false")
    return payload


def _manifest_reference_from_pointer(pointer_payload: Mapping[str, object]) -> Path:
    reference = _safe_reference(pointer_payload.get("accepted_manifest_reference"), "accepted_manifest_reference")
    return Path(reference)


def _calendar_base_url_contract() -> str:
    return CALENDAR_ENV_NAME + " or --calendar-base-url; default " + DEFAULT_MOEX_CALENDAR_BASE_URL + "; MOEX_API_URL is not used"


def _normalize_incremental_mode(incremental_mode: str | None) -> str:
    mode = str(incremental_mode or CALENDAR_MODE).strip().replace("_", "-")
    if mode not in (CALENDAR_MODE, OBSERVED_SOURCE_MODE):
        raise ValueError("incremental_mode must be calendar or observed-source")
    return mode


def _apply_incremental_mode_metadata(values: dict[str, object], incremental_mode: str) -> None:
    mode = _normalize_incremental_mode(incremental_mode)
    values["incremental_mode"] = mode
    values["calendar_source_artifact_id"] = CALENDAR_SOURCE_ARTIFACT_ID
    values.setdefault("skipped_empty_source_dates", [])
    values.setdefault("skipped_empty_source_date_count", 0)
    if mode == OBSERVED_SOURCE_MODE:
        values["calendar_contract"] = OBSERVED_SOURCE_CALENDAR_CONTRACT_ID
        values["calendar_binding_status"] = OBSERVED_SOURCE_CALENDAR_BINDING_STATUS
        values["calendar_base_url_contract"] = "not_used_in_observed_source_incremental_mode"
        values["calendar_endpoint"] = CALENDAR_ENDPOINT_PATH
        values["calendar_endpoint_call_allowed"] = False
        values["calendar_endpoint_unresolved_for_server_scheduler"] = True
        values["observed_source_artifact_id"] = SOURCE_ARTIFACT_ID
        values["observed_source_calendar_fallback_note"] = CALENDAR_ENDPOINT_UNRESOLVED_NOTE
    else:
        values["calendar_contract"] = CALENDAR_CONTRACT_ID
        values["calendar_binding_status"] = CALENDAR_ENDPOINT_BINDING_STATUS
        values["calendar_base_url_contract"] = _calendar_base_url_contract()
        values["calendar_endpoint"] = CALENDAR_ENDPOINT_PATH
        values["calendar_endpoint_call_allowed"] = True
        values["calendar_endpoint_unresolved_for_server_scheduler"] = False


def _mode_input_references(pointer_path: Path, base_manifest_path: Path, incremental_mode: str) -> list[str]:
    mode = _normalize_incremental_mode(incremental_mode)
    if mode == OBSERVED_SOURCE_MODE:
        return [
            pointer_path.as_posix(),
            base_manifest_path.as_posix(),
            SOURCE_ARTIFACT_ID,
            "calendar_binding_status:" + OBSERVED_SOURCE_CALENDAR_BINDING_STATUS,
            REGISTRY_PATH,
        ]
    return [
        pointer_path.as_posix(),
        base_manifest_path.as_posix(),
        SOURCE_ARTIFACT_ID,
        CALENDAR_SOURCE_ARTIFACT_ID,
        REGISTRY_PATH,
    ]


def _build_pointer_payload(
    *,
    pointer_path: Path,
    previous_pointer: Mapping[str, object],
    manifest_path: Path,
    quality_report_path: Path,
    manifest: Mapping[str, object],
    quality: Mapping[str, object],
) -> dict[str, object]:
    return {
        "artifact_id": POINTER_ARTIFACT_ID,
        "artifact_class": "accepted_manifest_pointer",
        "schema_version": POINTER_ARTIFACT_ID,
        "target_artifact_id": ARTIFACT_ID,
        "source_artifact_id": SOURCE_ARTIFACT_ID,
        "calendar_source_artifact_id": manifest.get("calendar_source_artifact_id", CALENDAR_SOURCE_ARTIFACT_ID),
        "producer": PRODUCER_ID,
        "path_contract_type": "external_pattern",
        "pointer_path": pointer_path.as_posix(),
        "accepted_manifest_reference": manifest_path.as_posix(),
        "accepted_quality_report_reference": quality_report_path.as_posix(),
        "accepted_artifact_version": manifest.get("artifact_version"),
        "previous_accepted_manifest_reference": previous_pointer.get("accepted_manifest_reference"),
        "previous_accepted_artifact_version": previous_pointer.get("accepted_artifact_version"),
        "quality_status": quality.get("quality_status"),
        "refresh_status": manifest.get("refresh_status"),
        "instrument_id_scope": list(manifest.get("instrument_id_scope") or []),
        "secid_scope": list(manifest.get("secid_scope") or []),
        "data_start": manifest.get("data_start"),
        "data_end": manifest.get("data_end"),
        "last_valid_trade_date": manifest.get("last_valid_trade_date"),
        "row_count": manifest.get("row_count"),
        "partition_count": manifest.get("partition_count"),
        "calendar_contract": manifest.get("calendar_contract"),
        "calendar_binding_status": manifest.get("calendar_binding_status"),
        "incremental_mode": manifest.get("incremental_mode"),
        "calendar_base_url_contract": manifest.get("calendar_base_url_contract"),
        "calendar_endpoint": manifest.get("calendar_endpoint"),
        "calendar_endpoint_call_allowed": manifest.get("calendar_endpoint_call_allowed"),
        "calendar_endpoint_unresolved_for_server_scheduler": manifest.get("calendar_endpoint_unresolved_for_server_scheduler"),
        "observed_source_artifact_id": manifest.get("observed_source_artifact_id"),
        "observed_source_effective_upper_bound": manifest.get("observed_source_effective_upper_bound"),
        "skipped_empty_source_dates": list(manifest.get("skipped_empty_source_dates") or []),
        "skipped_empty_source_date_count": manifest.get("skipped_empty_source_date_count", 0),
        "session_binding": manifest.get("session_binding"),
        "updated_at": base_refresh._utc_now(),
        "atomic_update_rule": "write_temp_file_in_pointer_directory_then_replace",
        "advance_rule": "only_after_quality_status_passed",
        "failed_quality_status_rule": "pointer_must_remain_unchanged",
        "latest_autodetect_used": False,
        "implicit_path_selection_used": False,
        "hardcoded_server_path_used": False,
    }


def _rewrite_manifest_and_quality(
    *,
    manifest_path: Path,
    quality_report_path: Path,
    pointer_path: Path,
    base_manifest_path: Path,
    incremental_mode: str,
) -> tuple[dict[str, object], dict[str, object]]:
    mode = _normalize_incremental_mode(incremental_mode)
    manifest = base_refresh._load_json(manifest_path, "refresh_manifest")
    quality = base_refresh._load_json(quality_report_path, "quality_report")
    input_references = _mode_input_references(pointer_path, base_manifest_path, mode)
    manifest["producer"] = PRODUCER_ID
    manifest["deterministic_builder_config_version"] = PRODUCER_ID
    manifest["base_manifest_pointer_reference"] = pointer_path.as_posix()
    manifest["base_manifest_reference"] = base_manifest_path.as_posix()
    manifest["input_references"] = input_references
    manifest["accepted_manifest_pointer_reference"] = pointer_path.as_posix()
    manifest["accepted_manifest_pointer_update_rule"] = "advance_only_after_quality_status_passed"
    _apply_incremental_mode_metadata(manifest, mode)
    quality["deterministic_builder_config_version"] = PRODUCER_ID
    quality["base_manifest_pointer_reference"] = pointer_path.as_posix()
    quality["input_references"] = input_references
    quality["accepted_manifest_pointer_reference"] = pointer_path.as_posix()
    quality["skipped_empty_source_dates"] = list(manifest.get("skipped_empty_source_dates") or [])
    quality["skipped_empty_source_date_count"] = manifest.get("skipped_empty_source_date_count", 0)
    _apply_incremental_mode_metadata(quality, mode)
    base_refresh._write_json_atomic(quality_report_path, quality)
    base_refresh._write_json_atomic(manifest_path, manifest)
    return manifest, quality


def _is_json_decode_error(exc: Exception) -> bool:
    return isinstance(exc, json.JSONDecodeError) or exc.__class__.__name__ == "JSONDecodeError"


def _resolve_calendar_base_url(calendar_base_url: str | None) -> str:
    explicit = str(calendar_base_url or "").strip()
    value = explicit or str(os.environ.get(CALENDAR_ENV_NAME, "")).strip() or DEFAULT_MOEX_CALENDAR_BASE_URL
    if any(marker in value for marker in ("*", "?", "[", "]", "{", "}", "$(", "`")):
        raise ValueError("calendar_base_url must be an explicit URL without glob/autodetect markers")
    if not (value.startswith("https://") or value.startswith("http://")):
        raise ValueError("calendar_base_url must be an explicit http(s) URL")
    return value.rstrip("/")


def _calendar_payload_from_response(response: object) -> Mapping[str, object]:
    headers = getattr(response, "headers", {})
    content_type = ""
    if isinstance(headers, Mapping):
        content_type = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
    if "json" not in content_type:
        raise ValueError("calendar_fetch_non_json_response: content_type=" + (content_type or "missing"))
    try:
        payload = response.json()
    except Exception as exc:
        if _is_json_decode_error(exc):
            raise ValueError("calendar_fetch_non_json_response: invalid_json_body") from None
        raise
    if not isinstance(payload, Mapping):
        raise ValueError("calendar_fetch_invalid_json_response")
    return payload


def fetch_futures_calendar_rows(
    date_start: str,
    date_end: str,
    *,
    timeout: float = 30.0,
    calendar_base_url: str | None = None,
) -> list[dict[str, object]]:
    import requests

    start_date = base_refresh._coerce_date(date_start, "date_start")
    end_date = base_refresh._coerce_date(date_end, "date_end")
    if start_date > end_date:
        raise ValueError("date_start must be <= date_end")
    base_url = _resolve_calendar_base_url(calendar_base_url)
    endpoint = base_url + CALENDAR_ENDPOINT_PATH
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
            payload = _calendar_payload_from_response(response)
            table = payload.get("off_days")
            if not isinstance(table, Mapping):
                raise ValueError("calendar_response_missing_off_days_table")
            columns = table.get("columns")
            data = table.get("data")
            if not isinstance(columns, list) or not isinstance(data, list):
                raise ValueError("calendar_response_invalid_off_days_table_shape")
            for item in data:
                if isinstance(item, list):
                    rows.append(dict(zip([str(column) for column in columns], item, strict=False)))
            next_start = base_refresh._next_cursor_start(payload.get("off_days.cursor"))
            if next_start is None or next_start <= cursor_start:
                break
            cursor_start = next_start
    return rows


def _classified_calendar_loader(
    calendar_loader: Callable[..., Sequence[Mapping[str, object]]],
) -> Callable[..., Sequence[Mapping[str, object]]]:
    def wrapped(*args: object, **kwargs: object) -> Sequence[Mapping[str, object]]:
        try:
            return calendar_loader(*args, **kwargs)
        except Exception as exc:
            if _is_json_decode_error(exc):
                raise ValueError("calendar_fetch_non_json_response") from None
            raise

    return wrapped


def _build_no_op_base_summary(
    *,
    artifact_version: str,
    base_manifest: Mapping[str, object],
    base_manifest_path: Path,
    instrument_id: str,
    secid: str,
    requested_end,
) -> base_refresh.RefreshSummary:
    paths = base_refresh.build_refresh_paths(artifact_version)
    build_started_at = base_refresh._utc_now()
    build_finished_at = base_refresh._utc_now()
    manifest = base_refresh._build_manifest(
        artifact_version=artifact_version,
        base_manifest=base_manifest,
        base_manifest_path=base_manifest_path,
        instrument_id=instrument_id,
        secid=secid,
        last_completed_valid_trading_day=requested_end,
        incremental_start=None,
        requested_dates=[],
        successes=[],
        failures=[],
        build_started_at=build_started_at,
        build_finished_at=build_finished_at,
        quality_report_path=paths.quality_report_path,
    )
    manifest["skipped_empty_source_dates"] = []
    manifest["skipped_empty_source_date_count"] = 0
    quality = base_refresh._quality_payload(artifact_version, manifest)
    quality["skipped_empty_source_dates"] = []
    quality["skipped_empty_source_date_count"] = 0
    base_refresh._write_json_atomic(paths.quality_report_path, quality)
    base_refresh._write_json_atomic(paths.manifest_path, manifest)
    payload = dict(manifest)
    payload["status"] = manifest["refresh_status"]
    payload["quality_status"] = quality["quality_status"]
    payload["manifest_reference"] = paths.manifest_path.as_posix()
    payload["quality_report_reference"] = paths.quality_report_path.as_posix()
    return base_refresh.RefreshSummary(payload=payload, manifest_path=paths.manifest_path, quality_report_path=paths.quality_report_path)


def _effective_upper_bound(as_of_date: str | None, date_end: str | None) -> date:
    if date_end:
        return base_refresh._coerce_date(date_end, "date_end")
    effective_as_of = base_refresh._coerce_date(as_of_date, "as_of_date") if as_of_date else datetime.now(timezone.utc).date()
    return effective_as_of - timedelta(days=1)


def _calendar_day_candidates(base_last: date, upper_bound: date) -> list[str]:
    dates: list[str] = []
    current = base_last + timedelta(days=1)
    while current <= upper_bound:
        dates.append(current.isoformat())
        current = current + timedelta(days=1)
    return dates


def _is_empty_source_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in EMPTY_SOURCE_ERROR_MARKERS)


def _payload_has_source_rows(payload: Mapping[str, object]) -> bool:
    if "row_count" not in payload:
        raise ValueError("partition runner payload missing row_count")
    try:
        return int(payload["row_count"] or 0) > 0
    except (TypeError, ValueError) as exc:
        raise ValueError("partition runner payload row_count is invalid") from exc


def _build_observed_source_summary(
    *,
    artifact_version: str,
    base_manifest: Mapping[str, object],
    base_manifest_path: Path,
    instrument_id: str,
    secid: str,
    upper_bound: date,
    requested_dates: Sequence[str],
    timeout: float,
    apim_base_url: str | None,
    runner: Callable[..., object],
) -> base_refresh.RefreshSummary:
    paths = base_refresh.build_refresh_paths(artifact_version)
    build_started_at = base_refresh._utc_now()
    successes: list[Mapping[str, object]] = []
    failures: list[Mapping[str, object]] = []
    skipped_empty_source_dates: list[str] = []
    for trade_date in requested_dates:
        try:
            result = runner(
                trade_date=trade_date,
                instrument_id=instrument_id,
                secid=secid,
                artifact_version=base_refresh._partition_version(artifact_version, trade_date, instrument_id, secid),
                timeout=timeout,
                apim_base_url=apim_base_url,
            )
            payload = base_refresh._result_payload(result)
            if _payload_has_source_rows(payload):
                successes.append(payload)
            else:
                skipped_empty_source_dates.append(trade_date)
        except Exception as exc:
            if _is_empty_source_error(exc):
                skipped_empty_source_dates.append(trade_date)
            else:
                failures.append({"trade_date": trade_date, "error": str(exc)})
    build_finished_at = base_refresh._utc_now()
    manifest = base_refresh._build_manifest(
        artifact_version=artifact_version,
        base_manifest=base_manifest,
        base_manifest_path=base_manifest_path,
        instrument_id=instrument_id,
        secid=secid,
        last_completed_valid_trading_day=upper_bound,
        incremental_start=requested_dates[0] if requested_dates else None,
        requested_dates=requested_dates,
        successes=successes,
        failures=failures,
        build_started_at=build_started_at,
        build_finished_at=build_finished_at,
        quality_report_path=paths.quality_report_path,
    )
    manifest["observed_source_candidate_start"] = requested_dates[0] if requested_dates else None
    manifest["observed_source_effective_upper_bound"] = upper_bound.isoformat()
    manifest["skipped_empty_source_dates"] = skipped_empty_source_dates
    manifest["skipped_empty_source_date_count"] = len(skipped_empty_source_dates)
    _apply_incremental_mode_metadata(manifest, OBSERVED_SOURCE_MODE)
    quality = base_refresh._quality_payload(artifact_version, manifest)
    quality["observed_source_effective_upper_bound"] = upper_bound.isoformat()
    quality["skipped_empty_source_dates"] = skipped_empty_source_dates
    quality["skipped_empty_source_date_count"] = len(skipped_empty_source_dates)
    _apply_incremental_mode_metadata(quality, OBSERVED_SOURCE_MODE)
    base_refresh._write_json_atomic(paths.quality_report_path, quality)
    base_refresh._write_json_atomic(paths.manifest_path, manifest)
    payload = dict(manifest)
    payload["status"] = manifest["refresh_status"]
    payload["quality_status"] = quality["quality_status"]
    payload["manifest_reference"] = paths.manifest_path.as_posix()
    payload["quality_report_reference"] = paths.quality_report_path.as_posix()
    return base_refresh.RefreshSummary(payload=payload, manifest_path=paths.manifest_path, quality_report_path=paths.quality_report_path)


def _finalize_pointer_summary(
    *,
    summary: base_refresh.RefreshSummary,
    pointer_path: Path,
    previous_pointer: Mapping[str, object],
    base_manifest_path: Path,
    incremental_mode: str,
) -> RefreshSummary:
    manifest, quality = _rewrite_manifest_and_quality(
        manifest_path=summary.manifest_path,
        quality_report_path=summary.quality_report_path,
        pointer_path=pointer_path,
        base_manifest_path=base_manifest_path,
        incremental_mode=incremental_mode,
    )
    pointer_updated = False
    if quality.get("quality_status") == "passed":
        pointer_payload = _build_pointer_payload(
            pointer_path=pointer_path,
            previous_pointer=previous_pointer,
            manifest_path=summary.manifest_path,
            quality_report_path=summary.quality_report_path,
            manifest=manifest,
            quality=quality,
        )
        base_refresh._write_json_atomic(pointer_path, pointer_payload)
        pointer_updated = True
    payload = dict(manifest)
    payload["status"] = manifest["refresh_status"]
    payload["quality_status"] = quality["quality_status"]
    payload["manifest_reference"] = summary.manifest_path.as_posix()
    payload["quality_report_reference"] = summary.quality_report_path.as_posix()
    payload["accepted_manifest_pointer_reference"] = pointer_path.as_posix()
    payload["accepted_manifest_pointer_updated"] = pointer_updated
    payload["latest_autodetect_used"] = False
    return RefreshSummary(
        payload=payload,
        manifest_path=summary.manifest_path,
        quality_report_path=summary.quality_report_path,
        accepted_manifest_pointer_path=pointer_path,
    )


def refresh_incremental(
    *,
    instrument_id: str,
    secid: str,
    artifact_version: str,
    registry_path: str | Path,
    as_of_date: str | None = None,
    date_end: str | None = None,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
    calendar_base_url: str | None = None,
    incremental_mode: str = CALENDAR_MODE,
    calendar_rows: Sequence[Mapping[str, object]] | None = None,
    calendar_loader: Callable[..., Sequence[Mapping[str, object]]] = fetch_futures_calendar_rows,
    runner: Callable[..., object] = base_refresh.materializer.materialize_instrument_partition,
) -> RefreshSummary:
    checked_instrument = base_refresh._require_token(instrument_id, "instrument_id")
    checked_secid = base_refresh._require_token(secid, "secid")
    checked_version = base_refresh._require_token(artifact_version, "artifact_version")
    mode = _normalize_incremental_mode(incremental_mode)
    if as_of_date and date_end:
        raise ValueError("use either as_of_date or date_end, not both")
    if not base_refresh.registry_allows_instrument(registry_path, checked_instrument, checked_secid):
        raise ValueError("instrument is not enabled by registry")
    pointer_path = build_accepted_manifest_pointer_path()
    previous_pointer = _load_pointer(pointer_path, checked_instrument, checked_secid)
    base_manifest_path = _manifest_reference_from_pointer(previous_pointer)
    base_manifest = base_refresh._load_json(base_manifest_path, "base_manifest")
    base_last_text = base_refresh._require_base_manifest(base_manifest, checked_instrument, checked_secid)
    base_last = base_refresh._coerce_date(base_last_text, "base_manifest.last_valid_trade_date")
    if mode == OBSERVED_SOURCE_MODE:
        upper_bound = _effective_upper_bound(as_of_date=as_of_date, date_end=date_end)
        if upper_bound <= base_last:
            summary = _build_no_op_base_summary(
                artifact_version=checked_version,
                base_manifest=base_manifest,
                base_manifest_path=base_manifest_path,
                instrument_id=checked_instrument,
                secid=checked_secid,
                requested_end=upper_bound,
            )
        else:
            summary = _build_observed_source_summary(
                artifact_version=checked_version,
                base_manifest=base_manifest,
                base_manifest_path=base_manifest_path,
                instrument_id=checked_instrument,
                secid=checked_secid,
                upper_bound=upper_bound,
                requested_dates=_calendar_day_candidates(base_last, upper_bound),
                timeout=timeout,
                apim_base_url=apim_base_url,
                runner=runner,
            )
        return _finalize_pointer_summary(
            summary=summary,
            pointer_path=pointer_path,
            previous_pointer=previous_pointer,
            base_manifest_path=base_manifest_path,
            incremental_mode=mode,
        )
    if date_end:
        requested_end = base_refresh._coerce_date(date_end, "date_end")
        if requested_end <= base_last:
            summary = _build_no_op_base_summary(
                artifact_version=checked_version,
                base_manifest=base_manifest,
                base_manifest_path=base_manifest_path,
                instrument_id=checked_instrument,
                secid=checked_secid,
                requested_end=requested_end,
            )
            return _finalize_pointer_summary(
                summary=summary,
                pointer_path=pointer_path,
                previous_pointer=previous_pointer,
                base_manifest_path=base_manifest_path,
                incremental_mode=mode,
            )
    summary = base_refresh.refresh_incremental(
        instrument_id=checked_instrument,
        secid=checked_secid,
        base_manifest=base_manifest_path,
        artifact_version=checked_version,
        registry_path=registry_path,
        as_of_date=as_of_date,
        date_end=date_end,
        timeout=timeout,
        apim_base_url=apim_base_url,
        calendar_base_url=_resolve_calendar_base_url(calendar_base_url),
        calendar_rows=calendar_rows,
        calendar_loader=_classified_calendar_loader(calendar_loader),
        runner=runner,
    )
    return _finalize_pointer_summary(
        summary=summary,
        pointer_path=pointer_path,
        previous_pointer=previous_pointer,
        base_manifest_path=base_manifest_path,
        incremental_mode=mode,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally refresh FORTS raw 5m tradestats from the stable accepted-manifest pointer.")
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--registry-path", required=True)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--date-end", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    parser.add_argument("--calendar-base-url", default=None)
    parser.add_argument("--incremental-mode", default=CALENDAR_MODE, choices=(CALENDAR_MODE, OBSERVED_SOURCE_MODE))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        summary = refresh_incremental(
            instrument_id=args.instrument_id,
            secid=args.secid,
            artifact_version=args.artifact_version,
            registry_path=args.registry_path,
            as_of_date=args.as_of_date,
            date_end=args.date_end,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
            calendar_base_url=args.calendar_base_url,
            incremental_mode=args.incremental_mode,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(summary.payload, ensure_ascii=False, sort_keys=True))
    return 1 if summary.payload["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
