from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

from . import refresh_forts_raw_5m_incremental as base_refresh

ARTIFACT_ID: Final[str] = base_refresh.ARTIFACT_ID
SOURCE_ARTIFACT_ID: Final[str] = base_refresh.SOURCE_ARTIFACT_ID
CALENDAR_SOURCE_ARTIFACT_ID: Final[str] = "external.moex.iss.futures_calendar.v1"
CALENDAR_CONTRACT_ID: Final[str] = "moex_iss_futures_calendar.off_days.v1"
CALENDAR_ENV_NAME: Final[str] = "MOEX_CALENDAR_BASE_URL"
CALENDAR_ENDPOINT_PATH: Final[str] = "/iss/calendars.json"
DEFAULT_MOEX_CALENDAR_BASE_URL: Final[str] = "https://iss.moex.com"
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
        "calendar_base_url_contract": manifest.get("calendar_base_url_contract"),
        "calendar_endpoint": manifest.get("calendar_endpoint"),
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
) -> tuple[dict[str, object], dict[str, object]]:
    manifest = base_refresh._load_json(manifest_path, "refresh_manifest")
    quality = base_refresh._load_json(quality_report_path, "quality_report")
    input_references = [
        pointer_path.as_posix(),
        base_manifest_path.as_posix(),
        SOURCE_ARTIFACT_ID,
        CALENDAR_SOURCE_ARTIFACT_ID,
        REGISTRY_PATH,
    ]
    manifest["producer"] = PRODUCER_ID
    manifest["deterministic_builder_config_version"] = PRODUCER_ID
    manifest["base_manifest_pointer_reference"] = pointer_path.as_posix()
    manifest["base_manifest_reference"] = base_manifest_path.as_posix()
    manifest["input_references"] = input_references
    manifest["accepted_manifest_pointer_reference"] = pointer_path.as_posix()
    manifest["accepted_manifest_pointer_update_rule"] = "advance_only_after_quality_status_passed"
    manifest["calendar_source_artifact_id"] = CALENDAR_SOURCE_ARTIFACT_ID
    manifest["calendar_contract"] = CALENDAR_CONTRACT_ID
    manifest["calendar_base_url_contract"] = _calendar_base_url_contract()
    manifest["calendar_endpoint"] = CALENDAR_ENDPOINT_PATH
    quality["deterministic_builder_config_version"] = PRODUCER_ID
    quality["base_manifest_pointer_reference"] = pointer_path.as_posix()
    quality["input_references"] = input_references
    quality["accepted_manifest_pointer_reference"] = pointer_path.as_posix()
    quality["calendar_source_artifact_id"] = CALENDAR_SOURCE_ARTIFACT_ID
    quality["calendar_contract"] = CALENDAR_CONTRACT_ID
    quality["calendar_base_url_contract"] = _calendar_base_url_contract()
    quality["calendar_endpoint"] = CALENDAR_ENDPOINT_PATH
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
    quality = base_refresh._quality_payload(artifact_version, manifest)
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
) -> RefreshSummary:
    manifest, quality = _rewrite_manifest_and_quality(
        manifest_path=summary.manifest_path,
        quality_report_path=summary.quality_report_path,
        pointer_path=pointer_path,
        base_manifest_path=base_manifest_path,
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
    calendar_rows: Sequence[Mapping[str, object]] | None = None,
    calendar_loader: Callable[..., Sequence[Mapping[str, object]]] = fetch_futures_calendar_rows,
    runner: Callable[..., object] = base_refresh.materializer.materialize_instrument_partition,
) -> RefreshSummary:
    checked_instrument = base_refresh._require_token(instrument_id, "instrument_id")
    checked_secid = base_refresh._require_token(secid, "secid")
    checked_version = base_refresh._require_token(artifact_version, "artifact_version")
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
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(summary.payload, ensure_ascii=False, sort_keys=True))
    return 1 if summary.payload["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
