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

import requests

from . import materialize_forts_raw_5m_instrument as materializer

ARTIFACT_ID: Final[str] = "dataset.forts.raw_5m.tradestats.v1"
SOURCE_ARTIFACT_ID: Final[str] = "external.apim.fo.tradestats.v1"
PRODUCER_ID: Final[str] = "moex_data.futures.refresh_forts_raw_5m_incremental.v2"
REGISTRY_PATH: Final[str] = "configs/instruments/forts_instrument_registry.v1.yaml"
OBSERVED_DATE_SOURCE_ID: Final[str] = materializer.SOURCE_ID
OBSERVED_DATE_SOURCE_ENDPOINT: Final[str] = materializer.SOURCE_ENDPOINT
MAX_APIM_PAGES: Final[int] = 500


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


def _source_error(*, secid: str, date_start: str, date_end: str, detail: str) -> ValueError:
    return ValueError(
        "fetch_observed_tradestats_dates source="
        + SOURCE_ARTIFACT_ID
        + " endpoint="
        + OBSERVED_DATE_SOURCE_ENDPOINT
        + " secid="
        + secid
        + " range="
        + date_start
        + ".."
        + date_end
        + ": "
        + detail
    )


def _page_signature(frame: object) -> tuple[object, ...]:
    if not hasattr(frame, "empty") or bool(frame.empty):
        return (0,)
    first_row = tuple(str(value) for value in frame.iloc[0].tolist())
    last_row = tuple(str(value) for value in frame.iloc[-1].tolist())
    return (int(len(frame.index)), first_row, last_row)


def fetch_observed_tradestats_dates(
    date_start: str,
    date_end: str,
    *,
    secid: str,
    timeout: float = 30.0,
    apim_base_url: str | None = None,
) -> list[str]:
    start_date = _coerce_date(date_start, "date_start")
    end_date = _coerce_date(date_end, "date_end")
    checked_secid = _require_token(secid, "secid")
    if start_date > end_date:
        raise ValueError("date_start must be <= date_end")
    base_url = materializer.core._apim_base_url(apim_base_url, None)
    endpoint = materializer.core._source_url(base_url, OBSERVED_DATE_SOURCE_ENDPOINT)
    headers = materializer._auth_headers_with_bearer(None)
    observed: set[str] = set()
    seen_signatures: set[tuple[object, ...]] = set()
    start = 0
    try:
        for _ in range(MAX_APIM_PAGES):
            params = {
                "from": start_date.isoformat(),
                "till": end_date.isoformat(),
                "secid": checked_secid,
                "start": start,
                "iss.meta": "off",
                "iss.only": "tradestats",
            }
            response = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise ValueError("response JSON root is not an object")
            frame = materializer.core._block_to_frame(payload)
            if frame.empty:
                break
            signature = _page_signature(frame)
            if signature in seen_signatures:
                raise ValueError("pagination did not advance")
            seen_signatures.add(signature)
            secid_col = materializer.core._canonical_column(frame, ("secid",))
            date_col = materializer.core._canonical_column(frame, ("tradedate", "date"))
            if secid_col is None or date_col is None:
                raise ValueError("tradestats response missing secid/tradedate columns")
            scoped = frame.loc[frame[secid_col].astype(str).str.strip().str.upper() == checked_secid.upper()]
            for raw_value in scoped[date_col].tolist():
                parsed = materializer.core._parse_trade_date(raw_value)
                if parsed is None:
                    raise ValueError("tradestats response contains invalid trade date")
                parsed_date = date.fromisoformat(parsed)
                if start_date <= parsed_date <= end_date:
                    observed.add(parsed)
            start += int(len(frame.index))
        else:
            raise ValueError("pagination exceeded max_pages guard")
    except Exception as exc:
        if isinstance(exc, ValueError) and str(exc).startswith("fetch_observed_tradestats_dates source="):
            raise
        raise _source_error(
            secid=checked_secid,
            date_start=start_date.isoformat(),
            date_end=end_date.isoformat(),
            detail=str(exc),
        ) from exc
    if not observed:
        raise _source_error(
            secid=checked_secid,
            date_start=start_date.isoformat(),
            date_end=end_date.isoformat(),
            detail="authoritative AlgoPack TradeStats source returned no observed trade dates",
        )
    return sorted(observed)


def _normalize_observed_dates(values: Sequence[str], *, date_start: date, date_end: date) -> list[str]:
    normalized: set[str] = set()
    for raw_value in values:
        value = _coerce_date(raw_value, "observed trade date")
        if value < date_start or value > date_end:
            raise ValueError("observed trade date escaped requested source range: " + value.isoformat())
        normalized.add(value.isoformat())
    if not normalized:
        raise ValueError(
            "observed TradeStats date source is empty for requested range "
            + date_start.isoformat()
            + ".."
            + date_end.isoformat()
        )
    return sorted(normalized)


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
        "date_source_artifact_id": SOURCE_ARTIFACT_ID,
        "date_source_id": OBSERVED_DATE_SOURCE_ID,
        "date_source_endpoint": OBSERVED_DATE_SOURCE_ENDPOINT,
        "date_selection_rule": "observed_trade_dates_only",
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
        "date_source_artifact_id": manifest["date_source_artifact_id"],
        "date_source_id": manifest["date_source_id"],
        "date_selection_rule": manifest["date_selection_rule"],
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
    observed_dates: Sequence[str] | None = None,
    source_date_loader: Callable[..., Sequence[str]] = fetch_observed_tradestats_dates,
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
    if upper_bound < base_last:
        raise ValueError("requested upper bound is older than base_manifest.last_valid_trade_date")
    source_start = base_last
    source_end = upper_bound
    raw_dates = list(observed_dates) if observed_dates is not None else list(
        source_date_loader(
            source_start.isoformat(),
            source_end.isoformat(),
            secid=checked_secid,
            timeout=timeout,
            apim_base_url=apim_base_url,
        )
    )
    source_dates = _normalize_observed_dates(raw_dates, date_start=source_start, date_end=source_end)
    requested_dates = [value for value in source_dates if value > base_last.isoformat()]
    incremental_start = requested_dates[0] if requested_dates else None
    last_completed = date.fromisoformat(requested_dates[-1]) if requested_dates else base_last
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
    parser = argparse.ArgumentParser(description="Incrementally refresh FORTS raw 5m tradestats from an explicit base manifest using observed source dates only.")
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
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(summary.payload, ensure_ascii=False, sort_keys=True))
    return 1 if summary.payload["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
