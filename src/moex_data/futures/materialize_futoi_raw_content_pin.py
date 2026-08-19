from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Final, Sequence

from . import materialize_futoi_instrument as raw_materializer

DATASET_ID: Final[str] = "futures_futoi_raw_content_pin"
RAW_DATASET_ID: Final[str] = raw_materializer.DATASET_ID
SOURCE_ID: Final[str] = raw_materializer.SOURCE_ID
REGISTRY_PATH: Final[str] = raw_materializer.REGISTRY_PATH
CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_raw_content_pin.v1.yaml"
SCHEMA_VERSION: Final[str] = "futures_futoi_raw_content_pin.v1"


class FutoiRawContentPinError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiRawContentPinError(message)


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
        raise FutoiRawContentPinError(field_name + " must be YYYY-MM-DD") from exc


def _require_sha256(value: object, field_name: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        _fail(field_name + " must be a lowercase SHA-256 hex digest")
    return text


def _data_root() -> Path:
    return raw_materializer._data_root().resolve()


def _resolve_root_reference(value: object, field_name: str) -> Path:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    root = _data_root()
    prefix = "${MOEX_DATA_ROOT}/"
    candidate = (root / text[len(prefix) :]).resolve() if text.startswith(prefix) else Path(text).expanduser().resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        _fail(field_name + " must resolve inside MOEX_DATA_ROOT")
    return candidate


def _portable_ref(path: Path) -> str:
    root = _data_root()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError:
        _fail("path must resolve inside MOEX_DATA_ROOT")
    return "${MOEX_DATA_ROOT}/" + relative.as_posix()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_bytes_with_sha256(path: Path) -> tuple[bytes, str]:
    data = path.read_bytes()
    return data, _sha256_bytes(data)


def _load_json_bytes(data: bytes, name: str) -> dict[str, object]:
    try:
        values = json.loads(data.decode("utf-8"))
    except Exception as exc:
        raise FutoiRawContentPinError(name + " is not valid UTF-8 JSON") from exc
    if not isinstance(values, dict):
        _fail(name + " root must be an object")
    return values


def _canonical_raw_partition_path(reference: object, instrument_id: str) -> tuple[str, Path]:
    path = _resolve_root_reference(reference, "raw partition reference")
    base = (
        _data_root()
        / "market"
        / "supplementary"
        / ("dataset_id=" + RAW_DATASET_ID)
        / ("instrument_id=" + instrument_id)
    ).resolve()
    try:
        relative = path.relative_to(base)
    except ValueError:
        _fail("raw partition is outside canonical raw instrument root")
    parts = relative.parts
    if len(parts) != 3 or not parts[0].startswith("trade_date=") or parts[1] != "source=" + SOURCE_ID or parts[2] != "part.parquet":
        _fail("raw partition path does not match canonical FUTOI raw pattern")
    trade_date = _require_date(parts[0].split("=", 1)[1], "raw partition trade_date")
    expected = (base / ("trade_date=" + trade_date) / ("source=" + SOURCE_ID) / "part.parquet").resolve()
    if path != expected or not path.exists() or not path.is_file():
        _fail("raw partition is missing or not canonical: " + trade_date)
    return trade_date, path


def _expected_dates(requested_from: str, requested_till: str) -> set[str]:
    start = date.fromisoformat(requested_from)
    end = date.fromisoformat(requested_till)
    result: set[str] = set()
    current = start
    while current <= end:
        result.add(current.isoformat())
        current += timedelta(days=1)
    return result


def _validate_run_issued_partition_pins(
    manifest: dict[str, object],
    partition_pairs: list[tuple[str, Path]],
    raw_run_id: str,
) -> dict[str, dict[str, object]]:
    if str(manifest.get("content_digest_algorithm")) != "sha256":
        _fail("raw manifest content_digest_algorithm must be sha256")
    if manifest.get("partition_hashes_issued_by_raw_run") is not True:
        _fail("raw manifest does not prove partition hashes were issued by the selected raw run")
    values = manifest.get("partition_content_pins")
    if not isinstance(values, list) or len(values) != len(partition_pairs):
        _fail("raw manifest partition_content_pins do not cover every written partition")

    path_by_date = {trade_date: path.resolve() for trade_date, path in partition_pairs}
    pins: dict[str, dict[str, object]] = {}
    for value in values:
        if not isinstance(value, dict):
            _fail("raw manifest partition content pin must be an object")
        trade_date = _require_date(value.get("trade_date"), "raw manifest content pin trade_date")
        if trade_date in pins or trade_date not in path_by_date:
            _fail("raw manifest partition content pin has duplicate or unexpected trade_date")
        path = _resolve_root_reference(value.get("partition_reference"), "raw manifest content pin partition_reference")
        if path != path_by_date[trade_date]:
            _fail("raw manifest partition content pin path mismatch: " + trade_date)
        digest = _require_sha256(value.get("sha256"), "raw manifest partition sha256")
        size = int(value.get("size_bytes") or 0)
        if size <= 0:
            _fail("raw manifest partition size_bytes must be positive")
        expected_subrun = raw_run_id + "_partition_" + trade_date.replace("-", "")
        if str(value.get("issued_by_run_id")) != expected_subrun:
            _fail("raw manifest partition hash was not issued by the expected raw subrun: " + trade_date)
        data, current_digest = _read_bytes_with_sha256(path)
        if current_digest != digest or len(data) != size:
            _fail("canonical raw partition does not match selected raw-run-issued hash: " + trade_date)
        pins[trade_date] = {
            "trade_date": trade_date,
            "path": path,
            "sha256": digest,
            "size_bytes": size,
            "issued_by_run_id": expected_subrun,
        }
    if set(pins) != set(path_by_date):
        _fail("raw manifest partition content pins are incomplete")
    return pins


def validate_raw_manifest(
    raw_manifest_path: str | Path,
    instrument_id: str,
) -> tuple[dict[str, object], Path, list[tuple[str, Path]], dict[str, object], str]:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    manifest_path = _resolve_root_reference(raw_manifest_path, "raw_manifest_path")
    expected_manifest_root = (_data_root() / "state" / "refresh" / ("dataset_id=" + RAW_DATASET_ID)).resolve()
    try:
        relative = manifest_path.relative_to(expected_manifest_root)
    except ValueError:
        _fail("raw manifest is outside canonical raw refresh root")
    if len(relative.parts) != 3 or not relative.parts[0].startswith("run_date=") or not relative.parts[1].startswith("run_id=") or relative.parts[2] != "manifest.json":
        _fail("raw manifest path does not match canonical raw refresh pattern")

    manifest_bytes, manifest_sha256 = _read_bytes_with_sha256(manifest_path)
    manifest = _load_json_bytes(manifest_bytes, "raw manifest")
    if str(manifest.get("dataset_id")) != RAW_DATASET_ID:
        _fail("raw manifest dataset_id mismatch")
    if list(manifest.get("instrument_scope") or []) != [checked_instrument]:
        _fail("raw manifest instrument_scope mismatch")
    if list(manifest.get("source_scope") or []) != [SOURCE_ID]:
        _fail("raw manifest source_scope mismatch")
    if str(manifest.get("refresh_status")) != "succeeded" or list(manifest.get("failed_dates") or []):
        _fail("raw manifest is not a complete succeeded backfill")

    raw_run_id = _require_token(manifest.get("run_id"), "raw manifest run_id")
    requested_from = _require_date(manifest.get("requested_from"), "raw manifest requested_from")
    requested_till = _require_date(manifest.get("requested_till"), "raw manifest requested_till")
    run_date = _require_date(manifest.get("run_date"), "raw manifest run_date")
    if requested_from > requested_till or run_date != requested_till:
        _fail("raw manifest date identity mismatch")
    path_run_date = _require_date(relative.parts[0].split("=", 1)[1], "raw manifest path run_date")
    path_run_id = _require_token(relative.parts[1].split("=", 1)[1], "raw manifest path run_id")
    if path_run_date != run_date or path_run_id != raw_run_id:
        _fail("raw manifest path identity does not match manifest content")

    written = manifest.get("partitions_written")
    if not isinstance(written, list) or not written:
        _fail("raw manifest partitions_written must be a non-empty list")
    partition_pairs = [_canonical_raw_partition_path(item, checked_instrument) for item in written]
    trade_dates = [item[0] for item in partition_pairs]
    if len(trade_dates) != len(set(trade_dates)) or any(value < requested_from or value > requested_till for value in trade_dates):
        _fail("raw manifest written partition dates are invalid")

    skipped_values = manifest.get("partitions_skipped") or []
    if not isinstance(skipped_values, list):
        _fail("raw manifest partitions_skipped must be a list")
    skipped = [_require_date(item, "raw manifest skipped trade_date") for item in skipped_values]
    if len(skipped) != len(set(skipped)) or any(value < requested_from or value > requested_till for value in skipped):
        _fail("raw manifest skipped dates are invalid or outside requested range")
    if set(skipped) & set(trade_dates):
        _fail("raw manifest written and skipped dates overlap")
    if set(trade_dates) | set(skipped) != _expected_dates(requested_from, requested_till):
        _fail("raw manifest written plus skipped dates do not exactly reconcile to requested range")

    _validate_run_issued_partition_pins(manifest, partition_pairs, raw_run_id)

    quality_path = _resolve_root_reference(manifest.get("quality_report_ref"), "raw quality_report_ref")
    expected_quality_path = (
        _data_root()
        / "state"
        / "quality"
        / ("dataset_id=" + RAW_DATASET_ID)
        / ("run_date=" + run_date)
        / ("run_id=" + raw_run_id)
        / "quality_report.json"
    ).resolve()
    if quality_path != expected_quality_path:
        _fail("raw quality report path does not match raw manifest identity")
    quality = _load_json_bytes(quality_path.read_bytes(), "raw quality report")
    if str(quality.get("dataset_id")) != RAW_DATASET_ID or str(quality.get("run_id")) != raw_run_id:
        _fail("raw quality dataset/run identity mismatch")
    if str(quality.get("instrument_id")) != checked_instrument or str(quality.get("source_id")) != SOURCE_ID:
        _fail("raw quality instrument/source identity mismatch")
    if _require_date(quality.get("requested_from"), "raw quality requested_from") != requested_from or _require_date(quality.get("requested_till"), "raw quality requested_till") != requested_till:
        _fail("raw quality range mismatch")
    if str(quality.get("quality_status")) != "pass" or int(quality.get("partition_count") or 0) != len(partition_pairs) or int(quality.get("row_count") or 0) <= 0:
        _fail("raw quality status/count mismatch")
    if quality.get("partition_content_pins_complete") is not True or int(quality.get("partition_content_pin_count") or 0) != len(partition_pairs):
        _fail("raw quality does not confirm complete raw-run-issued partition hashes")
    if str(quality.get("content_digest_algorithm")) != "sha256":
        _fail("raw quality content_digest_algorithm mismatch")
    if list(quality.get("failed_dates") or []):
        _fail("raw quality report contains failed dates")
    if set(str(item) for item in (quality.get("skipped_empty_source_dates") or [])) != set(skipped):
        _fail("raw quality skipped dates do not match manifest")
    for field in ("duplicate_key_count", "null_required_count", "invalid_position_count"):
        if int(quality.get(field) or 0) != 0:
            _fail("raw quality " + field + " is nonzero")

    return manifest, manifest_path, sorted(partition_pairs), quality, manifest_sha256


def _pin_path(run_date: str, run_id: str) -> Path:
    return (
        _data_root()
        / "state"
        / "refresh"
        / ("dataset_id=" + DATASET_ID)
        / ("run_date=" + run_date)
        / ("run_id=" + run_id)
        / "manifest.json"
    )


def _write_json_exclusive(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        _fail("content pin run path already exists; overwrite is forbidden")
    else:
        os.close(fd)
    raw_materializer._write_json_atomic(path, values)


def _run_pin_by_date(manifest: dict[str, object], raw_run_id: str) -> dict[str, dict[str, object]]:
    values = manifest.get("partition_content_pins")
    if not isinstance(values, list):
        _fail("raw manifest partition_content_pins must be a list")
    result: dict[str, dict[str, object]] = {}
    for value in values:
        if not isinstance(value, dict):
            _fail("raw manifest partition content pin must be an object")
        trade_date = _require_date(value.get("trade_date"), "raw manifest content pin trade_date")
        if trade_date in result:
            _fail("raw manifest partition content pin has duplicate trade_date")
        expected_subrun = raw_run_id + "_partition_" + trade_date.replace("-", "")
        if str(value.get("issued_by_run_id")) != expected_subrun:
            _fail("raw manifest partition content pin issuer mismatch: " + trade_date)
        result[trade_date] = value
    return result


def create_content_pin(
    *,
    instrument_id: str,
    run_id: str,
    raw_manifest_path: str | Path,
) -> dict[str, object]:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_run_id = _require_token(run_id, "run_id")
    binding = raw_materializer._registry_binding(REGISTRY_PATH, checked_instrument)
    if str(binding["futoi.source_id"]) != SOURCE_ID:
        _fail("registry FUTOI source_id does not match canonical source")
    manifest, manifest_path, partition_pairs, _, manifest_sha256 = validate_raw_manifest(raw_manifest_path, checked_instrument)
    raw_run_id = _require_token(manifest.get("run_id"), "raw manifest run_id")
    requested_from = _require_date(manifest.get("requested_from"), "requested_from")
    requested_till = _require_date(manifest.get("requested_till"), "requested_till")
    run_pins = _run_pin_by_date(manifest, raw_run_id)

    entries: list[dict[str, object]] = []
    total_size = 0
    for trade_date, path in partition_pairs:
        issued = run_pins.get(trade_date)
        if issued is None:
            _fail("selected raw run did not issue a hash for partition: " + trade_date)
        expected_digest = _require_sha256(issued.get("sha256"), "run-issued partition sha256")
        expected_size = int(issued.get("size_bytes") or 0)
        data, actual_digest = _read_bytes_with_sha256(path)
        if actual_digest != expected_digest or len(data) != expected_size:
            _fail("canonical raw partition no longer matches selected raw run: " + trade_date)
        total_size += expected_size
        entries.append(
            {
                "trade_date": trade_date,
                "raw_partition_reference": _portable_ref(path),
                "sha256": expected_digest,
                "size_bytes": expected_size,
                "raw_hash_issued_by_run_id": str(issued.get("issued_by_run_id")),
            }
        )

    pin_path = _pin_path(requested_till, checked_run_id)
    pin = {
        "schema_version": SCHEMA_VERSION,
        "run_id": checked_run_id,
        "run_date": requested_till,
        "dataset_id": DATASET_ID,
        "raw_dataset_id": RAW_DATASET_ID,
        "instrument_id": checked_instrument,
        "source_id": SOURCE_ID,
        "raw_run_id": raw_run_id,
        "raw_manifest_reference": _portable_ref(manifest_path),
        "raw_manifest_sha256": manifest_sha256,
        "requested_from": requested_from,
        "requested_till": requested_till,
        "partition_count": len(entries),
        "total_size_bytes": total_size,
        "skipped_empty_source_dates": list(manifest.get("partitions_skipped") or []),
        "partitions": entries,
        "pin_status": "complete",
        "created_at_utc": raw_materializer._utc_now(),
        "content_digest_algorithm": "sha256",
        "raw_partition_hash_source": "selected_raw_run_writer",
        "contract_ref": CONTRACT_REF,
        "hardcoded_server_path_used": False,
        "dynamic_scan_used": False,
        "direct_source_refetch_used": False,
        "accepted_manifest_pointer_reference": None,
    }
    _write_json_exclusive(pin_path, pin)
    pin_bytes, pin_sha256 = _read_bytes_with_sha256(pin_path)
    _load_json_bytes(pin_bytes, "content pin")
    return {
        "status": "succeeded",
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "raw_run_id": raw_run_id,
        "raw_manifest_reference": pin["raw_manifest_reference"],
        "raw_manifest_sha256": manifest_sha256,
        "partition_count": len(entries),
        "content_pin_reference": pin_path.as_posix(),
        "content_pin_sha256": pin_sha256,
        "pin_status": "complete",
        "raw_partition_hash_source": "selected_raw_run_writer",
        "hardcoded_server_path_used": False,
        "dynamic_scan_used": False,
        "direct_source_refetch_used": False,
    }


def load_and_verify_content_pin(
    pin_path_value: str | Path,
    expected_pin_sha256: str,
    instrument_id: str,
) -> tuple[dict[str, object], Path, dict[str, object], Path, list[dict[str, object]]]:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    expected_digest = _require_sha256(expected_pin_sha256, "expected_pin_sha256")
    pin_path = _resolve_root_reference(pin_path_value, "raw_content_pin_path")
    pin_bytes, actual_digest = _read_bytes_with_sha256(pin_path)
    if actual_digest != expected_digest:
        _fail("raw content pin SHA-256 mismatch")
    pin = _load_json_bytes(pin_bytes, "raw content pin")
    if str(pin.get("schema_version")) != SCHEMA_VERSION or str(pin.get("dataset_id")) != DATASET_ID:
        _fail("raw content pin schema/dataset mismatch")
    if str(pin.get("instrument_id")) != checked_instrument or str(pin.get("source_id")) != SOURCE_ID:
        _fail("raw content pin instrument/source mismatch")
    if str(pin.get("raw_dataset_id")) != RAW_DATASET_ID or str(pin.get("pin_status")) != "complete":
        _fail("raw content pin upstream/status mismatch")
    if str(pin.get("raw_partition_hash_source")) != "selected_raw_run_writer":
        _fail("raw content pin does not use selected raw-run-issued partition hashes")
    if pin.get("dynamic_scan_used") is not False or pin.get("direct_source_refetch_used") is not False:
        _fail("raw content pin used forbidden discovery or source refetch")

    run_id = _require_token(pin.get("run_id"), "content pin run_id")
    run_date = _require_date(pin.get("run_date"), "content pin run_date")
    expected_path = _pin_path(run_date, run_id).resolve()
    if pin_path != expected_path:
        _fail("raw content pin path identity mismatch")

    raw_manifest, raw_manifest_path, partition_pairs, _, manifest_sha256 = validate_raw_manifest(
        pin.get("raw_manifest_reference"),
        checked_instrument,
    )
    if manifest_sha256 != _require_sha256(pin.get("raw_manifest_sha256"), "raw_manifest_sha256"):
        _fail("raw aggregate manifest content changed after pin creation")
    raw_run_id = _require_token(raw_manifest.get("run_id"), "raw manifest run_id")
    if str(pin.get("raw_run_id")) != raw_run_id:
        _fail("raw content pin raw_run_id mismatch")
    if _require_date(pin.get("requested_from"), "pin requested_from") != _require_date(raw_manifest.get("requested_from"), "raw requested_from") or _require_date(pin.get("requested_till"), "pin requested_till") != _require_date(raw_manifest.get("requested_till"), "raw requested_till"):
        _fail("raw content pin requested range mismatch")

    run_pins = _run_pin_by_date(raw_manifest, raw_run_id)
    entries = pin.get("partitions")
    if not isinstance(entries, list) or len(entries) != len(partition_pairs) or int(pin.get("partition_count") or 0) != len(partition_pairs):
        _fail("raw content pin partition count mismatch")
    by_date = {trade_date: path for trade_date, path in partition_pairs}
    seen: set[str] = set()
    normalized_entries: list[dict[str, object]] = []
    for item in entries:
        if not isinstance(item, dict):
            _fail("raw content pin partition entry must be an object")
        trade_date = _require_date(item.get("trade_date"), "pin partition trade_date")
        if trade_date in seen or trade_date not in by_date:
            _fail("raw content pin contains duplicate or unexpected trade_date")
        seen.add(trade_date)
        path = _resolve_root_reference(item.get("raw_partition_reference"), "pin raw_partition_reference")
        if path != by_date[trade_date].resolve():
            _fail("raw content pin partition path mismatch: " + trade_date)
        digest = _require_sha256(item.get("sha256"), "partition sha256")
        size = int(item.get("size_bytes") or -1)
        issued = run_pins.get(trade_date)
        if issued is None:
            _fail("selected raw run hash missing for content-pin partition: " + trade_date)
        if digest != _require_sha256(issued.get("sha256"), "run-issued partition sha256") or size != int(issued.get("size_bytes") or -1):
            _fail("content pin partition digest does not match selected raw run: " + trade_date)
        if str(item.get("raw_hash_issued_by_run_id")) != str(issued.get("issued_by_run_id")):
            _fail("content pin partition issuer does not match selected raw run: " + trade_date)
        data, actual_digest = _read_bytes_with_sha256(path)
        if actual_digest != digest or len(data) != size:
            _fail("raw partition content changed after pin creation: " + trade_date)
        normalized_entries.append(
            {
                "trade_date": trade_date,
                "path": path,
                "sha256": digest,
                "size_bytes": size,
                "raw_hash_issued_by_run_id": str(item.get("raw_hash_issued_by_run_id")),
            }
        )
    if seen != set(by_date):
        _fail("raw content pin does not cover every raw manifest partition")
    return pin, pin_path, raw_manifest, raw_manifest_path, sorted(normalized_entries, key=lambda item: str(item["trade_date"]))


def read_verified_partition_bytes(entry: dict[str, object]) -> bytes:
    path = Path(str(entry["path"])).resolve()
    expected_digest = _require_sha256(entry.get("sha256"), "partition sha256")
    data, actual_digest = _read_bytes_with_sha256(path)
    if actual_digest != expected_digest or len(data) != int(entry.get("size_bytes") or -1):
        _fail("raw partition content changed between pin validation and consumption: " + str(entry.get("trade_date")))
    return data


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create an immutable SHA-256 content pin for one completed canonical FUTOI raw aggregate backfill with raw-run-issued partition hashes.")
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--raw-manifest-path", required=True)
    parser.add_argument("--env-file", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        raw_materializer.load_env_file(args.env_file)
        payload = create_content_pin(
            instrument_id=args.instrument_id,
            run_id=args.run_id,
            raw_manifest_path=args.raw_manifest_path,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "dynamic_scan_used": False, "direct_source_refetch_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
