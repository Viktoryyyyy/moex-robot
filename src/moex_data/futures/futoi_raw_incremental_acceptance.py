from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import pandas as pd

from . import backfill_futoi_instrument as backfill
from . import materialize_futoi_instrument as materializer

CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_raw_incremental_acceptance.v1.yaml"
SCHEMA_VERSION: Final[str] = "futures_futoi_raw_incremental_acceptance.v1"
PRODUCER_ID: Final[str] = "moex_data.futures.futoi_raw_incremental_acceptance.v1"
DATASET_ID: Final[str] = materializer.DATASET_ID
SOURCE_ID: Final[str] = materializer.SOURCE_ID
ALLOWED_INSTRUMENTS: Final[frozenset[str]] = frozenset({"si_futures_family", "cr_futures_family"})
ROOT_REF_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"


class FutoiIncrementalAcceptanceError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiIncrementalAcceptanceError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or any(marker in text for marker in ("/", "\\", "*", "?", "[", "]", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError(field + " must be YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        _fail(field + " must be canonical YYYY-MM-DD")
    return text


def _data_root() -> Path:
    raw = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        _fail("MOEX_DATA_ROOT is required")
    root = Path(raw)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    if root.is_symlink() or not root.is_dir():
        _fail("MOEX_DATA_ROOT must be an existing non-symlink directory")
    return root.resolve(strict=True)


def _load_json(path: Path, field: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        _fail(field + " must be a regular non-symlink file")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FutoiIncrementalAcceptanceError(field + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must be a JSON object")
    return values


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _rooted_ref(root: Path, path: Path) -> str:
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError("artifact escaped MOEX_DATA_ROOT") from exc
    return ROOT_REF_PREFIX + relative.as_posix()


def _resolve_ref(root: Path, value: object, field: str) -> Path:
    text = str(value or "").strip()
    if not text or any(marker in text for marker in ("*", "?", "[", "]", "{INSTRUMENT", "$(", "`")):
        _fail(field + " must be an explicit path reference")
    if text.startswith(ROOT_REF_PREFIX):
        candidate = root / text[len(ROOT_REF_PREFIX) :]
    else:
        candidate = Path(text)
    if not candidate.is_absolute():
        _fail(field + " must resolve to an absolute path")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError(field + " escaped MOEX_DATA_ROOT") from exc
    if resolved.is_symlink() or not resolved.is_file():
        _fail(field + " must resolve to a regular non-symlink file")
    return resolved


def _historical_pointer_path(root: Path, instrument_id: str) -> Path:
    return root / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def incremental_pointer_path(root: Path, instrument_id: str) -> Path:
    return root / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + instrument_id) / "current_incremental_accepted_manifest.json"


def accepted_manifest_path(root: Path, instrument_id: str, run_id: str) -> Path:
    return root / "state" / "accepted_manifests" / "futoi_raw_incremental" / ("instrument_id=" + instrument_id) / ("run_id=" + run_id) / "manifest.json"


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _fail(field + " must be a positive integer")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(field + " must be a nonnegative integer")
    return value


def _parent_state(root: Path, instrument_id: str) -> dict[str, object]:
    incremental_path = incremental_pointer_path(root, instrument_id)
    if incremental_path.exists() or incremental_path.is_symlink():
        pointer_path = incremental_path
        pointer = _load_json(pointer_path, "incremental parent pointer")
        if pointer.get("schema_version") != SCHEMA_VERSION or pointer.get("producer") != PRODUCER_ID:
            _fail("incremental parent pointer identity mismatch")
        if pointer.get("dataset_id") != DATASET_ID or pointer.get("instrument_id") != instrument_id or pointer.get("source_id") != SOURCE_ID:
            _fail("incremental parent pointer scope mismatch")
        if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass":
            _fail("incremental parent pointer is not accepted")
        manifest_path = _resolve_ref(root, pointer.get("manifest_ref"), "incremental parent manifest_ref")
        manifest_sha = str(pointer.get("manifest_sha256") or "")
        if len(manifest_sha) != 64 or _sha256(manifest_path) != manifest_sha:
            _fail("incremental parent manifest SHA mismatch")
        manifest = _load_json(manifest_path, "incremental parent manifest")
        if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("acceptance_status") != "pass":
            _fail("incremental parent manifest identity/status mismatch")
        if manifest.get("instrument_id") != instrument_id or manifest.get("source_id") != SOURCE_ID:
            _fail("incremental parent manifest scope mismatch")
        parent_start = _iso_date(manifest.get("cumulative_from"), "parent cumulative_from")
        parent_end = _iso_date(manifest.get("cumulative_till"), "parent cumulative_till")
        parent_partitions = _positive_int(manifest.get("cumulative_partition_count"), "parent cumulative_partition_count")
        parent_rows = _positive_int(manifest.get("cumulative_row_count"), "parent cumulative_row_count")
        parent_kind = "incremental"
    else:
        pointer_path = _historical_pointer_path(root, instrument_id)
        pointer = _load_json(pointer_path, "historical parent pointer")
        if pointer.get("dataset_id") != DATASET_ID or pointer.get("instrument_id") != instrument_id:
            _fail("historical parent pointer scope mismatch")
        if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass":
            _fail("historical parent pointer is not accepted")
        manifest_path = _resolve_ref(root, pointer.get("manifest_ref"), "historical parent manifest_ref")
        manifest = _load_json(manifest_path, "historical parent manifest")
        if manifest.get("target_dataset_id") != DATASET_ID or manifest.get("instrument_id") != instrument_id or manifest.get("source_id") != SOURCE_ID:
            _fail("historical parent manifest scope mismatch")
        if manifest.get("acceptance_status") != "pass":
            _fail("historical parent manifest is not accepted")
        parent_start = _iso_date(manifest.get("requested_from"), "historical requested_from")
        parent_end = _iso_date(manifest.get("requested_till"), "historical requested_till")
        parent_partitions = _positive_int(manifest.get("partition_count"), "historical partition_count")
        parent_rows = _positive_int(manifest.get("row_count"), "historical row_count")
        parent_kind = "historical_stage2"
    return {
        "kind": parent_kind,
        "pointer_path": pointer_path,
        "pointer_sha256": _sha256(pointer_path),
        "manifest_path": manifest_path,
        "manifest_sha256": _sha256(manifest_path),
        "start": parent_start,
        "end": parent_end,
        "partition_count": parent_partitions,
        "row_count": parent_rows,
    }


def _expected_dates(start: str, end: str) -> list[str]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    if first > last:
        _fail("incremental requested range is inverted")
    values: list[str] = []
    current = first
    while current <= last:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _validate_partition(root: Path, path_value: object, instrument_id: str, requested_start: str, requested_end: str) -> dict[str, object]:
    path = Path(str(path_value or ""))
    if not path.is_absolute():
        _fail("backfill partition path must be absolute")
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError("backfill partition escaped MOEX_DATA_ROOT") from exc
    if resolved.is_symlink() or not resolved.is_file():
        _fail("backfill partition must be a regular non-symlink file")
    frame = pd.read_parquet(resolved)
    if frame.empty:
        _fail("accepted incremental partition must not be empty")
    required = set(materializer.SOURCE_RECORD_KEY_FIELDS) | {"instrument_id", "source_id", "clgroup"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        _fail("incremental partition missing required columns: " + ",".join(missing))
    trade_dates = sorted(set(frame["trade_date"].astype(str)))
    if len(trade_dates) != 1:
        _fail("incremental partition must contain exactly one trade_date")
    trade_date = _iso_date(trade_dates[0], "partition trade_date")
    if trade_date < requested_start or trade_date > requested_end:
        _fail("incremental partition trade_date is outside requested range")
    if set(frame["instrument_id"].astype(str)) != {instrument_id}:
        _fail("incremental partition instrument_id mismatch")
    if set(frame["source_id"].astype(str)) != {SOURCE_ID}:
        _fail("incremental partition source_id mismatch")
    groups = set(frame["clgroup"].astype(str).str.upper().str.strip())
    if groups != {"FIZ", "YUR"}:
        _fail("incremental partition must contain exactly FIZ and YUR clgroups")
    duplicate_count = int(frame.duplicated(subset=list(materializer.SOURCE_RECORD_KEY_FIELDS)).sum())
    if duplicate_count:
        _fail("incremental partition contains duplicate source keys")
    expected_path = materializer._partition_path(trade_date, instrument_id, SOURCE_ID).resolve()
    if resolved != expected_path:
        _fail("incremental partition path differs from canonical path contract")
    return {
        "trade_date": trade_date,
        "partition_ref": _rooted_ref(root, resolved),
        "sha256": _sha256(resolved),
        "row_count": int(len(frame.index)),
        "clgroups": ["FIZ", "YUR"],
        "duplicate_source_key_count": 0,
    }


def _validate_backfill(root: Path, instrument_id: str, run_id: str, date_end: str, parent_end: str) -> dict[str, object]:
    expected_start = (date.fromisoformat(parent_end) + timedelta(days=1)).isoformat()
    manifest_path = backfill._aggregate_manifest_path(date_end, run_id).resolve()
    quality_path = backfill._aggregate_quality_path(date_end, run_id).resolve()
    manifest = _load_json(manifest_path, "incremental backfill manifest")
    quality = _load_json(quality_path, "incremental backfill quality")
    if manifest.get("producer") != backfill.PRODUCER_ID or manifest.get("dataset_id") != DATASET_ID:
        _fail("incremental backfill manifest identity mismatch")
    if manifest.get("instrument_scope") != [instrument_id] or manifest.get("source_scope") != [SOURCE_ID]:
        _fail("incremental backfill manifest scope mismatch")
    if manifest.get("stage2_controlled_backfill") is not True or manifest.get("latest_autodetect_used") is not False or manifest.get("hardcoded_server_path_used") is not False:
        _fail("incremental backfill manifest governance flags mismatch")
    if manifest.get("refresh_status") != "succeeded" or manifest.get("failed_dates") != []:
        _fail("incremental backfill did not succeed without failures")
    requested_start = _iso_date(manifest.get("requested_from"), "backfill requested_from")
    requested_end = _iso_date(manifest.get("requested_till"), "backfill requested_till")
    if requested_start != expected_start or requested_end != date_end:
        _fail("incremental backfill must start at parent end plus one calendar day and end at explicit date_end")
    if quality.get("dataset_id") != DATASET_ID or quality.get("instrument_id") != instrument_id or quality.get("source_id") != SOURCE_ID:
        _fail("incremental backfill quality scope mismatch")
    if quality.get("quality_status") != "pass" or quality.get("failed_dates") != []:
        _fail("incremental backfill quality is not pass")
    for field in ("duplicate_key_count", "null_required_count", "invalid_position_count"):
        if _nonnegative_int(quality.get(field), "quality " + field) != 0:
            _fail("incremental backfill quality defect: " + field)
    quality_ref = str(manifest.get("quality_report_ref") or "")
    if Path(quality_ref).resolve() != quality_path:
        _fail("incremental backfill manifest quality_report_ref mismatch")
    written = manifest.get("partitions_written")
    skipped = manifest.get("partitions_skipped")
    if isinstance(written, (str, bytes)) or not isinstance(written, Sequence):
        _fail("incremental backfill partitions_written must be a list")
    if isinstance(skipped, (str, bytes)) or not isinstance(skipped, Sequence):
        _fail("incremental backfill partitions_skipped must be a list")
    skipped_dates = [_iso_date(value, "skipped date") for value in skipped]
    if skipped_dates != sorted(set(skipped_dates)):
        _fail("incremental skipped dates must be sorted and unique")
    records = [_validate_partition(root, value, instrument_id, requested_start, requested_end) for value in written]
    trade_dates = [str(row["trade_date"]) for row in records]
    if trade_dates != sorted(set(trade_dates)):
        _fail("incremental written partition dates must be sorted and unique")
    expected = _expected_dates(requested_start, requested_end)
    if sorted(trade_dates + skipped_dates) != expected or set(trade_dates) & set(skipped_dates):
        _fail("incremental backfill coverage must exactly partition requested calendar dates into written and skipped")
    row_count = sum(int(row["row_count"]) for row in records)
    if _positive_int(quality.get("partition_count"), "quality partition_count") != len(records):
        _fail("incremental backfill partition_count mismatch")
    if _positive_int(quality.get("row_count"), "quality row_count") != row_count:
        _fail("incremental backfill row_count mismatch")
    if list(quality.get("skipped_empty_source_dates") or []) != skipped_dates:
        _fail("incremental backfill skipped dates mismatch between manifest and quality")
    return {
        "manifest_path": manifest_path,
        "manifest_sha256": _sha256(manifest_path),
        "quality_path": quality_path,
        "quality_sha256": _sha256(quality_path),
        "requested_from": requested_start,
        "requested_till": requested_end,
        "records": records,
        "skipped_dates": skipped_dates,
        "partition_count": len(records),
        "row_count": row_count,
    }


def _json_bytes(values: Mapping[str, object]) -> bytes:
    return (json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def _write_create_only(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        _fail("immutable incremental acceptance artifact already exists")
    payload = _json_bytes(values)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise FutoiIncrementalAcceptanceError("immutable incremental acceptance artifact appeared concurrently") from exc


def _write_pointer_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        _fail("incremental accepted pointer must not be a symlink")
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".tmp") as handle:
        handle.write(_json_bytes(values))
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _publication_lock(root: Path, instrument_id: str):
    path = incremental_pointer_path(root, instrument_id).with_name(".futoi_incremental_acceptance.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def accept_incremental_backfill(*, instrument_id: str, backfill_run_id: str, date_end: str) -> dict[str, object]:
    checked_instrument = _safe_token(instrument_id, "instrument_id")
    if checked_instrument not in ALLOWED_INSTRUMENTS:
        _fail("instrument_id is outside canonical Stage 5 FUTOI scope")
    checked_run = _safe_token(backfill_run_id, "backfill_run_id")
    checked_end = _iso_date(date_end, "date_end")
    root = _data_root()
    with _publication_lock(root, checked_instrument):
        parent = _parent_state(root, checked_instrument)
        if checked_end <= str(parent["end"]):
            _fail("date_end must be later than parent accepted end")
        source = _validate_backfill(root, checked_instrument, checked_run, checked_end, str(parent["end"]))
        parent_pointer = Path(parent["pointer_path"])
        parent_manifest = Path(parent["manifest_path"])
        if _sha256(parent_pointer) != parent["pointer_sha256"] or _sha256(parent_manifest) != parent["manifest_sha256"]:
            _fail("parent accepted state changed during incremental validation")
        manifest_path = accepted_manifest_path(root, checked_instrument, checked_run)
        manifest_values: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "producer": PRODUCER_ID,
            "contract_ref": CONTRACT_REF,
            "dataset_id": DATASET_ID,
            "instrument_id": checked_instrument,
            "source_id": SOURCE_ID,
            "acceptance_run_id": checked_run,
            "acceptance_status": "pass",
            "quality_status": "pass",
            "parent_kind": parent["kind"],
            "parent_pointer_ref": _rooted_ref(root, parent_pointer),
            "parent_pointer_sha256": parent["pointer_sha256"],
            "parent_manifest_ref": _rooted_ref(root, parent_manifest),
            "parent_manifest_sha256": parent["manifest_sha256"],
            "parent_accepted_till": parent["end"],
            "source_backfill_manifest_ref": _rooted_ref(root, Path(source["manifest_path"])),
            "source_backfill_manifest_sha256": source["manifest_sha256"],
            "source_backfill_quality_ref": _rooted_ref(root, Path(source["quality_path"])),
            "source_backfill_quality_sha256": source["quality_sha256"],
            "incremental_from": source["requested_from"],
            "incremental_till": source["requested_till"],
            "incremental_partition_count": source["partition_count"],
            "incremental_row_count": source["row_count"],
            "skipped_empty_source_dates": source["skipped_dates"],
            "partitions": source["records"],
            "cumulative_from": parent["start"],
            "cumulative_till": source["requested_till"],
            "cumulative_partition_count": int(parent["partition_count"]) + int(source["partition_count"]),
            "cumulative_row_count": int(parent["row_count"]) + int(source["row_count"]),
            "historical_baseline_pointer_mutated": False,
            "source_materialization_network_access_used": True,
            "acceptance_network_access_used": False,
            "latest_autodetect_used": False,
            "implicit_partition_discovery_used": False,
            "historical_pit_research_ready_claimed": False,
            "directional_signal_authority": False,
            "trading_action_authority": False,
        }
        _write_create_only(manifest_path, manifest_values)
        manifest_sha = _sha256(manifest_path)
        if _sha256(parent_pointer) != parent["pointer_sha256"] or _sha256(parent_manifest) != parent["manifest_sha256"]:
            _fail("parent accepted state changed before pointer publication")
        pointer_path = incremental_pointer_path(root, checked_instrument)
        if parent["kind"] == "historical_stage2" and (pointer_path.exists() or pointer_path.is_symlink()):
            _fail("incremental pointer appeared concurrently")
        pointer_values: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "producer": PRODUCER_ID,
            "dataset_id": DATASET_ID,
            "instrument_id": checked_instrument,
            "source_id": SOURCE_ID,
            "acceptance_status": "pass",
            "quality_status": "pass",
            "run_id": checked_run,
            "manifest_ref": _rooted_ref(root, manifest_path),
            "manifest_sha256": manifest_sha,
            "previous_parent_kind": parent["kind"],
            "previous_parent_pointer_ref": _rooted_ref(root, parent_pointer),
            "previous_parent_pointer_sha256": parent["pointer_sha256"],
            "cumulative_from": parent["start"],
            "cumulative_till": source["requested_till"],
            "cumulative_partition_count": manifest_values["cumulative_partition_count"],
            "cumulative_row_count": manifest_values["cumulative_row_count"],
            "promotion_basis": "validated_explicit_date_futoi_backfill_increment",
            "historical_baseline_pointer_mutated": False,
            "latest_autodetect_used": False,
            "historical_pit_research_ready_claimed": False,
            "directional_signal_authority": False,
            "trading_action_authority": False,
        }
        _write_pointer_atomic(pointer_path, pointer_values)
        return {
            "status": "accepted",
            "dataset_id": DATASET_ID,
            "instrument_id": checked_instrument,
            "source_id": SOURCE_ID,
            "acceptance_run_id": checked_run,
            "incremental_from": source["requested_from"],
            "incremental_till": source["requested_till"],
            "incremental_partition_count": source["partition_count"],
            "incremental_row_count": source["row_count"],
            "cumulative_from": manifest_values["cumulative_from"],
            "cumulative_till": manifest_values["cumulative_till"],
            "cumulative_partition_count": manifest_values["cumulative_partition_count"],
            "cumulative_row_count": manifest_values["cumulative_row_count"],
            "accepted_manifest_ref": _rooted_ref(root, manifest_path),
            "accepted_manifest_sha256": manifest_sha,
            "incremental_pointer_ref": _rooted_ref(root, pointer_path),
            "historical_baseline_pointer_mutated": False,
            "acceptance_network_access_used": False,
            "latest_autodetect_used": False,
            "directional_signal_authority": False,
            "trading_action_authority": False,
        }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Accept an explicit canonical FUTOI backfill increment without mutating the historical Stage 2 baseline pointer.")
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--backfill-run-id", required=True)
    parser.add_argument("--date-end", required=True)
    parser.add_argument("--env-file", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        materializer.load_env_file(args.env_file)
        result = accept_incremental_backfill(
            instrument_id=args.instrument_id,
            backfill_run_id=args.backfill_run_id,
            date_end=args.date_end,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "incremental_pointer_updated": False, "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
