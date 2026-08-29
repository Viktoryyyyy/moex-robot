from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
import stat
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
RAW_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "instrument_id",
    "trade_date",
    "ts",
    "moment",
    "systime",
    "sess_id",
    "seqnum",
    "secid",
    "board",
    "market",
    "engine",
    "source_id",
    "source_ticker",
    "clgroup",
    "pos",
    "pos_long",
    "pos_short",
    "pos_long_num",
    "pos_short_num",
    "availability_ts_utc",
    "ingest_ts",
)


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


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _snapshot_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (int(value.st_dev), int(value.st_ino), int(value.st_size), int(value.st_mtime_ns))


def _read_regular_snapshot(path: Path, field: str) -> dict[str, object]:
    if path.is_symlink():
        _fail(field + " must not be a symlink")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise FutoiIncrementalAcceptanceError(field + " must be an existing regular non-symlink file: " + str(exc)) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail(field + " must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read()
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _snapshot_identity(before) != _snapshot_identity(after) or len(raw) != int(before.st_size):
        _fail(field + " changed while snapshot was read")
    resolved = path.resolve(strict=True)
    return {
        "path": resolved,
        "raw": raw,
        "sha256": _sha256_bytes(raw),
        "identity": _snapshot_identity(before),
    }


def _load_json_snapshot(path: Path, field: str) -> tuple[dict[str, object], dict[str, object]]:
    snapshot = _read_regular_snapshot(path, field)
    try:
        values = json.loads(bytes(snapshot["raw"]).decode("utf-8"))
    except Exception as exc:
        raise FutoiIncrementalAcceptanceError(field + " is not valid UTF-8 JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must be a JSON object")
    return values, snapshot


def _rooted_ref(root: Path, path: Path) -> str:
    if path.is_symlink():
        _fail("artifact reference must not be a symlink")
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
    candidate = root / text[len(ROOT_REF_PREFIX) :] if text.startswith(ROOT_REF_PREFIX) else Path(text)
    if not candidate.is_absolute():
        _fail(field + " must resolve to an absolute path")
    if candidate.is_symlink():
        _fail(field + " must not be a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError(field + " escaped MOEX_DATA_ROOT") from exc
    if not resolved.is_file():
        _fail(field + " must resolve to a regular file")
    return resolved


def _historical_pointer_path(root: Path, instrument_id: str) -> Path:
    return root / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def incremental_pointer_path(root: Path, instrument_id: str) -> Path:
    return root / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + instrument_id) / "current_incremental_accepted_manifest.json"


def _accepted_run_root(root: Path, instrument_id: str, run_id: str) -> Path:
    return root / "state" / "accepted_manifests" / "futoi_raw_incremental" / ("instrument_id=" + instrument_id) / ("run_id=" + run_id)


def accepted_manifest_path(root: Path, instrument_id: str, run_id: str) -> Path:
    return _accepted_run_root(root, instrument_id, run_id) / "manifest.json"


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
        pointer, pointer_snapshot = _load_json_snapshot(pointer_path, "incremental parent pointer")
        if pointer.get("schema_version") != SCHEMA_VERSION or pointer.get("producer") != PRODUCER_ID:
            _fail("incremental parent pointer identity mismatch")
        if pointer.get("dataset_id") != DATASET_ID or pointer.get("instrument_id") != instrument_id or pointer.get("source_id") != SOURCE_ID:
            _fail("incremental parent pointer scope mismatch")
        if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass":
            _fail("incremental parent pointer is not accepted")
        manifest_path = _resolve_ref(root, pointer.get("manifest_ref"), "incremental parent manifest_ref")
        manifest, manifest_snapshot = _load_json_snapshot(manifest_path, "incremental parent manifest")
        if pointer.get("manifest_sha256") != manifest_snapshot["sha256"]:
            _fail("incremental parent manifest SHA mismatch")
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
        pointer, pointer_snapshot = _load_json_snapshot(pointer_path, "historical parent pointer")
        if pointer.get("dataset_id") != DATASET_ID or pointer.get("instrument_id") != instrument_id:
            _fail("historical parent pointer scope mismatch")
        if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass":
            _fail("historical parent pointer is not accepted")
        manifest_path = _resolve_ref(root, pointer.get("manifest_ref"), "historical parent manifest_ref")
        manifest, manifest_snapshot = _load_json_snapshot(manifest_path, "historical parent manifest")
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
        "pointer_snapshot": pointer_snapshot,
        "manifest_path": manifest_path,
        "manifest_snapshot": manifest_snapshot,
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
    result: list[str] = []
    current = first
    while current <= last:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _validate_utc_timestamp_column(frame: pd.DataFrame, field: str) -> None:
    for value in frame[field].tolist():
        try:
            timestamp = pd.Timestamp(value)
        except Exception as exc:
            raise FutoiIncrementalAcceptanceError(field + " contains invalid timestamp: " + str(exc)) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None or timestamp.utcoffset() != timedelta(0):
            _fail(field + " must contain timezone-aware UTC timestamps")


def _validate_raw_contract_and_registry(frame: pd.DataFrame, instrument_id: str) -> dict[str, object]:
    missing = sorted(set(RAW_REQUIRED_COLUMNS).difference(frame.columns))
    if missing:
        _fail("incremental partition missing raw-contract required columns: " + ",".join(missing))
    if bool(frame[list(RAW_REQUIRED_COLUMNS)].isna().any(axis=1).any()):
        _fail("incremental partition contains null raw-contract required columns")
    binding = materializer._registry_binding(materializer.REGISTRY_PATH, instrument_id)
    expected = {
        "instrument_id": instrument_id,
        "secid": str(binding["secid"]),
        "board": str(binding["board"]),
        "market": str(binding["market"]),
        "engine": str(binding["engine"]),
        "source_id": SOURCE_ID,
        "source_ticker": str(binding["futoi.ticker"]),
    }
    normalizers = {
        "instrument_id": lambda value: str(value).strip(),
        "secid": lambda value: str(value).strip().upper(),
        "board": lambda value: str(value).strip().upper(),
        "market": lambda value: str(value).strip().lower(),
        "engine": lambda value: str(value).strip().lower(),
        "source_id": lambda value: str(value).strip().lower(),
        "source_ticker": lambda value: str(value).strip().lower(),
    }
    expected_normalized = {
        "instrument_id": str(expected["instrument_id"]).strip(),
        "secid": str(expected["secid"]).strip().upper(),
        "board": str(expected["board"]).strip().upper(),
        "market": str(expected["market"]).strip().lower(),
        "engine": str(expected["engine"]).strip().lower(),
        "source_id": str(expected["source_id"]).strip().lower(),
        "source_ticker": str(expected["source_ticker"]).strip().lower(),
    }
    for field, normalize in normalizers.items():
        observed = {normalize(value) for value in frame[field].tolist()}
        if observed != {expected_normalized[field]}:
            _fail("incremental partition registry binding mismatch: " + field)
    _validate_utc_timestamp_column(frame, "availability_ts_utc")
    _validate_utc_timestamp_column(frame, "ingest_ts")
    return binding


def _validate_partition(root: Path, path_value: object, instrument_id: str, requested_start: str, requested_end: str) -> dict[str, object]:
    path = Path(str(path_value or ""))
    if not path.is_absolute():
        _fail("backfill partition path must be absolute")
    if path.is_symlink():
        _fail("backfill partition must not be a symlink")
    snapshot = _read_regular_snapshot(path, "backfill partition")
    resolved = Path(snapshot["path"])
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError("backfill partition escaped MOEX_DATA_ROOT") from exc
    try:
        frame = pd.read_parquet(io.BytesIO(bytes(snapshot["raw"])))
    except Exception as exc:
        raise FutoiIncrementalAcceptanceError("backfill partition is not readable parquet: " + str(exc)) from exc
    if frame.empty:
        _fail("accepted incremental partition must not be empty")
    binding = _validate_raw_contract_and_registry(frame, instrument_id)
    trade_dates = sorted(set(frame["trade_date"].astype(str)))
    if len(trade_dates) != 1:
        _fail("incremental partition must contain exactly one trade_date")
    trade_date = _iso_date(trade_dates[0], "partition trade_date")
    if trade_date < requested_start or trade_date > requested_end:
        _fail("incremental partition trade_date is outside requested range")
    groups = set(frame["clgroup"].astype(str).str.upper().str.strip())
    if groups != {"FIZ", "YUR"}:
        _fail("incremental partition must contain exactly FIZ and YUR clgroups")
    raw_ts = pd.to_datetime(frame["ts"], errors="coerce")
    raw_moment = pd.to_datetime(frame["moment"], errors="coerce")
    if bool(raw_ts.isna().any()) or bool(raw_moment.isna().any()) or not bool(raw_ts.eq(raw_moment).all()):
        _fail("incremental partition raw ts must equal raw moment")
    validated_frame = materializer._validate_required_source_identifiers(frame)
    validated_frame = materializer._enforce_publication_timestamp(validated_frame)
    counts = materializer._quality_counts(validated_frame)
    if int(counts.get("rows") or 0) <= 0:
        _fail("incremental partition quality row_count is zero")
    for field in ("duplicate_key_count", "null_required_count", "invalid_position_count"):
        if int(counts.get(field) or 0) != 0:
            _fail("incremental partition quality defect: " + field)
    expected_path = materializer._partition_path(trade_date, instrument_id, SOURCE_ID)
    if path != expected_path or resolved != expected_path.resolve(strict=True):
        _fail("incremental partition path differs from canonical path contract")
    return {
        "trade_date": trade_date,
        "canonical_source_ref": _rooted_ref(root, path),
        "sha256": snapshot["sha256"],
        "row_count": int(counts["rows"]),
        "secid": str(binding["secid"]),
        "source_ticker": str(binding["futoi.ticker"]),
        "clgroups": ["FIZ", "YUR"],
        "duplicate_source_key_count": 0,
        "null_required_count": 0,
        "invalid_position_count": 0,
        "_raw_bytes": snapshot["raw"],
    }


def _validate_backfill(root: Path, instrument_id: str, run_id: str, date_end: str, parent_end: str) -> dict[str, object]:
    expected_start = (date.fromisoformat(parent_end) + timedelta(days=1)).isoformat()
    manifest_path = backfill._aggregate_manifest_path(date_end, run_id)
    quality_path = backfill._aggregate_quality_path(date_end, run_id)
    manifest, manifest_snapshot = _load_json_snapshot(manifest_path, "incremental backfill manifest")
    quality, quality_snapshot = _load_json_snapshot(quality_path, "incremental backfill quality")
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
    quality_ref = _resolve_ref(root, manifest.get("quality_report_ref"), "backfill quality_report_ref")
    if quality_ref != Path(quality_snapshot["path"]):
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
        "manifest_path": Path(manifest_snapshot["path"]),
        "manifest_snapshot": manifest_snapshot,
        "quality_path": Path(quality_snapshot["path"]),
        "quality_snapshot": quality_snapshot,
        "requested_from": requested_start,
        "requested_till": requested_end,
        "records": records,
        "skipped_dates": skipped_dates,
        "partition_count": len(records),
        "row_count": row_count,
    }


def _json_bytes(values: Mapping[str, object]) -> bytes:
    return (json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def _write_bytes_create_only(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        _fail("immutable incremental acceptance artifact already exists")
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise FutoiIncrementalAcceptanceError("immutable incremental acceptance artifact appeared concurrently") from exc


def _write_json_create_only(path: Path, values: Mapping[str, object]) -> None:
    _write_bytes_create_only(path, _json_bytes(values))


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


def _current_snapshot_matches(path: Path, expected_sha256: object, field: str) -> None:
    current = _read_regular_snapshot(path, field)
    if current["sha256"] != expected_sha256:
        _fail(field + " changed after validation")


@contextmanager
def _publication_lock(root: Path, instrument_id: str):
    path = incremental_pointer_path(root, instrument_id).with_name(".futoi_incremental_acceptance.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise FutoiIncrementalAcceptanceError("cannot open FUTOI incremental acceptance lock: " + str(exc)) from exc
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


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
        _current_snapshot_matches(parent_pointer, parent["pointer_snapshot"]["sha256"], "parent accepted pointer")
        _current_snapshot_matches(parent_manifest, parent["manifest_snapshot"]["sha256"], "parent accepted manifest")

        run_root = _accepted_run_root(root, checked_instrument, checked_run)
        parent_pointer_snapshot_path = run_root / "parent" / "pointer.json"
        parent_manifest_snapshot_path = run_root / "parent" / "manifest.json"
        source_manifest_snapshot_path = run_root / "source" / "backfill_manifest.json"
        source_quality_snapshot_path = run_root / "source" / "backfill_quality.json"
        _write_bytes_create_only(parent_pointer_snapshot_path, bytes(parent["pointer_snapshot"]["raw"]))
        _write_bytes_create_only(parent_manifest_snapshot_path, bytes(parent["manifest_snapshot"]["raw"]))
        _write_bytes_create_only(source_manifest_snapshot_path, bytes(source["manifest_snapshot"]["raw"]))
        _write_bytes_create_only(source_quality_snapshot_path, bytes(source["quality_snapshot"]["raw"]))

        accepted_records: list[dict[str, object]] = []
        for source_record in source["records"]:
            trade_date = str(source_record["trade_date"])
            raw_snapshot_path = run_root / "raw" / ("trade_date=" + trade_date) / "part.parquet"
            _write_bytes_create_only(raw_snapshot_path, bytes(source_record["_raw_bytes"]))
            if _sha256_bytes(raw_snapshot_path.read_bytes()) != source_record["sha256"]:
                _fail("accepted raw snapshot SHA mismatch after write")
            accepted_records.append(
                {
                    "trade_date": trade_date,
                    "accepted_partition_ref": _rooted_ref(root, raw_snapshot_path),
                    "canonical_source_ref": source_record["canonical_source_ref"],
                    "sha256": source_record["sha256"],
                    "row_count": source_record["row_count"],
                    "secid": source_record["secid"],
                    "source_ticker": source_record["source_ticker"],
                    "clgroups": source_record["clgroups"],
                    "duplicate_source_key_count": 0,
                    "null_required_count": 0,
                    "invalid_position_count": 0,
                }
            )

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
            "parent_pointer_snapshot_ref": _rooted_ref(root, parent_pointer_snapshot_path),
            "parent_pointer_sha256": parent["pointer_snapshot"]["sha256"],
            "parent_manifest_snapshot_ref": _rooted_ref(root, parent_manifest_snapshot_path),
            "parent_manifest_sha256": parent["manifest_snapshot"]["sha256"],
            "parent_accepted_till": parent["end"],
            "source_backfill_manifest_snapshot_ref": _rooted_ref(root, source_manifest_snapshot_path),
            "source_backfill_manifest_sha256": source["manifest_snapshot"]["sha256"],
            "source_backfill_quality_snapshot_ref": _rooted_ref(root, source_quality_snapshot_path),
            "source_backfill_quality_sha256": source["quality_snapshot"]["sha256"],
            "incremental_from": source["requested_from"],
            "incremental_till": source["requested_till"],
            "incremental_partition_count": source["partition_count"],
            "incremental_row_count": source["row_count"],
            "skipped_empty_source_dates": source["skipped_dates"],
            "partitions": accepted_records,
            "cumulative_from": parent["start"],
            "cumulative_till": source["requested_till"],
            "cumulative_partition_count": int(parent["partition_count"]) + int(source["partition_count"]),
            "cumulative_row_count": int(parent["row_count"]) + int(source["row_count"]),
            "historical_baseline_pointer_mutated": False,
            "source_materialization_network_access_used": True,
            "acceptance_network_access_used": False,
            "accepted_partition_snapshots_immutable": True,
            "canonical_raw_read_after_acceptance_required": False,
            "full_raw_contract_revalidated": True,
            "registry_binding_revalidated": True,
            "latest_autodetect_used": False,
            "implicit_partition_discovery_used": False,
            "historical_pit_research_ready_claimed": False,
            "directional_signal_authority": False,
            "trading_action_authority": False,
        }
        _write_json_create_only(manifest_path, manifest_values)
        manifest_snapshot = _read_regular_snapshot(manifest_path, "accepted incremental manifest")

        _current_snapshot_matches(parent_pointer, parent["pointer_snapshot"]["sha256"], "parent accepted pointer")
        _current_snapshot_matches(parent_manifest, parent["manifest_snapshot"]["sha256"], "parent accepted manifest")
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
            "manifest_sha256": manifest_snapshot["sha256"],
            "previous_parent_kind": parent["kind"],
            "previous_parent_manifest_snapshot_ref": _rooted_ref(root, parent_manifest_snapshot_path),
            "previous_parent_manifest_sha256": parent["manifest_snapshot"]["sha256"],
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
            "accepted_manifest_sha256": manifest_snapshot["sha256"],
            "incremental_pointer_ref": _rooted_ref(root, pointer_path),
            "accepted_partition_snapshots_immutable": True,
            "full_raw_contract_revalidated": True,
            "registry_binding_revalidated": True,
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