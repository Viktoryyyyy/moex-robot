from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_futoi_instrument as futoi_materializer
from . import refresh_forts_raw_5m_incremental as futures_calendar
from . import stage2_raw_history_content_reattestation as content_attestation

TARGET_DATASET_ID: Final[str] = "futures_futoi_raw"
ARTIFACT_ID: Final[str] = "futoi_incremental_stage2_acceptance.v1"
PRODUCER_ID: Final[str] = "moex_data.futures.futoi_incremental_stage2_acceptance.v1"
CONTRACT_REF: Final[str] = "contracts/datasets/futoi_incremental_stage2_acceptance.v1.yaml"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
REGISTRY_PATH: Final[str] = futoi_materializer.REGISTRY_PATH
REQUIRED_GROUPS: Final[frozenset[str]] = frozenset({"FIZ", "YUR"})
SOURCE_ID: Final[str] = "moex_algopack_futoi"


class FutoiIncrementalAcceptanceError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiIncrementalAcceptanceError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if (
        not text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
        or any(marker in text for marker in ("*", "?", "[", "]", "{", "}", "$(", "`"))
    ):
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
    if not root.exists() or not root.is_dir() or root.is_symlink():
        _fail("MOEX_DATA_ROOT must be an existing non-symlink directory")
    return root.resolve(strict=True)


def _rooted_ref(path: str | Path) -> str:
    root = _data_root()
    resolved = Path(path).resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError("artifact escaped MOEX_DATA_ROOT") from exc
    if resolved.is_symlink() or not resolved.is_file():
        _fail("artifact must be a regular non-symlink file")
    return ROOT_PREFIX + relative.as_posix()


def _expand_root_ref(value: object, field: str) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail(field + " must be a ${MOEX_DATA_ROOT} rooted reference")
    relative = text[len(ROOT_PREFIX) :]
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        _fail(field + " contains invalid rooted path")
    root = _data_root()
    path = (root / relative).resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError(field + " escaped MOEX_DATA_ROOT") from exc
    if path.is_symlink() or not path.is_file():
        _fail(field + " must resolve to a regular non-symlink file")
    return path


def _load_json(path: str | Path, field: str) -> dict[str, object]:
    candidate = Path(path)
    if not candidate.is_file() or candidate.is_symlink():
        _fail(field + " must be a regular non-symlink JSON file")
    try:
        values = json.loads(candidate.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FutoiIncrementalAcceptanceError(field + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must contain a JSON object")
    return values


def _json_bytes(values: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            values,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            separators=(",", ": "),
            default=str,
        )
        + "\n"
    ).encode("utf-8")


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_bytes_create_only(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        _fail("immutable artifact already exists: " + path.as_posix())
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb", dir=path.parent, delete=False, suffix=".tmp"
        ) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise FutoiIncrementalAcceptanceError(
                "immutable artifact appeared concurrently: " + path.as_posix()
            ) from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _write_json_create_only(path: Path, values: Mapping[str, object]) -> None:
    _write_bytes_create_only(path, _json_bytes(values))


def _atomic_replace_json(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        _fail("pointer target must not be a symlink")
    payload = _json_bytes(values)
    with tempfile.NamedTemporaryFile(
        "wb", dir=path.parent, delete=False, suffix=".pointer"
    ) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _pointer_path(instrument_id: str) -> Path:
    return (
        _data_root()
        / "state"
        / "datasets"
        / ("dataset_id=" + TARGET_DATASET_ID)
        / ("instrument_id=" + _safe_token(instrument_id, "instrument_id"))
        / "current_incremental_accepted_manifest.json"
    )


def _lock_path(instrument_id: str) -> Path:
    return _pointer_path(instrument_id).with_name(".incremental_acceptance.lock")


@contextmanager
def _publication_lock(instrument_id: str):
    path = _lock_path(instrument_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise FutoiIncrementalAcceptanceError(
            "cannot open incremental acceptance lock: " + str(exc)
        ) from exc
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _current_base(
    *,
    repo_root: Path,
    instrument_id: str,
    resolver: Callable[..., Mapping[str, object]],
) -> dict[str, object]:
    resolved = dict(
        resolver(
            dataset_id=TARGET_DATASET_ID,
            instrument_id=instrument_id,
            repo_root=repo_root,
        )
    )
    generation_id = _safe_token(resolved.get("generation_id"), "base generation_id")
    marker_path = Path(str(resolved.get("marker_path") or "")).resolve(strict=True)
    manifest_path = Path(str(resolved.get("manifest_path") or "")).resolve(strict=True)
    marker_sha = str(resolved.get("marker_sha256") or "").strip().lower()
    manifest_sha = str(resolved.get("manifest_sha256") or "").strip().lower()
    accepted_dates_raw = resolved.get("accepted_dates")
    if isinstance(accepted_dates_raw, (str, bytes)) or not isinstance(
        accepted_dates_raw, Sequence
    ):
        _fail("base accepted_dates must be a sequence")
    accepted_dates = [_iso_date(value, "base accepted_date") for value in accepted_dates_raw]
    if not accepted_dates or accepted_dates != sorted(set(accepted_dates)):
        _fail("base accepted_dates must be nonempty, sorted, and unique")
    for digest, field in ((marker_sha, "base marker_sha256"), (manifest_sha, "base manifest_sha256")):
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            _fail(field + " must be SHA-256")
    if _sha256_file(marker_path) != marker_sha:
        _fail("base marker SHA changed")
    if _sha256_file(manifest_path) != manifest_sha:
        _fail("base manifest SHA changed")
    return {
        "generation_id": generation_id,
        "marker_ref": _rooted_ref(marker_path),
        "marker_sha256": marker_sha,
        "manifest_ref": _rooted_ref(manifest_path),
        "manifest_sha256": manifest_sha,
        "base_last_trade_date": accepted_dates[-1],
        "accepted_partition_dates_sha256": hashlib.sha256(
            (("\n".join(accepted_dates) + "\n")).encode("utf-8")
        ).hexdigest(),
    }


def _same_base(left: Mapping[str, object], right: Mapping[str, object]) -> bool:
    fields = (
        "generation_id",
        "marker_ref",
        "marker_sha256",
        "manifest_ref",
        "manifest_sha256",
        "base_last_trade_date",
        "accepted_partition_dates_sha256",
    )
    return all(left.get(field) == right.get(field) for field in fields)


def _load_previous_pointer(
    instrument_id: str,
) -> tuple[dict[str, object] | None, bytes | None, list[dict[str, object]]]:
    path = _pointer_path(instrument_id)
    if path.is_symlink():
        _fail("incremental pointer must not be a symlink")
    if not path.exists():
        return None, None, []
    before = path.read_bytes()
    pointer = _load_json(path, "incremental pointer")
    if pointer.get("artifact_id") != ARTIFACT_ID:
        _fail("incremental pointer artifact_id mismatch")
    if pointer.get("target_dataset_id") != TARGET_DATASET_ID:
        _fail("incremental pointer target_dataset_id mismatch")
    if pointer.get("instrument_id") != instrument_id:
        _fail("incremental pointer instrument_id mismatch")
    if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass":
        _fail("incremental pointer is not PASS")
    if pointer.get("latest_autodetect_used") is not False:
        _fail("incremental pointer latest_autodetect_used must be false")
    manifest_path = _expand_root_ref(pointer.get("manifest_ref"), "incremental manifest_ref")
    if _sha256_file(manifest_path) != str(pointer.get("manifest_sha256") or ""):
        _fail("incremental pointer manifest SHA mismatch")
    manifest = _load_json(manifest_path, "incremental manifest")
    if manifest.get("artifact_id") != ARTIFACT_ID:
        _fail("incremental manifest artifact_id mismatch")
    if manifest.get("instrument_id") != instrument_id:
        _fail("incremental manifest instrument_id mismatch")
    records_raw = manifest.get("records")
    if isinstance(records_raw, (str, bytes)) or not isinstance(records_raw, Sequence):
        _fail("incremental manifest records must be a sequence")
    records: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in records_raw:
        if not isinstance(raw, Mapping):
            _fail("incremental manifest record must be an object")
        row = dict(raw)
        trade_date = _iso_date(row.get("trade_date"), "incremental record trade_date")
        if trade_date in seen:
            _fail("incremental manifest contains duplicate trade_date")
        seen.add(trade_date)
        frozen = _expand_root_ref(row.get("frozen_partition_ref"), "frozen_partition_ref")
        expected_sha = str(row.get("frozen_sha256") or "").strip().lower()
        if _sha256_file(frozen) != expected_sha:
            _fail("prior incremental frozen SHA mismatch: " + trade_date)
        records.append(row)
    if [str(row["trade_date"]) for row in records] != sorted(seen):
        _fail("incremental manifest records must be sorted")
    return pointer, before, records


def _calendar_dates(
    *,
    start_date: str,
    through_date: str,
    timeout: float,
    calendar_base_url: str | None,
    fetcher: Callable[..., Sequence[Mapping[str, object]]],
) -> list[str]:
    rows = fetcher(
        start_date,
        through_date,
        timeout=timeout,
        calendar_base_url=calendar_base_url,
    )
    calendar = futures_calendar._calendar_map(rows)
    return futures_calendar._trading_days_between(
        calendar,
        date.fromisoformat(start_date),
        date.fromisoformat(through_date),
    )


def _validate_materialized_partition(
    *,
    payload: Mapping[str, object],
    instrument_id: str,
    trade_date: str,
) -> dict[str, object]:
    if payload.get("status") != "succeeded":
        _fail("raw materialization status is not succeeded: " + trade_date)
    if payload.get("dataset_id") != TARGET_DATASET_ID:
        _fail("raw materialization dataset_id mismatch: " + trade_date)
    if payload.get("instrument_id") != instrument_id:
        _fail("raw materialization instrument_id mismatch: " + trade_date)
    if payload.get("source_id") != SOURCE_ID:
        _fail("raw materialization source_id mismatch: " + trade_date)
    if payload.get("quality_status") != "pass":
        _fail("raw materialization quality_status is not pass: " + trade_date)
    if payload.get("latest_autodetect_used") is not False:
        _fail("raw materialization used latest autodetect: " + trade_date)
    if str(payload.get("trade_date") or "") != trade_date:
        _fail("raw materialization trade_date mismatch: " + trade_date)
    if int(payload.get("row_count") or 0) <= 0:
        _fail("raw materialization returned no rows: " + trade_date)

    partition = Path(str(payload.get("storage_partition_path") or "")).resolve(strict=True)
    root = _data_root()
    try:
        partition.relative_to(root)
    except ValueError as exc:
        raise FutoiIncrementalAcceptanceError(
            "raw partition escaped MOEX_DATA_ROOT: " + trade_date
        ) from exc
    if partition.is_symlink() or not partition.is_file():
        _fail("raw partition must be regular non-symlink: " + trade_date)

    frame = pd.read_parquet(partition)
    if len(frame.index) != int(payload["row_count"]):
        _fail("raw partition row_count mismatch: " + trade_date)
    required = {
        "trade_date",
        "secid",
        "clgroup",
        "sess_id",
        "seqnum",
        "ts",
        "systime",
        "availability_ts_utc",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        _fail("raw partition schema missing " + ",".join(missing) + ": " + trade_date)
    if set(frame["trade_date"].astype(str).unique()) != {trade_date}:
        _fail("raw partition contains wrong trade_date: " + trade_date)
    groups = set(frame["clgroup"].astype(str).unique())
    if groups != REQUIRED_GROUPS:
        _fail("raw partition must contain exactly FIZ and YUR: " + trade_date)
    secids = sorted(frame["secid"].astype(str).unique().tolist())
    if not secids or any(not item for item in secids):
        _fail("raw partition secid scope invalid: " + trade_date)
    duplicate_keys = int(
        frame.duplicated(
            subset=["trade_date", "sess_id", "seqnum", "secid", "clgroup"]
        ).sum()
    )
    if duplicate_keys:
        _fail("raw partition has duplicate source keys: " + trade_date)
    for field in ("ts", "systime", "availability_ts_utc"):
        parsed = pd.to_datetime(frame[field], errors="coerce", utc=(field == "availability_ts_utc"))
        if bool(parsed.isna().any()):
            _fail("raw partition has invalid " + field + ": " + trade_date)

    manifest = Path(str(payload.get("manifest_reference") or "")).resolve(strict=True)
    quality = Path(str(payload.get("quality_report_reference") or "")).resolve(strict=True)
    manifest_values = _load_json(manifest, "raw refresh manifest")
    quality_values = _load_json(quality, "raw quality report")
    if manifest_values.get("refresh_status") != "succeeded":
        _fail("raw refresh manifest is not succeeded: " + trade_date)
    if quality_values.get("quality_status") != "pass":
        _fail("raw quality report is not pass: " + trade_date)
    if int(quality_values.get("row_count") or 0) != len(frame.index):
        _fail("raw quality report row_count mismatch: " + trade_date)
    if int(quality_values.get("duplicate_key_count") or 0) != 0:
        _fail("raw quality report duplicate keys: " + trade_date)
    if int(quality_values.get("invalid_position_count") or 0) != 0:
        _fail("raw quality report invalid positions: " + trade_date)
    if int(quality_values.get("null_required_count") or 0) != 0:
        _fail("raw quality report null required fields: " + trade_date)

    return {
        "partition_path": partition,
        "canonical_partition_ref": _rooted_ref(partition),
        "canonical_sha256": _sha256_file(partition),
        "row_count": int(len(frame.index)),
        "secid_scope": secids,
        "ts_min": str(frame["ts"].min()),
        "ts_max": str(frame["ts"].max()),
        "systime_min": str(frame["systime"].min()),
        "systime_max": str(frame["systime"].max()),
        "availability_ts_utc_min": str(frame["availability_ts_utc"].min()),
        "availability_ts_utc_max": str(frame["availability_ts_utc"].max()),
        "source_manifest_ref": _rooted_ref(manifest),
        "source_manifest_sha256": _sha256_file(manifest),
        "source_quality_report_ref": _rooted_ref(quality),
        "source_quality_report_sha256": _sha256_file(quality),
    }


def _freeze_partition(
    *,
    source: Path,
    target: Path,
    expected_sha256: str,
) -> str:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        _fail("frozen target already exists: " + target.as_posix())
    before = _sha256_file(source)
    if before != expected_sha256:
        _fail("canonical partition changed before freeze")
    shutil.copyfile(source, target)
    if source.stat().st_dev == target.stat().st_dev and source.stat().st_ino == target.stat().st_ino:
        target.unlink(missing_ok=True)
        _fail("frozen partition unexpectedly shares source inode")
    frozen_sha = _sha256_file(target)
    after = _sha256_file(source)
    if before != frozen_sha or before != after:
        target.unlink(missing_ok=True)
        _fail("canonical partition changed during freeze")
    return frozen_sha


def _validate_prior_base_binding(
    pointer: Mapping[str, object] | None, base: Mapping[str, object]
) -> None:
    if pointer is None:
        return
    fields = {
        "base_generation_id": "generation_id",
        "base_marker_ref": "marker_ref",
        "base_marker_sha256": "marker_sha256",
        "base_manifest_ref": "manifest_ref",
        "base_manifest_sha256": "manifest_sha256",
        "base_last_trade_date": "base_last_trade_date",
        "base_partition_dates_sha256": "accepted_partition_dates_sha256",
    }
    for pointer_field, base_field in fields.items():
        if pointer.get(pointer_field) != base.get(base_field):
            _fail("previous incremental pointer base binding mismatch: " + pointer_field)


def accept_incremental(
    *,
    instrument_id: str,
    through_date: str,
    run_id: str,
    repo_root: str | Path = ".",
    env_file: str | None = "/home/trader/moex_bot/.env",
    timeout: float = 60.0,
    calendar_base_url: str | None = None,
    base_resolver: Callable[..., Mapping[str, object]] = content_attestation.resolve_content_attested_history,
    calendar_fetcher: Callable[..., Sequence[Mapping[str, object]]] = futures_calendar.fetch_futures_calendar_rows,
    partition_materializer: Callable[..., Mapping[str, object]] = futoi_materializer.materialize_futoi_partition,
) -> dict[str, object]:
    futoi_materializer.load_env_file(env_file)
    checked_instrument = _safe_token(instrument_id, "instrument_id")
    checked_through = _iso_date(through_date, "through_date")
    checked_run = _safe_token(run_id, "run_id")
    repo = Path(repo_root).resolve(strict=True)
    root = _data_root()

    run_root = (
        root
        / "state"
        / "acceptance"
        / "futoi_incremental_stage2"
        / ("instrument_id=" + checked_instrument)
        / ("run_id=" + checked_run)
    )
    if run_root.exists() or run_root.is_symlink():
        _fail("immutable incremental acceptance run_id already exists")

    with _publication_lock(checked_instrument):
        base_before = _current_base(
            repo_root=repo, instrument_id=checked_instrument, resolver=base_resolver
        )
        previous_pointer, previous_pointer_bytes, previous_records = _load_previous_pointer(
            checked_instrument
        )
        _validate_prior_base_binding(previous_pointer, base_before)

        previous_last = (
            str(previous_records[-1]["trade_date"])
            if previous_records
            else str(base_before["base_last_trade_date"])
        )
        if date.fromisoformat(checked_through) < date.fromisoformat(previous_last):
            _fail("through_date is older than current accepted incremental state")
        start = (date.fromisoformat(previous_last) + timedelta(days=1)).isoformat()
        if date.fromisoformat(start) <= date.fromisoformat(checked_through):
            requested_dates = _calendar_dates(
                start_date=start,
                through_date=checked_through,
                timeout=timeout,
                calendar_base_url=calendar_base_url,
                fetcher=calendar_fetcher,
            )
        else:
            requested_dates = []

        if not requested_dates:
            return {
                "artifact_id": ARTIFACT_ID,
                "status": "no_op",
                "target_dataset_id": TARGET_DATASET_ID,
                "instrument_id": checked_instrument,
                "through_date": checked_through,
                "base_last_trade_date": base_before["base_last_trade_date"],
                "last_incremental_trade_date": previous_last
                if previous_records
                else None,
                "accepted_incremental_trade_date_count": len(previous_records),
                "accepted_pointer_written": False,
                "latest_autodetect_used": False,
            }

        run_root.mkdir(parents=True, exist_ok=False)
        new_records: list[dict[str, object]] = []
        try:
            registry = repo / REGISTRY_PATH
            for trade_date in requested_dates:
                materialize_run_id = (
                    checked_run + "_" + checked_instrument + "_" + trade_date.replace("-", "")
                )
                payload = partition_materializer(
                    trade_date=trade_date,
                    instrument_id=checked_instrument,
                    run_id=materialize_run_id,
                    registry_path=registry,
                    timeout=timeout,
                    require_enabled=False,
                )
                if not isinstance(payload, Mapping):
                    _fail("raw materializer returned invalid payload: " + trade_date)
                checked = _validate_materialized_partition(
                    payload=payload,
                    instrument_id=checked_instrument,
                    trade_date=trade_date,
                )
                frozen = (
                    run_root
                    / "frozen_raw"
                    / ("trade_date=" + trade_date)
                    / "part.parquet"
                )
                frozen_sha = _freeze_partition(
                    source=Path(checked["partition_path"]),
                    target=frozen,
                    expected_sha256=str(checked["canonical_sha256"]),
                )
                record = {
                    "trade_date": trade_date,
                    "source_id": SOURCE_ID,
                    "canonical_partition_ref": checked["canonical_partition_ref"],
                    "canonical_sha256_at_acceptance": checked["canonical_sha256"],
                    "frozen_partition_ref": _rooted_ref(frozen),
                    "frozen_sha256": frozen_sha,
                    "independent_inode_exact_byte_copy": True,
                    "row_count": checked["row_count"],
                    "secid_scope": checked["secid_scope"],
                    "clgroup_scope": ["FIZ", "YUR"],
                    "duplicate_source_key_count": 0,
                    "ts_min": checked["ts_min"],
                    "ts_max": checked["ts_max"],
                    "systime_min": checked["systime_min"],
                    "systime_max": checked["systime_max"],
                    "availability_ts_utc_min": checked["availability_ts_utc_min"],
                    "availability_ts_utc_max": checked["availability_ts_utc_max"],
                    "source_manifest_ref": checked["source_manifest_ref"],
                    "source_manifest_sha256": checked["source_manifest_sha256"],
                    "source_quality_report_ref": checked["source_quality_report_ref"],
                    "source_quality_report_sha256": checked["source_quality_report_sha256"],
                    "quality_status": "pass",
                }
                new_records.append(record)

            cumulative = [*previous_records, *new_records]
            dates = [str(row["trade_date"]) for row in cumulative]
            if dates != sorted(set(dates)):
                _fail("cumulative incremental dates are not sorted and unique")
            manifest = {
                "artifact_id": ARTIFACT_ID,
                "schema_version": ARTIFACT_ID,
                "producer": PRODUCER_ID,
                "contract_ref": CONTRACT_REF,
                "target_dataset_id": TARGET_DATASET_ID,
                "instrument_id": checked_instrument,
                "run_id": checked_run,
                "base_generation_id": base_before["generation_id"],
                "base_marker_ref": base_before["marker_ref"],
                "base_marker_sha256": base_before["marker_sha256"],
                "base_manifest_ref": base_before["manifest_ref"],
                "base_manifest_sha256": base_before["manifest_sha256"],
                "base_last_trade_date": base_before["base_last_trade_date"],
                "base_partition_dates_sha256": base_before[
                    "accepted_partition_dates_sha256"
                ],
                "previous_incremental_manifest_ref": (
                    previous_pointer.get("manifest_ref")
                    if previous_pointer is not None
                    else None
                ),
                "calendar_source_id": "moex_iss_futures_calendar",
                "calendar_endpoint": "/iss/calendars.json",
                "requested_start_date": start,
                "requested_through_date": checked_through,
                "requested_trade_dates": requested_dates,
                "new_record_count": len(new_records),
                "records": cumulative,
                "incremental_trade_date_count": len(cumulative),
                "incremental_row_count": sum(int(row["row_count"]) for row in cumulative),
                "last_incremental_trade_date": dates[-1],
                "quality_status": "pass",
                "acceptance_status": "pass",
                "historical_pointer_replaced": False,
                "latest_autodetect_used": False,
                "implicit_partition_discovery_used": False,
                "raw_source_transport": "authenticated_apim",
                "raw_source_latest_parameter": 0,
            }
            quality = {
                "artifact_id": ARTIFACT_ID,
                "producer": PRODUCER_ID,
                "target_dataset_id": TARGET_DATASET_ID,
                "instrument_id": checked_instrument,
                "run_id": checked_run,
                "quality_status": "pass",
                "acceptance_status": "pass",
                "requested_trade_dates": requested_dates,
                "accepted_trade_dates": [str(row["trade_date"]) for row in new_records],
                "accepted_trade_date_count": len(new_records),
                "accepted_row_count": sum(int(row["row_count"]) for row in new_records),
                "required_clgroups": ["FIZ", "YUR"],
                "duplicate_source_key_count": 0,
                "base_generation_unchanged_required": True,
                "historical_pointer_replaced": False,
                "latest_autodetect_used": False,
            }
            manifest_path = run_root / "accepted_manifest.json"
            quality_path = run_root / "quality_report.json"
            _write_json_create_only(manifest_path, manifest)
            _write_json_create_only(quality_path, quality)

            base_after = _current_base(
                repo_root=repo, instrument_id=checked_instrument, resolver=base_resolver
            )
            if not _same_base(base_before, base_after):
                _fail("base content-attestation generation changed during incremental acceptance")

            pointer_path = _pointer_path(checked_instrument)
            current_bytes = (
                pointer_path.read_bytes()
                if pointer_path.exists() and not pointer_path.is_symlink()
                else None
            )
            if current_bytes != previous_pointer_bytes:
                _fail("incremental pointer changed concurrently")

            pointer = {
                "artifact_id": ARTIFACT_ID,
                "target_dataset_id": TARGET_DATASET_ID,
                "instrument_id": checked_instrument,
                "run_id": checked_run,
                "manifest_ref": _rooted_ref(manifest_path),
                "manifest_sha256": _sha256_file(manifest_path),
                "quality_report_ref": _rooted_ref(quality_path),
                "quality_report_sha256": _sha256_file(quality_path),
                "quality_status": "pass",
                "acceptance_status": "pass",
                "base_generation_id": base_before["generation_id"],
                "base_marker_ref": base_before["marker_ref"],
                "base_marker_sha256": base_before["marker_sha256"],
                "base_manifest_ref": base_before["manifest_ref"],
                "base_manifest_sha256": base_before["manifest_sha256"],
                "base_last_trade_date": base_before["base_last_trade_date"],
                "base_partition_dates_sha256": base_before[
                    "accepted_partition_dates_sha256"
                ],
                "last_incremental_trade_date": dates[-1],
                "incremental_trade_date_count": len(cumulative),
                "incremental_row_count": manifest["incremental_row_count"],
                "previous_incremental_manifest_ref": manifest[
                    "previous_incremental_manifest_ref"
                ],
                "historical_pointer_replaced": False,
                "promotion_basis": "content_attested_history_plus_incremental_live_overlay",
                "latest_autodetect_used": False,
            }
            _atomic_replace_json(pointer_path, pointer)
            return {
                "artifact_id": ARTIFACT_ID,
                "status": "accepted",
                "target_dataset_id": TARGET_DATASET_ID,
                "instrument_id": checked_instrument,
                "run_id": checked_run,
                "base_generation_id": base_before["generation_id"],
                "base_last_trade_date": base_before["base_last_trade_date"],
                "requested_trade_dates": requested_dates,
                "new_accepted_trade_date_count": len(new_records),
                "accepted_incremental_trade_date_count": len(cumulative),
                "last_incremental_trade_date": dates[-1],
                "incremental_row_count": manifest["incremental_row_count"],
                "manifest_ref": pointer["manifest_ref"],
                "quality_report_ref": pointer["quality_report_ref"],
                "accepted_pointer_ref": _rooted_ref(pointer_path),
                "accepted_pointer_written": True,
                "historical_pointer_replaced": False,
                "latest_autodetect_used": False,
            }
        except Exception:
            raise


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Accept explicit completed FUTOI live history as a Stage 2 incremental overlay."
    )
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--through-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--env-file", default="/home/trader/moex_bot/.env")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--calendar-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = accept_incremental(
            instrument_id=args.instrument_id,
            through_date=args.through_date,
            run_id=args.run_id,
            repo_root=args.repo_root,
            env_file=args.env_file,
            timeout=args.timeout,
            calendar_base_url=args.calendar_base_url,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "artifact_id": ARTIFACT_ID,
                    "status": "failed",
                    "error": str(exc),
                    "accepted_pointer_written": False,
                    "historical_pointer_replaced": False,
                    "latest_autodetect_used": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
