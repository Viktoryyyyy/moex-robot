from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_futoi_instrument as futoi_materializer
from . import stage2_raw_history_acceptance as stage2
from . import stage2_raw_history_acceptance_gate as stage2_gate

ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
SCHEMA_VERSION: Final[str] = "futures_raw_history_content_attested_manifest.v1"
PRODUCER: Final[str] = "moex_data.futures.stage2_raw_history_content_reattestation.v1"
ACCEPTANCE_CONTRACT_REF: Final[str] = "contracts/datasets/futures_raw_history_content_attestation.v1.yaml"
PROMOTION_BASIS: Final[str] = "raw_history_content_attestation"
CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
SCOPES: Final[tuple[tuple[str, str], ...]] = (
    (stage2.QUOTE_DATASET_ID, "usdrubf_futures_family"),
    (stage2.QUOTE_DATASET_ID, "cnyrubf_futures_family"),
    (stage2.FUTOI_DATASET_ID, "si_futures_family"),
    (stage2.FUTOI_DATASET_ID, "cr_futures_family"),
)


class RawHistoryContentReattestationError(ValueError):
    pass


@dataclass(frozen=True)
class PriorPointerSnapshot:
    target_dataset_id: str
    instrument_id: str
    path: Path
    values: dict[str, object]
    raw_bytes: bytes
    sha256: str


@dataclass(frozen=True)
class PriorAcceptedState:
    pointer: PriorPointerSnapshot
    manifest_path: Path
    manifest_values: dict[str, object]
    manifest_sha256: str
    expectation: stage2.HistoryExpectation
    accepted_dates: tuple[str, ...]
    missing_dates: tuple[str, ...]
    target_contract_ref: str


def _fail(message: str) -> None:
    raise RawHistoryContentReattestationError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _require_sha256(value: object, field: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        _fail(field + " must be SHA-256 hex")
    return text


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.is_file():
        _fail("env_file does not exist")
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    root = Path(value)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return root.resolve()


def _rooted_ref(path: Path) -> str:
    root = _data_root().resolve(strict=True)
    try:
        relative = path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise RawHistoryContentReattestationError("artifact must be inside MOEX_DATA_ROOT") from exc
    return ROOT_PREFIX + relative.as_posix()


def _expand_root_ref(value: object, field: str) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail(field + " must be ${MOEX_DATA_ROOT}-rooted")
    relative = text[len(ROOT_PREFIX):]
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        _fail(field + " is invalid")
    root = _data_root().resolve(strict=True)
    path = (root / relative).resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RawHistoryContentReattestationError(field + " escaped MOEX_DATA_ROOT") from exc
    if not path.is_file() or path.is_symlink():
        _fail(field + " must resolve to regular non-symlink file")
    return path


def _load_json_bytes(raw: bytes, field: str) -> dict[str, object]:
    try:
        values = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RawHistoryContentReattestationError(field + " invalid UTF-8 JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must be object")
    return values


def _read_regular_bytes(path: Path, field: str) -> bytes:
    if not path.is_file() or path.is_symlink():
        _fail(field + " must be regular non-symlink file")
    return path.read_bytes()


def _date_set_sha(values: Sequence[str]) -> str:
    payload = (("\n".join(values) + "\n") if values else "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _content_set_sha(records: Sequence[Mapping[str, object]]) -> str:
    payload = "".join(str(row["trade_date"]) + "\t" + str(row["sha256"]) + "\n" for row in records).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _pointer_snapshot(repo_root: Path, target_dataset_id: str, instrument_id: str) -> PriorPointerSnapshot:
    path = stage2_gate._pointer_path(repo_root, target_dataset_id, instrument_id)
    raw = _read_regular_bytes(path, "current accepted pointer")
    values = _load_json_bytes(raw, "current accepted pointer")
    if values.get("dataset_id") != target_dataset_id or values.get("instrument_id") != instrument_id:
        _fail("current accepted pointer identity mismatch")
    if values.get("quality_status") != "pass" or values.get("acceptance_status") != "pass":
        _fail("current accepted pointer is not PASS")
    basis = str(values.get("promotion_basis") or "")
    if basis not in {"raw_history_acceptance", PROMOTION_BASIS}:
        _fail("current accepted pointer promotion_basis is unsupported")
    _safe_token(values.get("run_id"), "current accepted pointer run_id")
    return PriorPointerSnapshot(
        target_dataset_id=target_dataset_id,
        instrument_id=instrument_id,
        path=path,
        values=values,
        raw_bytes=raw,
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def _common_manifest_identity(values: Mapping[str, object], target_dataset_id: str, instrument_id: str, pointer_run_id: str) -> None:
    if values.get("dataset_id") != "futures_raw_history_accepted_manifest":
        _fail("prior accepted manifest dataset_id mismatch")
    if values.get("target_dataset_id") != target_dataset_id or values.get("instrument_id") != instrument_id:
        _fail("prior accepted manifest target identity mismatch")
    if values.get("acceptance_run_id") != pointer_run_id:
        _fail("prior accepted manifest run identity mismatch")
    if values.get("acceptance_status") != "pass":
        _fail("prior accepted manifest is not PASS")
    if values.get("network_access_used") is not False or values.get("historical_backfill_used") is not False:
        _fail("prior accepted manifest execution-boundary mismatch")


def _build_expectation(repo_root: Path, target_dataset_id: str, instrument_id: str, values: Mapping[str, object]) -> stage2.HistoryExpectation:
    source_id = _safe_token(values.get("source_id"), "prior source_id")
    expected_source = stage2.QUOTE_SOURCE_ID if target_dataset_id == stage2.QUOTE_DATASET_ID else stage2.FUTOI_SOURCE_ID
    if source_id != expected_source:
        _fail("prior accepted manifest source_id mismatch")
    secids = values.get("secid_scope")
    if not isinstance(secids, list) or len(secids) != 1:
        _fail("prior accepted manifest must bind exactly one secid")
    expected_secid = _safe_token(secids[0], "prior secid_scope")
    source_ticker: str | None = None
    if target_dataset_id == stage2.FUTOI_DATASET_ID:
        binding = futoi_materializer._registry_binding(repo_root / futoi_materializer.REGISTRY_PATH, instrument_id)
        if str(binding.get("secid")) != expected_secid:
            _fail("prior FUTOI secid differs from current canonical registry binding")
        if str(binding.get("futoi.source_id")) != stage2.FUTOI_SOURCE_ID:
            _fail("current FUTOI registry source binding mismatch")
        source_ticker = _safe_token(binding.get("futoi.ticker"), "registry FUTOI ticker")
    start = str(values.get("requested_from") or "")
    end = str(values.get("requested_till") or "")
    try:
        dates = stage2._date_range(start, end)
    except Exception as exc:
        raise RawHistoryContentReattestationError("prior accepted manifest date range invalid: " + str(exc)) from exc
    partitions = int(values.get("partition_count") or -1)
    rows = int(values.get("row_count") or -1)
    if partitions <= 0 or rows <= 0:
        _fail("prior accepted manifest partition/row counts must be positive")
    missing_raw = values.get("missing_partition_dates")
    if not isinstance(missing_raw, list):
        _fail("prior accepted manifest missing_partition_dates must be list")
    missing = [str(item) for item in missing_raw]
    if missing != sorted(missing) or len(missing) != len(set(missing)) or not set(missing).issubset(set(dates)):
        _fail("prior accepted manifest missing dates invalid")
    present = [item for item in dates if item not in set(missing)]
    if len(present) != partitions:
        _fail("prior accepted manifest partition_count/date-set mismatch")
    if _require_sha256(values.get("partition_dates_sha256"), "prior partition_dates_sha256") != _date_set_sha(present):
        _fail("prior accepted manifest partition date digest mismatch")
    if _require_sha256(values.get("missing_dates_sha256"), "prior missing_dates_sha256") != _date_set_sha(missing):
        _fail("prior accepted manifest missing date digest mismatch")
    return stage2.HistoryExpectation(
        target_dataset_id=target_dataset_id,
        instrument_id=instrument_id,
        source_id=source_id,
        date_start=start,
        date_end=end,
        expected_partitions=partitions,
        expected_rows=rows,
        expected_secid=expected_secid,
        expected_source_ticker=source_ticker,
        expected_missing_dates=len(missing),
    )


def _prior_state(repo_root: Path, target_dataset_id: str, instrument_id: str) -> PriorAcceptedState:
    pointer = _pointer_snapshot(repo_root, target_dataset_id, instrument_id)
    manifest_path = _expand_root_ref(pointer.values.get("manifest_ref"), "current accepted manifest_ref")
    raw = _read_regular_bytes(manifest_path, "prior accepted manifest")
    values = _load_json_bytes(raw, "prior accepted manifest")
    pointer_run_id = _safe_token(pointer.values.get("run_id"), "current accepted pointer run_id")
    _common_manifest_identity(values, target_dataset_id, instrument_id, pointer_run_id)
    schema = str(values.get("schema_version") or "")
    if schema not in {"futures_raw_history_accepted_manifest.v1", SCHEMA_VERSION}:
        _fail("prior accepted manifest schema unsupported")
    report_path = _expand_root_ref(values.get("acceptance_report_ref"), "prior acceptance_report_ref")
    if hashlib.sha256(report_path.read_bytes()).hexdigest() != _require_sha256(values.get("acceptance_report_sha256"), "prior acceptance_report_sha256"):
        _fail("prior accepted manifest acceptance report hash mismatch")
    expectation = _build_expectation(repo_root, target_dataset_id, instrument_id, values)
    dates = stage2._date_range(expectation.date_start, expectation.date_end)
    missing = tuple(str(item) for item in values.get("missing_partition_dates") or [])
    accepted = tuple(item for item in dates if item not in set(missing))
    target_contract_ref = stage2.QUOTE_CONTRACT_PATH if target_dataset_id == stage2.QUOTE_DATASET_ID else stage2.FUTOI_CONTRACT_PATH
    return PriorAcceptedState(
        pointer=pointer,
        manifest_path=manifest_path,
        manifest_values=values,
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        expectation=expectation,
        accepted_dates=accepted,
        missing_dates=missing,
        target_contract_ref=target_contract_ref,
    )


def prior_state(repo_root: str | Path = ".") -> dict[str, object]:
    repo = Path(repo_root).resolve()
    snapshots = [_prior_state(repo, dataset, instrument) for dataset, instrument in SCOPES]
    lines = [state.pointer.target_dataset_id + "\t" + state.pointer.instrument_id + "\t" + state.pointer.sha256 + "\n" for state in snapshots]
    digest = hashlib.sha256("".join(lines).encode("utf-8")).hexdigest()
    return {
        "project": "MOEX_Bot",
        "status": "prior_state_probed",
        "pointer_count": len(snapshots),
        "prior_state_sha256": digest,
        "pointers": [
            {
                "target_dataset_id": state.pointer.target_dataset_id,
                "instrument_id": state.pointer.instrument_id,
                "run_id": str(state.pointer.values.get("run_id")),
                "pointer_sha256": state.pointer.sha256,
                "pointer_path": state.pointer.path.as_posix(),
                "manifest_ref": str(state.pointer.values.get("manifest_ref")),
            }
            for state in snapshots
        ],
        "network_access_used": False,
        "historical_backfill_used": False,
    }


def _read_partition_exact_bytes(path: Path) -> tuple[bytes, tuple[int, int, int, int]]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RawHistoryContentReattestationError("canonical partition open failed: " + str(exc)) from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            _fail("canonical partition is not regular file")
        chunks: list[bytes] = []
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
        after = os.fstat(fd)
        identity_before = (int(before.st_dev), int(before.st_ino), int(before.st_size), int(before.st_mtime_ns))
        identity_after = (int(after.st_dev), int(after.st_ino), int(after.st_size), int(after.st_mtime_ns))
        if identity_before != identity_after:
            _fail("canonical partition changed while exact bytes were read")
        raw = b"".join(chunks)
        if len(raw) != before.st_size:
            _fail("canonical partition size changed while exact bytes were read")
        current = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(current.st_mode) or (int(current.st_dev), int(current.st_ino)) != (int(before.st_dev), int(before.st_ino)):
            _fail("canonical partition pathname changed during content attestation")
        return raw, identity_before
    finally:
        os.close(fd)


def _validate_and_hash_partition(repo_root: Path, state: PriorAcceptedState, trade_date: str, validation_run_id: str) -> dict[str, object]:
    pattern = stage2._contract_path(repo_root, state.expectation.target_dataset_id)
    path = stage2._partition_path(repo_root=repo_root, pattern=pattern, expectation=state.expectation, trade_date=trade_date)
    raw, identity = _read_partition_exact_bytes(path)
    try:
        frame = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        raise RawHistoryContentReattestationError("canonical partition exact bytes are unreadable parquet: " + str(exc)) from exc
    if state.expectation.target_dataset_id == stage2.QUOTE_DATASET_ID:
        rows, secids = stage2._validate_quote_partition(repo_root, frame, state.expectation, trade_date, validation_run_id)
    else:
        rows, secids = stage2._validate_futoi_partition(frame, state.expectation, trade_date)
    return {
        "trade_date": trade_date,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "row_count": int(rows),
        "secid_scope": list(secids),
        "canonical_partition_ref": _rooted_ref(path),
        "validated_inode": {"st_dev": identity[0], "st_ino": identity[1], "st_size": identity[2], "st_mtime_ns": identity[3]},
    }


def _write_json_create_only(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        staged = Path(handle.name)
    try:
        os.link(staged, path)
    except FileExistsError as exc:
        raise RawHistoryContentReattestationError("immutable artifact already exists: " + path.as_posix()) from exc
    finally:
        staged.unlink(missing_ok=True)


def _instrument_artifacts(repo_root: Path, state: PriorAcceptedState, batch_run_id: str) -> dict[str, object]:
    target = state.expectation.target_dataset_id
    instrument = state.expectation.instrument_id
    producer_run_id = batch_run_id + "_" + instrument
    records = [_validate_and_hash_partition(repo_root, state, trade_date, producer_run_id + "_validation") for trade_date in state.accepted_dates]
    if len(records) != state.expectation.expected_partitions:
        _fail("content attestation partition count mismatch")
    total_rows = sum(int(record["row_count"]) for record in records)
    if total_rows != state.expectation.expected_rows:
        _fail("content attestation row count mismatch")
    secids = sorted({secid for record in records for secid in record["secid_scope"]})
    if secids != [str(state.expectation.expected_secid)]:
        _fail("content attestation secid scope mismatch")
    content_digest = _content_set_sha(records)
    evidence_dir = _data_root() / "state" / "attestation" / "stage2_raw_history" / ("run_id=" + batch_run_id) / ("target_dataset_id=" + target) / ("instrument_id=" + instrument)
    report_path = evidence_dir / "content_attestation.json"
    report: dict[str, object] = {
        "schema_version": "futures_raw_history_content_attestation.v1",
        "producer": PRODUCER,
        "project": "MOEX_Bot",
        "batch_run_id": batch_run_id,
        "run_id": producer_run_id,
        "target_dataset_id": target,
        "instrument_id": instrument,
        "source_id": state.expectation.source_id,
        "secid_scope": secids,
        "requested_from": state.expectation.date_start,
        "requested_till": state.expectation.date_end,
        "partition_count": len(records),
        "row_count": total_rows,
        "partition_dates_sha256": _date_set_sha(state.accepted_dates),
        "missing_partition_dates": list(state.missing_dates),
        "missing_dates_sha256": _date_set_sha(state.missing_dates),
        "partition_content_records": records,
        "partition_content_set_sha256": content_digest,
        "prior_accepted_run_id": str(state.pointer.values.get("run_id")),
        "prior_accepted_manifest_ref": _rooted_ref(state.manifest_path),
        "prior_accepted_manifest_sha256": state.manifest_sha256,
        "prior_pointer_sha256": state.pointer.sha256,
        "acceptance_status": "pass",
        "network_access_used": False,
        "historical_backfill_used": False,
        "canonical_raw_mutation_used": False,
        "same_exact_bytes_validated_and_hashed": True,
    }
    _write_json_create_only(report_path, report)
    report_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
    manifest_path = _data_root() / "state" / "accepted_manifests" / ("target_dataset_id=" + target) / ("instrument_id=" + instrument) / ("acceptance_run_id=" + producer_run_id) / "accepted_manifest.json"
    manifest: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER,
        "dataset_id": "futures_raw_history_accepted_manifest",
        "target_dataset_id": target,
        "target_dataset_contract_ref": state.target_contract_ref,
        "instrument_id": instrument,
        "acceptance_run_id": producer_run_id,
        "acceptance_contract_ref": ACCEPTANCE_CONTRACT_REF,
        "source_acceptance_report_ref": str(state.manifest_values.get("source_acceptance_report_ref") or state.manifest_values.get("acceptance_report_ref") or ""),
        "acceptance_report_ref": _rooted_ref(report_path),
        "acceptance_report_sha256": report_sha,
        "source_id": state.expectation.source_id,
        "secid_scope": secids,
        "requested_from": state.expectation.date_start,
        "requested_till": state.expectation.date_end,
        "partition_count": len(records),
        "row_count": total_rows,
        "partition_dates_sha256": _date_set_sha(state.accepted_dates),
        "missing_partition_dates": list(state.missing_dates),
        "missing_dates_sha256": _date_set_sha(state.missing_dates),
        "calendar_missing_partition_count": len(state.missing_dates),
        "partition_content_records": records,
        "partition_content_set_sha256": content_digest,
        "prior_accepted_run_id": str(state.pointer.values.get("run_id")),
        "prior_accepted_manifest_ref": _rooted_ref(state.manifest_path),
        "prior_accepted_manifest_sha256": state.manifest_sha256,
        "prior_pointer_sha256": state.pointer.sha256,
        "acceptance_status": "pass",
        "network_access_used": False,
        "historical_backfill_used": False,
        "promotion_basis": PROMOTION_BASIS,
    }
    _write_json_create_only(manifest_path, manifest)
    pointer_values: dict[str, object] = {
        "dataset_id": target,
        "instrument_id": instrument,
        "run_id": producer_run_id,
        "acceptance_run_id": producer_run_id,
        "manifest_ref": _rooted_ref(manifest_path),
        "quality_report_ref": _rooted_ref(report_path),
        "acceptance_report_ref": _rooted_ref(report_path),
        "source_acceptance_report_ref": manifest["source_acceptance_report_ref"],
        "quality_status": "pass",
        "acceptance_status": "pass",
        "promotion_basis": PROMOTION_BASIS,
        "content_attestation_status": "pass",
        "partition_content_set_sha256": content_digest,
        "prior_accepted_run_id": manifest["prior_accepted_run_id"],
        "prior_pointer_sha256": state.pointer.sha256,
    }
    return {
        "state": state,
        "producer_run_id": producer_run_id,
        "report_path": report_path,
        "report_sha256": report_sha,
        "manifest_path": manifest_path,
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "pointer_values": pointer_values,
        "partition_count": len(records),
        "row_count": total_rows,
        "partition_content_set_sha256": content_digest,
    }


def _stage_json(path: Path, values: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        return Path(handle.name)


def _restore(path: Path, previous: bytes | None) -> None:
    if previous is None:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".rollback") as handle:
        handle.write(previous)
        staged = Path(handle.name)
    staged.replace(path)


def _transactional_replace(records: Sequence[tuple[Path, Mapping[str, object]]]) -> None:
    paths = [path for path, _ in records]
    if len(paths) != len(set(paths)):
        _fail("transaction target paths must be unique")
    previous = {path: path.read_bytes() if path.exists() else None for path in paths}
    staged: list[tuple[Path, Path]] = []
    applied: list[Path] = []
    try:
        for final, values in records:
            staged.append((_stage_json(final, values), final))
        for source, final in staged:
            source.replace(final)
            applied.append(final)
    except Exception as exc:
        rollback_errors: list[str] = []
        for final in reversed(applied):
            try:
                _restore(final, previous[final])
            except Exception as rollback_exc:
                rollback_errors.append(str(rollback_exc))
        if rollback_errors:
            raise RawHistoryContentReattestationError("pointer transaction failed and rollback incomplete: " + ";".join(rollback_errors)) from exc
        raise RawHistoryContentReattestationError("pointer transaction failed: " + str(exc)) from exc
    finally:
        for staged_path, _ in staged:
            staged_path.unlink(missing_ok=True)


def _assert_prior_state_unchanged(repo_root: Path, states: Sequence[PriorAcceptedState], expected_digest: str) -> str:
    lines: list[str] = []
    for state in states:
        current = _read_regular_bytes(state.pointer.path, "current accepted pointer before transaction")
        digest = hashlib.sha256(current).hexdigest()
        if digest != state.pointer.sha256:
            _fail("accepted pointer changed during content re-attestation: " + state.pointer.instrument_id)
        lines.append(state.pointer.target_dataset_id + "\t" + state.pointer.instrument_id + "\t" + digest + "\n")
    actual = hashlib.sha256("".join(lines).encode("utf-8")).hexdigest()
    if actual != expected_digest:
        _fail("current four-pointer prior-state digest mismatch before transaction")
    return actual


def reattest(*, run_id: str, expected_prior_state_sha256: str, repo_root: str | Path = ".") -> dict[str, object]:
    checked_run = _safe_token(run_id, "run_id")
    expected_prior = _require_sha256(expected_prior_state_sha256, "expected_prior_state_sha256")
    repo = Path(repo_root).resolve()
    states = [_prior_state(repo, dataset, instrument) for dataset, instrument in SCOPES]
    initial_lines = [state.pointer.target_dataset_id + "\t" + state.pointer.instrument_id + "\t" + state.pointer.sha256 + "\n" for state in states]
    initial_digest = hashlib.sha256("".join(initial_lines).encode("utf-8")).hexdigest()
    if initial_digest != expected_prior:
        _fail("explicit expected prior four-pointer state does not match current state")
    artifacts = [_instrument_artifacts(repo, state, checked_run) for state in states]
    _assert_prior_state_unchanged(repo, states, expected_prior)
    batch_dir = _data_root() / "state" / "attestation" / "stage2_raw_history" / ("run_id=" + checked_run)
    marker = batch_dir / "batch_accepted.json"
    if marker.exists():
        _fail("batch acceptance marker already exists")
    summaries = [
        {
            "target_dataset_id": item["state"].expectation.target_dataset_id,
            "instrument_id": item["state"].expectation.instrument_id,
            "producer_run_id": item["producer_run_id"],
            "prior_accepted_run_id": str(item["state"].pointer.values.get("run_id")),
            "prior_pointer_sha256": item["state"].pointer.sha256,
            "manifest_ref": _rooted_ref(item["manifest_path"]),
            "manifest_sha256": item["manifest_sha256"],
            "content_attestation_report_ref": _rooted_ref(item["report_path"]),
            "content_attestation_report_sha256": item["report_sha256"],
            "partition_count": item["partition_count"],
            "row_count": item["row_count"],
            "partition_content_set_sha256": item["partition_content_set_sha256"],
        }
        for item in artifacts
    ]
    result: dict[str, object] = {
        "project": "MOEX_Bot",
        "status": "accepted",
        "action": "stage2_raw_history_content_reattestation",
        "run_id": checked_run,
        "prior_state_sha256": expected_prior,
        "pointer_count": len(artifacts),
        "expected_pointer_count": len(SCOPES),
        "promotion_basis": PROMOTION_BASIS,
        "pointers": summaries,
        "transactional_pointer_replacement": True,
        "rollback_required": True,
        "final_marker_same_transaction": True,
        "partial_new_pointer_set_without_marker_is_not_accepted": True,
        "network_access_used": False,
        "historical_backfill_used": False,
    }
    transaction: list[tuple[Path, Mapping[str, object]]] = [
        (item["state"].pointer.path, item["pointer_values"]) for item in artifacts
    ]
    transaction.append((marker, result))
    _transactional_replace(transaction)
    result["acceptance_marker_path"] = marker.as_posix()
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Content-attest the four existing Stage 2 accepted raw-history pointers.")
    parser.add_argument("action", choices=("probe", "run"))
    parser.add_argument("--run-id")
    parser.add_argument("--expected-prior-state-sha256")
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_env_file(args.env_file)
    try:
        if args.action == "probe":
            result = prior_state(args.repo_root)
        else:
            if not args.run_id or not args.expected_prior_state_sha256:
                _fail("run requires --run-id and --expected-prior-state-sha256")
            result = reattest(run_id=args.run_id, expected_prior_state_sha256=args.expected_prior_state_sha256, repo_root=args.repo_root)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "status": "failed", "action": "stage2_raw_history_content_reattestation", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
