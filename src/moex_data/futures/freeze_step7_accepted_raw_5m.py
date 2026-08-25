from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import pandas as pd

from . import stage2_raw_history_acceptance as stage2

SOURCE_DATASET_ID: Final[str] = "futures_raw_5m"
SOURCE_ID: Final[str] = "moex_algopack_fo_tradestats_5m"
SOURCE_CONTRACT_REF: Final[str] = "contracts/datasets/futures_raw_5m.v1.yaml"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
ACCEPTED_MANIFEST_SCHEMA: Final[str] = "futures_raw_history_accepted_manifest.v1"
ACCEPTED_MANIFEST_DATASET: Final[str] = "futures_raw_history_accepted_manifest"
ACCEPTED_MANIFEST_PRODUCER: Final[str] = "moex_data.futures.stage2_raw_history_promotion.v1"
EXPECTED_SECID: Final[dict[str, str]] = {
    "usdrubf_futures_family": "USDRUBF",
    "cnyrubf_futures_family": "CNYRUBF",
}
ALLOWED_INSTRUMENTS: Final[frozenset[str]] = frozenset(EXPECTED_SECID)


class Step7RawFreezeError(ValueError):
    pass


@dataclass(frozen=True)
class AcceptedQuoteHistory:
    instrument_id: str
    source_id: str
    secid: str
    accepted_dates: tuple[str, ...]
    missing_dates: tuple[str, ...]
    acceptance_run_id: str
    pointer_ref: str
    manifest_ref: str
    acceptance_report_ref: str
    partition_dates_sha256: str


def _fail(message: str) -> None:
    raise Step7RawFreezeError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise Step7RawFreezeError(field + " must be YYYY-MM-DD") from exc


def _date_range(start: str, end: str) -> list[str]:
    a = date.fromisoformat(start)
    b = date.fromisoformat(end)
    if a > b:
        _fail("start_date must be <= end_date")
    return [(a + timedelta(days=n)).isoformat() for n in range((b - a).days + 1)]


def _date_set_sha(values: list[str] | tuple[str, ...]) -> str:
    payload = (("\n".join(values) + "\n") if values else "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _rooted_ref(root: Path, path: Path) -> str:
    try:
        rel = path.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step7RawFreezeError("artifact must be inside MOEX_DATA_ROOT") from exc
    return ROOT_PREFIX + rel.as_posix()


def _expand_root_ref(root: Path, value: object, field: str) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail(field + " must be a ${MOEX_DATA_ROOT} rooted reference")
    rel = text[len(ROOT_PREFIX):]
    if not rel or rel.startswith("/") or ".." in Path(rel).parts:
        _fail(field + " contains invalid path")
    candidate = (root / rel).resolve(strict=True)
    try:
        candidate.relative_to(root.resolve(strict=True))
    except ValueError as exc:
        raise Step7RawFreezeError(field + " escaped MOEX_DATA_ROOT") from exc
    if not candidate.is_file() or candidate.is_symlink():
        _fail(field + " must resolve to a regular non-symlink file")
    return candidate


def _load_json(path: Path, field: str) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        _fail(field + " must be a regular non-symlink file")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step7RawFreezeError(field + " is invalid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must be an object")
    return values


def accepted_pointer_path(root: Path, instrument_id: str) -> Path:
    return root / "state" / "datasets" / ("dataset_id=" + SOURCE_DATASET_ID) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def accepted_quote_history(root: Path, instrument_id: str, start_date: str, end_date: str) -> AcceptedQuoteHistory:
    instrument = _safe_token(instrument_id, "instrument_id")
    if instrument not in ALLOWED_INSTRUMENTS:
        _fail("Stage 7 production quote scope is USDRUBF/CNYRUBF only")
    expected_secid = EXPECTED_SECID[instrument]
    start = _iso_date(start_date, "start_date")
    end = _iso_date(end_date, "end_date")
    pointer_path = accepted_pointer_path(root, instrument)
    pointer = _load_json(pointer_path, "accepted raw pointer")
    if pointer.get("dataset_id") != SOURCE_DATASET_ID or pointer.get("instrument_id") != instrument:
        _fail("accepted raw pointer identity mismatch")
    if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass":
        _fail("accepted raw pointer is not PASS")
    if pointer.get("promotion_basis") != "raw_history_acceptance":
        _fail("accepted raw pointer promotion_basis mismatch")
    acceptance_run_id = _safe_token(pointer.get("run_id"), "accepted raw pointer run_id")

    manifest_path = _expand_root_ref(root, pointer.get("manifest_ref"), "accepted raw manifest_ref")
    manifest = _load_json(manifest_path, "accepted raw manifest")
    if manifest.get("schema_version") != ACCEPTED_MANIFEST_SCHEMA or manifest.get("producer") != ACCEPTED_MANIFEST_PRODUCER:
        _fail("accepted raw manifest schema/producer mismatch")
    if manifest.get("dataset_id") != ACCEPTED_MANIFEST_DATASET or manifest.get("target_dataset_id") != SOURCE_DATASET_ID:
        _fail("accepted raw manifest dataset binding mismatch")
    if manifest.get("target_dataset_contract_ref") != SOURCE_CONTRACT_REF:
        _fail("accepted raw manifest target contract mismatch")
    if manifest.get("instrument_id") != instrument or manifest.get("acceptance_run_id") != acceptance_run_id:
        _fail("accepted raw manifest identity/run mismatch")
    if manifest.get("source_id") != SOURCE_ID or manifest.get("acceptance_status") != "pass":
        _fail("accepted raw manifest source/status mismatch")
    secid_scope = manifest.get("secid_scope")
    if not isinstance(secid_scope, list) or set(str(x) for x in secid_scope) != {expected_secid}:
        _fail("accepted raw manifest SECID scope mismatch")
    if manifest.get("network_access_used") is not False or manifest.get("historical_backfill_used") is not False:
        _fail("accepted raw manifest execution boundary mismatch")

    accepted_start = _iso_date(manifest.get("requested_from"), "accepted raw requested_from")
    accepted_end = _iso_date(manifest.get("requested_till"), "accepted raw requested_till")
    if start < accepted_start or end > accepted_end:
        _fail("requested Stage 7 range is outside accepted raw history")
    missing_raw = manifest.get("missing_partition_dates")
    if not isinstance(missing_raw, list):
        _fail("accepted raw missing_partition_dates must be a list")
    missing = [_iso_date(item, "missing_partition_dates") for item in missing_raw]
    if missing != sorted(missing) or len(missing) != len(set(missing)):
        _fail("accepted raw missing dates must be sorted and unique")
    full = _date_range(accepted_start, accepted_end)
    if not set(missing).issubset(set(full)):
        _fail("accepted raw missing dates escape accepted range")
    present = [item for item in full if item not in set(missing)]
    if int(manifest.get("partition_count") or -1) != len(present):
        _fail("accepted raw partition_count mismatch")
    partition_digest = str(manifest.get("partition_dates_sha256") or "").strip().lower()
    if partition_digest != _date_set_sha(present):
        _fail("accepted raw partition date digest mismatch")
    if str(manifest.get("missing_dates_sha256") or "").strip().lower() != _date_set_sha(missing):
        _fail("accepted raw missing date digest mismatch")

    report_path = _expand_root_ref(root, manifest.get("acceptance_report_ref"), "accepted raw acceptance_report_ref")
    expected_report_sha = str(manifest.get("acceptance_report_sha256") or "").strip().lower()
    if len(expected_report_sha) != 64 or hashlib.sha256(report_path.read_bytes()).hexdigest() != expected_report_sha:
        _fail("accepted raw acceptance report SHA-256 mismatch")
    report_ref = str(manifest.get("acceptance_report_ref") or "")
    if pointer.get("quality_report_ref") != report_ref or pointer.get("acceptance_report_ref") != report_ref:
        _fail("accepted raw pointer/report binding mismatch")

    requested = _date_range(start, end)
    present_set = set(present)
    accepted = tuple(item for item in requested if item in present_set)
    requested_missing = tuple(item for item in requested if item not in present_set)
    if not accepted:
        _fail("accepted raw history has no partitions in requested range")
    return AcceptedQuoteHistory(
        instrument_id=instrument,
        source_id=SOURCE_ID,
        secid=expected_secid,
        accepted_dates=accepted,
        missing_dates=requested_missing,
        acceptance_run_id=acceptance_run_id,
        pointer_ref=_rooted_ref(root, pointer_path),
        manifest_ref=_rooted_ref(root, manifest_path),
        acceptance_report_ref=_rooted_ref(root, report_path),
        partition_dates_sha256=partition_digest,
    )


def quote_validation_expectation(instrument_id: str, start_date: str, end_date: str) -> stage2.HistoryExpectation:
    instrument = _safe_token(instrument_id, "instrument_id")
    if instrument not in EXPECTED_SECID:
        _fail("unsupported Stage 7 quote instrument")
    return stage2.HistoryExpectation(
        target_dataset_id=SOURCE_DATASET_ID,
        instrument_id=instrument,
        source_id=SOURCE_ID,
        date_start=_iso_date(start_date, "start_date"),
        date_end=_iso_date(end_date, "end_date"),
        expected_partitions=0,
        expected_rows=0,
        expected_secid=EXPECTED_SECID[instrument],
    )


def canonical_partition_path(root: Path, instrument_id: str, trade_date: str) -> Path:
    return root / "market" / "raw" / "timeframe=5m" / ("instrument_id=" + instrument_id) / ("trade_date=" + trade_date) / ("source=" + SOURCE_ID) / "part.parquet"


def _sha_fd(fd: int) -> str:
    digest = hashlib.sha256()
    os.lseek(fd, 0, os.SEEK_SET)
    while True:
        block = os.read(fd, 1024 * 1024)
        if not block:
            break
        digest.update(block)
    os.lseek(fd, 0, os.SEEK_SET)
    return digest.hexdigest()


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _freeze_one(*, repo_root: Path, data_root: Path, instrument_id: str, trade_date: str, freeze_root: Path, validation_run_id: str, expectation: stage2.HistoryExpectation | None = None) -> dict[str, object]:
    source = canonical_partition_path(data_root, instrument_id, trade_date)
    if not source.is_file() or source.is_symlink():
        _fail("accepted canonical raw partition is missing/non-regular: " + trade_date)
    flags = os.O_RDONLY | (getattr(os, "O_NOFOLLOW", 0))
    fd = os.open(source, flags)
    try:
        opened_stat = os.fstat(fd)
        if not stat.S_ISREG(opened_stat.st_mode):
            _fail("accepted raw partition fd is not regular")
        with os.fdopen(os.dup(fd), "rb") as handle:
            frame = pd.read_parquet(handle)
        checked_expectation = expectation or quote_validation_expectation(instrument_id, trade_date, trade_date)
        rows, secids = stage2._validate_quote_partition(repo_root, frame, checked_expectation, trade_date, validation_run_id)
        sha256 = _sha_fd(fd)
        current_stat = os.stat(source, follow_symlinks=False)
        if (current_stat.st_dev, current_stat.st_ino) != (opened_stat.st_dev, opened_stat.st_ino):
            _fail("canonical raw partition changed during validation")
        target = freeze_root / ("instrument_id=" + instrument_id) / ("trade_date=" + trade_date) / "part.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            _fail("immutable frozen raw target already exists")
        os.link(source, target, follow_symlinks=False)
        frozen_stat = os.stat(target, follow_symlinks=False)
        if (frozen_stat.st_dev, frozen_stat.st_ino) != (opened_stat.st_dev, opened_stat.st_ino):
            target.unlink(missing_ok=True)
            _fail("frozen hardlink is not bound to validated inode")
        if _sha_file(target) != sha256:
            target.unlink(missing_ok=True)
            _fail("frozen partition hash mismatch")
        return {
            "trade_date": trade_date,
            "instrument_id": instrument_id,
            "row_count": int(rows),
            "secids": list(secids),
            "sha256": sha256,
            "source_ref": _rooted_ref(data_root, source),
            "frozen_ref": _rooted_ref(data_root, target),
            "validated_inode": {"st_dev": int(opened_stat.st_dev), "st_ino": int(opened_stat.st_ino)},
        }
    finally:
        os.close(fd)


def _write_json_create_only(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temp = Path(handle.name)
    try:
        os.link(temp, path)
    except FileExistsError as exc:
        raise Step7RawFreezeError("immutable frozen manifest already exists") from exc
    finally:
        temp.unlink(missing_ok=True)


def freeze_accepted_quote_history(*, repo_root: str | Path, data_root: str | Path, run_root: str | Path, instrument_id: str, start_date: str, end_date: str, run_id: str) -> dict[str, object]:
    repo = Path(repo_root).resolve()
    root = Path(data_root).resolve()
    run = Path(run_root).resolve()
    checked_run = _safe_token(run_id, "run_id")
    scope = accepted_quote_history(root, instrument_id, start_date, end_date)
    expectation = quote_validation_expectation(scope.instrument_id, start_date, end_date)
    freeze_root = run / "inputs" / ("dataset_id=" + SOURCE_DATASET_ID)
    records = [
        _freeze_one(
            repo_root=repo,
            data_root=root,
            instrument_id=scope.instrument_id,
            trade_date=trade_date,
            freeze_root=freeze_root,
            validation_run_id=checked_run + "_freeze_validation",
            expectation=expectation,
        )
        for trade_date in scope.accepted_dates
    ]
    digest_payload = "".join(str(row["trade_date"]) + "\t" + str(row["sha256"]) + "\n" for row in records).encode("utf-8")
    content_digest = hashlib.sha256(digest_payload).hexdigest()
    manifest = run / "state" / "frozen_inputs" / ("instrument_id=" + scope.instrument_id) / "frozen_raw_manifest.json"
    values: dict[str, object] = {
        "schema_version": "step7_frozen_raw_5m_manifest.v1",
        "run_id": checked_run,
        "dataset_id": SOURCE_DATASET_ID,
        "instrument_id": scope.instrument_id,
        "source_id": SOURCE_ID,
        "secid": scope.secid,
        "accepted_raw_history_run_id": scope.acceptance_run_id,
        "accepted_raw_pointer_ref": scope.pointer_ref,
        "accepted_raw_manifest_ref": scope.manifest_ref,
        "accepted_raw_acceptance_report_ref": scope.acceptance_report_ref,
        "accepted_partition_dates_sha256": scope.partition_dates_sha256,
        "requested_start_date": _iso_date(start_date, "start_date"),
        "requested_end_date": _iso_date(end_date, "end_date"),
        "partition_count": len(records),
        "missing_dates": list(scope.missing_dates),
        "frozen_content_sha256": content_digest,
        "freeze_method": "validated_inode_create_only_hardlink",
        "mutable_canonical_raw_read_after_freeze_allowed": False,
        "partitions": records,
    }
    _write_json_create_only(manifest, values)
    values["manifest_path"] = manifest.as_posix()
    values["manifest_ref"] = _rooted_ref(root, manifest)
    return values
