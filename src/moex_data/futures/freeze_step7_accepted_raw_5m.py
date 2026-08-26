from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Mapping

import pandas as pd

from . import stage2_raw_history_acceptance as stage2
from . import stage2_raw_history_content_reattestation as content_attestation

SOURCE_DATASET_ID: Final[str] = "futures_raw_5m"
SOURCE_ID: Final[str] = "moex_algopack_fo_tradestats_5m"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
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
    marker_sha256: str
    manifest_sha256: str
    partition_content_set_sha256: str
    records: tuple[Mapping[str, object], ...]
    row_count: int


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
        return pd.Timestamp(text).date().isoformat()
    except Exception as exc:
        raise Step7RawFreezeError(field + " must be YYYY-MM-DD") from exc


def _date_set_sha(values: tuple[str, ...] | list[str]) -> str:
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


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _require_stage2_root(root: Path) -> None:
    configured = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not configured:
        _fail("MOEX_DATA_ROOT is required for Stage 2 content-attested reads")
    if Path(configured).resolve() != root.resolve():
        _fail("data_root differs from canonical MOEX_DATA_ROOT")


def accepted_quote_history(root: Path, instrument_id: str, start_date: str, end_date: str, *, repo_root: str | Path = ".") -> AcceptedQuoteHistory:
    instrument = _safe_token(instrument_id, "instrument_id")
    if instrument not in ALLOWED_INSTRUMENTS:
        _fail("Stage 7 production quote scope is USDRUBF/CNYRUBF only")
    start = _iso_date(start_date, "start_date")
    end = _iso_date(end_date, "end_date")
    _require_stage2_root(root)
    resolved = content_attestation.resolve_content_attested_history(
        dataset_id=SOURCE_DATASET_ID,
        instrument_id=instrument,
        repo_root=Path(repo_root).resolve(),
    )
    accepted_start = str(resolved.get("requested_from") or "")
    accepted_end = str(resolved.get("requested_till") or "")
    if start != accepted_start or end != accepted_end:
        _fail("Stage 7 requires the exact full content-attested quote-history range")
    accepted_dates = tuple(str(value) for value in resolved.get("accepted_dates", ()))
    records = tuple(row for row in resolved.get("records", ()) if isinstance(row, Mapping))
    if len(accepted_dates) != int(resolved.get("partition_count") or 0) or len(records) != len(accepted_dates) or not records:
        _fail("content-attested quote partition expectation is invalid")
    if tuple(str(row.get("trade_date") or "") for row in records) != accepted_dates:
        _fail("content-attested resolver record dates mismatch")
    if _date_set_sha(list(accepted_dates)) != str(_load_json(Path(str(resolved.get("manifest_path"))).resolve(strict=True), "content-attested manifest").get("partition_dates_sha256") or ""):
        _fail("content-attested partition date digest mismatch")
    marker_path = Path(str(resolved.get("marker_path") or "")).resolve(strict=True)
    manifest_path = Path(str(resolved.get("manifest_path") or "")).resolve(strict=True)
    manifest = _load_json(manifest_path, "content-attested manifest")
    report_path = _expand_root_ref(root, manifest.get("content_attestation_report_ref"), "content-attested report_ref")
    generation_id = _safe_token(resolved.get("generation_id"), "content-attested generation_id")
    marker_sha = str(resolved.get("marker_sha256") or "").strip().lower()
    manifest_sha = str(resolved.get("manifest_sha256") or "").strip().lower()
    content_set_sha = str(resolved.get("partition_content_set_sha256") or "").strip().lower()
    for value, field in ((marker_sha, "marker_sha256"), (manifest_sha, "manifest_sha256"), (content_set_sha, "partition_content_set_sha256")):
        if len(value) != 64:
            _fail("invalid content-attestation " + field)
    return AcceptedQuoteHistory(
        instrument_id=instrument,
        source_id=SOURCE_ID,
        secid=EXPECTED_SECID[instrument],
        accepted_dates=accepted_dates,
        missing_dates=tuple(str(value) for value in resolved.get("missing_dates", ())),
        acceptance_run_id=generation_id,
        pointer_ref=_rooted_ref(root, marker_path),
        manifest_ref=_rooted_ref(root, manifest_path),
        acceptance_report_ref=_rooted_ref(root, report_path),
        partition_dates_sha256=_date_set_sha(list(accepted_dates)),
        marker_sha256=marker_sha,
        manifest_sha256=manifest_sha,
        partition_content_set_sha256=content_set_sha,
        records=records,
        row_count=int(resolved.get("row_count") or 0),
    )


def quote_validation_expectation(instrument_id: str, start_date: str, end_date: str, *, expected_partitions: int = 0, expected_rows: int = 0) -> stage2.HistoryExpectation:
    instrument = _safe_token(instrument_id, "instrument_id")
    if instrument not in EXPECTED_SECID:
        _fail("unsupported Stage 7 quote instrument")
    return stage2.HistoryExpectation(
        target_dataset_id=SOURCE_DATASET_ID,
        instrument_id=instrument,
        source_id=SOURCE_ID,
        date_start=_iso_date(start_date, "start_date"),
        date_end=_iso_date(end_date, "end_date"),
        expected_partitions=expected_partitions,
        expected_rows=expected_rows,
        expected_secid=EXPECTED_SECID[instrument],
    )


def _freeze_attested_record(*, repo_root: Path, data_root: Path, source_record: Mapping[str, object], instrument_id: str, freeze_root: Path, validation_run_id: str, expectation: stage2.HistoryExpectation) -> dict[str, object]:
    trade_date = _iso_date(source_record.get("trade_date"), "content-attested trade_date")
    source = Path(str(source_record.get("snapshot_path") or "")).resolve(strict=True)
    try:
        source.relative_to(data_root.resolve(strict=True))
    except ValueError as exc:
        raise Step7RawFreezeError("content-attested snapshot escaped MOEX_DATA_ROOT") from exc
    if not source.is_file() or source.is_symlink():
        _fail("content-attested snapshot is missing/non-regular: " + trade_date)
    expected_sha = str(source_record.get("sha256") or "").strip().lower()
    if len(expected_sha) != 64:
        _fail("content-attested snapshot SHA-256 invalid")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(source, flags)
    try:
        opened_stat = os.fstat(fd)
        if not stat.S_ISREG(opened_stat.st_mode):
            _fail("content-attested snapshot fd is not regular")
        with os.fdopen(os.dup(fd), "rb") as handle:
            frame = pd.read_parquet(handle)
        rows, secids = stage2._validate_quote_partition(repo_root, frame, expectation, trade_date, validation_run_id)
        source_sha = hashlib.sha256()
        os.lseek(fd, 0, os.SEEK_SET)
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            source_sha.update(block)
        if source_sha.hexdigest() != expected_sha:
            _fail("content-attested snapshot SHA-256 differs from generation evidence")
        if int(source_record.get("row_count") or -1) != int(rows):
            _fail("content-attested snapshot row-count evidence mismatch")
        current_stat = os.stat(source, follow_symlinks=False)
        if (current_stat.st_dev, current_stat.st_ino) != (opened_stat.st_dev, opened_stat.st_ino):
            _fail("content-attested snapshot changed during validation")
        target = freeze_root / ("instrument_id=" + instrument_id) / ("trade_date=" + trade_date) / "part.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            _fail("immutable frozen raw target already exists")
        os.link(source, target, follow_symlinks=False)
        frozen_stat = os.stat(target, follow_symlinks=False)
        if (frozen_stat.st_dev, frozen_stat.st_ino) != (opened_stat.st_dev, opened_stat.st_ino):
            target.unlink(missing_ok=True)
            _fail("frozen hardlink is not bound to validated content-attested inode")
        if _sha_file(target) != expected_sha:
            target.unlink(missing_ok=True)
            _fail("frozen partition hash mismatch")
        return {
            "trade_date": trade_date,
            "instrument_id": instrument_id,
            "row_count": int(rows),
            "secids": list(secids),
            "sha256": expected_sha,
            "content_attested_snapshot_ref": str(source_record.get("snapshot_ref") or ""),
            "canonical_source_ref": str(source_record.get("canonical_ref") or ""),
            "frozen_ref": _rooted_ref(data_root, target),
            "validated_inode": {"st_dev": int(opened_stat.st_dev), "st_ino": int(opened_stat.st_ino)},
            "hardlink_same_validated_inode": True,
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
    scope = accepted_quote_history(root, instrument_id, start_date, end_date, repo_root=repo)
    expectation = quote_validation_expectation(
        scope.instrument_id,
        start_date,
        end_date,
        expected_partitions=len(scope.accepted_dates),
        expected_rows=scope.row_count,
    )
    freeze_root = run / "inputs" / ("dataset_id=" + SOURCE_DATASET_ID)
    records = [
        _freeze_attested_record(
            repo_root=repo,
            data_root=root,
            source_record=source_record,
            instrument_id=scope.instrument_id,
            freeze_root=freeze_root,
            validation_run_id=checked_run + "_freeze_validation",
            expectation=expectation,
        )
        for source_record in scope.records
    ]
    if len(records) != len(scope.accepted_dates) or sum(int(row["row_count"]) for row in records) != scope.row_count:
        _fail("frozen content-attested quote history count mismatch")
    digest_payload = "".join(str(row["trade_date"]) + "\t" + str(row["sha256"]) + "\n" for row in records).encode("utf-8")
    content_digest = hashlib.sha256(digest_payload).hexdigest()
    if content_digest != scope.partition_content_set_sha256:
        _fail("frozen content set differs from current content-attested generation")
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
        "content_attestation_generation_id": scope.acceptance_run_id,
        "content_attestation_marker_ref": scope.pointer_ref,
        "content_attestation_marker_sha256": scope.marker_sha256,
        "content_attested_manifest_ref": scope.manifest_ref,
        "content_attested_manifest_sha256": scope.manifest_sha256,
        "content_attested_partition_content_set_sha256": scope.partition_content_set_sha256,
        "requested_start_date": _iso_date(start_date, "start_date"),
        "requested_end_date": _iso_date(end_date, "end_date"),
        "partition_count": len(records),
        "row_count": sum(int(row["row_count"]) for row in records),
        "missing_dates": list(scope.missing_dates),
        "frozen_content_sha256": content_digest,
        "freeze_method": "validated_inode_create_only_hardlink",
        "source_mode": "stage2_content_attested_generation_snapshots_only",
        "legacy_pointer_consumption_used": False,
        "mutable_canonical_raw_read_after_freeze_allowed": False,
        "network_calls_used": False,
        "latest_autodetect_used": False,
        "partitions": records,
    }
    _write_json_create_only(manifest, values)
    values["manifest_path"] = manifest.as_posix()
    values["manifest_ref"] = _rooted_ref(root, manifest)
    return values
