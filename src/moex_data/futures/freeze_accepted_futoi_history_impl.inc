from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import stat
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_futoi_instrument as futoi_materializer
from . import stage2_raw_history_acceptance as raw_acceptance
from .materialize_futoi_eod import (
    SOURCE_DATASET_ID,
    SOURCE_ID,
    _accepted_history_scope,
    _expand_root_ref,
    _safe_token,
    raw_partition_path,
)

FREEZE_SCHEMA_VERSION: Final[str] = "step5_futoi_raw_frozen_input.v1"
FREEZE_PRODUCER: Final[str] = "moex_data.futures.freeze_accepted_futoi_history.v1"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"


class FutoiFreezeError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiFreezeError(message)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _rooted_ref(root: Path, path: Path) -> str:
    try:
        relative = path.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise FutoiFreezeError("frozen input artifact must be inside MOEX_DATA_ROOT") from exc
    return ROOT_PREFIX + relative.as_posix()


def _read_stable_regular_file(path: Path) -> tuple[bytes, os.stat_result, int]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise FutoiFreezeError("canonical raw partition must be an existing regular non-symlink file: " + str(exc)) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail("canonical raw partition is not a regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if identity_before != identity_after:
            _fail("canonical raw partition changed while freeze snapshot was read")
        payload = b"".join(chunks)
        if len(payload) != before.st_size:
            _fail("canonical raw partition size changed while freeze snapshot was read")
        return payload, before, descriptor
    except Exception:
        os.close(descriptor)
        raise


def _link_validated_inode(source_path: Path, frozen_path: Path, validated_stat: os.stat_result, descriptor: int, expected_sha256: str) -> None:
    frozen_path.parent.mkdir(parents=True, exist_ok=True)
    if frozen_path.exists() or frozen_path.is_symlink():
        _fail("immutable frozen partition target already exists")
    try:
        os.link(source_path, frozen_path, follow_symlinks=False)
    except OSError as exc:
        raise FutoiFreezeError("hardlink freeze failed; refusing non-immutable fallback: " + str(exc)) from exc
    try:
        linked = os.stat(frozen_path, follow_symlinks=False)
        current_fd = os.fstat(descriptor)
        validated_identity = (validated_stat.st_dev, validated_stat.st_ino, validated_stat.st_size, validated_stat.st_mtime_ns)
        fd_identity = (current_fd.st_dev, current_fd.st_ino, current_fd.st_size, current_fd.st_mtime_ns)
        linked_identity = (linked.st_dev, linked.st_ino, linked.st_size, linked.st_mtime_ns)
        if fd_identity != validated_identity or linked_identity != validated_identity:
            _fail("hardlink does not identify the exact validated raw partition inode")
        if _sha256_bytes(frozen_path.read_bytes()) != expected_sha256:
            _fail("frozen hardlink SHA-256 differs from validated raw bytes")
    except Exception:
        frozen_path.unlink(missing_ok=True)
        raise


def _write_json_create_only(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        _fail("immutable frozen input manifest already exists")
    payload = (json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".tmp") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.link(temporary, path)
    except FileExistsError as exc:
        raise FutoiFreezeError("immutable frozen input manifest appeared concurrently") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _expectation(repo_root: Path, instrument_id: str, start_date: str, end_date: str, expected_partitions: int, expected_rows: int) -> raw_acceptance.HistoryExpectation:
    binding = futoi_materializer._registry_binding(repo_root / futoi_materializer.REGISTRY_PATH, instrument_id)
    if str(binding.get("futoi.source_id")) != SOURCE_ID:
        _fail("registry FUTOI source binding mismatch")
    if str(binding.get("futoi.availability_status")) != "available" or str(binding.get("futoi.probe_status")) != "completed":
        _fail("registry FUTOI availability/probe evidence mismatch")
    return raw_acceptance.HistoryExpectation(
        target_dataset_id=SOURCE_DATASET_ID,
        instrument_id=instrument_id,
        source_id=SOURCE_ID,
        date_start=start_date,
        date_end=end_date,
        expected_partitions=expected_partitions,
        expected_rows=expected_rows,
        expected_secid=str(binding["secid"]),
        expected_source_ticker=str(binding["futoi.ticker"]),
    )


def freeze_accepted_history(
    *,
    data_root: str | Path,
    output_root: str | Path,
    repo_root: str | Path,
    instrument_id: str,
    start_date: str,
    end_date: str,
    run_id: str,
) -> dict[str, object]:
    root = Path(data_root).resolve()
    out_root = Path(output_root).resolve()
    repo = Path(repo_root).resolve()
    instrument = _safe_token(instrument_id, "instrument_id")
    checked_run = _safe_token(run_id, "run_id")
    scope = _accepted_history_scope(root, instrument, start_date, end_date)
    accepted_manifest_path = _expand_root_ref(root, scope.manifest_ref, "accepted raw manifest_ref")
    accepted_manifest = json.loads(accepted_manifest_path.read_text(encoding="utf-8"))
    accepted_start = str(accepted_manifest.get("requested_from") or "")
    accepted_end = str(accepted_manifest.get("requested_till") or "")
    if start_date != accepted_start or end_date != accepted_end:
        _fail("Stage 5 freeze must cover the exact full accepted raw-history range")
    expected_partitions = int(accepted_manifest.get("partition_count") or 0)
    expected_rows = int(accepted_manifest.get("row_count") or 0)
    if len(scope.accepted_dates) != expected_partitions or expected_partitions <= 0 or expected_rows <= 0:
        _fail("accepted raw manifest partition/row expectation is invalid")
    expectation = _expectation(repo, instrument, start_date, end_date, expected_partitions, expected_rows)

    freeze_root = out_root / "inputs" / ("dataset_id=" + SOURCE_DATASET_ID) / ("instrument_id=" + instrument) / ("freeze_run_id=" + checked_run)
    manifest_path = freeze_root / "frozen_input_manifest.json"
    if freeze_root.exists() or manifest_path.exists():
        _fail("immutable Stage 5 freeze target already exists")

    records: list[dict[str, object]] = []
    total_rows = 0
    for trade_date in scope.accepted_dates:
        source_path = raw_partition_path(root, instrument, trade_date)
        payload, source_stat, descriptor = _read_stable_regular_file(source_path)
        try:
            source_sha = _sha256_bytes(payload)
            try:
                frame = pd.read_parquet(io.BytesIO(payload))
            except Exception as exc:
                raise FutoiFreezeError("canonical raw FUTOI parquet is unreadable: " + str(exc)) from exc
            rows, secids = raw_acceptance._validate_futoi_partition(frame, expectation, trade_date)
            total_rows += rows
            frozen_path = freeze_root / "partitions" / ("trade_date=" + trade_date) / ("source=" + SOURCE_ID) / "part.parquet"
            _link_validated_inode(source_path, frozen_path, source_stat, descriptor, source_sha)
            frozen_sha = _sha256_bytes(frozen_path.read_bytes())
            if frozen_sha != source_sha:
                _fail("frozen partition content hash mismatch")
            records.append({
                "trade_date": trade_date,
                "row_count": rows,
                "secid_scope": list(secids),
                "canonical_source_ref": _rooted_ref(root, source_path),
                "source_sha256_at_freeze": source_sha,
                "frozen_partition_ref": _rooted_ref(root, frozen_path),
                "frozen_sha256": frozen_sha,
                "source_device": int(source_stat.st_dev),
                "source_inode": int(source_stat.st_ino),
                "hardlink_same_validated_inode": True,
                "physical_validation_status": "pass",
            })
        finally:
            os.close(descriptor)

    if len(records) != expected_partitions:
        _fail("frozen partition count does not match accepted raw history")
    if total_rows != expected_rows:
        _fail("frozen raw row count does not match accepted raw history")
    frozen_dates = [str(record["trade_date"]) for record in records]
    if _sha256_bytes((("\n".join(frozen_dates) + "\n") if frozen_dates else "").encode("utf-8")) != scope.partition_dates_sha256:
        _fail("frozen partition date set does not match accepted raw history digest")

    manifest_values: dict[str, object] = {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "producer": FREEZE_PRODUCER,
        "dataset_id": SOURCE_DATASET_ID,
        "instrument_id": instrument,
        "freeze_run_id": checked_run,
        "requested_from": start_date,
        "requested_till": end_date,
        "accepted_raw_pointer_ref": scope.pointer_ref,
        "accepted_raw_manifest_ref": scope.manifest_ref,
        "accepted_raw_acceptance_report_ref": scope.acceptance_report_ref,
        "accepted_raw_history_run_id": scope.acceptance_run_id,
        "accepted_partition_dates_sha256": scope.partition_dates_sha256,
        "partition_count": len(records),
        "row_count": total_rows,
        "physical_validation": "stage2_futoi_partition_validator_reapplied",
        "freeze_mode": "create_only_hardlink_same_validated_inode",
        "network_calls_used": False,
        "latest_autodetect_used": False,
        "records": records,
    }
    _write_json_create_only(manifest_path, manifest_values)
    return {
        "status": "succeeded",
        "dataset_id": SOURCE_DATASET_ID,
        "instrument_id": instrument,
        "run_id": checked_run,
        "partition_count": len(records),
        "row_count": total_rows,
        "manifest_path": manifest_path.as_posix(),
        "manifest_ref": _rooted_ref(root, manifest_path),
        "accepted_raw_history_run_id": scope.acceptance_run_id,
        "accepted_partition_dates_sha256": scope.partition_dates_sha256,
        "physical_validation_status": "pass",
        "freeze_mode": "create_only_hardlink_same_validated_inode",
        "network_calls_used": False,
        "latest_autodetect_used": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Freeze physically revalidated accepted FUTOI raw history into an immutable Stage 5 run input.")
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = freeze_accepted_history(
            data_root=args.data_root,
            output_root=args.output_root,
            repo_root=args.repo_root,
            instrument_id=args.instrument_id,
            start_date=args.start_date,
            end_date=args.end_date,
            run_id=args.run_id,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "network_calls_used": False}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
