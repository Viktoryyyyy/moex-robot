from __future__ import annotations

import os
import sys
from pathlib import Path

_IMPL_PATH = Path(__file__).with_name("freeze_accepted_futoi_history_impl.inc")
_REAL_NAME = __name__
_CANONICAL_NAME = "moex_data.futures.freeze_accepted_futoi_history"
_current_module = sys.modules[_REAL_NAME]
_existing = sys.modules.get(_CANONICAL_NAME)
if _existing is not None and _existing is not _current_module:
    raise RuntimeError("canonical Stage 5 freeze module already loaded as a different object")
sys.modules[_CANONICAL_NAME] = _current_module
globals()["__name__"] = _CANONICAL_NAME
try:
    exec(compile(_IMPL_PATH.read_text(encoding="utf-8"), _IMPL_PATH.as_posix(), "exec"), globals(), globals())
finally:
    globals()["__name__"] = _REAL_NAME

from . import stage2_raw_history_content_reattestation as _content_attestation


def _require_stage2_root(root: Path) -> None:
    configured = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not configured:
        _fail("MOEX_DATA_ROOT is required for Stage 2 content-attested reads")
    if Path(configured).resolve() != root.resolve():
        _fail("data_root differs from canonical MOEX_DATA_ROOT")


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
    _require_stage2_root(root)
    resolved = _content_attestation.resolve_content_attested_history(
        dataset_id=SOURCE_DATASET_ID,
        instrument_id=instrument,
        repo_root=repo,
    )
    accepted_start = str(resolved.get("requested_from") or "")
    accepted_end = str(resolved.get("requested_till") or "")
    if start_date != accepted_start or end_date != accepted_end:
        _fail("Stage 5 freeze must cover the exact full content-attested raw-history range")
    accepted_dates = tuple(str(value) for value in resolved.get("accepted_dates", ()))
    source_records = tuple(resolved.get("records", ()))
    expected_partitions = int(resolved.get("partition_count") or 0)
    expected_rows = int(resolved.get("row_count") or 0)
    if len(accepted_dates) != expected_partitions or len(source_records) != expected_partitions or expected_partitions <= 0 or expected_rows <= 0:
        _fail("content-attested raw partition/row expectation is invalid")
    if tuple(str(row.get("trade_date")) for row in source_records if isinstance(row, Mapping)) != accepted_dates:
        _fail("content-attested resolver record dates mismatch")
    expectation = _expectation(repo, instrument, start_date, end_date, expected_partitions, expected_rows)

    marker_path = Path(str(resolved.get("marker_path") or "")).resolve(strict=True)
    attested_manifest_path = Path(str(resolved.get("manifest_path") or "")).resolve(strict=True)
    attested_manifest = json.loads(attested_manifest_path.read_text(encoding="utf-8"))
    report_ref = str(attested_manifest.get("content_attestation_report_ref") or "").strip()
    _expand_root_ref(root, report_ref, "content-attested report_ref")
    marker_ref = _rooted_ref(root, marker_path)
    attested_manifest_ref = _rooted_ref(root, attested_manifest_path)
    generation_id = _safe_token(resolved.get("generation_id"), "content-attested generation_id")
    marker_sha = str(resolved.get("marker_sha256") or "").strip().lower()
    manifest_sha = str(resolved.get("manifest_sha256") or "").strip().lower()
    content_set_sha = str(resolved.get("partition_content_set_sha256") or "").strip().lower()
    for value, field in ((marker_sha, "marker_sha256"), (manifest_sha, "manifest_sha256"), (content_set_sha, "partition_content_set_sha256")):
        if len(value) != 64:
            _fail("invalid content-attestation " + field)

    freeze_root = out_root / "inputs" / ("dataset_id=" + SOURCE_DATASET_ID) / ("instrument_id=" + instrument) / ("freeze_run_id=" + checked_run)
    manifest_path = freeze_root / "frozen_input_manifest.json"
    if freeze_root.exists() or manifest_path.exists():
        _fail("immutable Stage 5 freeze target already exists")

    records: list[dict[str, object]] = []
    total_rows = 0
    for source_record in source_records:
        if not isinstance(source_record, Mapping):
            _fail("content-attested partition record must be object")
        trade_date = str(source_record.get("trade_date") or "")
        snapshot_path = Path(str(source_record.get("snapshot_path") or "")).resolve(strict=True)
        try:
            snapshot_path.relative_to(root.resolve(strict=True))
        except ValueError as exc:
            raise FutoiFreezeError("content-attested snapshot escaped MOEX_DATA_ROOT") from exc
        expected_sha = str(source_record.get("sha256") or "").strip().lower()
        if len(expected_sha) != 64:
            _fail("content-attested partition SHA-256 invalid")
        payload, source_stat, descriptor = _read_stable_regular_file(snapshot_path)
        try:
            source_sha = _sha256_bytes(payload)
            if source_sha != expected_sha:
                _fail("content-attested snapshot SHA-256 differs from generation evidence")
            try:
                frame = pd.read_parquet(io.BytesIO(payload))
            except Exception as exc:
                raise FutoiFreezeError("content-attested FUTOI parquet is unreadable: " + str(exc)) from exc
            rows, secids = raw_acceptance._validate_futoi_partition(frame, expectation, trade_date)
            if int(source_record.get("row_count") or 0) != rows:
                _fail("content-attested partition row-count evidence mismatch")
            total_rows += rows
            frozen_path = freeze_root / "partitions" / ("trade_date=" + trade_date) / ("source=" + SOURCE_ID) / "part.parquet"
            _link_validated_inode(snapshot_path, frozen_path, source_stat, descriptor, expected_sha)
            frozen_sha = _sha256_bytes(frozen_path.read_bytes())
            if frozen_sha != expected_sha:
                _fail("run-scoped frozen partition differs from content-attested bytes")
            records.append({
                "trade_date": trade_date,
                "row_count": rows,
                "secid_scope": list(secids),
                "canonical_source_ref": str(source_record.get("canonical_ref") or ""),
                "content_attested_snapshot_ref": str(source_record.get("snapshot_ref") or ""),
                "content_attested_sha256": expected_sha,
                "source_sha256_at_freeze": expected_sha,
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
        _fail("frozen partition count does not match content-attested raw history")
    if total_rows != expected_rows:
        _fail("frozen raw row count does not match content-attested raw history")
    frozen_dates = [str(record["trade_date"]) for record in records]
    partition_dates_sha = _sha256_bytes((("\n".join(frozen_dates) + "\n") if frozen_dates else "").encode("utf-8"))
    if frozen_dates != list(accepted_dates):
        _fail("frozen partition date set does not match content-attested raw history")

    manifest_values: dict[str, object] = {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "producer": FREEZE_PRODUCER,
        "dataset_id": SOURCE_DATASET_ID,
        "instrument_id": instrument,
        "freeze_run_id": checked_run,
        "requested_from": start_date,
        "requested_till": end_date,
        "accepted_raw_pointer_ref": marker_ref,
        "accepted_raw_manifest_ref": attested_manifest_ref,
        "accepted_raw_acceptance_report_ref": report_ref,
        "accepted_raw_history_run_id": generation_id,
        "accepted_partition_dates_sha256": partition_dates_sha,
        "content_attestation_generation_id": generation_id,
        "content_attestation_marker_ref": marker_ref,
        "content_attestation_marker_sha256": marker_sha,
        "content_attested_manifest_ref": attested_manifest_ref,
        "content_attested_manifest_sha256": manifest_sha,
        "content_attested_partition_content_set_sha256": content_set_sha,
        "partition_count": len(records),
        "row_count": total_rows,
        "physical_validation": "stage2_futoi_partition_validator_reapplied",
        "freeze_mode": "create_only_hardlink_same_validated_inode",
        "source_mode": "stage2_content_attested_generation_snapshots_only",
        "legacy_pointer_consumption_used": False,
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
        "accepted_raw_history_run_id": generation_id,
        "accepted_partition_dates_sha256": partition_dates_sha,
        "content_attestation_generation_id": generation_id,
        "content_attestation_marker_sha256": marker_sha,
        "content_attested_partition_content_set_sha256": content_set_sha,
        "physical_validation_status": "pass",
        "freeze_mode": "create_only_hardlink_same_validated_inode",
        "legacy_pointer_consumption_used": False,
        "network_calls_used": False,
        "latest_autodetect_used": False,
    }


if _REAL_NAME == "__main__":
    raise SystemExit(main())
