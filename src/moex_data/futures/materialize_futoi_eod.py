from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

_IMPL_PATH = Path(__file__).with_name("materialize_futoi_eod_impl.inc")
_REAL_NAME = __name__
_CANONICAL_NAME = "moex_data.futures.materialize_futoi_eod"
_current_module = sys.modules[_REAL_NAME]
_existing = sys.modules.get(_CANONICAL_NAME)
if _existing is not None and _existing is not _current_module:
    raise RuntimeError("canonical Stage 5 EOD module already loaded as a different object")
sys.modules[_CANONICAL_NAME] = _current_module
globals()["__name__"] = _CANONICAL_NAME
try:
    exec(compile(_IMPL_PATH.read_text(encoding="utf-8"), _IMPL_PATH.as_posix(), "exec"), globals(), globals())
finally:
    globals()["__name__"] = _REAL_NAME

from . import stage2_raw_history_content_reattestation as _content_attestation

SNAPSHOT_POLICY = "latest_resolved_complete_balanced_FIZ_YUR_event_ts"
_BASE_LOAD_FROZEN_INPUT_SCOPE = _load_frozen_input_scope
_BASE_SINGLE_EOD_ROW = _single_eod_row
_BASE_MATERIALIZE_EOD_HISTORY = materialize_eod_history


def _require_stage2_root(root: Path) -> None:
    configured = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not configured:
        _fail("MOEX_DATA_ROOT is required for Stage 2 content-attested reads")
    if Path(configured).resolve() != root.resolve():
        _fail("data_root differs from canonical MOEX_DATA_ROOT")


def _content_attested_scope(root: Path, instrument_id: str, start_date: str, end_date: str) -> tuple[AcceptedHistoryScope, dict[str, object]]:
    _require_stage2_root(root)
    resolved = _content_attestation.resolve_content_attested_history(
        dataset_id=SOURCE_DATASET_ID,
        instrument_id=instrument_id,
        repo_root=Path.cwd(),
    )
    accepted_start = _iso_date(resolved.get("requested_from"), "content-attested requested_from")
    accepted_end = _iso_date(resolved.get("requested_till"), "content-attested requested_till")
    if start_date < accepted_start or end_date > accepted_end:
        _fail("requested Stage 5 range is outside content-attested raw-history range")
    resolved_dates = tuple(str(value) for value in resolved.get("accepted_dates", ()))
    if not resolved_dates:
        _fail("content-attested raw history contains no accepted partitions")
    requested_dates = _date_range(start_date, end_date)
    present = set(resolved_dates)
    accepted_dates = tuple(value for value in requested_dates if value in present)
    missing_requested = tuple(value for value in requested_dates if value not in present)
    if not accepted_dates:
        _fail("content-attested raw history contains no partitions in requested Stage 5 range")
    manifest_path = Path(str(resolved.get("manifest_path") or "")).resolve(strict=True)
    manifest = _load_json(manifest_path, "content-attested raw manifest")
    report_ref = str(manifest.get("content_attestation_report_ref") or "").strip()
    _expand_root_ref(root, report_ref, "content-attested report_ref")
    marker_path = Path(str(resolved.get("marker_path") or "")).resolve(strict=True)
    scope = AcceptedHistoryScope(
        accepted_dates=accepted_dates,
        missing_requested_dates=missing_requested,
        pointer_ref=_rooted_ref(root, marker_path),
        manifest_ref=_rooted_ref(root, manifest_path),
        acceptance_report_ref=report_ref,
        acceptance_run_id=_safe_token(resolved.get("generation_id"), "content-attested generation_id"),
        partition_dates_sha256=_date_set_sha256(list(resolved_dates)),
    )
    return scope, resolved


def _accepted_history_scope(root: Path, instrument_id: str, start_date: str, end_date: str) -> AcceptedHistoryScope:
    scope, _ = _content_attested_scope(root, instrument_id, start_date, end_date)
    return scope


def _load_frozen_input_scope(
    root: Path,
    frozen_input_manifest: str | Path,
    instrument_id: str,
    start_date: str,
    end_date: str,
) -> FrozenInputScope:
    checked = _BASE_LOAD_FROZEN_INPUT_SCOPE(root, frozen_input_manifest, instrument_id, start_date, end_date)
    scope, resolved = _content_attested_scope(root, instrument_id, start_date, end_date)
    frozen = _load_json(checked.manifest_path, "frozen input manifest")

    marker_path = Path(str(resolved.get("marker_path") or "")).resolve(strict=True)
    manifest_path = Path(str(resolved.get("manifest_path") or "")).resolve(strict=True)
    expected_explicit = {
        "content_attestation_generation_id": str(resolved.get("generation_id") or ""),
        "content_attestation_marker_ref": _rooted_ref(root, marker_path),
        "content_attestation_marker_sha256": str(resolved.get("marker_sha256") or ""),
        "content_attested_manifest_ref": _rooted_ref(root, manifest_path),
        "content_attested_manifest_sha256": str(resolved.get("manifest_sha256") or ""),
        "content_attested_partition_content_set_sha256": str(resolved.get("partition_content_set_sha256") or ""),
    }
    for field, expected in expected_explicit.items():
        if frozen.get(field) != expected:
            _fail("frozen/current content-attestation mismatch during EOD materialization: " + field)
    if frozen.get("legacy_pointer_consumption_used") is not False or frozen.get("source_mode") != "stage2_content_attested_generation_snapshots_only":
        _fail("EOD materialization requires content-attested frozen snapshots only")

    attested_rows = resolved.get("records", ())
    if isinstance(attested_rows, (str, bytes)) or not isinstance(attested_rows, Sequence):
        _fail("current content-attested records missing during EOD materialization")
    accepted_set = set(scope.accepted_dates)
    attested_by_date: dict[str, Mapping[str, object]] = {}
    for item in attested_rows:
        if not isinstance(item, Mapping):
            _fail("current content-attested record must be object during EOD materialization")
        trade_date = str(item.get("trade_date") or "")
        if trade_date not in accepted_set:
            continue
        if trade_date in attested_by_date:
            _fail("duplicate current content-attested trade_date during EOD materialization")
        attested_by_date[trade_date] = item
    if tuple(attested_by_date) != scope.accepted_dates:
        _fail("current content-attested records do not cover EOD materialization scope")

    frozen_records = checked.records
    if tuple(str(record.get("trade_date") or "") for record in frozen_records) != tuple(attested_by_date):
        _fail("frozen record dates differ from current content-attested generation during EOD materialization")
    for frozen_record in frozen_records:
        trade_date = str(frozen_record.get("trade_date") or "")
        attested = attested_by_date[trade_date]
        expected_sha = str(attested.get("sha256") or "")
        for field in ("content_attested_sha256", "source_sha256_at_freeze", "frozen_sha256"):
            if frozen_record.get(field) != expected_sha:
                _fail("frozen record differs from attested SHA during EOD materialization: " + trade_date + " " + field)
        if frozen_record.get("content_attested_snapshot_ref") != attested.get("snapshot_ref"):
            _fail("frozen record snapshot ref differs from attested generation during EOD materialization: " + trade_date)
        if frozen_record.get("canonical_source_ref") != attested.get("canonical_ref"):
            _fail("frozen record canonical ref differs from attested generation during EOD materialization: " + trade_date)
    return checked


def _single_eod_row(
    frame,
    *,
    instrument_id: str,
    trade_date: str,
    frozen_ref: str,
    canonical_source_ref: str,
    frozen_sha256: str,
) -> dict[str, object]:
    """Select the latest resolved snapshot that is complete and exactly balanced."""
    work = _validate_raw(frame, instrument_id=instrument_id, trade_date=trade_date)
    resolved, revisions_dropped = _resolve_revisions(work)
    for candidate_ts in sorted(resolved["_ts_utc"].drop_duplicates().tolist(), reverse=True):
        candidate = resolved.loc[resolved["_ts_utc"].eq(candidate_ts)].copy()
        if len(candidate.index) != 2 or set(candidate["clgroup"].tolist()) != GROUPS:
            continue
        total_long = int(candidate["pos_long"].sum())
        total_short_abs = int(candidate["pos_short"].abs().sum())
        total_net = int(candidate["pos"].sum())
        if total_long != total_short_abs or total_net != 0:
            continue
        row = _BASE_SINGLE_EOD_ROW(
            candidate,
            instrument_id=instrument_id,
            trade_date=trade_date,
            frozen_ref=frozen_ref,
            canonical_source_ref=canonical_source_ref,
            frozen_sha256=frozen_sha256,
        )
        row["source_row_count"] = int(len(work.index))
        row["source_revision_rows_dropped"] = revisions_dropped
        return row
    _fail("no complete balanced FIZ/YUR snapshot exists for FUTOI trade_date")


def materialize_eod_history(
    *,
    data_root: str | Path,
    output_root: str | Path,
    frozen_input_manifest: str | Path,
    instrument_id: str,
    start_date: str,
    end_date: str,
    run_id: str,
) -> dict[str, object]:
    result = _BASE_MATERIALIZE_EOD_HISTORY(
        data_root=data_root,
        output_root=output_root,
        frozen_input_manifest=frozen_input_manifest,
        instrument_id=instrument_id,
        start_date=start_date,
        end_date=end_date,
        run_id=run_id,
    )
    manifest_path = Path(str(result["manifest_path"]))
    manifest_values = _load_json(manifest_path, "EOD manifest")
    manifest_values["snapshot_policy"] = SNAPSHOT_POLICY
    _atomic_json(manifest_path, manifest_values)
    return result


if _REAL_NAME == "__main__":
    raise SystemExit(main())
