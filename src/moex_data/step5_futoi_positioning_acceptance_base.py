from __future__ import annotations

import sys
from pathlib import Path

_IMPL_PATH = Path(__file__).with_name("step5_futoi_positioning_acceptance_base_impl.inc")
_REAL_NAME = __name__
_CANONICAL_NAME = "moex_data.step5_futoi_positioning_acceptance_base"
_current_module = sys.modules[_REAL_NAME]
_existing = sys.modules.get(_CANONICAL_NAME)
if _existing is not None and _existing is not _current_module:
    raise RuntimeError("canonical Stage 5 acceptance base already loaded as a different object")
sys.modules[_CANONICAL_NAME] = _current_module
globals()["__name__"] = _CANONICAL_NAME
try:
    exec(compile(_IMPL_PATH.read_text(encoding="utf-8"), _IMPL_PATH.as_posix(), "exec"), globals(), globals())
finally:
    globals()["__name__"] = _REAL_NAME

from moex_data.futures import stage2_raw_history_content_reattestation as _content_attestation

_BASE_VALIDATE_FROZEN_INPUT = _validate_frozen_input


def _current_content_attestation(instrument_id: str) -> tuple[dict[str, object], str, str, str]:
    resolved = _content_attestation.resolve_content_attested_history(
        dataset_id=RAW_DATASET,
        instrument_id=instrument_id,
        repo_root=Path.cwd(),
    )
    marker_ref = _rooted_ref(Path(str(resolved.get("marker_path") or "")).resolve(strict=True))
    manifest_path = Path(str(resolved.get("manifest_path") or "")).resolve(strict=True)
    manifest_ref = _rooted_ref(manifest_path)
    content_manifest = _load_json(manifest_path, "current content-attested manifest")
    report_ref = str(content_manifest.get("content_attestation_report_ref") or "").strip()
    _expand_root_ref(report_ref, "current content-attested report_ref")
    return resolved, marker_ref, manifest_ref, report_ref


def _validate_eod_raw_lineage(manifest_values: Mapping[str, object], instrument_id: str) -> None:
    resolved, marker_ref, manifest_ref, report_ref = _current_content_attestation(instrument_id)
    expected = {
        "accepted_raw_pointer_ref": marker_ref,
        "accepted_raw_manifest_ref": manifest_ref,
        "accepted_raw_acceptance_report_ref": report_ref,
        "accepted_raw_history_run_id": str(resolved.get("generation_id") or ""),
        "accepted_raw_partition_dates_sha256": _date_set_sha256(list(resolved.get("accepted_dates", ()))),
    }
    for field, wanted in expected.items():
        if manifest_values.get(field) != wanted:
            _fail("EOD content-attested raw lineage mismatch: " + field)


def _validate_frozen_input(
    manifest_values: Mapping[str, object],
    instrument_id: str,
    run_root: Path,
    expected_rows: int,
) -> dict[str, object]:
    checked = _BASE_VALIDATE_FROZEN_INPUT(manifest_values, instrument_id, run_root, expected_rows)
    frozen = _load_json(checked["manifest_path"], "frozen input manifest")
    resolved, marker_ref, manifest_ref, report_ref = _current_content_attestation(instrument_id)
    explicit = {
        "content_attestation_generation_id": str(resolved.get("generation_id") or ""),
        "content_attestation_marker_ref": marker_ref,
        "content_attestation_marker_sha256": str(resolved.get("marker_sha256") or ""),
        "content_attested_manifest_ref": manifest_ref,
        "content_attested_manifest_sha256": str(resolved.get("manifest_sha256") or ""),
        "content_attested_partition_content_set_sha256": str(resolved.get("partition_content_set_sha256") or ""),
    }
    for field, wanted in explicit.items():
        if frozen.get(field) != wanted:
            _fail("frozen/current content-attestation mismatch: " + field)
    if frozen.get("accepted_raw_pointer_ref") != marker_ref or frozen.get("accepted_raw_manifest_ref") != manifest_ref:
        _fail("frozen input attempted legacy-pointer or stale-generation bypass")
    if frozen.get("accepted_raw_acceptance_report_ref") != report_ref or frozen.get("accepted_raw_history_run_id") != resolved.get("generation_id"):
        _fail("frozen input content-attested generation/report mismatch")
    if frozen.get("legacy_pointer_consumption_used") is not False or frozen.get("source_mode") != "stage2_content_attested_generation_snapshots_only":
        _fail("frozen input must declare content-attested snapshots only")

    attested_rows = resolved.get("records", ())
    if isinstance(attested_rows, (str, bytes)) or not isinstance(attested_rows, Sequence):
        _fail("current content-attested records missing")
    attested_by_date: dict[str, Mapping[str, object]] = {}
    for item in attested_rows:
        if not isinstance(item, Mapping):
            _fail("current content-attested record must be object")
        trade_date = str(item.get("trade_date") or "")
        if trade_date in attested_by_date:
            _fail("duplicate current content-attested trade_date")
        attested_by_date[trade_date] = item
    frozen_by_date = checked.get("records_by_date")
    if not isinstance(frozen_by_date, Mapping) or tuple(frozen_by_date) != tuple(attested_by_date):
        _fail("frozen dates differ from current content-attested generation")
    for trade_date, frozen_record in frozen_by_date.items():
        if not isinstance(frozen_record, Mapping):
            _fail("frozen record must be object")
        attested = attested_by_date[str(trade_date)]
        expected_sha = str(attested.get("sha256") or "")
        for field in ("content_attested_sha256", "source_sha256_at_freeze", "frozen_sha256"):
            if frozen_record.get(field) != expected_sha:
                _fail("frozen partition differs from attested SHA: " + str(trade_date) + " " + field)
        if frozen_record.get("content_attested_snapshot_ref") != attested.get("snapshot_ref"):
            _fail("frozen partition attested snapshot ref mismatch: " + str(trade_date))
        if frozen_record.get("canonical_source_ref") != attested.get("canonical_ref"):
            _fail("frozen partition canonical lineage mismatch: " + str(trade_date))
    checked["content_attestation_generation_id"] = str(resolved.get("generation_id") or "")
    checked["content_attestation_marker_sha256"] = str(resolved.get("marker_sha256") or "")
    checked["content_attested_partition_content_set_sha256"] = str(resolved.get("partition_content_set_sha256") or "")
    return checked


if _REAL_NAME == "__main__":
    raise SystemExit(main())
