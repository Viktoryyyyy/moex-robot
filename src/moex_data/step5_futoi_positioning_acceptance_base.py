from __future__ import annotations

import fcntl
import hashlib
import os
import sys
from contextlib import contextmanager
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
_BASE_TRANSACTIONAL_REPLACE = _transactional_replace
_BASE_VALIDATE_PILOT = validate_pilot


def _content_date_set_sha256(values: Sequence[object]) -> str:
    normalized = [str(value) for value in values]
    payload = (("\n".join(normalized) + "\n") if normalized else "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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
        "accepted_raw_partition_dates_sha256": _content_date_set_sha256(resolved.get("accepted_dates", ())),
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


def _final_content_attestation_write_gate(records: Sequence[tuple[Path, Mapping[str, object]]]) -> tuple[str, str, str]:
    batch_keys: set[tuple[str, str, str]] = set()
    seen: set[str] = set()
    for _, values in records:
        if values.get("dataset_id") != EOD_DATASET:
            continue
        instrument_id = _safe_token(values.get("instrument_id"), "final gate instrument_id")
        if instrument_id in seen or instrument_id not in EXPECTED_INSTRUMENTS:
            _fail("final content-attestation gate EOD instrument set invalid")
        seen.add(instrument_id)

        resolved, marker_ref, manifest_ref, report_ref = _current_content_attestation(instrument_id)
        generation_id = str(resolved.get("generation_id") or "")
        marker_sha = str(resolved.get("marker_sha256") or "")
        if len(marker_sha) != 64 or not generation_id:
            _fail("final content-attestation marker identity invalid")

        eod_manifest_path = _expand_root_ref(values.get("manifest_ref"), "pending EOD manifest_ref")
        eod_manifest = _load_json(eod_manifest_path, "pending EOD manifest")
        frozen_manifest_path = _expand_root_ref(eod_manifest.get("frozen_input_manifest_ref"), "pending frozen_input_manifest_ref")
        frozen = _load_json(frozen_manifest_path, "pending frozen input manifest")
        expected = {
            "content_attestation_generation_id": generation_id,
            "content_attestation_marker_ref": marker_ref,
            "content_attestation_marker_sha256": marker_sha,
            "content_attested_manifest_ref": manifest_ref,
            "content_attested_manifest_sha256": str(resolved.get("manifest_sha256") or ""),
            "content_attested_partition_content_set_sha256": str(resolved.get("partition_content_set_sha256") or ""),
            "accepted_raw_pointer_ref": marker_ref,
            "accepted_raw_manifest_ref": manifest_ref,
            "accepted_raw_acceptance_report_ref": report_ref,
            "accepted_raw_history_run_id": generation_id,
        }
        for field, wanted in expected.items():
            if frozen.get(field) != wanted:
                _fail("final content-attestation write gate mismatch: " + instrument_id + " " + field)
        if frozen.get("legacy_pointer_consumption_used") is not False or frozen.get("source_mode") != "stage2_content_attested_generation_snapshots_only":
            _fail("final content-attestation write gate detected legacy/non-attested frozen lineage")
        batch_keys.add((generation_id, marker_ref, marker_sha))

    if seen != EXPECTED_INSTRUMENTS or len(batch_keys) != 1:
        _fail("final content-attestation write gate requires one shared current generation for Si/CR")
    return next(iter(batch_keys))


def _publication_lock_path() -> Path:
    return _data_root() / "state" / "acceptance" / "step5_futoi_positioning" / ".pointer_publication.lock"


@contextmanager
def _stage5_publication_lock():
    path = _publication_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise Step5AcceptanceError("cannot open Stage 5 publication lock: " + str(exc)) from exc
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _transactional_replace(records: Sequence[tuple[Path, Mapping[str, object]]]) -> None:
    with _stage5_publication_lock():
        paths = [path for path, _ in records]
        previous = {path: path.read_bytes() if path.exists() else None for path in paths}
        before = _final_content_attestation_write_gate(records)
        _BASE_TRANSACTIONAL_REPLACE(records)
        try:
            after = _final_content_attestation_write_gate(records)
            if after != before:
                _fail("content-attestation marker changed during Stage 5 pointer promotion")
        except Exception as exc:
            rollback_errors: list[str] = []
            for path in reversed(paths):
                try:
                    _restore(path, previous[path])
                except Exception as rollback_exc:
                    rollback_errors.append(str(rollback_exc))
            if rollback_errors:
                raise Step5AcceptanceError("content-attestation changed during promotion and rollback incomplete: " + ";".join(rollback_errors)) from exc
            raise Step5AcceptanceError("content-attestation changed during promotion; Stage 5 pointer set rolled back: " + str(exc)) from exc


def validate_pilot(values: Mapping[str, object], *, run_id: str) -> list[dict[str, object]]:
    if values.get("snapshot_policy") != "latest_resolved_complete_balanced_FIZ_YUR_event_ts":
        _fail("pilot snapshot policy mismatch")
    compatibility = dict(values)
    compatibility["snapshot_policy"] = "max_resolved_ts_requires_FIZ_and_YUR"
    return _BASE_VALIDATE_PILOT(compatibility, run_id=run_id)


def _reject_direct_base_cli() -> None:
    raise Step5AcceptanceError(
        "direct Stage 5 acceptance base CLI is forbidden; use moex_data.step5_futoi_positioning_acceptance"
    )


if _REAL_NAME == "__main__":
    _reject_direct_base_cli()
