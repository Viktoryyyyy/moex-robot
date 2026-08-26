from __future__ import annotations

import os
import sys
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


if _REAL_NAME == "__main__":
    raise SystemExit(main())
