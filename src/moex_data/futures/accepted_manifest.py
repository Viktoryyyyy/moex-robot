from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import tempfile
from typing import Final

from .contract_io import reject_dynamic_markers
from .manifest import validate_refresh_manifest_values
from .quality import validate_quality_report_rows


class FuturesAcceptedManifestError(ValueError):
    pass


_ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"


@dataclass(frozen=True)
class AcceptedManifestPointer:
    dataset_id: str
    instrument_id: str
    run_id: str
    manifest_ref: str
    quality_report_ref: str
    accepted_manifest_path: Path


def _fail(message: str) -> None:
    raise FuturesAcceptedManifestError(message)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(f"{field_name} must be text")
    try:
        return reject_dynamic_markers(value.strip(), field_name)
    except ValueError as exc:
        raise FuturesAcceptedManifestError(str(exc)) from exc


def _require_pointer_ref(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail("accepted_manifest_ref must be text")
    text = value.strip()
    if not text.startswith(_ROOT_PREFIX):
        _fail("accepted_manifest_ref must be rooted at MOEX_DATA_ROOT")
    if "{" in text.removeprefix(_ROOT_PREFIX) or "}" in text.removeprefix(_ROOT_PREFIX):
        _fail("accepted_manifest_ref must not contain unresolved placeholders")
    return text


def _env_root(env: Mapping[str, str] | None) -> str:
    root = str((os.environ if env is None else env).get("MOEX_DATA_ROOT", "")).strip()
    if not root:
        _fail("MOEX_DATA_ROOT is required")
    return root


def _pointer_path(env: Mapping[str, str] | None, accepted_manifest_ref: str, dataset_id: str, instrument_id: str) -> Path:
    root = _env_root(env)
    ref = _require_pointer_ref(accepted_manifest_ref)
    checked_dataset_id = _require_text(dataset_id, "dataset_id")
    checked_instrument_id = _require_text(instrument_id, "instrument_id")
    relative = ref.removeprefix(_ROOT_PREFIX)
    parts = Path(relative).parts
    if "dataset_id=" + checked_dataset_id not in parts:
        _fail("accepted_manifest_ref dataset_id does not match explicit dataset_id")
    if "instrument_id=" + checked_instrument_id not in parts:
        _fail("accepted_manifest_ref instrument_id does not match explicit instrument_id")
    if any(part in ("", ".", "..") for part in parts):
        _fail("accepted_manifest_ref must not contain path traversal")
    return Path(root) / relative


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_name = handle.name
    Path(temporary_name).replace(path)


def _quality_rows_for_scope(rows, dataset_id: str, instrument_id: str) -> tuple[object, ...]:
    scoped = tuple(row for row in rows if row.dataset_id == dataset_id and row.instrument_id == instrument_id)
    if not scoped:
        _fail("quality report has no row for explicit dataset_id/instrument_id")
    return scoped


def write_accepted_manifest_pointer(
    *,
    env: Mapping[str, str] | None,
    dataset_id: str,
    instrument_id: str,
    manifest_ref: str,
    manifest_values: Mapping[str, object],
    quality_rows: Sequence[Mapping[str, object]],
) -> AcceptedManifestPointer:
    checked_dataset_id = _require_text(dataset_id, "dataset_id")
    checked_instrument_id = _require_text(instrument_id, "instrument_id")
    checked_manifest_ref = _require_text(manifest_ref, "manifest_ref")
    manifest = validate_refresh_manifest_values(manifest_values)
    if manifest.refresh_status != "succeeded":
        _fail("accepted manifest pointer can be written only for succeeded refresh")
    if manifest.instrument_scope != (checked_instrument_id,):
        _fail("manifest instrument_scope must match exactly one explicit instrument_id")
    report = validate_quality_report_rows(quality_rows)
    scoped_rows = _quality_rows_for_scope(report.rows, checked_dataset_id, checked_instrument_id)
    if any(row.quality_status != "pass" for row in scoped_rows):
        _fail("accepted manifest pointer can be written only after quality pass")
    path = _pointer_path(env, manifest.accepted_manifest_ref, checked_dataset_id, checked_instrument_id)
    values = {
        "dataset_id": checked_dataset_id,
        "instrument_id": checked_instrument_id,
        "run_id": manifest.run_id,
        "manifest_ref": checked_manifest_ref,
        "quality_report_ref": manifest.quality_report_ref,
        "quality_status": "pass",
        "refresh_status": manifest.refresh_status,
    }
    _write_json_atomic(path, values)
    return AcceptedManifestPointer(
        dataset_id=checked_dataset_id,
        instrument_id=checked_instrument_id,
        run_id=manifest.run_id,
        manifest_ref=checked_manifest_ref,
        quality_report_ref=manifest.quality_report_ref,
        accepted_manifest_path=path,
    )


def read_accepted_manifest_pointer(
    *,
    env: Mapping[str, str] | None,
    dataset_id: str,
    instrument_id: str,
    accepted_manifest_ref: str,
) -> AcceptedManifestPointer:
    checked_dataset_id = _require_text(dataset_id, "dataset_id")
    checked_instrument_id = _require_text(instrument_id, "instrument_id")
    path = _pointer_path(env, accepted_manifest_ref, checked_dataset_id, checked_instrument_id)
    if not path.exists() or not path.is_file():
        _fail("accepted manifest pointer does not exist")
    values = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(values, Mapping):
        _fail("accepted manifest pointer must be a JSON object")
    if values.get("dataset_id") != checked_dataset_id:
        _fail("accepted manifest pointer dataset_id does not match explicit dataset_id")
    if values.get("instrument_id") != checked_instrument_id:
        _fail("accepted manifest pointer instrument_id does not match explicit instrument_id")
    return AcceptedManifestPointer(
        dataset_id=checked_dataset_id,
        instrument_id=checked_instrument_id,
        run_id=_require_text(values.get("run_id"), "run_id"),
        manifest_ref=_require_text(values.get("manifest_ref"), "manifest_ref"),
        quality_report_ref=_require_text(values.get("quality_report_ref"), "quality_report_ref"),
        accepted_manifest_path=path,
    )
