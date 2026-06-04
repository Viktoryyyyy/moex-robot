from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .experiment_registry_contracts import (
    ExperimentRegistryEntry,
    PMReviewCloseout,
    validate_registry_entry_against_bundle,
)
from .result_storage_contracts import (
    ResultStorageBundle,
    ResultStorageValidationError,
    validate_result_storage_bundle,
)

DRY_RUN = "dry_run"
CONTROLLED_FIXTURE = "controlled_fixture"
CONTROLLED_TEMPDIR = "controlled_tempdir"
CONTROLLED_LOCAL_TEST = "controlled_local_test"
PRODUCTION_WRITE = "production_write"
ALLOWED_CONTROLLED_WRITE_MODES = frozenset({CONTROLLED_FIXTURE, CONTROLLED_TEMPDIR, CONTROLLED_LOCAL_TEST})


@dataclass(frozen=True)
class ControlledWriteResult:
    storage_mode: str
    run_id: str
    persisted: bool
    idempotent: bool
    registry_entry: ExperimentRegistryEntry
    result_storage_bundle: ResultStorageBundle
    pm_review_closeout: PMReviewCloseout
    bundle_root: str | None = None
    bundle_ref: str | None = None
    manifest_ref: str | None = None
    registry_entry_ref: str | None = None
    pm_review_closeout_ref: str | None = None


def dry_run_validate_result_storage(
    registry_entry: ExperimentRegistryEntry,
    bundle: ResultStorageBundle,
    pm_review_closeout: PMReviewCloseout,
) -> ControlledWriteResult:
    validate_registry_entry_against_bundle(registry_entry, bundle, pm_review_closeout)
    return ControlledWriteResult(
        storage_mode=DRY_RUN,
        run_id=bundle.run_id,
        persisted=False,
        idempotent=False,
        registry_entry=registry_entry,
        result_storage_bundle=bundle,
        pm_review_closeout=pm_review_closeout,
    )


def write_controlled_result_storage(
    *,
    registry_entry: ExperimentRegistryEntry,
    bundle: ResultStorageBundle,
    pm_review_closeout: PMReviewCloseout,
    storage_mode: str,
    storage_root: Path,
) -> ControlledWriteResult:
    if storage_mode == PRODUCTION_WRITE:
        raise ResultStorageValidationError("production_write is blocked")
    if storage_mode not in ALLOWED_CONTROLLED_WRITE_MODES:
        raise ResultStorageValidationError("unsupported controlled write mode")
    validate_registry_entry_against_bundle(registry_entry, bundle, pm_review_closeout)
    validate_result_storage_bundle(bundle)
    root = _validate_controlled_storage_root(Path(storage_root), storage_mode)
    bundle_root = root / bundle.run_id
    bundle_file = bundle_root / "result_storage_bundle.json"
    manifest_file = bundle_root / "artifact_bundle_manifest.json"
    registry_file = bundle_root / "experiment_registry_entry.json"
    closeout_file = bundle_root / "pm_review_closeout.json"

    if bundle_file.exists():
        existing = json.loads(bundle_file.read_text(encoding="utf-8"))
        existing_hash = existing.get("immutable_inputs_hash")
        if existing_hash == bundle.immutable_inputs_hash:
            return ControlledWriteResult(
                storage_mode=storage_mode,
                run_id=bundle.run_id,
                persisted=True,
                idempotent=True,
                registry_entry=registry_entry,
                result_storage_bundle=bundle,
                pm_review_closeout=pm_review_closeout,
                bundle_root=str(bundle_root),
                bundle_ref=str(bundle_file),
                manifest_ref=str(manifest_file),
                registry_entry_ref=str(registry_file),
                pm_review_closeout_ref=str(closeout_file),
            )
        raise ResultStorageValidationError("run_id collision with different immutable inputs")

    tmp_root = root / (bundle.run_id + ".tmp")
    if tmp_root.exists():
        _remove_tree(tmp_root)
    tmp_root.mkdir(parents=True, exist_ok=False)
    try:
        _write_json_atomic(tmp_root / "result_storage_bundle.json", _to_jsonable_bundle(bundle))
        _write_json_atomic(tmp_root / "artifact_bundle_manifest.json", asdict(bundle.artifact_manifest))
        _write_json_atomic(tmp_root / "experiment_registry_entry.json", asdict(registry_entry))
        _write_json_atomic(tmp_root / "pm_review_closeout.json", asdict(pm_review_closeout))
        tmp_root.replace(bundle_root)
    except Exception:
        if tmp_root.exists():
            _remove_tree(tmp_root)
        raise

    return ControlledWriteResult(
        storage_mode=storage_mode,
        run_id=bundle.run_id,
        persisted=True,
        idempotent=False,
        registry_entry=registry_entry,
        result_storage_bundle=bundle,
        pm_review_closeout=pm_review_closeout,
        bundle_root=str(bundle_root),
        bundle_ref=str(bundle_file),
        manifest_ref=str(manifest_file),
        registry_entry_ref=str(registry_file),
        pm_review_closeout_ref=str(closeout_file),
    )


def _validate_controlled_storage_root(root: Path, storage_mode: str) -> Path:
    if not root:
        raise ResultStorageValidationError("storage_root is required")
    resolved = root.expanduser().resolve()
    normalized = str(resolved).replace("\\", "/")
    if "/home/trader/" in normalized or normalized in {"/", "/home", "/home/trader"}:
        raise ResultStorageValidationError("server-side result storage is blocked")
    lowered_parts = {part.casefold() for part in resolved.parts}
    if storage_mode == CONTROLLED_TEMPDIR and not ({"tmp", "temp"} & lowered_parts or "pytest" in normalized.casefold()):
        raise ResultStorageValidationError("controlled_tempdir writes require an explicit temp root")
    if storage_mode == CONTROLLED_FIXTURE and "fixture" not in resolved.name.casefold():
        raise ResultStorageValidationError("controlled_fixture writes require an explicit fixture root")
    if storage_mode == CONTROLLED_LOCAL_TEST and "test" not in resolved.name.casefold():
        raise ResultStorageValidationError("controlled_local_test writes require an explicit local test root")
    return resolved


def _write_json_atomic(path: Path, payload: Any) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _to_jsonable_bundle(bundle: ResultStorageBundle) -> dict[str, Any]:
    values = asdict(bundle)
    values["immutable_inputs_hash"] = bundle.immutable_inputs_hash
    return values


def _remove_tree(path: Path) -> None:
    for child in sorted(path.rglob("*"), reverse=True):
        if child.is_file() or child.is_symlink():
            child.unlink()
        else:
            child.rmdir()
    path.rmdir()
