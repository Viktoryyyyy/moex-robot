from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Final

from .schemas import ArtifactManifest, ExperimentRegistryEntry, RegistryValidationError
from .validation import validate_persistable_registry_entry

_FILE_WRITE_MODE: Final = "file_write"
_CATALOG_SCHEMA_VERSION: Final = "experiment_registry_catalog.v1"
_FORBIDDEN_DYNAMIC_PARTS: Final = frozenset({"latest", "current", "autodetect"})
_FORBIDDEN_PATH_CHARACTERS: Final = frozenset("*?[]{}")
_SAFE_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


@dataclass(frozen=True)
class FileRegistryWriteResult:
    run_id: str
    registry_entry_id: str
    artifact_manifest_ref: str
    write_mode: str
    persisted: bool
    idempotent: bool
    registry_root: str
    manifest_path: str
    entry_path: str
    catalog_path: str


class FileExperimentRegistryWriter:
    """Append-only file-backed registry writer for explicit local registry roots."""

    writer_id = "moex_research.registry.file_write"

    def __init__(self, registry_root: Path | str) -> None:
        self.registry_root = _validate_explicit_root(registry_root)

    def write(
        self,
        entry: ExperimentRegistryEntry,
        manifest: ArtifactManifest,
        *,
        mode: str = _FILE_WRITE_MODE,
    ) -> FileRegistryWriteResult:
        if mode != _FILE_WRITE_MODE:
            raise RegistryValidationError("unsupported file registry write mode")
        validate_persistable_registry_entry(entry, manifest)
        _validate_safe_id(entry.registry_entry_id, "registry_entry_id")
        _validate_safe_id(manifest.artifact_manifest_id, "artifact_manifest_id")

        manifests_root = self.registry_root / "manifests"
        entries_root = self.registry_root / "entries"
        manifest_path = manifests_root / f"{manifest.artifact_manifest_id}.json"
        entry_path = entries_root / f"{entry.registry_entry_id}.json"
        catalog_path = self.registry_root / "catalog.json"

        manifest_bytes = _canonical_json_bytes(asdict(manifest))
        entry_bytes = _canonical_json_bytes(asdict(entry))

        self.registry_root.mkdir(parents=True, exist_ok=True)
        manifests_root.mkdir(parents=True, exist_ok=True)
        entries_root.mkdir(parents=True, exist_ok=True)
        _reject_symlink_target(manifest_path)
        _reject_symlink_target(entry_path)
        _reject_symlink_target(catalog_path)

        catalog = _load_catalog(catalog_path)
        record = _catalog_record(entry, manifest)
        catalog_records = _merge_catalog_record(catalog["entries"], record)
        expected_catalog = {
            "schema_version": _CATALOG_SCHEMA_VERSION,
            "entries": catalog_records,
        }
        catalog_bytes = _canonical_json_bytes(expected_catalog)

        # Fail before any mutation when an immutable identifier already maps to
        # different content. This prevents an orphan manifest on entry conflict.
        _preflight_immutable_target(manifest_path, manifest_bytes)
        _preflight_immutable_target(entry_path, entry_bytes)

        manifest_created = _write_immutable_atomic(manifest_path, manifest_bytes)
        entry_created = _write_immutable_atomic(entry_path, entry_bytes)
        catalog_changed = _write_replace_if_changed(catalog_path, catalog_bytes)

        _verify_bytes(manifest_path, manifest_bytes)
        _verify_bytes(entry_path, entry_bytes)
        verified_catalog = _load_catalog(catalog_path)
        if verified_catalog != expected_catalog:
            raise RegistryValidationError("catalog verification failed after persistence")

        return FileRegistryWriteResult(
            run_id=entry.run_id,
            registry_entry_id=entry.registry_entry_id,
            artifact_manifest_ref=entry.artifact_manifest_ref,
            write_mode=mode,
            persisted=True,
            idempotent=not (manifest_created or entry_created or catalog_changed),
            registry_root=str(self.registry_root),
            manifest_path=str(manifest_path),
            entry_path=str(entry_path),
            catalog_path=str(catalog_path),
        )


def _validate_explicit_root(root: Path | str) -> Path:
    raw = os.fspath(root)
    if not isinstance(raw, str) or not raw.strip():
        raise RegistryValidationError("registry_root must be supplied explicitly")
    if any(character in raw for character in _FORBIDDEN_PATH_CHARACTERS):
        raise RegistryValidationError("registry_root must not contain glob or template syntax")
    path = Path(raw).expanduser()
    if path.is_symlink():
        raise RegistryValidationError("registry_root must not be a symlink")
    resolved = path.resolve(strict=False)
    lowered_parts = {part.casefold() for part in resolved.parts}
    if lowered_parts & _FORBIDDEN_DYNAMIC_PARTS:
        raise RegistryValidationError("registry_root must not use latest/current/autodetect aliases")
    if resolved.exists() and not resolved.is_dir():
        raise RegistryValidationError("registry_root must reference a directory")
    return resolved


def _validate_safe_id(value: str, field_name: str) -> None:
    if not _SAFE_ID_RE.fullmatch(value):
        raise RegistryValidationError(f"{field_name} is not safe for append-only file persistence")


def _catalog_record(
    entry: ExperimentRegistryEntry,
    manifest: ArtifactManifest,
) -> dict[str, str]:
    return {
        "artifact_manifest_id": manifest.artifact_manifest_id,
        "entry_ref": f"entries/{entry.registry_entry_id}.json",
        "manifest_ref": f"manifests/{manifest.artifact_manifest_id}.json",
        "registry_entry_id": entry.registry_entry_id,
        "repo_commit": entry.repo_commit,
        "run_id": entry.run_id,
    }


def _load_catalog(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": _CATALOG_SCHEMA_VERSION, "entries": []}
    if path.is_symlink() or not path.is_file():
        raise RegistryValidationError("catalog.json must be a regular file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryValidationError("catalog.json is not valid deterministic JSON") from exc
    if not isinstance(payload, dict):
        raise RegistryValidationError("catalog.json must contain an object")
    if payload.get("schema_version") != _CATALOG_SCHEMA_VERSION:
        raise RegistryValidationError("catalog.json schema_version mismatch")
    entries = payload.get("entries")
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise RegistryValidationError("catalog.json entries must be a list of objects")
    ids = [item.get("registry_entry_id") for item in entries]
    if any(not isinstance(item_id, str) or not item_id for item_id in ids):
        raise RegistryValidationError("catalog.json contains an invalid registry_entry_id")
    if len(ids) != len(set(ids)):
        raise RegistryValidationError("catalog.json contains duplicate registry_entry_id values")
    if ids != sorted(ids):
        raise RegistryValidationError("catalog.json entries must be sorted by registry_entry_id")
    return payload


def _merge_catalog_record(
    records: list[dict[str, Any]],
    record: dict[str, str],
) -> list[dict[str, Any]]:
    by_id = {item["registry_entry_id"]: item for item in records}
    existing = by_id.get(record["registry_entry_id"])
    if existing is not None and existing != record:
        raise RegistryValidationError("registry_entry_id collision with different catalog content")
    by_id[record["registry_entry_id"]] = record
    return [by_id[key] for key in sorted(by_id)]


def _canonical_json_bytes(payload: Any) -> bytes:
    try:
        text = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise RegistryValidationError("registry payload is not deterministic JSON") from exc
    return (text + "\n").encode("utf-8")


def _reject_symlink_target(path: Path) -> None:
    if path.is_symlink():
        raise RegistryValidationError(f"append-only target must not be a symlink: {path.name}")


def _preflight_immutable_target(path: Path, content: bytes) -> None:
    if path.is_symlink():
        raise RegistryValidationError(f"append-only target must not be a symlink: {path.name}")
    if path.exists():
        _verify_bytes(path, content, collision=True)


def _write_immutable_atomic(path: Path, content: bytes) -> bool:
    if path.exists():
        _verify_bytes(path, content, collision=True)
        return False
    if path.is_symlink():
        raise RegistryValidationError(f"append-only target must not be a symlink: {path.name}")

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, path)
            created = True
        except FileExistsError:
            _verify_bytes(path, content, collision=True)
            created = False
        _fsync_directory(path.parent)
        return created
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_replace_if_changed(path: Path, content: bytes) -> bool:
    if path.exists() and path.read_bytes() == content:
        return False
    if path.is_symlink():
        raise RegistryValidationError("catalog.json must not be a symlink")
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
        return True
    finally:
        temporary_path.unlink(missing_ok=True)


def _verify_bytes(path: Path, expected: bytes, *, collision: bool = False) -> None:
    if path.is_symlink() or not path.is_file():
        raise RegistryValidationError(f"persisted registry target is not a regular file: {path.name}")
    try:
        actual = path.read_bytes()
    except OSError as exc:
        raise RegistryValidationError(f"failed to verify persisted registry target: {path.name}") from exc
    if actual != expected:
        if collision:
            raise RegistryValidationError(f"append-only ID collision with different content: {path.name}")
        raise RegistryValidationError(f"persisted registry target verification failed: {path.name}")


def _fsync_directory(path: Path) -> None:
    flags = getattr(os, "O_DIRECTORY", 0) | os.O_RDONLY
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["FileExperimentRegistryWriter", "FileRegistryWriteResult"]
