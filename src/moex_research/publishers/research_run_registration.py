from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final, Mapping

from ..assets.run_archive import RunArchiveResult, create_deterministic_run_archive
from ..registry.file_write import FileExperimentRegistryWriter, FileRegistryWriteResult
from ..registry.schemas import (
    ACCEPTED_RESULT_STATUSES,
    ALLOWED_CANONICALITY_STATUSES,
    ALLOWED_RUN_STATUSES,
    REQUIRED_CANONICAL_ARTIFACT_ROLES,
    ArtifactManifest,
    ArtifactManifestItem,
    ExperimentRegistryEntry,
    RegistryValidationError,
    ResultStatus,
)
from ..registry.validation import validate_persistable_registry_entry

REGISTRATION_SCHEMA_VERSION: Final = "research_run_registration.v1"
UNDECLARED_ARTIFACT_POLICY: Final = "reject"
_REQUIRED_SPEC_FIELDS: Final = (
    "schema_version",
    "run_id",
    "strategy_id",
    "strategy_version",
    "test_type",
    "instrument_scope",
    "timeframe_scope",
    "repo_commit",
    "run_status",
    "result_status",
    "canonicality_status",
    "dataset_refs",
    "feature_refs",
    "label_refs",
    "parameter_set",
    "metrics",
    "artifacts",
)
_REQUIRED_ARTIFACT_FIELDS: Final = (
    "filename",
    "artifact_id",
    "artifact_role",
    "format",
    "schema_version",
    "required_for_canonical",
)
_OPTIONAL_SPEC_FIELDS: Final = frozenset({"created_ts"})
_OPTIONAL_ARTIFACT_FIELDS: Final = frozenset({"artifact_class", "producer", "consumer"})
_FORBIDDEN_DYNAMIC_PARTS: Final = frozenset({"latest", "current", "autodetect"})
_FORBIDDEN_PATH_CHARACTERS: Final = frozenset("*?[]{}")
_COMMIT_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-fA-F]{40}$")
_RUN_DATE_RE: Final[re.Pattern[str]] = re.compile(r"(20[0-9]{6})$")


class ResearchRunRegistrationError(RegistryValidationError):
    pass


@dataclass(frozen=True)
class RegistrationArtifactSpec:
    filename: str
    artifact_id: str
    artifact_role: str
    format: str
    schema_version: str
    required_for_canonical: bool
    artifact_class: str
    producer: str
    consumer: str


@dataclass(frozen=True)
class ResearchRunRegistrationSpec:
    schema_version: str
    run_id: str
    strategy_id: str
    strategy_version: str
    test_type: str
    instrument_scope: tuple[str, ...]
    timeframe_scope: tuple[str, ...]
    repo_commit: str
    run_status: str
    result_status: str
    canonicality_status: str
    dataset_refs: tuple[str, ...]
    feature_refs: tuple[str, ...]
    label_refs: tuple[str, ...]
    parameter_set: Mapping[str, Any]
    metrics: Mapping[str, Any]
    artifacts: tuple[RegistrationArtifactSpec, ...]
    created_ts: str


@dataclass(frozen=True)
class ResearchRunRegistrationResult:
    run_id: str
    artifact_manifest: ArtifactManifest
    registry_entry: ExperimentRegistryEntry
    archive: RunArchiveResult
    registry_write: FileRegistryWriteResult
    undeclared_artifact_policy: str
    outputs_created: tuple[str, ...]


def load_registration_spec(path: Path | str) -> ResearchRunRegistrationSpec:
    spec_path = _validate_explicit_file(path, "registration_spec_path")
    try:
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResearchRunRegistrationError("registration spec must be valid UTF-8 JSON") from exc
    return validate_registration_spec(payload)


def validate_registration_spec(payload: Mapping[str, Any]) -> ResearchRunRegistrationSpec:
    if not isinstance(payload, Mapping):
        raise ResearchRunRegistrationError("registration spec must contain a JSON object")
    missing = [field for field in _REQUIRED_SPEC_FIELDS if field not in payload]
    if missing:
        raise ResearchRunRegistrationError(
            "registration spec is missing required field(s): " + ", ".join(missing)
        )
    allowed_fields = set(_REQUIRED_SPEC_FIELDS) | set(_OPTIONAL_SPEC_FIELDS)
    unknown = sorted(set(payload).difference(allowed_fields))
    if unknown:
        raise ResearchRunRegistrationError(
            "registration spec contains unsupported field(s): " + ", ".join(unknown)
        )

    schema_version = _require_text(payload["schema_version"], "schema_version")
    if schema_version != REGISTRATION_SCHEMA_VERSION:
        raise ResearchRunRegistrationError("unsupported registration schema_version")
    run_id = _require_safe_id(payload["run_id"], "run_id")
    strategy_id = _require_text(payload["strategy_id"], "strategy_id")
    strategy_version = _require_text(payload["strategy_version"], "strategy_version")
    test_type = _require_text(payload["test_type"], "test_type")
    instrument_scope = _require_string_sequence(payload["instrument_scope"], "instrument_scope")
    timeframe_scope = _require_string_sequence(payload["timeframe_scope"], "timeframe_scope")
    repo_commit = _require_repo_commit(payload["repo_commit"])
    run_status = _require_text(payload["run_status"], "run_status")
    result_status = _require_text(payload["result_status"], "result_status")
    canonicality_status = _require_text(payload["canonicality_status"], "canonicality_status")
    if run_status not in ALLOWED_RUN_STATUSES:
        raise ResearchRunRegistrationError("unsupported run_status")
    if result_status not in ACCEPTED_RESULT_STATUSES:
        raise ResearchRunRegistrationError("unsupported result_status")
    if canonicality_status not in ALLOWED_CANONICALITY_STATUSES:
        raise ResearchRunRegistrationError("unsupported canonicality_status")
    if (
        result_status == ResultStatus.EVIDENCE_CANONICAL.value
        and canonicality_status != "canonical"
    ):
        raise ResearchRunRegistrationError(
            "evidence_canonical requires canonicality_status=canonical"
        )

    dataset_refs = _require_string_sequence(payload["dataset_refs"], "dataset_refs")
    feature_refs = _require_string_sequence(payload["feature_refs"], "feature_refs")
    label_refs = _require_string_sequence(
        payload["label_refs"],
        "label_refs",
        allow_empty=True,
    )
    parameter_set = _require_mapping(payload["parameter_set"], "parameter_set")
    metrics = _require_mapping(payload["metrics"], "metrics")
    artifacts = _parse_artifact_specs(
        payload["artifacts"],
        default_producer=f"research_run:{run_id}",
    )
    created_ts = _resolve_created_ts(payload.get("created_ts"), run_id)

    if canonicality_status == "canonical":
        if not parameter_set:
            raise ResearchRunRegistrationError(
                "canonical registration requires a non-empty parameter_set"
            )
        if not metrics:
            raise ResearchRunRegistrationError("canonical registration requires metrics")
        canonical_roles = {
            artifact.artifact_role
            for artifact in artifacts
            if artifact.required_for_canonical
        }
        missing_roles = set(REQUIRED_CANONICAL_ARTIFACT_ROLES).difference(canonical_roles)
        if missing_roles:
            raise ResearchRunRegistrationError(
                "canonical registration spec is missing required artifact role(s): "
                + ", ".join(sorted(missing_roles))
            )

    return ResearchRunRegistrationSpec(
        schema_version=schema_version,
        run_id=run_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        test_type=test_type,
        instrument_scope=instrument_scope,
        timeframe_scope=timeframe_scope,
        repo_commit=repo_commit,
        run_status=run_status,
        result_status=result_status,
        canonicality_status=canonicality_status,
        dataset_refs=dataset_refs,
        feature_refs=feature_refs,
        label_refs=label_refs,
        parameter_set=dict(parameter_set),
        metrics=dict(metrics),
        artifacts=artifacts,
        created_ts=created_ts,
    )


def register_existing_research_run(
    *,
    registration_spec_path: Path | str,
    run_dir: Path | str,
    registry_root: Path | str,
    archive_root: Path | str,
) -> ResearchRunRegistrationResult:
    spec = load_registration_spec(registration_spec_path)
    explicit_run_dir = _validate_explicit_directory(run_dir, "run_dir", must_exist=True)
    explicit_registry_root = _validate_explicit_directory(
        registry_root,
        "registry_root",
        must_exist=False,
    )
    explicit_archive_root = _validate_explicit_directory(
        archive_root,
        "archive_root",
        must_exist=False,
    )
    if _is_within(explicit_registry_root, explicit_run_dir):
        raise ResearchRunRegistrationError(
            "registry_root must remain outside the explicit run directory"
        )
    if _is_within(explicit_archive_root, explicit_run_dir):
        raise ResearchRunRegistrationError(
            "archive_root must remain outside the explicit run directory"
        )

    archive = create_deterministic_run_archive(
        run_dir=explicit_run_dir,
        archive_root=explicit_archive_root,
        run_id=spec.run_id,
        repo_commit=spec.repo_commit,
        artifact_filenames=(artifact.filename for artifact in spec.artifacts),
    )
    manifest_items = tuple(
        _manifest_item_from_spec(explicit_run_dir, artifact)
        for artifact in sorted(spec.artifacts, key=lambda item: item.filename)
    )
    archive_item = ArtifactManifestItem(
        artifact_id=f"{spec.run_id}.run_archive",
        artifact_role="run_archive",
        artifact_class="research_run_archive",
        producer="moex_research.assets.run_archive",
        consumer="moex_research.registry",
        format="zip",
        schema_version="run_archive.v1",
        path=archive.archive_path,
        required_for_canonical=False,
        sha256=archive.sha256,
        size_bytes=archive.size_bytes,
    )
    manifest = ArtifactManifest(
        artifact_manifest_id=f"artifact_manifest.{spec.run_id}",
        run_id=spec.run_id,
        schema_version="artifact_manifest.v1",
        created_ts=spec.created_ts,
        producer_component="moex_research.publishers.research_run_registration",
        repo_commit=spec.repo_commit,
        artifacts=manifest_items + (archive_item,),
    )
    entry = ExperimentRegistryEntry(
        registry_entry_id=f"registry.{spec.run_id}",
        run_id=spec.run_id,
        strategy_id=spec.strategy_id,
        strategy_version=spec.strategy_version,
        test_type=spec.test_type,
        instrument_scope=spec.instrument_scope,
        timeframe_scope=spec.timeframe_scope,
        run_status=spec.run_status,
        result_status=spec.result_status,
        canonicality_status=spec.canonicality_status,
        artifact_manifest_ref=manifest.artifact_manifest_id,
        repo_commit=spec.repo_commit,
        created_ts=spec.created_ts,
        metrics=spec.metrics,
        dataset_refs=spec.dataset_refs,
        feature_refs=spec.feature_refs,
        label_refs=spec.label_refs,
        parameter_set=spec.parameter_set,
    )
    validate_persistable_registry_entry(entry, manifest)
    registry_write = FileExperimentRegistryWriter(explicit_registry_root).write(
        entry,
        manifest,
    )
    outputs = (
        archive.archive_path,
        registry_write.manifest_path,
        registry_write.entry_path,
        registry_write.catalog_path,
    )
    return ResearchRunRegistrationResult(
        run_id=spec.run_id,
        artifact_manifest=manifest,
        registry_entry=entry,
        archive=archive,
        registry_write=registry_write,
        undeclared_artifact_policy=UNDECLARED_ARTIFACT_POLICY,
        outputs_created=outputs,
    )


def _parse_artifact_specs(
    raw_artifacts: Any,
    *,
    default_producer: str,
) -> tuple[RegistrationArtifactSpec, ...]:
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise ResearchRunRegistrationError("artifacts must be a non-empty list")
    parsed: list[RegistrationArtifactSpec] = []
    for index, raw in enumerate(raw_artifacts):
        if not isinstance(raw, Mapping):
            raise ResearchRunRegistrationError(
                f"artifacts[{index}] must contain an object"
            )
        missing = [field for field in _REQUIRED_ARTIFACT_FIELDS if field not in raw]
        if missing:
            raise ResearchRunRegistrationError(
                f"artifacts[{index}] is missing required field(s): "
                + ", ".join(missing)
            )
        allowed = set(_REQUIRED_ARTIFACT_FIELDS) | set(_OPTIONAL_ARTIFACT_FIELDS)
        unknown = sorted(set(raw).difference(allowed))
        if unknown:
            raise ResearchRunRegistrationError(
                f"artifacts[{index}] contains unsupported field(s): "
                + ", ".join(unknown)
            )
        required = raw["required_for_canonical"]
        if not isinstance(required, bool):
            raise ResearchRunRegistrationError(
                f"artifacts[{index}].required_for_canonical must be bool"
            )
        parsed.append(
            RegistrationArtifactSpec(
                filename=_validate_relative_filename(raw["filename"]),
                artifact_id=_require_safe_id(
                    raw["artifact_id"],
                    f"artifacts[{index}].artifact_id",
                ),
                artifact_role=_require_text(
                    raw["artifact_role"],
                    f"artifacts[{index}].artifact_role",
                ),
                format=_require_text(
                    raw["format"],
                    f"artifacts[{index}].format",
                ),
                schema_version=str(
                    _require_text_or_integer(
                        raw["schema_version"],
                        f"artifacts[{index}].schema_version",
                    )
                ),
                required_for_canonical=required,
                artifact_class=_require_text(
                    raw.get("artifact_class", "research_run_artifact"),
                    f"artifacts[{index}].artifact_class",
                ),
                producer=_require_text(
                    raw.get("producer", default_producer),
                    f"artifacts[{index}].producer",
                ),
                consumer=_require_text(
                    raw.get("consumer", "moex_research.registry"),
                    f"artifacts[{index}].consumer",
                ),
            )
        )
    filenames = [artifact.filename for artifact in parsed]
    artifact_ids = [artifact.artifact_id for artifact in parsed]
    if len(filenames) != len(set(filenames)):
        raise ResearchRunRegistrationError("artifact filenames must be unique")
    if len(artifact_ids) != len(set(artifact_ids)):
        raise ResearchRunRegistrationError("artifact_id values must be unique")
    return tuple(parsed)


def _manifest_item_from_spec(
    run_dir: Path,
    artifact: RegistrationArtifactSpec,
) -> ArtifactManifestItem:
    path = run_dir / PurePosixPath(artifact.filename)
    if path.is_symlink() or not path.is_file():
        raise ResearchRunRegistrationError(
            f"declared artifact is missing or is not a regular file: {artifact.filename}"
        )
    resolved = path.resolve(strict=True)
    if not _is_within(resolved, run_dir):
        raise ResearchRunRegistrationError(
            f"declared artifact resolves outside run_dir: {artifact.filename}"
        )
    digest, size = _hash_and_size(resolved)
    return ArtifactManifestItem(
        artifact_id=artifact.artifact_id,
        artifact_role=artifact.artifact_role,
        artifact_class=artifact.artifact_class,
        producer=artifact.producer,
        consumer=artifact.consumer,
        format=artifact.format,
        schema_version=artifact.schema_version,
        path=str(resolved),
        required_for_canonical=artifact.required_for_canonical,
        sha256=digest,
        size_bytes=size,
    )


def _validate_explicit_file(path: Path | str, field_name: str) -> Path:
    resolved = _validate_explicit_path(path, field_name)
    if resolved.is_symlink() or not resolved.exists() or not resolved.is_file():
        raise ResearchRunRegistrationError(f"{field_name} must reference an existing regular file")
    return resolved


def _validate_explicit_directory(
    path: Path | str,
    field_name: str,
    *,
    must_exist: bool,
) -> Path:
    resolved = _validate_explicit_path(path, field_name)
    if resolved.is_symlink():
        raise ResearchRunRegistrationError(f"{field_name} must not be a symlink")
    if must_exist and (not resolved.exists() or not resolved.is_dir()):
        raise ResearchRunRegistrationError(f"{field_name} must reference an existing directory")
    if resolved.exists() and not resolved.is_dir():
        raise ResearchRunRegistrationError(f"{field_name} must reference a directory")
    return resolved


def _validate_explicit_path(path: Path | str, field_name: str) -> Path:
    raw = os.fspath(path)
    if not isinstance(raw, str) or not raw.strip():
        raise ResearchRunRegistrationError(f"{field_name} must be supplied explicitly")
    if "\x00" in raw or any(character in raw for character in _FORBIDDEN_PATH_CHARACTERS):
        raise ResearchRunRegistrationError(
            f"{field_name} must not contain NUL, glob, or template syntax"
        )
    unresolved = Path(raw).expanduser()
    if unresolved.is_symlink():
        raise ResearchRunRegistrationError(f"{field_name} must not be a symlink")
    lowered_parts = {part.casefold() for part in unresolved.parts}
    if lowered_parts & _FORBIDDEN_DYNAMIC_PARTS:
        raise ResearchRunRegistrationError(
            f"{field_name} must not use latest/current/autodetect aliases"
        )
    return unresolved.resolve(strict=False)


def _validate_relative_filename(value: Any) -> str:
    filename = _require_text(value, "artifact filename")
    if "\\" in filename or "\x00" in filename:
        raise ResearchRunRegistrationError(
            "artifact filename must use normalized POSIX separators"
        )
    if any(character in filename for character in _FORBIDDEN_PATH_CHARACTERS):
        raise ResearchRunRegistrationError(
            "artifact filename must not contain glob or template syntax"
        )
    path = PurePosixPath(filename)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ResearchRunRegistrationError(
            "artifact filename must be an explicit relative path"
        )
    if any(part.casefold() in _FORBIDDEN_DYNAMIC_PARTS for part in path.parts):
        raise ResearchRunRegistrationError(
            "artifact filename must not use latest/current/autodetect aliases"
        )
    return path.as_posix()


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchRunRegistrationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_text_or_integer(value: Any, field_name: str) -> str | int:
    if isinstance(value, bool):
        raise ResearchRunRegistrationError(f"{field_name} must be text or integer")
    if isinstance(value, int):
        return value
    return _require_text(value, field_name)


def _require_safe_id(value: Any, field_name: str) -> str:
    text = _require_text(value, field_name)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", text):
        raise ResearchRunRegistrationError(f"{field_name} contains unsafe characters")
    return text


def _require_repo_commit(value: Any) -> str:
    text = _require_text(value, "repo_commit")
    if not _COMMIT_RE.fullmatch(text):
        raise ResearchRunRegistrationError(
            "repo_commit must be an explicit 40-character hexadecimal SHA"
        )
    return text.lower()


def _require_string_sequence(
    value: Any,
    field_name: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ResearchRunRegistrationError(f"{field_name} must be a JSON array")
    items = tuple(_require_text(item, field_name) for item in value)
    if not items and not allow_empty:
        raise ResearchRunRegistrationError(f"{field_name} must be non-empty")
    if len(items) != len(set(items)):
        raise ResearchRunRegistrationError(f"{field_name} must be duplicate-free")
    return items


def _require_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ResearchRunRegistrationError(f"{field_name} must contain a JSON object")
    try:
        json.dumps(value, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ResearchRunRegistrationError(
            f"{field_name} must contain deterministic JSON values"
        ) from exc
    return value


def _resolve_created_ts(value: Any, run_id: str) -> str:
    if value is not None:
        return _require_text(value, "created_ts")
    match = _RUN_DATE_RE.search(run_id)
    if match:
        try:
            date = datetime.strptime(match.group(1), "%Y%m%d")
        except ValueError:
            pass
        else:
            return date.strftime("%Y-%m-%dT00:00:00Z")
    return "1970-01-01T00:00:00Z"


def _hash_and_size(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


__all__ = [
    "REGISTRATION_SCHEMA_VERSION",
    "ResearchRunRegistrationError",
    "ResearchRunRegistrationResult",
    "ResearchRunRegistrationSpec",
    "UNDECLARED_ARTIFACT_POLICY",
    "load_registration_spec",
    "register_existing_research_run",
    "validate_registration_spec",
]
