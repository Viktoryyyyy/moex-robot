from __future__ import annotations

from .schemas import (
    CANONICAL_RESULT_STATUSES,
    REQUIRED_CANONICAL_ARTIFACT_ROLES,
    ArtifactManifest,
    ExperimentRegistryEntry,
    RegistryValidationError,
    ResultStatus,
)


def validate_artifact_manifest(manifest: ArtifactManifest) -> ArtifactManifest:
    if not isinstance(manifest, ArtifactManifest):
        raise TypeError("manifest must be ArtifactManifest")
    artifact_ids = [item.artifact_id for item in manifest.artifacts]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise RegistryValidationError("artifact manifest contains duplicate artifact_id values")
    return manifest


def validate_experiment_registry_entry(entry: ExperimentRegistryEntry) -> ExperimentRegistryEntry:
    if not isinstance(entry, ExperimentRegistryEntry):
        raise TypeError("entry must be ExperimentRegistryEntry")
    if not entry.artifact_manifest_ref:
        raise RegistryValidationError("artifact_manifest_ref is required")
    if (
        entry.result_status == ResultStatus.EVIDENCE_CANONICAL.value
        and entry.canonicality_status != "canonical"
    ):
        raise RegistryValidationError("evidence_canonical requires canonicality_status=canonical")
    return entry


def validate_registry_entry_against_manifest(
    entry: ExperimentRegistryEntry,
    manifest: ArtifactManifest,
) -> ExperimentRegistryEntry:
    validate_experiment_registry_entry(entry)
    validate_artifact_manifest(manifest)

    if entry.artifact_manifest_ref != manifest.artifact_manifest_id:
        raise RegistryValidationError("artifact_manifest_ref must match artifact_manifest_id")
    if entry.run_id != manifest.run_id:
        raise RegistryValidationError("registry entry and artifact manifest must use the same run_id")
    if entry.repo_commit != manifest.repo_commit:
        raise RegistryValidationError("registry entry and artifact manifest must use the same repo_commit")

    if entry.result_status in CANONICAL_RESULT_STATUSES or entry.canonicality_status == "canonical":
        canonical_roles = {item.artifact_role for item in manifest.artifacts if item.required_for_canonical}
        missing_roles = set(REQUIRED_CANONICAL_ARTIFACT_ROLES).difference(canonical_roles)
        if missing_roles:
            raise RegistryValidationError("canonical result is missing required canonical artifacts")
    return entry


def validate_persistable_registry_entry(
    entry: ExperimentRegistryEntry,
    manifest: ArtifactManifest,
) -> ExperimentRegistryEntry:
    """Apply the stricter invariants required by the file-backed canonical registry."""

    validate_registry_entry_against_manifest(entry, manifest)
    if entry.canonicality_status != "canonical":
        return entry

    if not entry.dataset_refs:
        raise RegistryValidationError("canonical persisted registration requires dataset_refs")
    if not entry.feature_refs:
        raise RegistryValidationError("canonical persisted registration requires feature_refs")
    if not entry.parameter_set:
        raise RegistryValidationError("canonical persisted registration requires an explicit parameter_set")
    if not entry.metrics:
        raise RegistryValidationError("canonical persisted registration requires metrics")

    required_items = [item for item in manifest.artifacts if item.required_for_canonical]
    for item in required_items:
        if item.sha256 is None:
            raise RegistryValidationError(
                f"canonical required artifact {item.artifact_id} requires sha256"
            )
        if item.size_bytes is None or item.size_bytes <= 0:
            raise RegistryValidationError(
                f"canonical required artifact {item.artifact_id} requires positive size_bytes"
            )
    return entry


__all__ = [
    "validate_artifact_manifest",
    "validate_experiment_registry_entry",
    "validate_persistable_registry_entry",
    "validate_registry_entry_against_manifest",
]
