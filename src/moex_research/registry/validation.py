from __future__ import annotations

from .schemas import (
    CANONICAL_RESULT_STATUSES,
    REQUIRED_CANONICAL_ARTIFACT_ROLES,
    ArtifactManifest,
    ExperimentRegistryEntry,
    RegistryValidationError,
)


def validate_artifact_manifest(manifest: ArtifactManifest) -> ArtifactManifest:
    if not isinstance(manifest, ArtifactManifest):
        raise TypeError("manifest must be ArtifactManifest")
    return manifest


def validate_experiment_registry_entry(entry: ExperimentRegistryEntry) -> ExperimentRegistryEntry:
    if not isinstance(entry, ExperimentRegistryEntry):
        raise TypeError("entry must be ExperimentRegistryEntry")
    if not entry.artifact_manifest_ref:
        raise RegistryValidationError("artifact_manifest_ref is required")
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
