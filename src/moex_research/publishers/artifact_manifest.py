from __future__ import annotations

from collections.abc import Iterable

from moex_research.registry.schemas import ArtifactManifest, ArtifactManifestItem
from moex_research.registry.validation import validate_artifact_manifest


def build_artifact_manifest(
    *,
    artifact_manifest_id: str,
    run_id: str,
    schema_version: str,
    created_ts: str,
    producer_component: str,
    repo_commit: str,
    artifacts: Iterable[ArtifactManifestItem],
) -> ArtifactManifest:
    return ArtifactManifest(
        artifact_manifest_id=artifact_manifest_id,
        run_id=run_id,
        schema_version=schema_version,
        created_ts=created_ts,
        producer_component=producer_component,
        repo_commit=repo_commit,
        artifacts=tuple(artifacts),
    )


def validate_publishable_artifact_manifest(manifest: ArtifactManifest) -> ArtifactManifest:
    return validate_artifact_manifest(manifest)
