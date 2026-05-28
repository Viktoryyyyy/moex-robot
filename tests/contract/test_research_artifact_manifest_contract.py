import pytest

from moex_research.publishers.artifact_manifest import build_artifact_manifest, validate_publishable_artifact_manifest
from moex_research.registry.schemas import (
    REQUIRED_ARTIFACT_MANIFEST_FIELDS,
    REQUIRED_ARTIFACT_MANIFEST_ITEM_FIELDS,
    ArtifactManifest,
    ArtifactManifestItem,
    RegistryValidationError,
)
from moex_research.registry.validation import validate_artifact_manifest


def _item_kwargs(**overrides):
    data = {
        "artifact_id": "run_metadata_json",
        "artifact_role": "run_metadata",
        "artifact_class": "metadata_table",
        "producer": "moex_research.runners",
        "consumer": "moex_research.registry",
        "format": "json",
        "schema_version": "research_artifact.v1",
        "path": "artifacts/research/run_001/run_metadata.json",
        "required_for_canonical": True,
    }
    data.update(overrides)
    return data


def _manifest_kwargs(**overrides):
    data = {
        "artifact_manifest_id": "artifact_manifest.run_001",
        "run_id": "run_001",
        "schema_version": "artifact_manifest.v1",
        "created_ts": "2026-05-28T00:00:00Z",
        "producer_component": "moex_research.publishers.artifact_manifest",
        "repo_commit": "abc123",
        "artifacts": (ArtifactManifestItem(**_item_kwargs()),),
    }
    data.update(overrides)
    return data


def test_artifact_manifest_declares_required_fields():
    manifest = ArtifactManifest(**_manifest_kwargs())

    assert REQUIRED_ARTIFACT_MANIFEST_FIELDS == (
        "artifact_manifest_id",
        "run_id",
        "schema_version",
        "created_ts",
        "producer_component",
        "repo_commit",
        "artifacts",
    )
    for field_name in REQUIRED_ARTIFACT_MANIFEST_FIELDS:
        assert hasattr(manifest, field_name)
    assert validate_artifact_manifest(manifest) is manifest


def test_artifact_manifest_item_declares_required_fields():
    item = ArtifactManifestItem(**_item_kwargs())

    assert REQUIRED_ARTIFACT_MANIFEST_ITEM_FIELDS == (
        "artifact_id",
        "artifact_role",
        "artifact_class",
        "producer",
        "consumer",
        "format",
        "schema_version",
        "path",
        "required_for_canonical",
    )
    for field_name in REQUIRED_ARTIFACT_MANIFEST_ITEM_FIELDS:
        assert hasattr(item, field_name)


@pytest.mark.parametrize("field_name", REQUIRED_ARTIFACT_MANIFEST_FIELDS)
def test_artifact_manifest_rejects_missing_required_fields(field_name):
    kwargs = _manifest_kwargs()
    kwargs.pop(field_name)

    with pytest.raises(TypeError):
        ArtifactManifest(**kwargs)


@pytest.mark.parametrize("field_name", REQUIRED_ARTIFACT_MANIFEST_ITEM_FIELDS)
def test_artifact_manifest_item_rejects_missing_required_fields(field_name):
    kwargs = _item_kwargs()
    kwargs.pop(field_name)

    with pytest.raises(TypeError):
        ArtifactManifestItem(**kwargs)


def test_artifact_manifest_rejects_empty_artifacts():
    with pytest.raises(RegistryValidationError):
        ArtifactManifest(**_manifest_kwargs(artifacts=()))


@pytest.mark.parametrize(
    "bad_path",
    (
        "stdout",
        "artifacts/research/latest/metrics.json",
        "artifacts/research/run_001/*.json",
        "artifacts/research/run_001/",
    ),
)
def test_artifact_manifest_item_requires_explicit_non_guessed_path(bad_path):
    with pytest.raises(RegistryValidationError):
        ArtifactManifestItem(**_item_kwargs(path=bad_path))


def test_artifact_manifest_item_rejects_stdout_only_format():
    with pytest.raises(RegistryValidationError):
        ArtifactManifestItem(**_item_kwargs(format="stdout"))


def test_artifact_manifest_publisher_builds_valid_manifest():
    item = ArtifactManifestItem(**_item_kwargs())
    manifest = build_artifact_manifest(
        artifact_manifest_id="artifact_manifest.run_001",
        run_id="run_001",
        schema_version="artifact_manifest.v1",
        created_ts="2026-05-28T00:00:00Z",
        producer_component="moex_research.publishers.artifact_manifest",
        repo_commit="abc123",
        artifacts=(item,),
    )

    assert validate_publishable_artifact_manifest(manifest) is manifest
