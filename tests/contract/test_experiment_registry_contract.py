import pytest

from moex_research.registry.schemas import (
    ALLOWED_CANONICALITY_STATUSES,
    ALLOWED_RESULT_STATUSES,
    ALLOWED_RUN_STATUSES,
    REQUIRED_CANONICAL_ARTIFACT_ROLES,
    REQUIRED_EXPERIMENT_REGISTRY_FIELDS,
    ArtifactManifest,
    ArtifactManifestItem,
    ExperimentRegistryEntry,
    RegistryValidationError,
)
from moex_research.registry.validation import validate_experiment_registry_entry, validate_registry_entry_against_manifest


def _entry_kwargs(**overrides):
    data = {
        "registry_entry_id": "registry.run_001",
        "run_id": "run_001",
        "strategy_id": "d1_tsmom",
        "strategy_version": "0.1.0",
        "test_type": "canonical_backtest",
        "instrument_scope": ("Si", "USDRUBF"),
        "timeframe_scope": ("D1",),
        "run_status": "executed",
        "result_status": "supported_provisional",
        "canonicality_status": "provisional",
        "artifact_manifest_ref": "artifact_manifest.run_001",
        "repo_commit": "abc123",
        "created_ts": "2026-05-28T00:00:00Z",
        "metrics": {"mean_return": 0.001},
    }
    data.update(overrides)
    return data


def _artifact(role, required_for_canonical=True):
    return ArtifactManifestItem(
        artifact_id=f"{role}_artifact",
        artifact_role=role,
        artifact_class="research_result",
        producer="moex_research.runners",
        consumer="moex_research.registry",
        format="json",
        schema_version="research_artifact.v1",
        path=f"artifacts/research/run_001/{role}.json",
        required_for_canonical=required_for_canonical,
    )


def _manifest(artifacts=None):
    return ArtifactManifest(
        artifact_manifest_id="artifact_manifest.run_001",
        run_id="run_001",
        schema_version="artifact_manifest.v1",
        created_ts="2026-05-28T00:00:00Z",
        producer_component="moex_research.publishers.artifact_manifest",
        repo_commit="abc123",
        artifacts=tuple(artifacts or (_artifact("run_metadata"),)),
    )


def test_experiment_registry_entry_declares_required_fields():
    entry = ExperimentRegistryEntry(**_entry_kwargs())

    assert REQUIRED_EXPERIMENT_REGISTRY_FIELDS == (
        "registry_entry_id",
        "run_id",
        "strategy_id",
        "strategy_version",
        "test_type",
        "instrument_scope",
        "timeframe_scope",
        "run_status",
        "result_status",
        "canonicality_status",
        "artifact_manifest_ref",
        "repo_commit",
        "created_ts",
    )
    for field_name in REQUIRED_EXPERIMENT_REGISTRY_FIELDS:
        assert hasattr(entry, field_name)
    assert validate_experiment_registry_entry(entry) is entry


@pytest.mark.parametrize("field_name", REQUIRED_EXPERIMENT_REGISTRY_FIELDS)
def test_experiment_registry_entry_rejects_missing_required_fields(field_name):
    kwargs = _entry_kwargs()
    kwargs.pop(field_name)

    with pytest.raises(TypeError):
        ExperimentRegistryEntry(**kwargs)


def test_registry_status_sets_are_explicit():
    assert ALLOWED_RUN_STATUSES == frozenset({"planned", "executed", "blocked", "failed", "invalidated", "superseded"})
    assert ALLOWED_RESULT_STATUSES == frozenset(
        {
            "supported_canonical",
            "not_supported_canonical",
            "supported_provisional",
            "not_supported_provisional",
            "blocked",
            "invalidated",
        }
    )
    assert ALLOWED_CANONICALITY_STATUSES == frozenset({"canonical", "provisional", "non_canonical", "blocked"})


@pytest.mark.parametrize(
    "field_name,bad_value",
    (
        ("run_status", "done"),
        ("result_status", "supported"),
        ("canonicality_status", "final"),
    ),
)
def test_experiment_registry_entry_rejects_unsupported_statuses(field_name, bad_value):
    with pytest.raises(RegistryValidationError):
        ExperimentRegistryEntry(**_entry_kwargs(**{field_name: bad_value}))


def test_registry_entry_requires_artifact_manifest_ref():
    with pytest.raises(RegistryValidationError):
        ExperimentRegistryEntry(**_entry_kwargs(artifact_manifest_ref=""))


def test_registry_rejects_canonical_result_without_required_canonical_artifacts():
    entry = ExperimentRegistryEntry(**_entry_kwargs(result_status="supported_canonical", canonicality_status="canonical"))
    manifest = _manifest(artifacts=(_artifact("run_metadata"),))

    with pytest.raises(RegistryValidationError):
        validate_registry_entry_against_manifest(entry, manifest)


def test_registry_accepts_canonical_result_with_required_canonical_artifacts():
    entry = ExperimentRegistryEntry(**_entry_kwargs(result_status="supported_canonical", canonicality_status="canonical"))
    manifest = _manifest(artifacts=tuple(_artifact(role) for role in REQUIRED_CANONICAL_ARTIFACT_ROLES))

    assert validate_registry_entry_against_manifest(entry, manifest) is entry


def test_registry_rejects_promotion_verdict_inside_metrics():
    with pytest.raises(RegistryValidationError):
        ExperimentRegistryEntry(**_entry_kwargs(metrics={"promotion_verdict": "promote"}))


def test_registry_allows_promotion_verdict_only_as_separate_reference():
    entry = ExperimentRegistryEntry(
        **_entry_kwargs(
            metrics={"mean_return": 0.001},
            promotion_verdict_ref="docs/sot/research_verdicts/run_001.md",
        )
    )

    assert entry.promotion_verdict_ref == "docs/sot/research_verdicts/run_001.md"


def test_registry_entry_manifest_ref_must_match_manifest_id():
    entry = ExperimentRegistryEntry(**_entry_kwargs(artifact_manifest_ref="artifact_manifest.other"))
    manifest = _manifest()

    with pytest.raises(RegistryValidationError):
        validate_registry_entry_against_manifest(entry, manifest)
