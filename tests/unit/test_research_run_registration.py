import json
import zipfile
from pathlib import Path

import pytest

from moex_research.assets.run_archive import RunArchiveError, create_deterministic_run_archive
from moex_research.registry.dry_write import DryExperimentRegistryWriter
from moex_research.registry.file_write import FileExperimentRegistryWriter
from moex_research.registry.schemas import (
    ACCEPTED_RESULT_STATUSES,
    ArtifactManifest,
    ArtifactManifestItem,
    ExperimentRegistryEntry,
    RegistryValidationError,
)
from moex_research.registry.validation import (
    validate_persistable_registry_entry,
    validate_registry_entry_against_manifest,
)


COMMIT = "a" * 40


def _artifact(
    role: str,
    *,
    artifact_id: str | None = None,
    required: bool = True,
    sha256: str | None = "b" * 64,
    size_bytes: int | None = 10,
) -> ArtifactManifestItem:
    return ArtifactManifestItem(
        artifact_id=artifact_id or role,
        artifact_role=role,
        artifact_class="research_result",
        producer="tests",
        consumer="moex_research.registry",
        format="json",
        schema_version="1",
        path=f"artifacts/run/{artifact_id or role}.json",
        required_for_canonical=required,
        sha256=sha256,
        size_bytes=size_bytes,
    )


def _pair(
    run_id: str = "run_001",
    *,
    result_status: str = "evidence_canonical",
    canonicality_status: str = "canonical",
    metrics: dict | None = None,
) -> tuple[ExperimentRegistryEntry, ArtifactManifest]:
    manifest = ArtifactManifest(
        artifact_manifest_id=f"artifact_manifest.{run_id}",
        run_id=run_id,
        schema_version="artifact_manifest.v1",
        created_ts="2026-06-19T00:00:00Z",
        producer_component="tests",
        repo_commit=COMMIT,
        artifacts=tuple(
            _artifact(role, artifact_id=f"{run_id}.{role}")
            for role in ("run_metadata", "metrics", "primary_result")
        ),
    )
    entry = ExperimentRegistryEntry(
        registry_entry_id=f"registry.{run_id}",
        run_id=run_id,
        strategy_id="ema_3_19_ai",
        strategy_version="research.v1",
        test_type="unit",
        instrument_scope=("USDRUBF",),
        timeframe_scope=("D1",),
        run_status="executed",
        result_status=result_status,
        canonicality_status=canonicality_status,
        artifact_manifest_ref=manifest.artifact_manifest_id,
        repo_commit=COMMIT,
        created_ts="2026-06-19T00:00:00Z",
        metrics=metrics or {"rows": 1},
        dataset_refs=("dataset.v1",),
        feature_refs=("feature.v1",),
        label_refs=("label.v1",),
        parameter_set={"ema_fast": 3, "ema_slow": 19},
    )
    return entry, manifest


def test_existing_dry_writer_remains_non_persistent():
    entry = ExperimentRegistryEntry(
        registry_entry_id="registry.legacy",
        run_id="legacy",
        strategy_id="legacy",
        strategy_version="1",
        test_type="dry",
        instrument_scope=("USDRUBF",),
        timeframe_scope=("D1",),
        run_status="executed",
        result_status="blocked",
        canonicality_status="non_canonical",
        artifact_manifest_ref="artifact_manifest.legacy",
        repo_commit="abc123",
        created_ts="2026-01-01T00:00:00Z",
    )
    manifest = ArtifactManifest(
        artifact_manifest_id="artifact_manifest.legacy",
        run_id="legacy",
        schema_version="artifact_manifest.v1",
        created_ts="2026-01-01T00:00:00Z",
        producer_component="tests",
        repo_commit="abc123",
        artifacts=(_artifact("run_metadata", sha256=None, size_bytes=None),),
    )

    result = DryExperimentRegistryWriter().write(entry, manifest)

    assert result.persisted is False
    assert validate_registry_entry_against_manifest(entry, manifest) is entry


def test_new_result_status_values_are_accepted():
    assert {"evidence_canonical", "evidence_provisional"} <= ACCEPTED_RESULT_STATUSES
    entry, _ = _pair(result_status="evidence_provisional", canonicality_status="provisional")
    assert entry.result_status == "evidence_provisional"


def test_evidence_canonical_requires_canonicality_status():
    entry, manifest = _pair(
        result_status="evidence_canonical",
        canonicality_status="provisional",
    )

    with pytest.raises(RegistryValidationError, match="canonicality_status=canonical"):
        validate_registry_entry_against_manifest(entry, manifest)


@pytest.mark.parametrize(
    "entry_change,artifact_change,error",
    [
        ({"dataset_refs": ()}, None, "dataset_refs"),
        ({"feature_refs": ()}, None, "feature_refs"),
        ({"parameter_set": {}}, None, "parameter_set"),
        ({"metrics": {}}, None, "metrics"),
        (None, {"sha256": None}, "sha256"),
        (None, {"size_bytes": 0}, "positive size_bytes"),
    ],
)
def test_persistable_canonical_registration_requires_lineage_and_integrity(
    entry_change,
    artifact_change,
    error,
):
    entry, manifest = _pair()
    if entry_change:
        values = entry.__dict__ | entry_change
        entry = ExperimentRegistryEntry(**values)
    if artifact_change:
        changed = manifest.artifacts[0]
        values = changed.__dict__ | artifact_change
        replacement = ArtifactManifestItem(**values)
        manifest = ArtifactManifest(
            **(manifest.__dict__ | {"artifacts": (replacement,) + manifest.artifacts[1:]})
        )

    with pytest.raises(RegistryValidationError, match=error):
        validate_persistable_registry_entry(entry, manifest)


def test_file_writer_is_atomic_idempotent_append_only_and_catalog_sorted(tmp_path):
    writer = FileExperimentRegistryWriter(tmp_path / "registry")
    second_entry, second_manifest = _pair("run_b")
    first_entry, first_manifest = _pair("run_a")

    second_result = writer.write(second_entry, second_manifest)
    first_result = writer.write(first_entry, first_manifest)
    replay = writer.write(first_entry, first_manifest)

    assert second_result.persisted is True
    assert first_result.persisted is True
    assert replay.persisted is True
    assert replay.idempotent is True
    catalog = json.loads((tmp_path / "registry" / "catalog.json").read_text())
    assert [item["registry_entry_id"] for item in catalog["entries"]] == [
        "registry.run_a",
        "registry.run_b",
    ]
    assert len(catalog["entries"]) == 2
    assert not list((tmp_path / "registry").rglob("*.tmp"))


def test_file_writer_rejects_same_id_with_different_content(tmp_path):
    writer = FileExperimentRegistryWriter(tmp_path / "registry")
    entry, manifest = _pair()
    writer.write(entry, manifest)
    conflicting = ExperimentRegistryEntry(
        **(entry.__dict__ | {"metrics": {"rows": 2}})
    )

    with pytest.raises(RegistryValidationError, match="collision"):
        writer.write(conflicting, manifest)


def test_conflicting_entry_preflight_does_not_leave_orphan_manifest(tmp_path):
    writer = FileExperimentRegistryWriter(tmp_path / "registry")
    entry, manifest = _pair("run_original")
    writer.write(entry, manifest)

    conflicting_entry, conflicting_manifest = _pair("run_new_manifest")
    conflicting_entry = ExperimentRegistryEntry(
        **(
            conflicting_entry.__dict__
            | {
                "registry_entry_id": entry.registry_entry_id,
                "artifact_manifest_ref": conflicting_manifest.artifact_manifest_id,
            }
        )
    )
    orphan_path = (
        tmp_path
        / "registry"
        / "manifests"
        / f"{conflicting_manifest.artifact_manifest_id}.json"
    )

    with pytest.raises(RegistryValidationError, match="collision"):
        writer.write(conflicting_entry, conflicting_manifest)

    assert not orphan_path.exists()


def test_dynamic_registry_root_is_rejected(tmp_path):
    with pytest.raises(RegistryValidationError, match="latest/current/autodetect"):
        FileExperimentRegistryWriter(tmp_path / "latest" / "registry")


def test_deterministic_archive_bytes_hash_order_and_metadata(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "b.csv").write_bytes(b"b\n")
    (run_dir / "a.json").write_bytes(b'{"a":1}\n')

    first = create_deterministic_run_archive(
        run_dir=run_dir,
        archive_root=tmp_path / "archive_a",
        run_id="run_001",
        repo_commit=COMMIT,
        artifact_filenames=("b.csv", "a.json"),
    )
    second = create_deterministic_run_archive(
        run_dir=run_dir,
        archive_root=tmp_path / "archive_b",
        run_id="run_001",
        repo_commit=COMMIT,
        artifact_filenames=("a.json", "b.csv"),
    )

    first_bytes = Path(first.archive_path).read_bytes()
    second_bytes = Path(second.archive_path).read_bytes()
    assert first_bytes == second_bytes
    assert first.sha256 == second.sha256
    assert first.size_bytes == len(first_bytes)
    assert first.archived_files == ("a.json", "b.csv")
    assert "run_001" in Path(first.archive_path).name
    assert COMMIT in Path(first.archive_path).name
    with zipfile.ZipFile(first.archive_path) as archive:
        assert archive.namelist() == ["a.json", "b.csv"]
        for info in archive.infolist():
            assert info.date_time == (1980, 1, 1, 0, 0, 0)
            assert (info.external_attr >> 16) & 0o777 == 0o644


def test_archive_replay_is_idempotent(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "result.json").write_text("{}\n", encoding="utf-8")
    kwargs = {
        "run_dir": run_dir,
        "archive_root": tmp_path / "archive",
        "run_id": "run_001",
        "repo_commit": COMMIT,
        "artifact_filenames": ("result.json",),
    }

    first = create_deterministic_run_archive(**kwargs)
    second = create_deterministic_run_archive(**kwargs)

    assert first.created is True
    assert second.created is False
    assert second.idempotent is True
    assert first.sha256 == second.sha256


@pytest.mark.parametrize(
    "declared,extra,error",
    [
        (("missing.json",), None, "missing declared"),
        (("result.json",), "extra.json", "undeclared artifact"),
        (("../result.json",), None, "explicit relative path"),
    ],
)
def test_archive_rejects_missing_extra_and_traversal(tmp_path, declared, extra, error):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "result.json").write_text("{}\n", encoding="utf-8")
    if extra:
        (run_dir / extra).write_text("{}\n", encoding="utf-8")

    with pytest.raises(RunArchiveError, match=error):
        create_deterministic_run_archive(
            run_dir=run_dir,
            archive_root=tmp_path / "archive",
            run_id="run_001",
            repo_commit=COMMIT,
            artifact_filenames=declared,
        )


def test_archive_rejects_symlink(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}\n", encoding="utf-8")
    link = run_dir / "result.json"
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")

    with pytest.raises(RunArchiveError, match="symlink"):
        create_deterministic_run_archive(
            run_dir=run_dir,
            archive_root=tmp_path / "archive",
            run_id="run_001",
            repo_commit=COMMIT,
            artifact_filenames=("result.json",),
        )
