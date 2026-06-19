import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from moex_research.assets.run_archive import RunArchiveError
from moex_research.publishers.research_run_registration import (
    ResearchRunRegistrationError,
    register_existing_research_run,
)
from moex_research.runners.register_existing_research_run import main


REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = (
    REPO_ROOT
    / "docs"
    / "sot"
    / "research"
    / "registration_specs"
    / "m2_1_usdrubf_ema_d1_baseline_20260618.json"
)


def _materialize_declared_run(run_dir: Path, spec_path: Path = SPEC_PATH) -> dict:
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    run_dir.mkdir(parents=True)
    for index, artifact in enumerate(payload["artifacts"], start=1):
        target = run_dir / artifact["filename"]
        target.parent.mkdir(parents=True, exist_ok=True)
        if artifact["format"] == "json":
            target.write_text(
                json.dumps(
                    {
                        "run_id": payload["run_id"],
                        "artifact_id": artifact["artifact_id"],
                        "fixture_row": index,
                    },
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        else:
            target.write_text("row,value\n1,%d\n" % index, encoding="utf-8")
    return payload


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_end_to_end_registration_persists_verified_manifest_entry_catalog_and_archive(tmp_path):
    run_dir = tmp_path / "declared_run"
    payload = _materialize_declared_run(run_dir)
    registry_root = tmp_path / "registry"
    archive_root = tmp_path / "archives"

    result = register_existing_research_run(
        registration_spec_path=SPEC_PATH,
        run_dir=run_dir,
        registry_root=registry_root,
        archive_root=archive_root,
    )

    assert result.run_id == payload["run_id"]
    assert result.registry_write.persisted is True
    assert result.registry_write.idempotent is False
    assert result.archive.created is True
    assert result.archive.idempotent is False
    assert result.undeclared_artifact_policy == "reject"

    archive_path = Path(result.archive.archive_path)
    manifest_path = Path(result.registry_write.manifest_path)
    entry_path = Path(result.registry_write.entry_path)
    catalog_path = Path(result.registry_write.catalog_path)
    assert set(map(Path, result.outputs_created)) == {
        archive_path,
        manifest_path,
        entry_path,
        catalog_path,
    }
    assert archive_path.parent == archive_root.resolve()
    assert manifest_path.parent == (registry_root / "manifests").resolve()
    assert entry_path.parent == (registry_root / "entries").resolve()
    assert catalog_path == (registry_root / "catalog.json").resolve()
    assert all(not Path(path).is_relative_to(run_dir.resolve()) for path in result.outputs_created)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = json.loads(entry_path.read_text(encoding="utf-8"))
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    assert entry["artifact_manifest_ref"] == manifest["artifact_manifest_id"]
    assert entry["dataset_refs"] == payload["dataset_refs"]
    assert entry["feature_refs"] == payload["feature_refs"]
    assert entry["label_refs"] == payload["label_refs"]
    assert entry["parameter_set"] == payload["parameter_set"]
    assert entry["metrics"] == payload["metrics"]
    assert catalog["entries"] == sorted(
        catalog["entries"], key=lambda item: item["registry_entry_id"]
    )
    assert len(catalog["entries"]) == 1

    by_role = {artifact["artifact_role"]: artifact for artifact in manifest["artifacts"]}
    for required_role in ("run_metadata", "metrics", "primary_result"):
        item = by_role[required_role]
        source = Path(item["path"])
        assert item["required_for_canonical"] is True
        assert item["sha256"] == _sha256(source)
        assert item["size_bytes"] == source.stat().st_size > 0
    archive_item = by_role["run_archive"]
    assert archive_item["sha256"] == _sha256(archive_path) == result.archive.sha256
    assert archive_item["size_bytes"] == archive_path.stat().st_size > 0

    declared = sorted(artifact["filename"] for artifact in payload["artifacts"])
    with zipfile.ZipFile(archive_path) as archive:
        assert archive.namelist() == declared
        assert "artifact_manifest.json" not in archive.namelist()
        assert "experiment_registry_entry.json" not in archive.namelist()


def test_identical_end_to_end_replay_is_idempotent_and_cli_returns_zero(tmp_path):
    run_dir = tmp_path / "declared_run"
    _materialize_declared_run(run_dir)
    registry_root = tmp_path / "registry"
    archive_root = tmp_path / "archives"
    kwargs = {
        "registration_spec_path": SPEC_PATH,
        "run_dir": run_dir,
        "registry_root": registry_root,
        "archive_root": archive_root,
    }

    first = register_existing_research_run(**kwargs)
    replay = register_existing_research_run(**kwargs)

    assert first.archive.sha256 == replay.archive.sha256
    assert replay.archive.idempotent is True
    assert replay.registry_write.idempotent is True
    assert main(
        [
            "--registration-spec-path",
            str(SPEC_PATH),
            "--run-dir",
            str(run_dir),
            "--registry-root",
            str(registry_root),
            "--archive-root",
            str(archive_root),
        ]
    ) == 0


def test_registration_rejects_missing_and_undeclared_artifacts(tmp_path):
    run_dir = tmp_path / "declared_run"
    payload = _materialize_declared_run(run_dir)
    (run_dir / payload["artifacts"][0]["filename"]).unlink()

    with pytest.raises(RunArchiveError, match="missing declared artifact"):
        register_existing_research_run(
            registration_spec_path=SPEC_PATH,
            run_dir=run_dir,
            registry_root=tmp_path / "registry_a",
            archive_root=tmp_path / "archives_a",
        )

    run_dir = tmp_path / "declared_run_with_extra"
    _materialize_declared_run(run_dir)
    (run_dir / "undeclared.txt").write_text("extra\n", encoding="utf-8")
    with pytest.raises(RunArchiveError, match="undeclared artifact"):
        register_existing_research_run(
            registration_spec_path=SPEC_PATH,
            run_dir=run_dir,
            registry_root=tmp_path / "registry_b",
            archive_root=tmp_path / "archives_b",
        )


def test_registration_rejects_symlinked_spec_and_cli_rejects_dynamic_paths(tmp_path):
    spec_link = tmp_path / "registration.json"
    try:
        spec_link.symlink_to(SPEC_PATH)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")

    run_dir = tmp_path / "declared_run"
    _materialize_declared_run(run_dir)
    with pytest.raises(ResearchRunRegistrationError, match="symlink"):
        register_existing_research_run(
            registration_spec_path=spec_link,
            run_dir=run_dir,
            registry_root=tmp_path / "registry",
            archive_root=tmp_path / "archives",
        )

    with pytest.raises(SystemExit):
        main(
            [
                "--registration-spec-path",
                str(SPEC_PATH),
                "--run-dir",
                str(run_dir),
                "--registry-root",
                str(tmp_path / "latest" / "registry"),
                "--archive-root",
                str(tmp_path / "archives"),
            ]
        )
