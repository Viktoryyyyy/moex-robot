from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_data.futures import stage2_raw_history_content_reattestation_durability as durability


def test_durable_replace_json_replaces_marker_and_cleans_stage(tmp_path: Path) -> None:
    marker = tmp_path / "state" / "current_batch.json"
    marker.parent.mkdir(parents=True)
    marker.write_text('{"generation_id":"old"}\n', encoding="utf-8")
    digest = durability.durable_replace_json(marker, {"generation_id": "new", "status": "accepted"})
    assert len(digest) == 64
    assert json.loads(marker.read_text(encoding="utf-8"))["generation_id"] == "new"
    assert not list(marker.parent.glob("*.stage"))


def test_fsync_generation_accepts_regular_generation_tree(tmp_path: Path) -> None:
    generation = tmp_path / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=fixture"
    child = generation / "raw" / "part.parquet"
    child.parent.mkdir(parents=True)
    child.write_bytes(b"fixture")
    manifest = generation / "manifests" / "accepted_manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}\n", encoding="utf-8")
    durability.fsync_generation(generation)


def test_fsync_generation_persists_attestation_root_entry_in_parent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    generation = tmp_path / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=fixture"
    artifact = generation / "raw" / "part.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"fixture")
    synced_dirs: list[Path] = []
    monkeypatch.setattr(durability, "fsync_file", lambda path: None)
    monkeypatch.setattr(durability, "fsync_dir", lambda path: synced_dirs.append(Path(path)))

    durability.fsync_generation(generation)

    assert generation.parent in synced_dirs
    assert generation.parent.parent in synced_dirs
    assert synced_dirs.index(generation.parent) < synced_dirs.index(generation.parent.parent)
