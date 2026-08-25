from __future__ import annotations

import json
from pathlib import Path

from moex_data.futures.stage2_raw_history_content_reattestation_durability import durable_replace_json, fsync_generation


def test_durable_replace_json_replaces_marker_and_cleans_stage(tmp_path: Path) -> None:
    marker = tmp_path / "state" / "current_batch.json"
    marker.parent.mkdir(parents=True)
    marker.write_text('{"generation_id":"old"}\n', encoding="utf-8")
    digest = durable_replace_json(marker, {"generation_id": "new", "status": "accepted"})
    assert len(digest) == 64
    assert json.loads(marker.read_text(encoding="utf-8"))["generation_id"] == "new"
    assert not list(marker.parent.glob("*.stage"))


def test_fsync_generation_accepts_regular_generation_tree(tmp_path: Path) -> None:
    generation = tmp_path / "generation_id=fixture"
    child = generation / "raw" / "part.parquet"
    child.parent.mkdir(parents=True)
    child.write_bytes(b"fixture")
    manifest = generation / "manifests" / "accepted_manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}\n", encoding="utf-8")
    fsync_generation(generation)
