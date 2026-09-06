from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from moex_data.futures import futoi_live_factual_refresh_source_native as factual


@pytest.mark.parametrize("suffix", [".parquet", ".json"])
def test_evidence_survives_canonical_overwrite(tmp_path, suffix):
    source = tmp_path / ("source" + suffix)
    source.write_bytes(b"accepted original")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    frozen = factual._freeze_artifact(tmp_path, source, digest)
    assert factual._freeze_artifact(tmp_path, source, digest) == frozen
    source.write_bytes(b"later refresh")
    assert frozen.read_bytes() == b"accepted original"
    assert factual._sha256_file(frozen) == digest


def test_changed_source_is_not_archived(tmp_path):
    source = tmp_path / "raw.parquet"
    source.write_bytes(b"replaced")
    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="SHA mismatch"):
        factual._freeze_artifact(tmp_path, source, hashlib.sha256(b"original").hexdigest())


def test_corrupt_archive_is_not_silently_overwritten(tmp_path):
    source = tmp_path / "raw.parquet"
    source.write_bytes(b"original")
    digest = factual._sha256_file(source)
    frozen = factual._freeze_artifact(tmp_path, source, digest)
    frozen.write_bytes(b"corrupted")
    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="archived FUTOI evidence SHA mismatch"):
        factual._freeze_artifact(tmp_path, source, digest)
    assert frozen.read_bytes() == b"corrupted"


@pytest.mark.parametrize("instrument_id", factual.LIVE_INSTRUMENT_IDS)
def test_materialization_returns_frozen_sources(monkeypatch, tmp_path, instrument_id):
    identity = factual.source_identity(instrument_id)
    day = "2026-09-05"
    run = "scheduled"
    raw_run = run + "_" + instrument_id + "_raw_20260905"
    partition = tmp_path / "part.parquet"
    partition.write_bytes(b"original raw evidence")
    digest = factual._sha256_file(partition)
    quality = tmp_path / "quality.json"
    quality.write_text(json.dumps({"quality_status": "pass", "row_count": 218,
        "instrument_id": instrument_id, "futoi_ticker": identity["source_ticker"],
        "secid": identity["secid"], "run_id": raw_run, "trade_date": day}))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"refresh_status": "succeeded", "publication_run_id": raw_run,
        "instrument_scope": [instrument_id], "published_partition_sha256": digest}))
    monkeypatch.setattr(factual.materializer, "materialize_futoi_partition", lambda **kwargs: {
        "status": "succeeded", "quality_status": "pass", "trade_date": day,
        "instrument_id": instrument_id, "source_id": factual.SOURCE_ID,
        "futoi_ticker": identity["source_ticker"], "secid": identity["secid"],
        "storage_partition_path": str(partition), "quality_report_reference": str(quality),
        "manifest_reference": str(manifest), "published_partition_sha256": digest})
    frozen, provenance = factual._materialize_target(tmp_path, day, run,
        instrument_id=instrument_id, timeout=1)
    for source in (partition, quality, manifest):
        source.write_bytes(b"overwritten by subsequent refresh")
    assert frozen.read_bytes() == b"original raw evidence"
    for kind in ("raw_partition", "raw_quality_report", "raw_refresh_manifest"):
        path = tmp_path / provenance[kind + "_ref"].removeprefix(factual.ROOT_REF_PREFIX)
        assert factual._sha256_file(path) == provenance[kind + "_sha256"]
