"""Recovery preserves evidence; a new acquisition never rewrites historical PIT."""
import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import algopack_availability_probe as registry
from moex_data.futures import date_source_provenance as provenance
from moex_data.futures import expiration_map_builder as expiry
from moex_data.futures import raw_admission_versions as versions
from test_futures_date_source_provenance import DAYS, INGEST, admit, continuous, make_admitted, sha


def freeze(root, qp, mp):
    return versions.publish_admission(root, qp, pd.read_parquet(qp), mp, json.loads(mp.read_text()))


def tree_hashes(root):
    return {str(p.relative_to(root)): sha(p) for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("old_label,layout", [(provenance.XML, "loader"),
    (provenance.XML, "chunk"), (provenance.OBSERVED, "chunk")])
def test_partial_reload_uses_frozen_full_cohort_and_preserves_originals(tmp_path, old_label, layout):
    paths, qp, mp = make_admitted(tmp_path, old_label, layout=layout)
    original = {p: p.read_bytes() for p in [*paths, qp, mp]}
    freeze(tmp_path, qp, mp)
    # Existing producer reloads only one day: counts are identical, ingest differs.
    newer, nqp, nmp = make_admitted(tmp_path, provenance.OBSERVED, days=[DAYS[1]])
    frame = pd.read_parquet(newer[0])
    frame["ingest_ts"] = "2026-05-21T08:00:30Z"
    versions.publish_partition(tmp_path, newer[0], frame)
    freeze(tmp_path, nqp, nmp)
    raw = admit(tmp_path, paths)
    assert raw.groupby("trade_date")[provenance.STATUS].first().tolist() == [old_label, provenance.OBSERVED]
    five, daily, _ = continuous(raw)
    assert provenance.continuous_seam_blockers(five, daily) == []
    for path, payload in original.items():
        assert versions.read_object(tmp_path, versions.sha256(payload)) == payload
    before = tree_hashes(tmp_path / versions.STORE)
    freeze(tmp_path, nqp, nmp)
    assert tree_hashes(tmp_path / versions.STORE) == before
    pd.testing.assert_frame_equal(admit(tmp_path, paths), raw)


def test_lost_original_cohort_is_not_reconstructed_from_current_parts(tmp_path):
    paths, _, _ = make_admitted(tmp_path, provenance.XML, layout="loader")
    new, qp, mp = make_admitted(tmp_path, days=[DAYS[1]])
    freeze(tmp_path, qp, mp)
    # Raw byte archival alone cannot supply the lost admission relationship.
    with pytest.raises(RuntimeError, match="unverifiable partition"):
        admit(tmp_path, paths)
    assert not admit(tmp_path, new).empty


@pytest.mark.parametrize("layout", ["loader", "chunk"])
def test_current_alias_cannot_admit_raw_different_from_its_recorded_version(tmp_path, layout):
    paths, qp, mp = make_admitted(tmp_path, provenance.XML if layout == "loader" else provenance.OBSERVED, layout=layout)
    freeze(tmp_path, qp, mp)
    raw = pd.read_parquet(paths[0])
    # All count, ingest, timestamps and quality bounds still agree.
    raw["volume"] += 1
    raw.to_parquet(paths[0], index=False)
    with pytest.raises(RuntimeError, match="unverifiable partition"):
        admit(tmp_path, paths)


def test_same_quality_bytes_retain_each_independent_manifest_binding(tmp_path):
    paths, qp, mp = make_admitted(tmp_path)
    q, m = pd.read_parquet(qp), json.loads(mp.read_text())
    first = versions.publish_admission(tmp_path, qp, q, mp, m)
    # A second publication can share quality bytes but cannot shadow its sibling.
    m["finished_at"] = "2026-05-21T08:02:00Z"
    second = versions.publish_admission(tmp_path, qp, q, mp, m)
    qp.unlink()
    mp.unlink()
    raw = admit(tmp_path, paths)
    evidence = json.loads(raw.iloc[0][provenance.EVIDENCE])
    assert {r["manifest_sha256"] for r in evidence} == {
        versions.sha256(versions.encoded(first)), versions.sha256(versions.encoded(second))}
    assert len({r["quality_sha256"] for r in evidence}) == 1


@pytest.mark.parametrize("fault", ["missing_mapping", "null_mapping", "missing_scope"])
def test_partial_version_metadata_does_not_fall_back_to_unpinned_alias(tmp_path, fault):
    paths, qp, mp = make_admitted(tmp_path)
    manifest = freeze(tmp_path, qp, mp)
    if fault == "missing_mapping":
        manifest.pop("raw_partition_versions")
    elif fault == "null_mapping":
        manifest["raw_partition_versions"] = None
    else:
        manifest.pop("admission_version_scope")
    mp.write_text(json.dumps(manifest))
    raw = pd.read_parquet(paths[0])
    raw["volume"] += 1
    raw.to_parquet(paths[0], index=False)
    with pytest.raises(RuntimeError, match="unverifiable partition"):
        admit(tmp_path, paths)


@pytest.mark.parametrize("fault", ["missing_raw", "tampered_raw", "missing_quality", "tampered_manifest", "tampered_receipt"])
def test_missing_or_tampered_immutable_evidence_fails_closed(tmp_path, fault):
    paths, qp, mp = make_admitted(tmp_path)
    m = freeze(tmp_path, qp, mp)
    # Remove current aliases so only the retained version can witness this raw.
    qp.unlink()
    mp.unlink()
    receipt_path = next((tmp_path / versions.STORE / "receipts").glob("*.json"))
    receipt = json.loads(receipt_path.read_text())
    if fault in {"missing_raw", "tampered_raw"}:
        victim = versions.object_path(tmp_path, next(iter(m["raw_partition_versions"].values())))
    elif fault == "missing_quality":
        victim = versions.object_path(tmp_path, receipt["quality_sha256"])
    elif fault == "tampered_manifest":
        victim = versions.object_path(tmp_path, receipt["manifest_sha256"])
    else:
        victim = receipt_path
    if fault.startswith("missing"):
        victim.unlink()
    else:
        victim.write_bytes(b"corrupted")
    with pytest.raises(RuntimeError, match="(raw admission|unverifiable partition)"):
        admit(tmp_path, paths)


@pytest.mark.parametrize("fault", ["rows", "membership", "ingest", "label"])
def test_frozen_receipt_does_not_override_producer_admission_checks(tmp_path, fault):
    paths, qp, mp = make_admitted(tmp_path)
    q, m = pd.read_parquet(qp), json.loads(mp.read_text())
    if fault == "rows":
        q.loc[0, "rows_written"] += 1
    elif fault == "membership":
        m["output_partitions"] = [str(paths[0])]
    elif fault == "ingest":
        m["finished_at"] = "2026-05-21T07:59:59Z"
    else:
        q.loc[0, "calendar_status"] = q.loc[0, "session_calendar_status"] = provenance.XML
    versions.publish_admission(tmp_path, qp, q, mp, m)
    with pytest.raises(RuntimeError, match="unverifiable partition"):
        admit(tmp_path, paths)


def test_failed_current_publication_retains_previous_bytes(tmp_path, monkeypatch):
    path = tmp_path / "futures/raw_5m/example"
    versions.publish_bytes(tmp_path, path, b"original")
    def unavailable(*args):
        raise OSError("publication unavailable")
    monkeypatch.setattr(versions.os, "replace", unavailable)
    with pytest.raises(OSError, match="publication unavailable"):
        versions.publish_bytes(tmp_path, path, b"new")
    assert path.read_bytes() == b"original"
    assert versions.read_object(tmp_path, versions.sha256(b"original")) == b"original"
    assert versions.read_object(tmp_path, versions.sha256(b"new")) == b"new"


def test_archival_rejects_symlink_parent(tmp_path):
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (tmp_path / "futures").symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlink"):
        versions.publish_bytes(tmp_path, tmp_path / "futures/raw_5m/example", b"new")
    assert list(outside.iterdir()) == []


def registry_snapshot(root, day, secid="SiM6", expiration="2026-06-18"):
    payload = pd.DataFrame([{"SECID": secid, "BOARDID": "RFUD", "SHORTNAME": secid,
        "LASTTRADEDATE": expiration, "EXPIRATIONDATE": expiration, "LOTSIZE": 1000}])
    raw = registry.build_registry_snapshot(payload, day)
    normalized = registry.build_normalized_registry(raw)
    directory = root / "futures/registry" / ("snapshot_date=" + day)
    directory.mkdir(parents=True, exist_ok=True)
    rp, np = directory / "futures_registry_snapshot.parquet", directory / "futures_normalized_instrument_registry.parquet"
    raw.to_parquet(rp, index=False)
    normalized.to_parquet(np, index=False)
    return rp, np


def test_expired_required_contract_resolved_without_substitution_or_source_rewrite(tmp_path):
    old = registry_snapshot(tmp_path, "2026-06-18")
    current = registry_snapshot(tmp_path, "2026-10-05", "SiZ6", "2026-12-17")
    before = {p: sha(p) for p in [*old, *current]}
    resolved = expiry.verified_historical_registry(tmp_path, "2026-10-05", ["SiM6", "SiZ6"])
    assert resolved["secid"].tolist() == ["SiM6", "SiZ6"]
    result = expiry.build_expiration_row(resolved.iloc[0], "SiM6", "2026-10-05", "test-run")
    assert result["roll_anchor_date"] == "2026-06-18"
    assert result["expiration_source_snapshot_date"] == "2026-06-18"
    evidence = json.loads(result["expiration_source_evidence_json"])["selected"]
    assert evidence["raw_registry_sha256"] == sha(old[0])
    assert evidence["normalized_sha256"] == sha(old[1])
    assert before == {p: sha(p) for p in before}


@pytest.mark.parametrize("fault", ["payload", "source", "normalized", "duplicate", "first_trade", "missing", "future_only", "conflict"])
def test_historical_expiration_evidence_is_not_silently_selected_around_conflicts(tmp_path, fault):
    day = "2026-10-06" if fault == "future_only" else "2026-06-18"
    rp, np = registry_snapshot(tmp_path, day)
    raw, normalized = pd.read_parquet(rp), pd.read_parquet(np)
    if fault == "payload":
        payload = json.loads(raw.loc[0, "raw_payload_json"])
        payload["LASTTRADEDATE"] = "2026-06-19"
        raw.loc[0, "raw_payload_json"] = json.dumps(payload)
    elif fault == "source":
        raw.loc[0, "source_system"] = "invented"
    elif fault == "normalized":
        normalized.loc[0, "expiration_date"] = "2026-06-19"
    elif fault == "duplicate":
        normalized = pd.concat([normalized, normalized], ignore_index=True)
    elif fault == "first_trade":
        normalized["first_trade_date"] = "2026-01-01"
    elif fault == "conflict":
        registry_snapshot(tmp_path, "2026-06-17", expiration="2026-06-19")
    raw.to_parquet(rp, index=False)
    normalized.to_parquet(np, index=False)
    if fault == "missing":
        rp.unlink()
    with pytest.raises((RuntimeError, FileNotFoundError)):
        expiry.verified_historical_registry(tmp_path, "2026-10-05", ["SiM6"])


def test_legitimate_non_anchor_registry_changes_are_allowed(tmp_path):
    rp, np = registry_snapshot(tmp_path, "2026-06-17")
    raw = pd.read_parquet(rp)
    payload = json.loads(raw.loc[0, "raw_payload_json"])
    payload["LOTSIZE"] = 10
    raw = registry.build_registry_snapshot(pd.DataFrame([payload]), "2026-06-17")
    raw.to_parquet(rp, index=False)
    registry.build_normalized_registry(raw).to_parquet(np, index=False)
    registry_snapshot(tmp_path, "2026-06-18")
    result = expiry.verified_historical_registry(tmp_path, "2026-10-05", ["SiM6"])
    assert result.iloc[0]["lot_size"] == 1000
    assert len(json.loads(result.iloc[0]["expiration_source_evidence_json"])["corroborating_versions"]) == 2
