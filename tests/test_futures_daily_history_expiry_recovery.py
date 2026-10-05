"""Recovery preserves evidence; a new acquisition never rewrites historical PIT."""
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moex_data.futures import algopack_availability_probe as registry
from moex_data.futures import date_source_provenance as provenance
from moex_data.futures import expiration_map_builder as expiry
from moex_data.futures import raw_admission_versions as versions
from moex_data.futures import universal_daily_refresh_runner as daily
from moex_data.futures import continuous_quality_report as quality_component
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


@pytest.mark.parametrize("fault", ["missing_mapping", "null_mapping", "missing_scope", "both_absent"])
def test_partial_version_metadata_does_not_fall_back_to_unpinned_alias(tmp_path, fault):
    paths, qp, mp = make_admitted(tmp_path)
    manifest = freeze(tmp_path, qp, mp)
    if fault == "both_absent":
        manifest.pop("raw_partition_versions")
        manifest.pop("admission_version_scope")
    elif fault == "missing_mapping":
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


def quality_fixture(root):
    args = SimpleNamespace(run_date="2026-10-05", snapshot_date="2026-10-05", data_root_resolved=root)
    mp = quality_component.resolve_contract_path(Path.cwd(), root, quality_component.CONTRACT_MANIFEST, {"run_date": args.run_date})
    qp = quality_component.resolve_contract_path(Path.cwd(), root, quality_component.CONTRACT_QUALITY_REPORT, {"run_date": args.run_date})
    identity = dict(run_id="current-quality-run", run_date=args.run_date, snapshot_date=args.snapshot_date,
                    roll_policy_id=daily.ROLL_POLICY_ID, adjustment_policy_id=daily.ADJUSTMENT_POLICY_ID)
    frame = pd.DataFrame([{**identity, "schema_version": quality_component.SCHEMA_QUALITY_REPORT,
                           "quality_report_id": family + "-" + check, "continuous_symbol": quality_component.family_symbol(family),
                           "dataset_id": "fixture", "calendar_status": provenance.OBSERVED,
                           "date_source_summary_json": "{}", "date_source_evidence_json": "[]",
                           "affected_source_secid": None, "affected_trade_date": None, "observed_value": None,
                           "expected_value": None, "review_notes": None,
                           "check_id": check, "check_status": "pass", "family_code": family}
                          for family in ("Si", "USDRUBF") for check in quality_component.REQUIRED_QUALITY_CHECKS])
    gap = frame["check_id"] == "explicit_partial_chain_gap_for_excluded_SiH7_SiM7"
    frame.loc[gap & (frame["family_code"] == "Si"), "check_status"] = "explicit_gap"
    frame.loc[gap & (frame["family_code"] == "USDRUBF"), "check_status"] = "not_applicable"
    frame.loc[(frame["check_id"] == "usdrubf_identity_validation") & (frame["family_code"] == "Si"), "check_status"] = "not_applicable"
    outputs = {"manifest": str(mp), "quality_report": str(qp)}
    manifest = {**identity, "schema_version": quality_component.SCHEMA_MANIFEST,
                "builder_result_verdict": "pass", "blockers": [], "quality_status_counts": quality_component.quality_status_counts(frame),
                "row_counts": {"quality_report": len(frame)}, "output_artifacts": outputs,
                "family_summaries": [{"family_code": "Si"}, {"family_code": "USDRUBF"}],
                "usdrubf_identity_check": {"status": "pass"}, "source_lineage_check": {"status": "pass"},
                "partial_chain_gap_summary": {"status": "explicit_gap"},
                "started_ts": "2026-10-05T12:00:00Z", "completed_ts": "2026-10-05T12:00:01Z"}
    mp.parent.mkdir(parents=True, exist_ok=True)
    qp.parent.mkdir(parents=True, exist_ok=True)
    mp.write_text(json.dumps(manifest))
    frame.to_parquet(qp, index=False)
    stdout = 'run_id: "current-quality-run"\noutput_artifacts_created: ' + json.dumps(outputs)
    return args, mp, qp, manifest, frame, stdout, pd.Timestamp("2026-10-05T12:00:00Z").timestamp()


@pytest.mark.parametrize("fault", [None, "missing", "stale", "wrong_run", "wrong_count", "failed_check", "missing_check", "wrong_path",
    "failed_identity_summary", "failed_lineage_summary", "missing_family_check", "missing_family", "duplicate_check", "missing_column",
    "null_row_counts", "null_outputs", "null_summary", "malformed_roster", "identity_not_applicable", "lineage_not_applicable", "gap_not_explicit"])
def test_required_quality_readback_rejects_stale_or_inconsistent_success(tmp_path, fault):
    args, mp, qp, manifest, frame, stdout, started = quality_fixture(tmp_path)
    if fault == "stale": manifest["started_ts"] = "2026-10-04T12:00:00Z"
    if fault == "wrong_run": frame["run_id"] = "previous-child"
    if fault == "wrong_count": manifest["row_counts"]["quality_report"] += 1
    if fault == "failed_check": frame.loc[0, "check_status"] = "fail"
    if fault == "missing_check": frame = frame.iloc[1:]
    if fault == "wrong_path": manifest["output_artifacts"]["quality_report"] = "/unexpected/report"
    if fault == "failed_identity_summary": manifest["usdrubf_identity_check"]["status"] = "fail"
    if fault == "failed_lineage_summary": manifest["source_lineage_check"]["status"] = "fail"
    if fault == "missing_family_check": frame = frame.loc[~((frame["family_code"] == "USDRUBF") & (frame["check_id"] == "usdrubf_identity_validation"))]
    if fault == "missing_family":
        frame = frame.loc[frame["family_code"] == "Si"]
        manifest["family_summaries"] = [{"family_code": "Si"}]
    if fault == "duplicate_check": frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    if fault == "missing_column": frame = frame.drop(columns=["quality_report_id"])
    if fault == "identity_not_applicable": frame.loc[frame["check_id"] == "usdrubf_identity_validation", "check_status"] = "not_applicable"
    if fault == "lineage_not_applicable": frame.loc[frame["check_id"] == "continuous_output_row_source_lineage_completeness", "check_status"] = "not_applicable"
    if fault == "gap_not_explicit": frame.loc[frame["check_id"] == "explicit_partial_chain_gap_for_excluded_SiH7_SiM7", "check_status"] = "pass"
    if fault in {"missing_family_check", "missing_family", "duplicate_check", "identity_not_applicable", "lineage_not_applicable", "gap_not_explicit"}:
        manifest["quality_status_counts"] = quality_component.quality_status_counts(frame)
        manifest["row_counts"]["quality_report"] = len(frame)
    if fault == "null_row_counts": manifest["row_counts"] = None
    if fault == "null_outputs": manifest["output_artifacts"] = None
    if fault == "null_summary": manifest["source_lineage_check"] = None
    if fault == "malformed_roster": manifest["family_summaries"] = [None]
    mp.write_text(json.dumps(manifest))
    frame.to_parquet(qp, index=False)
    if fault == "missing": qp.unlink()
    if fault:
        with pytest.raises(RuntimeError):
            daily.verify_quality_artifacts(Path.cwd(), args, stdout, started, started + 2)
    else:
        result = daily.verify_quality_artifacts(Path.cwd(), args, stdout, started, started + 2)
        assert result["manifest_sha256"] == sha(mp)
        assert result["quality_sha256"] == sha(qp)


def test_quality_stage_executes_real_component_and_propagates_missing_outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(daily, "load_dotenv", None)
    monkeypatch.setattr(daily, "CANONICAL_STAGE_IDS", ["quality_reports", "unified_manifest"])
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--data-root", str(tmp_path), "--snapshot-date", "2026-10-05", "--run-date", "2026-10-05"])
    with pytest.raises(RuntimeError, match="requires execution"):
        daily.metadata_gate("quality_reports")
    assert daily.main() == 1
    manifest = json.loads(Path(daily.output_paths(tmp_path, "2026-10-05")["manifest"]).read_text())
    assert manifest["executed_stage_order"] == ["quality_reports"]
    child = manifest["child_component_status"][0]
    assert child["returncode"] != 0
    assert child["status"] == "fail"
    assert "continuous_quality_report.py" in child["command"][1]
    assert manifest["universal_daily_refresh_result_verdict"] == "fail"


def test_malformed_quality_manifest_becomes_failed_child_and_fresh_universal_receipt(tmp_path, monkeypatch):
    args, mp, _, manifest, _, stdout, _ = quality_fixture(tmp_path)
    manifest["row_counts"] = None
    mp.write_text(json.dumps(manifest))
    monkeypatch.setattr(daily, "load_dotenv", None)
    monkeypatch.setattr(daily, "CANONICAL_STAGE_IDS", ["quality_reports", "unified_manifest"])
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--data-root", str(tmp_path), "--snapshot-date", args.snapshot_date, "--run-date", args.run_date])
    monkeypatch.setattr(daily.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=stdout, stderr=""))
    assert daily.main() == 1
    result = json.loads(Path(daily.output_paths(tmp_path, args.run_date)["manifest"]).read_text())
    assert result["universal_daily_refresh_result_verdict"] == "fail"
    assert result["executed_stage_order"] == ["quality_reports"]
    assert "row_counts" in result["child_component_status"][0]["failure_reason"]
