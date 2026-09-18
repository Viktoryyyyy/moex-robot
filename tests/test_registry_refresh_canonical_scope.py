"""Synthetic current-registry/legacy-scope regressions; no live source requests."""

from argparse import Namespace
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from moex_data.futures import registry_evidence_artifacts_producer as evidence
from moex_data.futures import registry_refresh_runner as registry
from moex_data.futures import universal_daily_refresh_runner as universal

DAY = "2026-09-18"
PROBE_FROM = "2026-09-04"
SCREEN_FROM = "2025-09-18"
EVIDENCE_KEYS = tuple(key for key in registry.CONTRACTS if key not in ("liquidity_screen", "history_depth_screen"))
ALL_KEYS = tuple(registry.CONTRACTS)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("live network forbidden")
    monkeypatch.setattr(requests.sessions.Session, "request", denied)
    monkeypatch.setattr(registry, "load_dotenv", None)
    monkeypatch.setattr(universal, "load_dotenv", None)


def fixture_frames():
    raw = pd.DataFrame({"SECID": ["USDRUBF", "SiZ6", "NODATAF"], "BOARDID": ["RFUD"] * 3})
    snapshot = evidence.availability.build_registry_snapshot(raw, DAY)
    normalized = evidence.availability.build_normalized_registry(snapshot)
    frames = {
        "registry_snapshot": snapshot,
        "normalized_registry": normalized,
        "family_mapping": evidence.build_family_mapping(normalized, DAY),
    }
    for endpoint in registry.AVAILABILITY_ENDPOINTS:
        rows = []
        for row in normalized.itertuples(index=False):
            status = "available" if row.secid != "NODATAF" else "unavailable"
            if endpoint != "algopack_fo_tradestats":
                status = "unavailable"
            rows.append({
                "availability_report_id": endpoint + "_" + row.secid,
                "snapshot_date": DAY, "board": row.board, "secid": row.secid,
                "family_code": row.family_code, "endpoint_id": endpoint,
                "source_endpoint_url": "https://source.invalid/" + row.secid,
                "availability_status": status, "probe_status": "completed",
                "validation_status": "not_validated", "probe_from": PROBE_FROM, "probe_till": DAY,
                "schema_version": evidence.availability.REPORT_SCHEMA_BY_ENDPOINT[endpoint],
            })
        frames[endpoint] = pd.DataFrame(rows)
    for key, field, schema in (
        ("liquidity_screen", "liquidity_status", registry.base.SCHEMA_LIQUIDITY_SCREEN),
        ("history_depth_screen", "history_depth_status", registry.base.SCHEMA_HISTORY_DEPTH_SCREEN),
    ):
        rows = []
        for row in normalized.loc[normalized.secid != "NODATAF"].itertuples(index=False):
            rows.append({
                key + "_id": key + "_" + row.secid,
                "snapshot_date": DAY, "board": row.board, "secid": row.secid, "family_code": row.family_code,
                "screen_from": SCREEN_FROM, "screen_till": DAY, field: "pass",
                "validation_status": "metrics_computed", "review_status": "ready_for_pm_review",
                "fetch_status": "completed", "review_notes": "synthetic computed outcome",
                "schema_version": schema,
            })
        frames[key] = pd.DataFrame(rows)
    return frames


def write_frames(tmp_path, frames):
    paths = {}
    for key, frame in frames.items():
        path = tmp_path / (key + ".parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        paths[key] = str(path)
    return paths


def validate(tmp_path, frames, **kwargs):
    return registry.validate_current_outputs(write_frames(tmp_path, frames), DAY, **kwargs)


def test_current_registry_without_old_contracts_passes_and_legacy_still_fails(tmp_path):
    frames = fixture_frames()
    assert not {"SiM6", "SiU6", "SiU7"} & set(frames["normalized_registry"].secid)
    outputs = write_frames(tmp_path, frames)
    before = {key: Path(path).read_bytes() for key, path in outputs.items()}
    summaries, blockers = registry.validate_current_outputs(outputs, DAY)
    assert blockers == [] and set(summaries) == set(ALL_KEYS)
    assert all(row["validation_status"] == "pass" for row in summaries.values())
    assert summaries["normalized_registry"]["candidate_count"] == 3
    assert summaries["liquidity_screen"]["expected_instrument_count"] == 2
    assert summaries["moex_futoi"]["status_counts"] == {"unavailable": 3}
    legacy, blockers = registry.validate_outputs(outputs, registry.DEFAULT_WHITELIST)
    assert blockers and legacy["algopack_fo_tradestats"]["whitelist_status"]["SiM6"] == "missing"
    assert before == {key: Path(path).read_bytes() for key, path in outputs.items()}


@pytest.mark.parametrize("status", ["unavailable", "partial", "error", "not_checked"])
@pytest.mark.parametrize("endpoint", registry.AVAILABILITY_ENDPOINTS)
def test_explicit_negative_availability_is_preserved_not_global_failure(tmp_path, status, endpoint):
    frames = fixture_frames()
    frame = frames[endpoint]
    frame.loc[frame.secid == "NODATAF", "availability_status"] = status
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == []
    assert summaries[endpoint]["status_counts"][status] >= 1
    assert summaries["liquidity_screen"]["rows"] == 2


@pytest.mark.parametrize("key,field", [("liquidity_screen", "liquidity_status"),
                                      ("history_depth_screen", "history_depth_status")])
@pytest.mark.parametrize("outcome", ["fail", "review_required"])
def test_negative_or_review_screen_outcome_is_evidence_not_admission(tmp_path, key, field, outcome):
    frames = fixture_frames()
    frame = frames[key]
    frame.loc[0, field] = outcome
    if outcome == "fail":
        frame.loc[0, ["validation_status", "review_status", "fetch_status"]] = ["failed", "blocked", "failed"]
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == [] and summaries[key]["status_counts"][outcome] == 1
    assert frame.loc[0, field] == outcome
    assert all("whitelist_status" not in row for row in summaries.values())


@pytest.mark.parametrize("key", ALL_KEYS)
@pytest.mark.parametrize("fault", ["missing_row", "duplicate", "extra_row", "wrong_date", "missing_secid", "null_board"])
def test_structurally_invalid_evidence_fails_closed(tmp_path, key, fault):
    frames = fixture_frames()
    frame = frames[key]
    if fault == "missing_row":
        frames[key] = frame.iloc[1:]
    elif fault == "duplicate":
        frames[key] = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    elif fault == "extra_row":
        extra = frame.iloc[:1].copy()
        extra["secid"] = "UNREQUESTED"
        frames[key] = pd.concat([frame, extra], ignore_index=True)
    elif fault == "wrong_date":
        frame.loc[0, "snapshot_date"] = "2026-09-17"
    elif fault == "missing_secid":
        frames[key] = frame.drop(columns="secid")
    else:
        frame.loc[0, "board"] = None
    summaries, blockers = validate(tmp_path, frames)
    assert blockers and any(row["validation_status"] == "fail" for row in summaries.values())


@pytest.mark.parametrize("key", ALL_KEYS)
def test_missing_or_unreadable_artifact_is_reported_not_raised(tmp_path, key):
    outputs = write_frames(tmp_path, fixture_frames())
    Path(outputs[key]).write_bytes(b"not parquet")
    summaries, blockers = registry.validate_current_outputs(outputs, DAY)
    assert blockers == [key + "_validation_failed"]
    assert summaries[key]["failure_reason"]
    Path(outputs[key]).unlink()
    summaries, blockers = registry.validate_current_outputs(outputs, DAY)
    assert blockers == [key + "_validation_failed"]
    assert "FileNotFoundError" in summaries[key]["failure_reason"]


@pytest.mark.parametrize("key,column,value", [
    ("normalized_registry", "source_snapshot_id", "other"),
    ("normalized_registry", "snapshot_id", "other"),
    ("normalized_registry", "engine", "other"),
    ("family_mapping", "snapshot_id", "other"),
    ("family_mapping", "family_code", "other"),
    ("algopack_fo_tradestats", "family_code", "other"),
    ("liquidity_screen", "family_code", "other"),
    ("history_depth_screen", "family_code", "other"),
    ("moex_futoi", "endpoint_id", "algopack_fo_hi2"),
    ("moex_futoi", "probe_status", "running"),
    ("moex_futoi", "availability_status", "invented"),
    ("liquidity_screen", "liquidity_status", "invented"),
    ("liquidity_screen", "fetch_status", "failed"),
    ("history_depth_screen", "review_status", "pending"),
    ("history_depth_screen", "schema_version", "wrong.v1"),
    ("algopack_fo_hi2", "schema_version", "wrong.v1"),
    ("family_mapping", "mapping_status", "invented"),
    ("family_mapping", "validation_status", "failed"),
])
def test_cross_artifact_and_status_inconsistency_fails(tmp_path, key, column, value):
    frames = fixture_frames()
    frames[key].loc[0, column] = value
    summaries, blockers = validate(tmp_path, frames)
    assert blockers and any(item["validation_status"] == "fail" for item in summaries.values())


@pytest.mark.parametrize("key,prefix", [(name, "probe") for name in registry.AVAILABILITY_ENDPOINTS]
                         + [("liquidity_screen", "screen"), ("history_depth_screen", "screen")])
@pytest.mark.parametrize("bound", ["from", "till"])
def test_wrong_window_even_with_right_snapshot_is_rejected(tmp_path, key, prefix, bound):
    frames = fixture_frames()
    frames[key].loc[0, prefix + "_" + bound] = "2026-01-01"
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == [key + "_validation_failed"]
    assert "request_window_mismatch" in summaries[key]["failure_reason"]


def test_explicit_bounds_follow_producer_defaults_and_overrides(tmp_path):
    frames = fixture_frames()
    for key in registry.AVAILABILITY_ENDPOINTS:
        frames[key]["probe_from"], frames[key]["probe_till"] = "2026-09-01", "2026-09-17"
    for key in ("liquidity_screen", "history_depth_screen"):
        frames[key]["screen_from"], frames[key]["screen_till"] = "2026-09-01", "2026-09-17"
    assert validate(tmp_path, frames, from_date="2026-09-01", till="2026-09-17")[1] == []


def test_unresolved_family_is_retained_without_inventing_mapping(tmp_path):
    frames = fixture_frames()
    frames["normalized_registry"].loc[2, "family_code"] = "UNKNOWN"
    mapping = frames["family_mapping"]
    index = mapping.index[mapping.secid == "NODATAF"][0]
    mapping.loc[index, ["family_code", "mapping_status", "mapping_source", "validation_status"]] = [
        None, "unresolved", "unresolved", "failed"]
    for key in registry.AVAILABILITY_ENDPOINTS:
        frames[key].loc[frames[key].secid == "NODATAF", "family_code"] = "UNKNOWN"
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == [] and summaries["family_mapping"]["status_counts"]["unresolved"] == 1


def test_no_available_tradestats_does_not_fabricate_empty_screens(tmp_path):
    frames = fixture_frames()
    frames["algopack_fo_tradestats"]["availability_status"] = "unavailable"
    summaries, blockers = validate(tmp_path, frames, evidence_only=True)
    assert blockers == ["liquidity_screen_validation_failed"]
    assert "No available instruments" in summaries["liquidity_screen"]["failure_reason"]


def simulate_main(monkeypatch, tmp_path, *, mode=registry.CURRENT_REGISTRY, fault="", bounds=()):
    frames = fixture_frames()
    data_root = tmp_path / "data"
    outputs = registry.output_paths(ROOT, data_root, DAY, DAY)
    calls = []
    if bounds:
        for key in registry.AVAILABILITY_ENDPOINTS:
            frames[key]["probe_from"], frames[key]["probe_till"] = bounds[1], bounds[3]
        for key in ("liquidity_screen", "history_depth_screen"):
            frames[key]["screen_from"], frames[key]["screen_till"] = bounds[1], bounds[3]
    def acquire(command, *, cwd, text, capture_output, env=None):
        is_screen = "-m" in command
        calls.append(command)
        if fault == ("screen_exit" if is_screen else "evidence_exit"):
            return subprocess.CompletedProcess(command, 9, "", "synthetic failure")
        keys = ("liquidity_screen", "history_depth_screen") if is_screen else EVIDENCE_KEYS
        for key in keys:
            path = Path(outputs[key])
            path.parent.mkdir(parents=True, exist_ok=True)
            if fault == "missing:" + key:
                continue
            frame = frames[key].copy()
            if fault == "wrong_date:" + key:
                frame.loc[0, "snapshot_date"] = "2026-09-17"
            frame.to_parquet(path, index=False)
            if fault == "stale:" + key:
                os.utime(path, (1, 1))
        return subprocess.CompletedProcess(command, 0, 'availability_probe_timing_summary: {"synthetic": {}}\n', "")
    with monkeypatch.context() as patch:
        patch.chdir(ROOT)
        patch.setattr(registry.subprocess, "run", acquire)
        options = [] if mode is None else ["--validation-mode", mode]
        patch.setattr(sys, "argv", [
            "registry_refresh_runner", "--snapshot-date", DAY, "--run-date", DAY,
            "--data-root", str(data_root), *options, *bounds])
        code = registry.main()
    return code, json.loads(Path(outputs["manifest"]).read_text()), calls, outputs


@pytest.mark.parametrize("bounds", [(), ("--from", "2026-09-01", "--till", "2026-09-17")])
def test_current_main_launches_both_children_and_retains_manifest_evidence(monkeypatch, tmp_path, bounds):
    code, manifest, calls, outputs = simulate_main(monkeypatch, tmp_path, bounds=bounds)
    assert code == 0 and len(calls) == 2
    assert manifest["validation_mode"] == "current_registry"
    assert manifest["runner_whitelist_applied"] == []
    assert manifest["excluded_instruments_confirmed"] == []
    assert manifest["registry_refresh_result_verdict"] == manifest["artifact_validation_status"] == "pass"
    assert manifest["blockers"] == [] and len(manifest["output_summaries"]) == 9
    assert "--full-history-proven" in calls[1]  # This task must not change this existing source policy.
    payload = Path(outputs["manifest"]).read_bytes()
    archived = Path(outputs["manifest"]).parent / "manifest_history" / (hashlib.sha256(payload).hexdigest() + ".json")
    assert archived.read_bytes() == payload


@pytest.mark.parametrize("mode", [None, "slice1_compat"])
def test_default_and_explicit_legacy_mode_keep_missing_whitelist_gate(monkeypatch, tmp_path, mode):
    code, manifest, calls, _ = simulate_main(monkeypatch, tmp_path, mode=mode)
    assert code == 1 and len(calls) == 2
    assert manifest["validation_mode"] == registry.SLICE1_COMPAT
    assert manifest["runner_whitelist_applied"] == registry.DEFAULT_WHITELIST
    assert manifest["excluded_instruments_confirmed"] == registry.DEFAULT_EXCLUDED
    assert manifest["output_summaries"]["algopack_fo_tradestats"]["whitelist_status"]["SiM6"] == "missing"


@pytest.mark.parametrize("key", EVIDENCE_KEYS)
def test_bad_evidence_blocks_before_expensive_screen_child(monkeypatch, tmp_path, key):
    code, manifest, calls, _ = simulate_main(monkeypatch, tmp_path, fault="wrong_date:" + key)
    assert code == 1 and len(calls) == 1
    assert manifest["blockers"] == [key + "_validation_failed"]
    assert manifest["artifact_validation_status"] == "fail"


@pytest.mark.parametrize("fault", ["evidence_exit", "screen_exit", "missing:normalized_registry",
                                    "stale:normalized_registry", "missing:liquidity_screen",
                                    "stale:history_depth_screen", "wrong_date:liquidity_screen"])
def test_main_does_not_hide_child_failure_missing_stale_or_invalid_screens(monkeypatch, tmp_path, fault):
    code, manifest, calls, _ = simulate_main(monkeypatch, tmp_path, fault=fault)
    assert code == 1 and manifest["blockers"]
    assert manifest["registry_refresh_result_verdict"] == manifest["artifact_validation_status"] == "fail"
    if fault in ("evidence_exit", "missing:normalized_registry", "stale:normalized_registry"):
        assert len(calls) == 1


@pytest.mark.parametrize("flag", ["--whitelist", "--excluded"])
@pytest.mark.parametrize("value", ["", "USDRUBF", "SiM6"])
def test_current_mode_rejects_legacy_overrides_before_any_child(monkeypatch, flag, value):
    def forbidden(*args, **kwargs):
        raise AssertionError("must reject incompatible scope before child execution")
    monkeypatch.setattr(registry, "run_child", forbidden)
    monkeypatch.setattr(sys, "argv", ["registry", "--validation-mode", "current_registry", flag, value])
    with pytest.raises(SystemExit) as error:
        registry.main()
    assert error.value.code == 2


@pytest.mark.parametrize("filters", [("", ""), ("Si", "SiZ6")])
def test_universal_selects_current_mode_without_legacy_or_debug_narrowing(tmp_path, filters):
    args = Namespace(snapshot_date=DAY, run_date=DAY, from_date="", till="", data_root_resolved=tmp_path,
                     timeout=60.0, availability_max_workers=4, iss_base_url="https://iss.invalid",
                     apim_base_url="https://apim.invalid", family=filters[0], secid=filters[1])
    command = universal.command_for_stage(ROOT, "registry_refresh", args)
    assert command[command.index("--validation-mode") + 1] == registry.CURRENT_REGISTRY
    assert not {"--whitelist", "--excluded", "--family", "--secid"} & set(command)
    for stage in ("all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh"):
        other = universal.command_for_stage(ROOT, stage, args)
        assert "--validation-mode" not in other
        assert other[other.index("--selection-mode") + 1] == "rfud_included_universe"


@pytest.mark.parametrize("outcome", [0, 1])
def test_universal_real_command_seam_propagates_registry_verdict(monkeypatch, tmp_path, outcome):
    calls = []
    def child(command, **kwargs):
        calls.append(command)
        assert command[command.index("--validation-mode") + 1] == "current_registry"
        return subprocess.CompletedProcess(command, outcome, "", "")
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr(universal.subprocess, "run", child)
    monkeypatch.setattr(sys, "argv", [
        "universal", "--snapshot-date", DAY, "--run-date", DAY, "--data-root", str(tmp_path),
        "--stop-after", "registry_refresh"])
    assert universal.main() == outcome
    manifest = json.loads(Path(universal.output_paths(tmp_path, DAY)["manifest"]).read_text())
    assert len(calls) == 1 and manifest["executed_stage_order"] == ["registry_refresh"]
    assert manifest["selection_model"] == "eligibility_snapshot_driven"
    assert manifest["slice1_whitelist_semantics"] == "forbidden_as_canonical_scope"
    assert manifest["universal_daily_refresh_result_verdict"] == ("pass" if outcome == 0 else "fail")


def test_contract_documents_mode_boundary_without_rewriting_legacy_history():
    text = (ROOT / "contracts/datasets/futures_registry_refresh_manifest_contract.md").read_text()
    assert "current_registry" in text and "slice1_compat" in text and "validation_mode" in text
    assert "candidate evidence only" in text
    assert "manifest_attempt_retention:" in text
