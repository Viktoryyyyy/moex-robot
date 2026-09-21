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
    monkeypatch.setenv("MOEX_API_URL", APIM)
    monkeypatch.setenv("MOEX_ISS_BASE_URL", ISS)
    monkeypatch.setattr(registry, "load_dotenv", None)
    monkeypatch.setattr(universal, "load_dotenv", None)



APIM = "https://apim.moex.com"
ISS = "https://iss.moex.com"
SOURCE_PATHS = {
    "algopack_fo_tradestats": "/iss/datashop/algopack/fo/tradestats/{secid}.json",
    "moex_futoi": "/iss/analyticalproducts/futoi/securities/{family}.json",
    "algopack_fo_obstats": "/iss/datashop/algopack/fo/obstats/{secid}.json",
    "algopack_fo_hi2": "/iss/datashop/algopack/fo/hi2/{secid}.json",
}


def source_url(endpoint, secid, family):
    return APIM + SOURCE_PATHS[endpoint].format(secid=secid, family=family.lower())


def produced_screen_pair(instrument, failed=False):
    """Use the real metrics producer; replace only its external history fetch."""
    from unittest.mock import patch

    secid = str(instrument["secid"])
    url = source_url("algopack_fo_tradestats", secid, str(instrument["family_code"]))
    history = pd.DataFrame([
        {"secid": secid, "tradedate": "2026-09-17", "tradetime": "10:00:00", "vol": 10, "val": 100, "trades": 2},
        {"secid": secid, "tradedate": "2026-09-17", "tradetime": "10:05:00", "vol": 20, "val": 200, "trades": 3},
    ])
    result = (pd.DataFrame(), url, "failed", "synthetic source failure") if failed else (history, url, "completed", "")
    with patch.object(registry.base, "fetch_tradestats", return_value=result):
        pair = registry.base.compute_one_metrics(
            instrument, SCREEN_FROM, DAY, {"2026-09-17"}, "synthetic date evidence",
            {}, {}, 10, True, False, 1.0, APIM, ISS)
    for row in pair:
        row["snapshot_date"] = DAY
    return pair


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
                "source_endpoint_url": source_url(endpoint, row.secid, row.family_code),
                "availability_status": status, "probe_status": "completed",
                "validation_status": "not_validated", "probe_from": PROBE_FROM, "probe_till": DAY,
                "schema_version": evidence.availability.REPORT_SCHEMA_BY_ENDPOINT[endpoint],
            })
        frames[endpoint] = pd.DataFrame(rows)
    liquidity, history = [], []
    for _, instrument in normalized.loc[normalized.secid != "NODATAF"].iterrows():
        liq, hist = produced_screen_pair(instrument)
        liquidity.append(liq)
        history.append(hist)
    frames["liquidity_screen"] = pd.DataFrame(liquidity)
    frames["history_depth_screen"] = pd.DataFrame(history)
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
        frame.loc[0, ["validation_status", "review_status", "fetch_status"]] = ["failed", "blocked", "completed"]
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
        mask = frames[key].secid == "NODATAF"
        frames[key].loc[mask, "family_code"] = "UNKNOWN"
        frames[key].loc[mask, "source_endpoint_url"] = source_url(key, "NODATAF", "UNKNOWN")
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



def required_field_cases():
    """Read the contract independently of the production field parser."""
    cases = []
    for key, rel in registry.CONTRACTS.items():
        section = (ROOT / rel).read_text().split("\nrequired_fields:\n", 1)[1]
        for line in section.splitlines():
            if not line.strip():
                continue
            if not line.startswith("- "):
                break
            cases.append((key, line[2:].strip()))
    return cases


@pytest.mark.parametrize("key,field", required_field_cases())
def test_every_contracted_field_is_required(tmp_path, key, field):
    frames = fixture_frames()
    assert field in frames[key].columns, "positive fixture must satisfy the real contract"
    frames[key] = frames[key].drop(columns=field)
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == [key + "_validation_failed"]
    assert "missing_fields:" in summaries[key]["failure_reason"]
    assert field in summaries[key]["failure_reason"]


def test_nullable_values_empty_contract_code_and_boolean_flags_are_preserved(tmp_path):
    frames = fixture_frames()
    frames["liquidity_screen"]["asset_class"] = None
    normalized = frames["normalized_registry"]
    assert normalized.loc[normalized.secid == "USDRUBF", "contract_code"].iloc[0] == ""
    assert set(normalized["is_perpetual_candidate"].tolist()) == {True, False}
    assert validate(tmp_path, frames)[1] == []


@pytest.mark.parametrize("key,field,value", [
    ("registry_snapshot", "source_system", "UNTRUSTED"),
    ("registry_snapshot", "source_system", None),
    ("registry_snapshot", "source_endpoint_id", None),
    ("registry_snapshot", "source_endpoint_id", "different_source"),
    ("registry_snapshot", "raw_payload_json", ""),
    ("registry_snapshot", "raw_payload_json", "{}"),
    ("registry_snapshot", "raw_payload_json", "[]"),
    ("registry_snapshot", "raw_payload_json", "not json"),
    ("registry_snapshot", "raw_payload_json", '{"SECID":"FOREIGN"}'),
    ("normalized_registry", "instrument_kind", "not_a_future_kind"),
    ("normalized_registry", "is_perpetual_candidate", "false"),
    ("normalized_registry", "is_perpetual_candidate", 1),
    ("normalized_registry", "contract_code", None),
])
def test_required_field_values_use_contract_semantics(tmp_path, key, field, value):
    frames = fixture_frames()
    if field == "is_perpetual_candidate":
        # Mixed bool/string or bool/int object columns cannot be written by Arrow.
        # Persist a homogeneous wrong type so the validator, not the writer, rejects it.
        frames[key][field] = pd.Series(value, index=frames[key].index, dtype=object)
    else:
        frames[key][field] = frames[key][field].astype(object)
        frames[key].loc[0, field] = value
    outputs = write_frames(tmp_path, frames)
    if field == "is_perpetual_candidate":
        restored = pd.read_parquet(outputs[key])
        assert restored[field].tolist() == [value] * len(frames[key])
    summaries, blockers = registry.validate_current_outputs(outputs, DAY)
    assert blockers == [key + "_validation_failed"]
    if field == "is_perpetual_candidate":
        assert "invalid_perpetual_flag" in summaries[key]["failure_reason"]


@pytest.mark.parametrize("endpoint", registry.AVAILABILITY_ENDPOINTS)
@pytest.mark.parametrize("fault", ["foreign_host", "public_iss", "http", "foreign_instrument",
                                    "wrong_route", "query", "fragment", "userinfo"])
def test_availability_rejects_wrong_source_routes(tmp_path, endpoint, fault):
    frames = fixture_frames()
    row = frames[endpoint].iloc[0]
    url = source_url(endpoint, row.secid, row.family_code)
    if fault == "foreign_host":
        url = url.replace("apim.moex.com", "other.invalid")
    elif fault == "public_iss":
        url = url.replace("apim.moex.com", "iss.moex.com")
    elif fault == "http":
        url = url.replace("https:", "http:")
    elif fault == "foreign_instrument":
        url = url.rsplit("/", 1)[0] + "/FOREIGN.json"
    elif fault == "wrong_route":
        url = APIM + "/iss/unrelated.json"
    elif fault == "query":
        url += "?secid=FOREIGN"
    elif fault == "fragment":
        url += "#different-source"
    else:
        url = url.replace("https://", "https://user@")
    frames[endpoint].loc[0, "source_endpoint_url"] = url
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == [endpoint + "_validation_failed"]
    assert "source_endpoint_url_mismatch" in summaries[endpoint]["failure_reason"]


def test_futoi_uses_family_not_contract_identity(tmp_path):
    frames = fixture_frames()
    mask = frames["moex_futoi"].secid == "SiZ6"
    assert frames["moex_futoi"].loc[mask, "source_endpoint_url"].iloc[0].endswith("/si.json")
    assert validate(tmp_path, frames)[1] == []
    frames["moex_futoi"].loc[mask, "source_endpoint_url"] = APIM + "/iss/analyticalproducts/futoi/securities/siz6.json"
    assert validate(tmp_path, frames)[1] == ["moex_futoi_validation_failed"]


@pytest.mark.parametrize("endpoint,suffix", [
    ("algopack_fo_obstats", "obstats"), ("algopack_fo_hi2", "hi2"),
])
def test_other_sources_keep_existing_general_candidate(tmp_path, endpoint, suffix):
    frames = fixture_frames()
    frames[endpoint]["source_endpoint_url"] = APIM + "/iss/datashop/algopack/fo/" + suffix + ".json"
    assert validate(tmp_path, frames)[1] == []


def test_general_tradestats_route_is_never_historical_provenance(tmp_path):
    frames = fixture_frames()
    frames["algopack_fo_tradestats"]["source_endpoint_url"] = APIM + "/iss/datashop/algopack/fo/tradestats.json"
    assert validate(tmp_path, frames)[1] == ["algopack_fo_tradestats_validation_failed"]


@pytest.mark.parametrize("key", ["liquidity_screen", "history_depth_screen"])
@pytest.mark.parametrize("fault", ["missing", "foreign_instrument", "public_iss", "general_route"])
def test_each_screen_requires_instrument_specific_source(tmp_path, key, fault):
    frames = fixture_frames()
    if fault == "missing":
        frames[key] = frames[key].drop(columns="source_endpoint_url")
    else:
        url = source_url("algopack_fo_tradestats", "FOREIGN", "")
        if fault == "public_iss":
            url = source_url("algopack_fo_tradestats", "USDRUBF", "").replace("apim.moex.com", "iss.moex.com")
        if fault == "general_route":
            url = APIM + "/iss/datashop/algopack/fo/tradestats.json"
        frames[key].loc[0, "source_endpoint_url"] = url
    assert validate(tmp_path, frames)[1] == [key + "_validation_failed"]


@pytest.mark.parametrize("key", ["liquidity_screen", "history_depth_screen"])
@pytest.mark.parametrize("field", registry.SCREEN_PROVENANCE_FIELDS)
def test_screen_shared_provenance_cannot_be_missing(tmp_path, key, field):
    frames = fixture_frames()
    frames[key] = frames[key].drop(columns=field)
    assert validate(tmp_path, frames)[1] == [key + "_validation_failed"]


@pytest.mark.parametrize("field,value", [
    ("trade_stats_rows", 999), ("daily_rows", 99), ("duplicate_intraday_rows", 1),
    ("first_available_date", "2026-09-16"), ("last_available_date", "2026-09-16"),
    ("available_trading_days", 99), ("expected_trading_days", 99), ("coverage_ratio", 0.2),
    ("recent_gap_count", 1), ("missing_day_diagnostics", "{}"),
    ("calendar_status", "unavailable"), ("calendar_note", "different denominator"),
    ("full_history_proven", False), ("history_proof_scope", "bounded_probe"),
    ("metric_columns_json", "{}"), ("fetch_error", "not the same fetch"),
])
def test_screen_shared_fetch_disagreement_is_rejected(tmp_path, field, value):
    frames = fixture_frames()
    frames["history_depth_screen"][field] = frames["history_depth_screen"][field].astype(object)
    frames["history_depth_screen"].loc[0, field] = value
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == ["history_depth_screen_validation_failed"]
    assert "screen_provenance_mismatch:" + field in summaries["history_depth_screen"]["failure_reason"]


def test_consistent_failed_fetch_is_retained_as_two_negative_outcomes(tmp_path):
    frames = fixture_frames()
    instrument = frames["normalized_registry"].iloc[0]
    pair = produced_screen_pair(instrument, failed=True)
    for key, row in zip(("liquidity_screen", "history_depth_screen"), pair):
        frames[key] = pd.concat([pd.DataFrame([row]), frames[key].iloc[1:]], ignore_index=True)
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == []
    assert summaries["liquidity_screen"]["status_counts"]["fail"] == 1
    assert summaries["history_depth_screen"]["status_counts"]["fail"] == 1


def test_failed_fetch_in_only_one_screen_is_not_coherent(tmp_path):
    frames = fixture_frames()
    _, failed_history = produced_screen_pair(frames["normalized_registry"].iloc[0], failed=True)
    frames["history_depth_screen"] = pd.concat(
        [pd.DataFrame([failed_history]), frames["history_depth_screen"].iloc[1:]], ignore_index=True)
    summaries, blockers = validate(tmp_path, frames)
    assert blockers == ["history_depth_screen_validation_failed"]
    assert "screen_provenance_mismatch" in summaries["history_depth_screen"]["failure_reason"]


def test_provenance_pairing_uses_identity_not_row_position(tmp_path):
    frames = fixture_frames()
    frames["history_depth_screen"] = frames["history_depth_screen"].iloc[::-1].reset_index(drop=True)
    assert validate(tmp_path, frames)[1] == []


def test_explicit_source_origin_is_checked_and_public_iss_cannot_be_apim(tmp_path):
    frames = fixture_frames()
    for key in (*registry.AVAILABILITY_ENDPOINTS, "liquidity_screen", "history_depth_screen"):
        frames[key]["source_endpoint_url"] = frames[key]["source_endpoint_url"].str.replace(
            APIM, "https://configured-apim.invalid", regex=False)
    assert validate(tmp_path, frames, apim_base_url="https://configured-apim.invalid")[1] == []
    assert validate(tmp_path, frames)[1]
    for key in (*registry.AVAILABILITY_ENDPOINTS, "liquidity_screen", "history_depth_screen"):
        frames[key]["source_endpoint_url"] = frames[key]["source_endpoint_url"].str.replace(
            "https://configured-apim.invalid", ISS, regex=False)
    summaries, blockers = validate(tmp_path, frames, apim_base_url=ISS)
    assert blockers and "public_iss_forbidden" in summaries["algopack_fo_tradestats"]["failure_reason"]


def test_main_validates_the_same_origin_it_passes_to_children(monkeypatch, tmp_path):
    calls = []
    actual = registry.validate_current_outputs
    def validate_at_origin(*args, **kwargs):
        calls.append((kwargs["apim_base_url"], kwargs["iss_base_url"]))
        return actual(*args, **kwargs)
    monkeypatch.setattr(registry, "validate_current_outputs", validate_at_origin)
    code, _, _, _ = simulate_main(monkeypatch, tmp_path)
    assert code == 0 and calls == [(APIM, ISS), (APIM, ISS)]


@pytest.mark.parametrize("kind", ["expiring_future", "perpetual_future_candidate", "technical", "unknown"])
@pytest.mark.parametrize("flag", [False, True])
@pytest.mark.parametrize("dtype", ["bool", "boolean", "object"])
def test_perpetual_classification_requires_a_coherent_typed_pair(kind, flag, dtype):
    frame = fixture_frames()["normalized_registry"].iloc[:1].copy()
    frame["instrument_kind"] = kind
    frame["is_perpetual_candidate"] = pd.Series([flag], index=frame.index, dtype=dtype)
    before = frame.copy(deep=True)
    if flag == (kind == "perpetual_future_candidate"):
        registry._current_schema_values(frame, "normalized_registry")
    else:
        with pytest.raises(ValueError, match="inconsistent_perpetual_classification"):
            registry._current_schema_values(frame, "normalized_registry")
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("kind", ["expiring_future", "perpetual_future_candidate", "technical", "unknown"])
@pytest.mark.parametrize("evidence_only", [True, False])
def test_inconsistent_perpetual_pair_blocks_preflight_and_final_validation(tmp_path, kind, evidence_only):
    frames = fixture_frames()
    normalized = frames["normalized_registry"]
    index = normalized.index[normalized.secid == "SiZ6"][0]
    normalized.loc[index, "instrument_kind"] = kind
    normalized.loc[index, "is_perpetual_candidate"] = kind != "perpetual_future_candidate"
    summaries, blockers = validate(tmp_path, frames, evidence_only=evidence_only)
    assert blockers == ["normalized_registry_validation_failed"]
    assert "inconsistent_perpetual_classification" in summaries["normalized_registry"]["failure_reason"]
    assert "liquidity_screen" not in summaries
    assert "history_depth_screen" not in summaries


@pytest.mark.parametrize("evidence_only", [True, False])
def test_real_normalizer_classifications_pass_without_relabeling(tmp_path, evidence_only):
    frames = fixture_frames()
    raw = pd.DataFrame({
        "SECID": ["USDRUBF", "SiZ6", "NODATAF"], "BOARDID": ["RFUD"] * 3,
        "LASTTRADEDATE": ["", "2026-12-17", ""],
    })
    snapshot = evidence.availability.build_registry_snapshot(raw, DAY)
    normalized = evidence.availability.build_normalized_registry(snapshot)
    pairs = {row.secid: (row.instrument_kind, bool(row.is_perpetual_candidate))
             for row in normalized.itertuples(index=False)}
    assert pairs == {
        "USDRUBF": ("perpetual_future_candidate", True),
        "SiZ6": ("expiring_future", False),
        "NODATAF": ("unknown", False),
    }
    frames["registry_snapshot"] = snapshot
    frames["normalized_registry"] = normalized
    frames["family_mapping"] = evidence.build_family_mapping(normalized, DAY)
    before = normalized.copy(deep=True)
    summaries, blockers = validate(tmp_path, frames, evidence_only=evidence_only)
    assert blockers == [] and summaries["normalized_registry"]["validation_status"] == "pass"
    pd.testing.assert_frame_equal(normalized, before)


@pytest.mark.parametrize("secid", ["USDRUBF", "SiZ6"])
def test_main_blocks_inconsistent_perpetual_flag_before_metrics_child(monkeypatch, tmp_path, secid):
    frames = fixture_frames()
    normalized = frames["normalized_registry"]
    index = normalized.index[normalized.secid == secid][0]
    normalized.loc[index, "is_perpetual_candidate"] = not bool(normalized.loc[index, "is_perpetual_candidate"])
    monkeypatch.setitem(globals(), "fixture_frames", lambda: frames)
    code, manifest, calls, outputs = simulate_main(monkeypatch, tmp_path)
    assert code == 1 and len(calls) == 1
    assert manifest["blockers"] == ["normalized_registry_validation_failed"]
    assert manifest["artifact_validation_status"] == manifest["registry_refresh_result_verdict"] == "fail"
    assert "inconsistent_perpetual_classification" in manifest["output_summaries"]["normalized_registry"]["failure_reason"]
    assert not Path(outputs["liquidity_screen"]).exists()
    assert not Path(outputs["history_depth_screen"]).exists()


@pytest.mark.parametrize("value", ["false", 1])
@pytest.mark.parametrize("row_index", [0, 1, 2])
def test_mixed_invalid_perpetual_flag_is_rejected_in_memory(value, row_index):
    frame = fixture_frames()["normalized_registry"]
    frame["is_perpetual_candidate"] = frame["is_perpetual_candidate"].astype(object)
    frame.loc[row_index, "is_perpetual_candidate"] = value
    before = frame.copy(deep=True)
    with pytest.raises(ValueError, match="invalid_perpetual_flag"):
        registry._current_schema_values(frame, "normalized_registry")
    pd.testing.assert_frame_equal(frame, before)


def mapping_fixture_with_unknown(family="UNKNOWN"):
    frames = fixture_frames()
    normalized = frames["normalized_registry"]
    normalized.loc[normalized.secid == "NODATAF", "family_code"] = family
    frames["family_mapping"] = evidence.build_family_mapping(normalized, DAY)
    for key in registry.AVAILABILITY_ENDPOINTS:
        mask = frames[key].secid == "NODATAF"
        frames[key].loc[mask, "family_code"] = family
        frames[key].loc[mask, "source_endpoint_url"] = source_url(key, "NODATAF", family)
    return frames


@pytest.mark.parametrize("secid", ["USDRUBF", "SiZ6"])
@pytest.mark.parametrize("family", [None, "FOREIGN", "UNKNOWN"])
@pytest.mark.parametrize("evidence_only", [True, False])
def test_resolved_registry_family_cannot_be_relabelled_unresolved(tmp_path, secid, family, evidence_only):
    frames = fixture_frames()
    mapping = frames["family_mapping"]
    mask = mapping.secid == secid
    mapping.loc[mask, ["family_code", "mapping_status", "mapping_source", "validation_status"]] = [
        family, "unresolved", "unresolved", "failed"]
    summaries, blockers = validate(tmp_path, frames, evidence_only=evidence_only)
    assert blockers == ["family_mapping_validation_failed"]
    assert "inconsistent_registry_field:mapping_status" in summaries["family_mapping"]["failure_reason"]


@pytest.mark.parametrize("family", ["UNKNOWN", "FOREIGN", ""])
@pytest.mark.parametrize("evidence_only", [True, False])
def test_unresolved_mapping_cannot_retain_a_family_value(tmp_path, family, evidence_only):
    frames = mapping_fixture_with_unknown()
    mapping = frames["family_mapping"]
    mapping.loc[mapping.secid == "NODATAF", "family_code"] = family
    summaries, blockers = validate(tmp_path, frames, evidence_only=evidence_only)
    assert blockers == ["family_mapping_validation_failed"]
    assert "incoherent_unresolved_family" in summaries["family_mapping"]["failure_reason"]


@pytest.mark.parametrize("evidence_only", [True, False])
def test_unknown_registry_family_cannot_be_relabelled_pass(tmp_path, evidence_only):
    frames = mapping_fixture_with_unknown()
    mapping = frames["family_mapping"]
    mapping.loc[mapping.secid == "NODATAF", ["family_code", "mapping_status", "mapping_source", "validation_status"]] = [
        "UNKNOWN", "pass", "derived_rule", "pass"]
    summaries, blockers = validate(tmp_path, frames, evidence_only=evidence_only)
    assert blockers == ["family_mapping_validation_failed"]
    assert "inconsistent_registry_field:mapping_status" in summaries["family_mapping"]["failure_reason"]


@pytest.mark.parametrize("family", ["UNKNOWN", "unknown", "UnKnOwN"])
@pytest.mark.parametrize("evidence_only", [True, False])
def test_actual_mapping_producer_unknowns_pass_by_identity_without_rewriting(tmp_path, family, evidence_only):
    frames = mapping_fixture_with_unknown(family)
    frames["family_mapping"] = frames["family_mapping"].iloc[::-1].reset_index(drop=True)
    before = frames["family_mapping"].copy(deep=True)
    summaries, blockers = validate(tmp_path, frames, evidence_only=evidence_only)
    assert blockers == []
    assert summaries["family_mapping"]["status_counts"] == {"pass": 2, "unresolved": 1}
    assert pd.isna(frames["family_mapping"].loc[frames["family_mapping"].secid == "NODATAF", "family_code"]).all()
    pd.testing.assert_frame_equal(frames["family_mapping"], before)


@pytest.mark.parametrize("secid", ["USDRUBF", "SiZ6"])
def test_main_blocks_false_unresolved_mapping_before_metrics_child(monkeypatch, tmp_path, secid):
    frames = fixture_frames()
    mapping = frames["family_mapping"]
    mapping.loc[mapping.secid == secid, ["family_code", "mapping_status", "mapping_source", "validation_status"]] = [
        None, "unresolved", "unresolved", "failed"]
    monkeypatch.setitem(globals(), "fixture_frames", lambda: frames)
    code, manifest, calls, outputs = simulate_main(monkeypatch, tmp_path)
    assert code == 1 and len(calls) == 1
    assert manifest["blockers"] == ["family_mapping_validation_failed"]
    assert manifest["artifact_validation_status"] == manifest["registry_refresh_result_verdict"] == "fail"
    assert "inconsistent_registry_field:mapping_status" in manifest["output_summaries"]["family_mapping"]["failure_reason"]
    assert not Path(outputs["liquidity_screen"]).exists()
    assert not Path(outputs["history_depth_screen"]).exists()
