"""FUTOI availability isolation; external responses never authorize hidden PASS."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from moex_data.futures import all_universe_futoi_raw_backfill_slice as runner

DAY = "2026-09-18"
ROOT = Path(__file__).resolve().parents[1]


def frames():
    eligibility = pd.DataFrame([
        dict(secid=secid, board="RFUD", family_code=family,
             registry_snapshot_date=DAY, eligibility_snapshot_date=DAY,
             registry_snapshot_id="registry-test", eligibility_snapshot_id="elig-" + secid,
             classification_status="included", raw_5m_eligible=True,
             raw_d1_eligible=False, futoi_eligible=False)
        for secid, family in (("SiZ6", "Si"), ("USDRUBF", "USDRUBF"))
    ])
    availability = pd.DataFrame([
        dict(secid=row.secid, board=row.board, family_code=row.family_code,
             snapshot_date=DAY, availability_status="available", probe_status="completed",
             observed_rows=2, error_code=None, error_message=None)
        for row in eligibility.itertuples(index=False)
    ])
    return eligibility, availability


def test_mixed_availability_preserves_generic_classification_and_input():
    e, a = frames()
    before_e, before_a = e.copy(deep=True), a.copy(deep=True)
    a.loc[1, ["availability_status", "observed_rows"]] = ["unavailable", 0]
    actual_a = a.copy(deep=True)
    derived = runner.derive_futoi_eligibility(e, a, DAY)
    assert derived.futoi_eligible.tolist() == [True, False]
    assert derived.futoi_deferral_reason.tolist() == ["", "futoi_unavailable"]
    assert derived.futoi_retry_required.tolist() == [False, False]
    for field in ("classification_status", "raw_5m_eligible", "raw_d1_eligible"):
        pd.testing.assert_series_equal(derived[field], before_e[field])
    pd.testing.assert_frame_equal(e, before_e)
    pd.testing.assert_frame_equal(a, actual_a)
    assert before_a.loc[1, "availability_status"] == "available"


@pytest.mark.parametrize("status,probe,error", [
    ("error", "completed", "timeout"),
    ("partial", "completed", None),
    ("not_checked", "completed", None),
    ("available", "failed", "timeout"),
    ("unavailable", "failed", "timeout"),
    ("unavailable", "completed", "futoi_schema_invalid"),
    ("unavailable", "completed", "ERROR_MESSAGE"),
])
def test_unresolved_evidence_is_isolated_but_never_accepted(status, probe, error):
    e, a = frames()
    a.loc[1, ["availability_status", "probe_status", "observed_rows", "error_code"]] = [
        status, probe, 0, error]
    derived = runner.derive_futoi_eligibility(e, a, DAY)
    assert derived.futoi_eligible.tolist() == [True, False]
    assert derived.futoi_retry_required.tolist() == [False, True]
    selected = runner.selected_universe(derived)
    assert selected.secid.tolist() == ["SiZ6"]


@pytest.mark.parametrize("count", [None, 0, -1, 1.5, float("nan"), float("inf"), True, "2"])
def test_available_requires_real_positive_integer_observations(count):
    e, a = frames()
    a["observed_rows"] = a["observed_rows"].astype(object)
    a.at[1, "observed_rows"] = count
    result = runner.derive_futoi_eligibility(e, a, DAY)
    assert result.futoi_eligible.tolist() == [True, False]
    assert result.futoi_retry_required.tolist() == [False, True]


def test_missing_row_remains_retryable_and_does_not_remove_other_instrument():
    e, a = frames()
    result = runner.derive_futoi_eligibility(e, a.iloc[:1], DAY)
    assert result.futoi_eligible.tolist() == [True, False]
    assert result.loc[1, "futoi_deferral_reason"] == "missing_futoi_availability_row"
    assert bool(result.loc[1, "futoi_retry_required"])


@pytest.mark.parametrize("target,field,value", [
    ("availability", "snapshot_date", "2026-09-17"),
    ("availability", "snapshot_date", None),
    ("availability", "board", "OTHER"),
    ("availability", "family_code", "CR"),
    ("availability", "secid", "SIZ6"),
    ("eligibility", "registry_snapshot_date", "2026-09-17"),
    ("eligibility", "eligibility_snapshot_date", None),
    ("eligibility", "registry_snapshot_id", None),
])
def test_misbound_evidence_is_structural_failure(target, field, value):
    e, a = frames()
    (a if target == "availability" else e).loc[0, field] = value
    with pytest.raises(RuntimeError, match="Canonical FUTOI availability validation failed"):
        runner.derive_futoi_eligibility(e, a, DAY)


@pytest.mark.parametrize("target", ["availability", "eligibility"])
@pytest.mark.parametrize("alias", [False, True])
def test_duplicate_identity_is_not_resolved_by_last_row(target, alias):
    e, a = frames()
    source = a if target == "availability" else e
    duplicate = source.iloc[:1].copy()
    if alias:
        duplicate.loc[0, "secid"] = "SIZ6"
    combined = pd.concat([source, duplicate], ignore_index=True)
    with pytest.raises(RuntimeError, match="ambiguous_secid"):
        runner.derive_futoi_eligibility(
            combined if target == "eligibility" else e,
            combined if target == "availability" else a, DAY)


def test_excluded_and_deferred_generic_rows_are_not_promoted():
    e, a = frames()
    e["classification_status"] = ["excluded", "deferred"]
    result = runner.derive_futoi_eligibility(e, a, DAY)
    assert not result.futoi_eligible.any()
    assert not result.futoi_retry_required.any()
    assert result.futoi_check_status.tolist() == ["not_applicable_not_included"] * 2
    with pytest.raises(RuntimeError, match="No eligibility_snapshot rows"):
        runner.selected_universe(result)


def test_scope_filter_preserves_deferred_target():
    e, a = frames()
    a.loc[1, ["availability_status", "observed_rows"]] = ["unavailable", 0]
    derived = runner.derive_futoi_eligibility(e, a, DAY)
    scoped = runner.apply_scope_filters(derived, ["USDRUBF"], [])
    assert scoped.secid.tolist() == ["USDRUBF"]
    assert runner.selected_universe(scoped, allow_empty=True).empty
    with pytest.raises(RuntimeError, match="scope filters"):
        runner.apply_scope_filters(derived, ["DOES_NOT_EXIST"], [])


def test_empty_chunk_does_not_query_dates_or_positions(monkeypatch, tmp_path):
    def denied(*args, **kwargs):
        raise AssertionError("source request for all-deferred stage")
    monkeypatch.setattr(runner.base, "fetch_observed_trading_dates", denied)
    monkeypatch.setattr(runner, "run_instrument", denied)
    e, a = frames()
    a["availability_status"], a["observed_rows"] = "unavailable", 0
    derived = runner.derive_futoi_eligibility(e, a, DAY)
    empty = runner.selected_universe(derived, allow_empty=True)
    manifest, quality = runner.run_chunk(
        SimpleNamespace(exact_contract_only=False), tmp_path, empty, "run", "chunk")
    manifest, quality = runner.record_availability_outcomes(
        manifest, quality, derived, "run", "chunk")
    assert manifest["status"] == "deferred"
    assert manifest["secid_list"] == [] and manifest["output_partitions"] == []
    assert manifest["deferred_secid"] == ["SiZ6", "USDRUBF"]
    assert manifest["date_from"] is None and manifest["date_till"] is None
    assert manifest["futoi_coverage_complete"] is False
    assert quality.quality_status.tolist() == ["deferred", "deferred"]
    assert quality.rows_written.tolist() == [0, 0]


@pytest.mark.parametrize("initial", ["succeeded", "partial_failed", "failed"])
def test_retryable_availability_never_turns_failed_chunk_into_success(initial):
    e, a = frames()
    a.loc[1, ["availability_status", "observed_rows", "error_code"]] = ["error", 0, "timeout"]
    derived = runner.derive_futoi_eligibility(e, a, DAY)
    bad = [] if initial == "succeeded" else ["SiZ6"]
    manifest = dict(status=initial, secid_list=["SiZ6"], failed_secid=bad,
                    output_partitions=["preserved.parquet"] if not bad else [])
    quality = pd.DataFrame([dict(secid="SiZ6", quality_status="pass" if not bad else "fail")])
    result, report = runner.record_availability_outcomes(manifest, quality, derived, "run", "chunk")
    assert result["status"] == ("partial_failed" if not bad else "failed")
    assert "USDRUBF" in result["failed_secid"]
    assert result["availability_retry_secid"] == ["USDRUBF"]
    assert report.iloc[-1].quality_status == "fail"

@pytest.fixture
def real_chain(monkeypatch, tmp_path, capsys):
    # Reuse the accepted #544 fixture, not substitutes for loaders or Parquet.
    spec = importlib.util.spec_from_file_location(
        "futoi_isolation_chain_fixture", ROOT / "tests" / "test_futures_refresh_observed_date_integration.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    def denied(*args, **kwargs):
        raise AssertionError("live network forbidden")
    monkeypatch.setattr(requests.sessions.Session, "request", denied)
    for module in (helper.eligibility, helper.futoi, helper.universal):
        monkeypatch.setattr(module, "load_dotenv", None)
    monkeypatch.setenv("MOEX_API_KEY", "synthetic-test-token")
    monkeypatch.setenv("MOEX_API_URL", helper.APIM)
    monkeypatch.setenv("MOEX_ISS_BASE_URL", helper.ISS)
    args, esummary, _, manifest, dates, calls, inputs = helper.connected_run(
        monkeypatch, tmp_path, capsys)
    # Preserve raw bytes for the instrument which is about to become deferred.
    preserved = {
        Path(path): Path(path).read_bytes()
        for path in manifest["output_partitions"]
        if pd.read_parquet(path)["secid"].eq("USDRUBF").all()
    }
    assert preserved
    dates.clear()
    calls.clear()
    availability_path = helper.futoi.resolve_availability_path(ROOT, tmp_path, DAY)
    return helper, args, esummary, availability_path, dates, calls, preserved, inputs


@pytest.mark.parametrize("outcome", ["unavailable", "error", "missing"])
def test_real_parquet_mixed_outcome_loads_other_instrument_and_preserves_prior_raw(
    monkeypatch, tmp_path, capsys, real_chain, outcome,
):
    import json
    helper, args, esummary, availability_path, dates, calls, preserved, inputs = real_chain
    evidence = pd.read_parquet(availability_path)
    mask = evidence.secid == "USDRUBF"
    if outcome == "missing":
        evidence = evidence.loc[~mask].copy()
    else:
        evidence.loc[mask, "availability_status"] = outcome
        evidence.loc[mask, "observed_rows"] = 0
        if outcome == "error":
            evidence.loc[mask, "error_code"] = "timeout"
    evidence.to_parquet(availability_path, index=False)
    before = {path: path.read_bytes() for path in inputs}
    code = helper.invoke(monkeypatch, runner, "futoi_raw_refresh", args)
    summary = json.loads(capsys.readouterr().out)
    manifest = json.loads(Path(summary["outputs"]["chunk_manifest"]).read_text())
    assert code == (0 if outcome == "unavailable" else 1)
    assert manifest["status"] == ("succeeded" if outcome == "unavailable" else "partial_failed")
    assert manifest["secid_list"] == ["SiZ6"]
    assert manifest["deferred_secid"] == ["USDRUBF"]
    assert manifest["availability_retry_secid"] == ([] if outcome == "unavailable" else ["USDRUBF"])
    assert manifest["futoi_coverage_complete"] is False
    assert len(dates) == 1 and len(calls) == 1
    assert calls[0][1].endswith("/si.json")
    assert all(path.read_bytes() == content for path, content in preserved.items())
    assert all(path.read_bytes() == content for path, content in before.items())
    quality = pd.read_parquet(summary["outputs"]["quality_report"])
    assert set(quality.secid) == {"SiZ6", "USDRUBF"}
    assert quality.loc[quality.secid == "USDRUBF", "rows_written"].item() == 0
    assert summary["aggregate_report"]["scope_deferred_secid_count"] == 1


@pytest.mark.parametrize("explicit_scope", [False, True])
def test_real_all_deferred_writes_diagnostics_without_sources(
    monkeypatch, tmp_path, capsys, real_chain, explicit_scope,
):
    import json
    helper, args, _, availability_path, dates, calls, preserved, _ = real_chain
    evidence = pd.read_parquet(availability_path)
    mask = evidence.secid == "USDRUBF" if explicit_scope else evidence.secid.notna()
    evidence.loc[mask, "availability_status"] = "unavailable"
    evidence.loc[mask, "observed_rows"] = 0
    evidence.to_parquet(availability_path, index=False)
    if explicit_scope:
        args.secid = "USDRUBF"
    code = helper.invoke(monkeypatch, runner, "futoi_raw_refresh", args)
    summary = json.loads(capsys.readouterr().out)
    manifest = json.loads(Path(summary["outputs"]["chunk_manifest"]).read_text())
    assert code == 1 and manifest["status"] == "deferred"
    assert not dates and not calls
    assert manifest["output_partitions"] == [] and manifest["secid_list"] == []
    expected = ["USDRUBF"] if explicit_scope else ["SiZ6", "USDRUBF"]
    assert manifest["deferred_secid"] == expected
    assert not pd.read_parquet(summary["outputs"]["futoi_eligibility_snapshot"]).empty
    report = pd.read_parquet(summary["outputs"]["quality_report"])
    assert set(report.quality_status) == {"deferred"}
    assert report.rows_written.sum() == 0
    assert all(path.read_bytes() == content for path, content in preserved.items())


def test_real_fetch_failure_does_not_exit_zero_or_overwrite_failed_raw(
    monkeypatch, tmp_path, capsys, real_chain,
):
    import json
    helper, args, _, _, dates, calls, preserved, _ = real_chain
    original_request = helper.base.request_json
    def request(origin, path, params, timeout, use_apim):
        if "/usdrubf." in path:
            raise RuntimeError("synthetic FUTOI source failure")
        return original_request(origin, path, params, timeout, use_apim)
    monkeypatch.setattr(helper.base, "request_json", request)
    code = helper.invoke(monkeypatch, runner, "futoi_raw_refresh", args)
    summary = json.loads(capsys.readouterr().out)
    manifest = json.loads(Path(summary["outputs"]["chunk_manifest"]).read_text())
    assert code == 1 and manifest["status"] == "partial_failed"
    assert manifest["failed_secid"] == ["USDRUBF"]
    assert manifest["deferred_secid"] == []
    assert manifest["output_partitions"]
    assert all(path.read_bytes() == content for path, content in preserved.items())
