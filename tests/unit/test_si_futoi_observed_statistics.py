"""Observed-slot subset, byte binding and independent arithmetic regressions."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import json
import runpy

import pytest
from moex_data import rub_si_futoi_observed_statistics as stats
from moex_data import rub_si_futoi_dated_context as dated

HELPERS = runpy.run_path(str(Path(__file__).with_name("test_si_futoi_dated_comparisons.py")))
NOW, GOV = HELPERS["NOW"], HELPERS["GOV"]


def snapshot(count=504, missing=()):
    linked = dated.retain(None, HELPERS["candidate"](), now=NOW, governance=GOV)
    anchor = linked["evidence"]["anchor"]
    end = dated.date.fromisoformat(anchor["factual"]["trade_date"])
    slots = [(end-timedelta(days=i)).isoformat() for i in reversed(range(count))]
    proof = {"source_kind": "canonical_raw", "provenance": {
        "raw_partition_ref": "${MOEX_DATA_ROOT}/test/raw.parquet", "raw_partition_sha256": "a"*64,
        "factual_validation": "PASS", "source_id": dated.SOURCE}}
    proof_id = dated._digest(proof)
    rows = []
    for i, day in enumerate(slots):
        fact = HELPERS["factual"](day, 100+i%200)
        if day == slots[-1]: fact = anchor["factual"]
        row = stats._encode_row(fact, proof_id)
        if i in missing:
            row.update(status="UNAVAILABLE", values=None, clocks=None, proof_id=None, reason="invalid_latest_raw_no_fallback")
        rows.append(row)
    evidence = {"schema_version": stats.SCHEMA, "policy": stats.POLICY,
        "linked_dated_evidence_sha256": linked["evidence_sha256"], "accepted_at_utc": NOW.isoformat(),
        "causal_cutoff_at_utc": NOW.isoformat(), "columns": list(stats.COLUMNS), "slots": slots,
        "rows": rows, "proofs": {proof_id: proof}, "governance_at_acceptance": deepcopy(GOV),
        "observed_date_witness": {"observed_trade_dates": slots,
            "current_observed_trade_date": "2026-09-14", "previous_observed_trade_date": slots[-1],
            "provenance": deepcopy(linked["evidence"]["observed_date_witness"]["provenance"])}}
    return {dated.STORE_KEY: linked, "components": {"futoi_live": {"status": "UNAVAILABLE", "data": {"governance": deepcopy(GOV), "instrument_id": dated.INSTRUMENT, "source_id": dated.SOURCE}}},
        stats.STORE_KEY: {"schema_version": stats.SCHEMA, "evidence": evidence,
            "evidence_sha256": dated._digest(evidence), "last_capture_attempt_at_utc": NOW.isoformat(), "last_capture_error": None}}


def rehash(value):
    value[stats.STORE_KEY]["evidence_sha256"] = dated._digest(value[stats.STORE_KEY]["evidence"])


def release(value, now=NOW, comparisons=None):
    result = {"futoi_context": {"futoi_live": {"comparisons": comparisons}, "futoi_live_cr": {"unchanged": True}}}
    stats.attach_consumer(value, result, now=now)
    return result


def test_exact_504_slots_keep_hole_and_252_can_be_complete():
    value = snapshot(missing=(10,))
    result = release(value)
    view = result["futoi_context"]["futoi_live"]["observed_statistics"]
    assert view["status"] == "AVAILABLE", view
    windows = view["dated"]["windows"]
    assert windows["504"]["sample_count"] == 503
    assert windows["504"]["coverage_status"] == "PARTIAL"
    assert len(windows["504"]["excluded_dates"]) == 1
    assert windows["252"]["sample_count"] == 252
    assert windows["252"]["coverage_status"] == "COMPLETE"
    assert windows["504"]["variables"]["fiz.net"]["status"] == "AVAILABLE"
    assert result["futoi_context"]["futoi_live_cr"] == {"unchanged": True}
    stats.verify_projection(value, result, now=NOW)


@pytest.mark.parametrize("count", [1, 2, 251, 252, 503, 504])
def test_window_minimum_and_exact_boundaries(count):
    value = snapshot(count)
    result = release(value)
    window = result["futoi_context"]["futoi_live"]["observed_statistics"]["dated"]["windows"]["252"]
    assert window["sample_count"] == min(count, 252)
    assert window["variables"]["total_open_interest"]["status"] == ("AVAILABLE" if count >= 2 else "UNAVAILABLE")
    if count >= 2:
        assert window["variables"]["total_open_interest"]["zscore"] is None
        assert window["variables"]["total_open_interest"]["zscore_reason"] == "zero_population_variance"
    stats.verify_projection(value, result, now=NOW)


def test_exact_lag_hole_not_replaced_and_gross_denominator_is_two_sided():
    value = snapshot(missing=(498,))  # Exact lag five in 504 slots.
    view = release(value)["futoi_context"]["futoi_live"]["observed_statistics"]["dated"]
    assert view["changes"]["5"]["status"] == "UNAVAILABLE"
    assert view["changes"]["1"]["status"] == "AVAILABLE"
    assert view["anchor_values"]["fiz.gross"] == 7600
    assert view["anchor_values"]["fiz.gross_share_of_two_sided_oi"] == .38
    assert view["anchor_values"]["fiz.long_share_of_oi"] == .4
    assert "unique_participants" not in view["anchor_values"]


@pytest.mark.parametrize("defect", ["extra_row", "duplicate_date", "shift_date", "future_receipt", "bad_oi", "bad_proof", "extra_row_field", "changed_link", "future_acceptance", "metadata_object"])
def test_rehashed_malformed_evidence_is_refused(defect):
    value = snapshot()
    e = value[stats.STORE_KEY]["evidence"]
    if defect == "extra_row": e["rows"].append(deepcopy(e["rows"][0]))
    elif defect == "duplicate_date": e["slots"][1] = e["slots"][0]
    elif defect == "shift_date": e["rows"][5]["trade_date"] = e["slots"][4]
    elif defect == "future_receipt": e["rows"][5]["clocks"]["availability_ts_utc"] = (NOW+timedelta(seconds=1)).isoformat()
    elif defect == "bad_oi": e["rows"][5]["values"][0] = 0
    elif defect == "bad_proof": e["rows"][5]["proof_id"] = "bad"
    elif defect == "extra_row_field": e["rows"][5]["invented_fact"] = 4
    elif defect == "changed_link": e["linked_dated_evidence_sha256"] = "b"*64
    elif defect == "future_acceptance": e["accepted_at_utc"] = (NOW+timedelta(seconds=1)).isoformat()
    else: value[stats.STORE_KEY]["last_capture_error"] = {}
    rehash(value)
    result = release(value)
    assert result["futoi_context"]["futoi_live"]["observed_statistics"]["status"] == "UNAVAILABLE"
    stats.verify_projection(value, result, now=NOW)


@pytest.mark.parametrize("defect", ["whole", "window", "field", "dates", "extra", "scope", "gross", "zscore"])
def test_projection_tampering_and_omission_are_rejected(defect):
    value = snapshot()
    result = release(value)
    output = result["futoi_context"]["futoi_live"]["observed_statistics"]
    if defect == "whole": del result["futoi_context"]["futoi_live"]["observed_statistics"]
    elif defect == "window": del output["dated"]["windows"]["504"]
    elif defect == "field": del output["dated"]["anchor_values"]["fiz.gross"]
    elif defect == "dates": output["dated"]["windows"]["504"]["sample_dates"].pop(3)
    elif defect == "extra": output["dated"]["windows"]["504"]["invented_fact"] = 9
    elif defect == "scope": output["current_usable"] = True
    elif defect == "gross": output["dated"]["anchor_values"]["fiz.gross_share_of_two_sided_oi"] = .76
    else: output["dated"]["windows"]["504"]["variables"]["fiz.net"]["zscore"] = 99
    with pytest.raises(AssertionError): stats.verify_projection(value, result, now=NOW)


def test_shared_summary_arithmetic_fault_cannot_pass_independent_oracle(monkeypatch):
    original = stats._summary
    def wrong(*args):
        result = original(*args)
        result["windows"]["504"]["variables"]["fiz.net"]["population_mean"] += 1
        return result
    monkeypatch.setattr(stats, "_summary", wrong)
    value = snapshot()
    with pytest.raises(AssertionError, match="independent arithmetic"):
        stats.verify_projection(value, release(value), now=NOW)


@pytest.mark.parametrize("defect", ["minimum", "scope", "excluded", "history_count"])
def test_shared_metadata_fault_is_rejected_independently(monkeypatch, defect):
    if defect in ("minimum", "scope"):
        original = stats._render
        def wrong(*args):
            result = original(*args)
            result["minimum_sample_count" if defect == "minimum" else "scope"] = 1 if defect == "minimum" else "confidence"
            return result
        monkeypatch.setattr(stats, "_render", wrong)
    else:
        original = stats._summary
        def wrong(*args):
            result = original(*args)
            result["windows"]["504"]["excluded_dates" if defect == "excluded" else "missing_observed_history_slots"] = [] if defect == "excluded" else 99
            return result
        monkeypatch.setattr(stats, "_summary", wrong)
    value = snapshot(missing=(10,))
    with pytest.raises(AssertionError): stats.verify_projection(value, release(value), now=NOW)


def test_missing_store_cannot_leave_available_legacy_statistics():
    value = snapshot(); del value[stats.STORE_KEY]
    result = release(value, comparisons={})
    stats.verify_projection(value, result, now=NOW)
    result["futoi_context"]["futoi_live"]["comparisons"]["statistics"] = {"status": "AVAILABLE", "invented_value": 777}
    with pytest.raises(AssertionError, match="refused legacy"):
        stats.verify_projection(value, result, now=NOW)


def test_shared_current_admission_fault_is_rejected(monkeypatch):
    original = stats._render
    def wrong(*args):
        result = original(*args)
        result["current"] = {"status": "AVAILABLE", "invented_value": 777}
        return result
    monkeypatch.setattr(stats, "_render", wrong)
    value = snapshot()
    with pytest.raises(AssertionError, match="current refusal"):
        stats.verify_projection(value, release(value), now=NOW)


@pytest.mark.parametrize("clock", ["source_publication_time", "ingest_ts_utc"])
def test_raw_receipt_clock_cannot_be_deleted_from_retained_sample(clock):
    value = snapshot(30)
    value[stats.STORE_KEY]["evidence"]["rows"][3]["clocks"][clock] = None
    rehash(value)
    result = release(value)
    assert result["futoi_context"]["futoi_live"]["observed_statistics"]["status"] == "UNAVAILABLE"
    stats.verify_projection(value, result, now=NOW)


def test_current_and_dated_share_slots_and_legacy_statistics_are_replaced(monkeypatch):
    value = snapshot()
    fact = HELPERS["factual"]("2026-09-14", 450)
    fact.update(snapshot_ts=(NOW-timedelta(seconds=20)).isoformat(), source_publication_time=(NOW-timedelta(seconds=15)).isoformat(),
        availability_ts_utc=(NOW-timedelta(seconds=10)).isoformat(), ingest_ts_utc=(NOW-timedelta(seconds=5)).isoformat())
    component = value["components"]["futoi_live"]
    component.update(status="READY")
    component["data"].update(consumer_factual_use_allowed=True, factual_authority=True, current_intraday={"factual": fact})
    monkeypatch.setattr("moex_data.rub_snapshot_read_freshness.apply_read_freshness", lambda s, **k: deepcopy(s))
    result = release(value, comparisons={"statistics": {"legacy_gap": 26}})
    context = result["futoi_context"]["futoi_live"]
    current = context["observed_statistics"]["current"]
    assert current["status"] == "AVAILABLE"
    assert current["windows"]["504"]["sample_dates"][-1] == "2026-09-14"
    assert len(current["windows"]["504"]["sample_dates"]) == 504
    assert context["comparisons"]["statistics"] == current
    stats.verify_projection(value, result, now=NOW)


def test_compact_keeps_exact_dates_and_locatable_audit_without_full_rows():
    from moex_data.rub_factual_package import compact_values
    value = snapshot()
    compact = compact_values(release(value))
    output = compact["futoi_context"]["futoi_live"]["observed_statistics"]
    assert len(output["dated"]["windows"]["504"]["sample_dates"]) == 504
    assert output["audit_reference"].startswith("input_snapshot.json#/")
    assert '"rows"' not in json.dumps(output) and '"proofs"' not in json.dumps(output)


@pytest.mark.parametrize("failure", [False, True])
def test_capture_preserves_522_and_identical_statistics_first_acceptance(monkeypatch, failure):
    previous = snapshot()
    before = deepcopy(previous)
    def collect(*args, **kwargs):
        if failure: raise ValueError("source_failure")
        e = deepcopy(previous[stats.STORE_KEY]["evidence"])
        e["causal_cutoff_at_utc"] = kwargs["cutoff"].isoformat()
        return e
    monkeypatch.setattr(stats, "_capture", collect)
    current = deepcopy(previous)
    ticks = iter(NOW+timedelta(seconds=n) for n in (1, 3))
    completed = stats.capture_snapshot(current, previous, now_fn=lambda: next(ticks), refresh_started_at=NOW)
    assert completed == NOW+timedelta(seconds=3)
    assert current[dated.STORE_KEY] == before[dated.STORE_KEY]
    assert current[stats.STORE_KEY]["evidence"] == before[stats.STORE_KEY]["evidence"]
    assert current[stats.STORE_KEY]["evidence_sha256"] == before[stats.STORE_KEY]["evidence_sha256"]
    assert previous == before


@pytest.mark.parametrize("ticks", [(0, -1), (-1, 2)])
def test_statistics_clock_reversal_aborts_without_publication(monkeypatch, ticks):
    value = snapshot(); before = deepcopy(value)
    monkeypatch.setattr(stats, "_capture", lambda *a, **k: deepcopy(value[stats.STORE_KEY]["evidence"]))
    clock = iter(NOW+timedelta(seconds=n) for n in ticks)
    with pytest.raises(ValueError): stats.capture_snapshot(value, before, now_fn=lambda: next(clock), refresh_started_at=NOW)
    assert value == before


def test_optimized_python_still_rejects_false_statistics(tmp_path):
    import subprocess, sys
    value = snapshot(); result = release(value)
    result["futoi_context"]["futoi_live"]["observed_statistics"]["dated"]["anchor_values"]["fiz.gross"] = 1
    fixture = tmp_path / "stats.json"; fixture.write_text(json.dumps([value, result]))
    program = """import json,sys
from datetime import datetime
from moex_data.rub_si_futoi_observed_statistics import verify_projection
s,r=json.load(open(sys.argv[1]))
try: verify_projection(s,r,now=datetime.fromisoformat(sys.argv[2]))
except AssertionError: sys.exit(0)
sys.exit(7)
"""
    result = subprocess.run([sys.executable, "-O", "-c", program, str(fixture), NOW.isoformat()], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("defect", [None, "invalid_raw_with_good_eod", "loaded_fact_mismatch", "witness_mismatch"])
def test_capture_rederives_verified_raw_bytes_and_never_falls_back_from_invalid_raw(tmp_path, monkeypatch, defect):
    import pandas as pd
    from hashlib import sha256
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    value = snapshot(3)
    e = value[stats.STORE_KEY]["evidence"]
    monkeypatch.setattr(source, "_data_root", lambda: tmp_path)
    monkeypatch.setattr(source, "source_identity", lambda *a: {"source_ticker": "si", "secid": "SiU6"})
    monkeypatch.setattr(engine.raw_materializer, "_partition_path", lambda day, *a: tmp_path / (day + ".parquet"))
    for i, day in enumerate(e["slots"]):
        fact = stats._decode_row(e["rows"][i])
        rows = []
        for side, group in (("fiz", "FIZ"), ("yur", "YUR")):
            f = fact[side]
            rows.append(dict(trade_date=day, ts=day+" 23:50:00", systime=day+" 23:50:10",
                availability_ts_utc=fact["availability_ts_utc"], ingest_ts=fact["ingest_ts_utc"],
                sess_id=1, seqnum=1, clgroup=group, pos=f["net"], pos_long=f["long"], pos_short=-f["short"],
                pos_long_num=f["long_participants"], pos_short_num=f["short_participants"],
                source_id=dated.SOURCE, instrument_id=dated.INSTRUMENT, source_ticker="si", secid="SiU6"))
        if defect == "invalid_raw_with_good_eod" and i == 0: rows[0]["pos"] += 1
        pd.DataFrame(rows).to_parquet(tmp_path / (day + ".parquet"))
    def proof(name, frame):
        path = tmp_path / (name+".parquet"); frame.to_parquet(path)
        p = {"partition_ref": "${MOEX_DATA_ROOT}/"+path.name, "partition_sha256": sha256(path.read_bytes()).hexdigest()}
        (tmp_path/"manifest.json").write_text("{}")
        for key in ("manifest", "quality_report"):
            p[key+"_ref"] = "${MOEX_DATA_ROOT}/manifest.json"; p[key+"_sha256"] = sha256(b"{}").hexdigest()
        return p
    witness = {"observed_trade_dates": deepcopy(e["slots"]), "current_observed_trade_date": "2026-09-14",
        "previous_observed_trade_date": e["slots"][-1], "provenance": proof("dates", pd.DataFrame({"trade_date": e["slots"]}))}
    witness["provenance"]["acceptance_contract_id"] = "step7_rub_native_d1_w1_technical_acceptance.v1"
    if defect == "witness_mismatch": witness["observed_trade_dates"].pop(0)
    monkeypatch.setattr(engine, "_observed_witness", lambda *a, **k: deepcopy(witness))
    # A valid EOD candidate exists for the corrupt raw date, but must never be selected.
    eod_frame = pd.DataFrame({"trade_date": [e["slots"][0]], "instrument_id": [dated.INSTRUMENT]})
    eod_proof = proof("eod", eod_frame)
    monkeypatch.setattr(engine, "_accepted_eod", lambda *a, **k: (eod_frame, eod_proof))
    monkeypatch.setattr(engine, "_eod_factual", lambda *a, **k: pytest.fail("raw exists: EOD revision fallback forbidden"))
    if defect == "loaded_fact_mismatch":
        original = engine._raw_factual
        def mismatched(*a, **k):
            result = original(*a, **k)
            if k["trade_date"] == e["slots"][0]: result["factual"]["fiz"]["long_participants"] += 1
            return result
        monkeypatch.setattr(engine, "_raw_factual", mismatched)
    value["components"]["futoi_live"]["data"].update(context_refresh={}, previous_completed_session={})
    if defect == "witness_mismatch":
        with pytest.raises(ValueError, match="verified_observed_date"):
            stats._capture(value, cutoff=NOW)
    else:
        evidence = stats._capture(value, cutoff=NOW)
        assert evidence["rows"][0]["status"] == ("AVAILABLE" if defect is None else "UNAVAILABLE")
        assert evidence["rows"][-1]["status"] == "AVAILABLE"
        assert len(evidence["slots"]) == 3
        if defect == "loaded_fact_mismatch": assert "frozen_raw_fact_mismatch" in evidence["rows"][0]["reason"]
