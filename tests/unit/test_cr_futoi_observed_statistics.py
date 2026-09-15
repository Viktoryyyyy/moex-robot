"""CR-only statistics scope and neutral-kernel integration regressions."""
from copy import deepcopy
from datetime import date, timedelta
from hashlib import sha256
import json
from pathlib import Path
import runpy
import subprocess
import sys

import pytest

from moex_data import rub_cr_futoi_observed_statistics as stats
from moex_data import rub_cr_futoi_dated_context as dated

H = runpy.run_path(str(Path(__file__).with_name("test_cr_futoi_dated_comparisons.py")))
NOW = H["NOW"]


def grant():
    raw = (Path(__file__).resolve().parents[2]/stats.CONTRACT).read_bytes()
    return {"contract_ref": stats.CONTRACT, "artifact_text": raw.decode(), "artifact_sha256": sha256(raw).hexdigest()}


def snapshot(count=504, missing=()):
    s = H["snapshot"](); anchor = s[dated.STORE_KEY]["evidence"]["records"][-1]["factual"]
    end = date.fromisoformat(anchor["trade_date"])
    slots = [(end-timedelta(days=i)).isoformat() for i in reversed(range(count))]
    proof = {"source_kind": "canonical_raw", "provenance": {"raw_partition_ref": "${MOEX_DATA_ROOT}/test/cr.parquet", "raw_partition_sha256": "a"*64,
        "source_id": dated.SOURCE, "factual_validation": "PASS"}}
    proof_id = stats.common._digest(proof); rows = []
    for i, day in enumerate(slots):
        fact = deepcopy(anchor) if day == end.isoformat() else H["fact"](day, 100+i%100)
        row = stats.core._encode_row(fact, proof_id)
        if i in missing: row.update(status="UNAVAILABLE", values=None, clocks=None, proof_id=None, reason="invalid_latest_raw_no_fallback")
        rows.append(row)
    e = {"schema_version": stats.SCHEMA, "instrument_id": stats.PROFILE.instrument_id, "source_id": dated.SOURCE, "policy": stats.POLICY,
        "admission_at_acceptance": grant(), "linked_dated_evidence_sha256": s[dated.STORE_KEY]["evidence_sha256"],
        "accepted_at_utc": NOW.isoformat(), "causal_cutoff_at_utc": NOW.isoformat(), "columns": list(stats.core.COLUMNS), "slots": slots, "rows": rows,
        "proofs": {proof_id: proof} if any(r["proof_id"] for r in rows) else {},
        "observed_date_witness": {"observed_trade_dates": slots, "previous_observed_trade_date": end.isoformat(), "current_observed_trade_date": "2026-09-14",
            "provenance": deepcopy(s[dated.STORE_KEY]["evidence"]["witness"]["provenance"])}}
    s[stats.GRANT_KEY] = grant()
    s[stats.STORE_KEY] = {"schema_version": stats.SCHEMA, "evidence": e, "evidence_sha256": stats.common._digest(e),
        "last_capture_attempt_at_utc": NOW.isoformat(), "last_capture_error": None, "first_anchor_rejection": None,
        "latest_sample_failures": [{"trade_date": r["trade_date"], "reason": r["reason"]} for r in rows if r["status"] == "UNAVAILABLE"]}
    return s


def rehash(s):
    s[stats.STORE_KEY]["evidence_sha256"] = stats.common._digest(s[stats.STORE_KEY]["evidence"])


def release(s, now=NOW):
    r = {"futoi_context": {"futoi_live": {"unchanged_si": True}, "futoi_live_cr": {"previous_observation": None, "comparisons": None}}}
    stats.attach_consumer(s, r, now=now)
    return r


def verify(s, r=None, now=NOW):
    stats.verify_projection(s, r or release(s, now), now=now)


def test_exact_sparse_windows_and_current_independent_refusal():
    s = snapshot(missing=(10, 100, 300, 400)); before = deepcopy(s)
    r = release(s); out = r["futoi_context"]["futoi_live_cr"]["observed_statistics"]
    assert out["status"] == "AVAILABLE" and out["current"]["status"] == "UNAVAILABLE"
    assert out["dated"]["windows"]["252"]["sample_count"] == 250
    assert out["dated"]["windows"]["504"]["sample_count"] == 500
    assert out["dated"]["windows"]["504"]["coverage_status"] == "PARTIAL"
    assert len(out["dated"]["windows"]["504"]["excluded_dates"]) == 4
    assert out["dated"]["anchor_values"]["fiz.gross_share_of_two_sided_oi"] == pytest.approx((4000+3880)/20000)
    assert out["dated"]["statistical_semantics"]["percentile_formula"] == "count(sample_value <= anchor_value) / sample_count"
    assert "rows" not in json.dumps(out) and "input_snapshot.json#/accepted_cr_observed_statistics/evidence" == out["audit_reference"]
    assert r["futoi_context"]["futoi_live"] == {"unchanged_si": True}
    assert r["futoi_context"]["futoi_live_cr"]["comparisons"] is None
    verify(s, r); assert s == before


@pytest.mark.parametrize("count", [0, 1, 2, 251, 252, 504, 505])
def test_exact_window_bounds_and_minimum(count):
    s = snapshot(count); out = stats.describe(s, now=NOW)
    if count in (0, 505): assert out["status"] == "UNAVAILABLE"
    else:
        metric = out["dated"]["windows"]["504"]["variables"]["total_open_interest"]
        assert metric["status"] == ("UNAVAILABLE" if count == 1 else "AVAILABLE")
        assert metric["zscore"] is None
        if count >= 2: assert metric["zscore_reason"] == "zero_population_variance"
    verify(s)


def test_current_uses_a3_proof_and_shifts_exact_slots(monkeypatch):
    s = snapshot(); H["install_current"](s, monkeypatch)
    r = release(s); out = r["futoi_context"]["futoi_live_cr"]["observed_statistics"]
    assert out["current"]["status"] == "AVAILABLE"
    assert out["current"]["current_pair_usable_at_read"] is True and out["dated"]["current_pair_usable_at_read"] is False
    assert out["current"]["changes"]["20"]["target_trade_date"] == s[stats.STORE_KEY]["evidence"]["slots"][-20]
    assert out["dated"]["changes"]["20"]["target_trade_date"] == s[stats.STORE_KEY]["evidence"]["slots"][-21]
    verify(s, r)
    s.pop(dated.CURRENT_KEY)
    assert stats.describe(s, now=NOW)["current"]["status"] == "UNAVAILABLE"
    verify(s)


def test_missing_exact_lag_never_substituted():
    s = snapshot(30, missing=(24,)); out = stats.describe(s, now=NOW)
    assert out["dated"]["changes"]["5"]["target_trade_date"] == s[stats.STORE_KEY]["evidence"]["slots"][24]
    assert out["dated"]["changes"]["5"]["values"] is None
    verify(s)


@pytest.mark.parametrize("mutation", ["instrument", "source", "schema", "policy", "link", "slot_order", "proof_hash", "proof_kind", "clock", "clock_missing", "counts", "anchor_version", "extra_row"])
def test_rehashed_malformed_evidence_refused(mutation):
    s = snapshot(30); e = s[stats.STORE_KEY]["evidence"]
    if mutation == "instrument": e["instrument_id"] = "si_futures_family"
    elif mutation == "source": e["source_id"] = "alien"
    elif mutation == "schema": e["schema_version"] = "other"
    elif mutation == "policy": e["policy"] = "older_slot_fallback"
    elif mutation == "link": e["linked_dated_evidence_sha256"] = "0"*64
    elif mutation == "slot_order": e["slots"] = list(reversed(e["slots"]))
    elif mutation == "proof_hash": next(iter(e["proofs"].values()))["provenance"]["raw_partition_sha256"] = "0"*64
    elif mutation == "proof_kind": next(iter(e["proofs"].values()))["source_kind"] = "research_only"
    elif mutation == "clock": e["rows"][0]["clocks"]["ingest_ts_utc"] = (NOW+timedelta(seconds=1)).isoformat()
    elif mutation == "clock_missing": e["rows"][0]["clocks"]["source_publication_time"] = None
    elif mutation == "counts": e["rows"][0]["values"][0] = True
    elif mutation == "anchor_version": e["rows"][-1]["clocks"]["source_publication_time"] = "2026-09-11T20:50:02+00:00"
    elif mutation == "extra_row": e["rows"].append(deepcopy(e["rows"][0]))
    rehash(s); assert stats.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("target", ["current_grant", "retained_grant", "a3_grant"])
def test_separate_grants_and_revocation(target):
    s = snapshot(30)
    value = s[stats.GRANT_KEY] if target == "current_grant" else s[stats.STORE_KEY]["evidence"]["admission_at_acceptance"] if target == "retained_grant" else s[dated.ADMISSION_KEY]
    doc = json.loads(value["artifact_text"])
    doc["admission"]["descriptive_statistics_allowed" if target != "a3_grant" else "dated_factual_use_allowed"] = False
    value["artifact_text"] = json.dumps(doc); value["artifact_sha256"] = sha256(value["artifact_text"].encode()).hexdigest()
    rehash(s); assert stats.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("now", [None, "invalid", "2026-09-14T17:30:00", NOW-timedelta(microseconds=1), NOW+timedelta(days=5)])
def test_clock_refusals(now):
    s = snapshot(30); assert stats.describe(s, now=now)["status"] == "UNAVAILABLE"; verify(s, now=now)


@pytest.mark.parametrize("mutation", ["omit", "scope", "minimum", "formula", "holes", "count", "value", "extra", "current_forgery"])
def test_independent_oracle_rejects_projection_tampering(mutation):
    s = snapshot(30, missing=(4,)); r = release(s); parent = r["futoi_context"]["futoi_live_cr"]; out = parent["observed_statistics"]
    if mutation == "omit": parent.pop("observed_statistics")
    elif mutation == "scope": out["scope"] = "STATISTICAL_CONFIDENCE"
    elif mutation == "minimum": out["minimum_sample_count"] = 1
    elif mutation == "formula": out["dated"]["statistical_semantics"]["standard_deviation_ddof"] = 1
    elif mutation == "holes": out["dated"]["windows"]["504"]["excluded_dates"] = []
    elif mutation == "count": out["dated"]["windows"]["504"]["sample_count"] = 30
    elif mutation == "value": out["dated"]["windows"]["504"]["variables"]["fiz.net"]["percentile"] = 0.777
    elif mutation == "extra": out["dated"]["anchor_values"]["invented"] = 1
    elif mutation == "current_forgery": out["current"] = {"status": "AVAILABLE", "invented_fact": 1}
    with pytest.raises((AssertionError, KeyError, TypeError)): verify(s, r)


def test_shared_producer_faults_do_not_define_oracle(monkeypatch):
    s = snapshot(30); original = stats._summary
    def broken(*args):
        out = original(*args); out["windows"]["504"]["variables"]["fiz.net"]["population_mean"] = 777
        return out
    monkeypatch.setattr(stats, "_summary", broken)
    with pytest.raises(AssertionError): verify(s)


def test_shared_current_adapter_cannot_invent_current_authority(monkeypatch):
    s = snapshot(30)
    monkeypatch.setattr(stats, "_current", lambda *a: H["fact"]("2026-09-14", 121, True))
    with pytest.raises(AssertionError): verify(s)


@pytest.mark.parametrize("kind", ["same", "transient", "definitive", "publication_revision"])
def test_capture_retains_only_valid_original_semantics(monkeypatch, kind):
    previous = snapshot(30); candidate = deepcopy(previous[stats.STORE_KEY]["evidence"])
    if kind in ("transient", "definitive"):
        candidate["rows"][-6].update(status="UNAVAILABLE", values=None, clocks=None, proof_id=None,
            reason=dated.TRANSIENT_READ+"PermissionError: read denied" if kind == "transient" else "invalid_latest_raw_no_fallback")
    if kind == "publication_revision": candidate["rows"][0]["clocks"]["source_publication_time"] = candidate["rows"][0]["clocks"]["source_publication_time"].replace("20:50:01", "20:50:02")
    monkeypatch.setattr(stats, "_capture", lambda *a: deepcopy(candidate))
    current = deepcopy(previous); before_a3 = deepcopy(previous[dated.STORE_KEY]); times = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    completed = stats.capture_snapshot(current, previous, now_fn=lambda: next(times), refresh_started_at=NOW)
    assert current[dated.STORE_KEY] == before_a3
    if kind in ("same", "transient"): assert current[stats.STORE_KEY]["evidence_sha256"] == previous[stats.STORE_KEY]["evidence_sha256"]
    else: assert current[stats.STORE_KEY]["evidence_sha256"] != previous[stats.STORE_KEY]["evidence_sha256"]
    if kind == "transient": assert "read denied" in json.dumps(stats.describe(current, now=completed)["latest_sample_failures"])
    if kind == "definitive": assert stats.describe(current, now=completed)["dated"]["changes"]["5"]["values"] is None
    verify(current, now=completed)


@pytest.mark.parametrize("source_revision", [False, True])
def test_exclusion_diagnostic_only_change_preserves_original_acceptance(monkeypatch, source_revision):
    previous = snapshot(30, missing=(4,))
    candidate = deepcopy(previous[stats.STORE_KEY]["evidence"])
    candidate["rows"][4]["reason"] = "latest validation diagnostic wording changed"
    if source_revision:
        proof = {"source_kind": "excluded_raw", "provenance": {
            "raw_partition_ref": "${MOEX_DATA_ROOT}/test/revised-cr.parquet", "raw_partition_sha256": "b"*64}}
        key = stats.common._digest(proof)
        candidate["proofs"][key] = proof
        candidate["rows"][4]["proof_id"] = key
    monkeypatch.setattr(stats, "_capture", lambda *args: deepcopy(candidate))
    current = deepcopy(previous)
    ticks = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    completed = stats.capture_snapshot(current, previous, now_fn=lambda: next(ticks), refresh_started_at=NOW)
    old, new = previous[stats.STORE_KEY], current[stats.STORE_KEY]
    if source_revision:
        assert new["evidence_sha256"] != old["evidence_sha256"]
        assert new["evidence"]["accepted_at_utc"] == completed.isoformat()
    else:
        assert new["evidence"] == old["evidence"]
        assert new["evidence_sha256"] == old["evidence_sha256"]
    assert stats.describe(current, now=completed)["latest_sample_failures"][0]["reason"] == candidate["rows"][4]["reason"]
    verify(current, now=completed)


@pytest.mark.parametrize("first", [False, True])
def test_capture_failure_visible_without_rejuvenation(monkeypatch, first):
    previous = snapshot(30); current = deepcopy(previous)
    if first: previous.pop(stats.STORE_KEY); current.pop(stats.STORE_KEY)
    def fail(*a): raise PermissionError("statistics source read denied")
    monkeypatch.setattr(stats, "_capture", fail)
    ticks = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    completed = stats.capture_snapshot(current, previous, now_fn=lambda: next(ticks), refresh_started_at=NOW)
    out = stats.describe(current, now=completed)
    assert "statistics source read denied" in out["latest_capture_failure"]["error"]
    if first: assert stats.STORE_KEY not in current and out["status"] == "UNAVAILABLE"
    else: assert current[stats.STORE_KEY]["evidence_sha256"] == previous[stats.STORE_KEY]["evidence_sha256"]
    verify(current, now=completed)


def test_capture_clock_reversal_is_atomic(monkeypatch):
    s = snapshot(30); before = deepcopy(s)
    monkeypatch.setattr(stats, "_capture", lambda *a: deepcopy(s[stats.STORE_KEY]["evidence"]))
    ticks = iter([NOW+timedelta(seconds=2), NOW+timedelta(seconds=1)])
    with pytest.raises(ValueError, match="completion_clock_reversed"):
        stats.capture_snapshot(s, before, now_fn=lambda: next(ticks), refresh_started_at=NOW)
    assert s == before


def test_invalid_anchor_after_a3_capture_never_restores_old_statistics(monkeypatch):
    previous = snapshot(30); valid = deepcopy(previous[stats.STORE_KEY]["evidence"]); candidate = deepcopy(valid)
    candidate["rows"][-1].update(status="UNAVAILABLE", values=None, clocks=None, proof_id=None, reason="new_latest_raw_pair_invalid")
    monkeypatch.setattr(stats, "_capture", lambda *a: deepcopy(candidate))
    def refresh(prior, offset):
        result = deepcopy(prior); ticks = iter([NOW+timedelta(seconds=offset), NOW+timedelta(seconds=offset+1)])
        stats.capture_snapshot(result, prior, now_fn=lambda: next(ticks), refresh_started_at=NOW)
        return result
    first = refresh(previous, 1); second = refresh(first, 4)
    assert second[stats.STORE_KEY]["evidence_sha256"] == previous[stats.STORE_KEY]["evidence_sha256"]
    assert second[stats.STORE_KEY]["first_anchor_rejection"] == first[stats.STORE_KEY]["first_anchor_rejection"]
    assert stats.describe(second, now=NOW+timedelta(seconds=3))["status"] == "UNAVAILABLE"
    verify(second, now=NOW+timedelta(seconds=3))
    candidate = valid
    recovered = refresh(second, 6)
    assert recovered[stats.STORE_KEY]["first_anchor_rejection"] is None
    assert stats.describe(recovered, now=NOW+timedelta(seconds=7))["status"] == "AVAILABLE"
    verify(recovered, now=NOW+timedelta(seconds=7))


def test_optimized_oracle_refuses_omission():
    code = "from moex_data import rub_cr_futoi_observed_statistics as c; c.verify_projection({}, {'futoi_context': {'futoi_live_cr': {}}}, now=None)"
    result = subprocess.run([sys.executable, "-O", "-c", code], capture_output=True, text=True)
    assert result.returncode != 0 and "CR statistics canonical refusal" in result.stderr


@pytest.mark.parametrize("invalid_latest", [False, True])
def test_source_row_is_bound_to_actual_frozen_latest_raw_bytes(monkeypatch, tmp_path, invalid_latest):
    import pandas as pd
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    day = "2026-09-11"; path = tmp_path/"raw.parquet"
    rows = [dict(trade_date=day, ts=day+" 23:50:00", systime=day+" 23:55:00", availability_ts_utc=day+"T20:56:00+00:00", ingest_ts=day+"T20:57:00+00:00",
        sess_id=1, seqnum=1, clgroup=side, pos=net, pos_long=long, pos_short=short, pos_long_num=10, pos_short_num=11,
        source_id=dated.SOURCE, instrument_id=stats.PROFILE.instrument_id, source_ticker="cr", secid="CRU6")
        for side, net, long, short in (("FIZ",20,100,-80),("YUR",-20,80,-100))]
    if invalid_latest:
        later = deepcopy(rows)
        for r in later: r.update(ts=day+" 23:51:00", seqnum=2)
        later[1].update(pos_long=81, pos=-19); rows += later
    pd.DataFrame(rows).to_parquet(path); original_hash = sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(engine.raw_materializer, "_partition_path", lambda *a: path)
    monkeypatch.setattr(source, "source_identity", lambda *a: {"source_ticker": "cr", "secid": "CRU6"})
    row, proof = stats._source_row(tmp_path, day, None, None, None, NOW)
    assert row["status"] == ("UNAVAILABLE" if invalid_latest else "AVAILABLE")
    assert proof["source_kind"] == ("excluded_raw" if invalid_latest else "canonical_raw")
    frozen = tmp_path/proof["provenance"]["raw_partition_ref"][len("${MOEX_DATA_ROOT}/"):]
    assert sha256(frozen.read_bytes()).hexdigest() == proof["provenance"]["raw_partition_sha256"] == original_hash
    assert sha256(path.read_bytes()).hexdigest() == original_hash


def test_invalid_latest_does_not_become_transient_when_proof_copy_fails(monkeypatch, tmp_path):
    from moex_data.futures import futoi_delta_statistics_context as engine
    monkeypatch.setattr(engine, "_raw_factual", lambda *a, **k: {"status": "UNAVAILABLE", "reason": "invalid_latest_net_balance",
        "provenance": {"raw_partition_ref": "${MOEX_DATA_ROOT}/raw.parquet", "raw_partition_sha256": "a"*64}})
    def denied(*a): raise PermissionError("proof copy denied")
    monkeypatch.setattr(stats.common, "_check_source_refs", denied)
    row, proof = stats._source_row(tmp_path, "2026-09-11", None, None, None, NOW)
    assert row["status"] == "UNAVAILABLE" and not row["reason"].startswith(dated.TRANSIENT_READ)
    assert "invalid_latest_net_balance" in row["reason"] and "proof copy denied" in row["reason"]


def test_eod_step9_wrapped_read_error_keeps_transient_classification(monkeypatch, tmp_path):
    from moex_data import step9_rub_analysis_bundle as step9
    from moex_data.futures import futoi_delta_statistics_context as engine
    pointer = tmp_path/"pointer.json"; pointer.write_text("{}")
    read_text = Path.read_text
    def denied(path, *args, **kwargs):
        if path == pointer: raise PermissionError("EOD read denied")
        return read_text(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", denied)
    with pytest.raises(step9.Step9AnalysisBundleError) as caught: step9._load_json(pointer, "accepted_eod")
    monkeypatch.setattr(engine, "_raw_factual", lambda *a, **k: {"status": "UNAVAILABLE", "reason": "canonical_raw_partition_missing"})
    row, proof = stats._source_row(tmp_path, "2026-09-11", None, None, caught.value, NOW)
    assert row["status"] == "UNAVAILABLE" and row["reason"].startswith(dated.TRANSIENT_READ)
    assert "EOD read denied" in row["reason"] and proof is None


def test_frozen_selected_fact_mismatch_is_definitive(monkeypatch, tmp_path):
    from moex_data.futures import futoi_delta_statistics_context as engine
    original = H["record"]("2026-09-11")
    monkeypatch.setattr(engine, "_raw_factual", lambda *a, **k: deepcopy(original))
    monkeypatch.setattr(stats.common, "_freeze_raw_fact", lambda *a, **k: (H["fact"]("2026-09-11", 101), original["provenance"]))
    row, proof = stats._source_row(tmp_path, "2026-09-11", None, None, None, NOW)
    assert row["status"] == "UNAVAILABLE" and "frozen_raw_fact_mismatch" in row["reason"]
    assert not row["reason"].startswith(dated.TRANSIENT_READ) and proof is None
