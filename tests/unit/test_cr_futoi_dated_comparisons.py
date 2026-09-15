"""Synthetic CR scope, source integrity and portable read regressions."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys

import pytest

from moex_data import rub_cr_futoi_dated_context as cr

NOW = datetime(2026, 9, 14, 17, 30, tzinfo=timezone.utc)


def artifact():
    raw = (Path(__file__).resolve().parents[2] / cr.CONTRACT).read_bytes()
    return {"contract_ref": cr.CONTRACT, "artifact_text": raw.decode(), "artifact_sha256": sha256(raw).hexdigest()}


def proof(prefixes):
    return {key + suffix: ("${MOEX_DATA_ROOT}/synthetic/" + key if suffix == "_ref" else str(i+1)*64)
            for i, key in enumerate(prefixes) for suffix in ("_ref", "_sha256")}


def fact(day, net=100, current=False):
    event = NOW - timedelta(seconds=30) if current else datetime.fromisoformat(day + "T20:50:00+00:00")
    return {"trade_date": day, "snapshot_ts": event.isoformat(),
        "source_publication_time": (event+timedelta(seconds=1)).isoformat(),
        "availability_ts_utc": (NOW-timedelta(seconds=2)).isoformat(), "ingest_ts_utc": (NOW-timedelta(seconds=1)).isoformat(),
        "total_open_interest": 10000,
        "fiz": {"long": 4000, "short": 4000-net, "net": net, "long_participants": 40, "short_participants": 30},
        "yur": {"long": 6000, "short": 6000+net, "net": -net, "long_participants": 20, "short_participants": 25}}


def record(day, net=100, current=False):
    return cr._record(fact(day, net, current), "canonical_raw",
        {**proof(("raw_partition",)), "source_id": cr.SOURCE, "factual_validation": "PASS"}, day)


def snapshot():
    dates = [(datetime(2026, 8, 22) + timedelta(days=i)).date().isoformat() for i in range(21)]
    grant = artifact()
    e = {"schema_version": cr.SCHEMA, "instrument_id": cr.INSTRUMENT, "source_id": cr.SOURCE,
        "admission": deepcopy(grant), "accepted_at_utc": NOW.isoformat(), "causal_cutoff_at_utc": NOW.isoformat(),
        "witness": {"dates": dates, "previous_observed_trade_date": dates[-1], "current_observed_trade_date": "2026-09-14",
            "provenance": {**proof(("partition", "manifest", "quality_report")), "acceptance_contract_id": "step7_rub_native_d1_w1_technical_acceptance.v1"}},
        "records": [record(day, 100+i) for i, day in enumerate(dates)]}
    return {cr.ADMISSION_KEY: grant,
        cr.STORE_KEY: {"schema_version": cr.SCHEMA, "evidence": e, "evidence_sha256": cr.common._digest(e),
            "last_capture_attempt_at_utc": NOW.isoformat(), "last_capture_error": None, "latest_diagnostics": None, "latest_source_rejection": None},
        "components": {"futoi_live_cr": {"status": "UNAVAILABLE", "data": {"instrument_id": cr.INSTRUMENT, "source_id": cr.SOURCE}}}}


def rehash(s):
    s[cr.STORE_KEY]["evidence_sha256"] = cr.common._digest(s[cr.STORE_KEY]["evidence"])


def release(s, now=NOW):
    return {"futoi_context": {"futoi_live_cr": {"previous_observation": None, "comparisons": None,
        "scoped_observed_comparisons": cr.describe(s, now=now)}}}


def verify(s, r=None, now=NOW):
    cr.verify_projection(s, r or release(s, now), now=now)


def install_current(s, monkeypatch):
    from moex_data import rub_snapshot_read_freshness as freshness
    from moex_data.futures import futoi_publication_audit as audit
    r = record("2026-09-14", 121, True)
    original = proof(("raw_partition", "raw_quality_report", "raw_refresh_manifest"))
    report = {"schema_version": audit.SCHEMA, "policy": audit.POLICY, "instrument_id": cr.INSTRUMENT,
        "latest_status": "PASS", "latest_factual": deepcopy(r["factual"]), "provenance": deepcopy(original)}
    text = json.dumps(report); digest = sha256(text.encode()).hexdigest()
    original["publication_audit"] = {"ref": "${MOEX_DATA_ROOT}/synthetic/audit.json", "sha256": digest}
    s["components"]["futoi_live_cr"] = {"status": "READY", "data": {"instrument_id": cr.INSTRUMENT, "source_id": cr.SOURCE,
        "consumer_factual_use_allowed": True, "factual_authority": True,
        "current_intraday": {"factual": deepcopy(r["factual"]), "provenance": deepcopy(original)},
        "current_pair_admission": {"allowed": True, "scope": "current_intraday_latest_pair_only", "audit_sha256": digest}}}
    e = {"record": r, "captured_at_utc": NOW.isoformat(), "causal_cutoff_at_utc": NOW.isoformat(),
        "publication_audit": {"text": text, "sha256": digest}, "original_provenance": original}
    s[cr.CURRENT_KEY] = {"evidence": e, "evidence_sha256": cr.common._digest(e)}
    # Unit fixture isolates existing freshness; its real TTL/admission has separate integration coverage.
    monkeypatch.setattr(freshness, "apply_read_freshness", lambda value, now: deepcopy(value))


def test_exact_both_views_and_legacy_isolation(monkeypatch):
    s = snapshot(); install_current(s, monkeypatch); before = deepcopy(s)
    r = release(s); out = r["futoi_context"]["futoi_live_cr"]["scoped_observed_comparisons"]
    assert out["status"] == "AVAILABLE" and "current_usable" not in out
    for view in ("dated", "current"):
        assert out[view]["status"] == "AVAILABLE"
        for lag in (1, 5, 20):
            delta = out[view]["changes"][str(lag)]
            assert delta["values"]["fiz.net"] == lag
            assert delta["values"]["fiz.gross_share_of_two_sided_oi"] == pytest.approx(-lag/20000)
    assert out["dated"]["changes"]["20"]["target_trade_date"] == "2026-08-22"
    assert out["current"]["changes"]["20"]["target_trade_date"] == "2026-08-23"
    assert out["dated"]["current_pair_usable_at_read"] is False
    assert out["current"]["current_pair_usable_at_read"] is True
    assert r["futoi_context"]["futoi_live_cr"]["comparisons"] is None
    verify(s, r); assert s == before


def test_dated_survives_current_refusal_and_exact_missing_lag():
    s = snapshot(); e = s[cr.STORE_KEY]["evidence"]; missing = e["records"][-6]
    missing.update(status="UNAVAILABLE", source_kind=None, factual=None, provenance=None, reason="invalid_latest_raw_pair")
    rehash(s); out = cr.describe(s, now=NOW+timedelta(minutes=21))
    assert out["status"] == "AVAILABLE" and out["current"]["status"] == "UNAVAILABLE"
    assert out["dated"]["changes"]["5"]["target_trade_date"] == missing["trade_date"]
    assert out["dated"]["changes"]["5"]["values"] is None
    assert out["dated"]["changes"]["20"]["status"] == "AVAILABLE"
    verify(s, now=NOW+timedelta(minutes=21))


@pytest.mark.parametrize("now", [None, "invalid", "2026-09-14T17:30:00", NOW-timedelta(microseconds=1), NOW+timedelta(days=5)])
def test_read_clock_refuses_without_fallback(now):
    s = snapshot(); out = cr.describe(s, now=now)
    assert out["status"] == "UNAVAILABLE" and "dated" not in out
    verify(s, now=now)


@pytest.mark.parametrize("field", ["instrument_id", "source_id", "trade_date"])
def test_retained_identity_not_relabelled(field):
    s = snapshot(); s[cr.STORE_KEY]["evidence"]["records"][0][field] = "alien"
    rehash(s); assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("field", cr.CLOCKS)
@pytest.mark.parametrize("invalid", [None, "naive", "2026-09-14T17:30:01+00:00"])
def test_original_raw_clocks_required(field, invalid):
    s = snapshot(); s[cr.STORE_KEY]["evidence"]["records"][0]["factual"][field] = invalid
    rehash(s); assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("field,value", [("dated_factual_use_allowed", False), ("action_authority", True), ("maximum_dated_age_seconds", 999999)])
def test_live_scoped_revocation_blocks_old_self_hashed_grant(field, value):
    s = snapshot(); doc = json.loads(s[cr.ADMISSION_KEY]["artifact_text"]); doc["admission"][field] = value
    text = json.dumps(doc); s[cr.ADMISSION_KEY].update(artifact_text=text, artifact_sha256=sha256(text.encode()).hexdigest())
    assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("mutation", ["raw_digest", "audit_fact", "capture_future", "missing_witness", "missing_admission", "factual_mismatch"])
def test_current_byte_and_scope_refusals(monkeypatch, mutation):
    s = snapshot(); install_current(s, monkeypatch)
    e = s[cr.CURRENT_KEY]["evidence"]
    if mutation == "raw_digest": e["record"]["provenance"]["raw_partition_sha256"] = "f"*64
    elif mutation == "audit_fact":
        report = json.loads(e["publication_audit"]["text"]); report["latest_factual"]["total_open_interest"] = 1
        e["publication_audit"]["text"] = json.dumps(report)
    elif mutation == "capture_future": e["captured_at_utc"] = (NOW+timedelta(seconds=1)).isoformat()
    elif mutation == "missing_admission": s["components"]["futoi_live_cr"]["data"]["current_pair_admission"] = None
    elif mutation == "factual_mismatch": e["record"]["factual"]["fiz"]["net"] += 1
    s[cr.CURRENT_KEY]["evidence_sha256"] = cr.common._digest(e)
    if mutation == "missing_witness": s.pop(cr.CURRENT_KEY)
    out = cr.describe(s, now=NOW)
    assert out["dated"]["status"] == "AVAILABLE" and out["current"]["status"] == "UNAVAILABLE"
    verify(s)


@pytest.mark.parametrize("mutation", ["omit", "scope", "authority", "extra", "lag", "value", "current_false_alias"])
def test_independent_projection_rejects_tampering(mutation):
    s = snapshot(); r = release(s); parent = r["futoi_context"]["futoi_live_cr"]; out = parent["scoped_observed_comparisons"]
    if mutation == "omit": parent.pop("scoped_observed_comparisons")
    elif mutation == "scope": out["scope"] = "CURRENT_USABLE"
    elif mutation == "authority": out["action_authority"] = True
    elif mutation == "extra": out["dated"]["invented"] = 3
    elif mutation == "lag": out["dated"]["changes"].pop("20")
    elif mutation == "value": out["dated"]["changes"]["1"]["values"]["fiz.net"] += 2
    elif mutation == "current_false_alias": out["current"]["current_pair_usable_at_read"] = 0
    with pytest.raises((AssertionError, KeyError, TypeError)): verify(s, r)


def test_shared_producer_wrong_math_rejected(monkeypatch):
    original = cr._view
    def broken(*args):
        result = original(*args); result["changes"]["1"]["values"]["fiz.net"] = 555
        return result
    monkeypatch.setattr(cr, "_view", broken)
    with pytest.raises(AssertionError): verify(snapshot())


@pytest.mark.parametrize("where", ["anchor_fact", "anchor_side", "baseline_fact", "proof", "witness"])
def test_only_validated_nested_fields_are_projected(where):
    s = snapshot(); e = s[cr.STORE_KEY]["evidence"]
    targets = {"anchor_fact": e["records"][-1]["factual"], "anchor_side": e["records"][-1]["factual"]["fiz"],
        "baseline_fact": e["records"][-2]["factual"], "proof": e["records"][-1]["provenance"], "witness": e["witness"]["provenance"]}
    targets[where]["invented_trading_signal"] = "BUY"; rehash(s)
    out = cr.describe(s, now=NOW)
    assert out["status"] == "AVAILABLE" and "invented_trading_signal" not in json.dumps(out)
    verify(s)


def test_shared_public_record_extra_rejected(monkeypatch):
    original = cr._public_record
    def broken(record):
        result = original(record); result["factual"]["invented_signal"] = "BUY"
        return result
    monkeypatch.setattr(cr, "_public_record", broken)
    with pytest.raises(AssertionError): verify(snapshot())


@pytest.mark.parametrize("value", [10000.0, True, "10000"])
def test_non_integer_representation_never_admitted(value):
    s = snapshot(); s[cr.STORE_KEY]["evidence"]["records"][-1]["factual"]["total_open_interest"] = value
    rehash(s); assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("value", [True, 12, [], {}])
def test_invalid_retry_text_refuses(value):
    s = snapshot(); s[cr.STORE_KEY]["last_capture_error"] = value
    assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


def test_eod_original_optional_clocks_and_proof():
    s = snapshot(); r = s[cr.STORE_KEY]["evidence"]["records"][0]
    r["source_kind"] = "accepted_eod"
    r["factual"].update(source_publication_time=None, ingest_ts_utc=None)
    r["provenance"] = {"source_kind": "accepted_stage5_eod_historical_context_only", "accepted_pointer": {
        **proof(("partition", "manifest", "quality_report")), "acceptance_contract_id": "step5_futoi_positioning_acceptance.v1"}}
    rehash(s); assert cr.describe(s, now=NOW)["dated"]["changes"]["20"]["status"] == "AVAILABLE"; verify(s)


def test_invalid_latest_raw_never_uses_eod(monkeypatch):
    from moex_data.futures import futoi_delta_statistics_context as engine
    monkeypatch.setattr(engine, "_raw_factual", lambda *a, **k: {"status": "UNAVAILABLE", "reason": "latest_pair_net_imbalance"})
    monkeypatch.setattr(engine, "_eod_factual", lambda *a, **k: pytest.fail("invalid raw must not fall back"))
    result = cr._load_record(Path("."), "2026-09-11", object(), {}, NOW)
    assert result["status"] == "UNAVAILABLE" and "latest_pair_net_imbalance" in result["reason"]


def test_frozen_raw_must_match_selected_fact(monkeypatch):
    from moex_data.futures import futoi_delta_statistics_context as engine
    selected = record("2026-09-11")
    monkeypatch.setattr(engine, "_raw_factual", lambda *a, **k: deepcopy(selected))
    frozen = fact("2026-09-11", 101)
    monkeypatch.setattr(cr.common, "_freeze_raw_fact", lambda *a, **k: (frozen, selected["provenance"]))
    result = cr._load_record(Path("."), "2026-09-11", None, None, NOW)
    assert result["status"] == "UNAVAILABLE" and "frozen_raw_fact_mismatch" in result["reason"]


@pytest.mark.parametrize("failed_baseline", [False, True])
def test_repeat_preserves_first_acceptance_and_retry_diagnostics(monkeypatch, failed_baseline):
    previous = snapshot(); candidate = deepcopy(previous[cr.STORE_KEY]["evidence"])
    if failed_baseline: candidate["records"][-6].update(status="UNAVAILABLE", factual=None, provenance=None, source_kind=None, reason=cr.TRANSIENT_READ + "PermissionError: temporary read error")
    monkeypatch.setattr(cr, "_capture", lambda *a: deepcopy(candidate))
    monkeypatch.setattr(cr, "_capture_current", lambda *a: (_ for _ in ()).throw(ValueError("current unavailable")))
    times = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    current = deepcopy(previous)
    completed = cr.capture_snapshot(current, previous, now_fn=lambda: next(times), refresh_started_at=NOW)
    assert current[cr.STORE_KEY]["evidence"] == previous[cr.STORE_KEY]["evidence"]
    assert current[cr.STORE_KEY]["evidence_sha256"] == previous[cr.STORE_KEY]["evidence_sha256"]
    assert current[cr.STORE_KEY]["last_capture_attempt_at_utc"] == completed.isoformat()
    if failed_baseline: assert cr.describe(current, now=completed)["latest_diagnostics"]["records"][candidate["records"][-6]["trade_date"]]["status"] == "UNAVAILABLE"
    verify(current, now=completed)


def test_failed_attempt_preserves_facts_but_expiry_still_applies(monkeypatch):
    previous = snapshot(); current = deepcopy(previous)
    def fail(*a): raise ValueError("source temporarily unavailable")
    monkeypatch.setattr(cr, "_capture", fail); monkeypatch.setattr(cr, "_capture_current", fail)
    times = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    cr.capture_snapshot(current, previous, now_fn=lambda: next(times), refresh_started_at=NOW)
    assert current[cr.STORE_KEY]["evidence"] == previous[cr.STORE_KEY]["evidence"]
    assert "temporarily unavailable" in current[cr.STORE_KEY]["last_capture_error"]
    assert cr.describe(current, now=NOW+timedelta(days=5))["status"] == "UNAVAILABLE"


def test_reversed_completion_is_atomic(monkeypatch):
    s = snapshot(); before = deepcopy(s)
    monkeypatch.setattr(cr, "_capture", lambda *a: deepcopy(s[cr.STORE_KEY]["evidence"]))
    monkeypatch.setattr(cr, "_capture_current", lambda *a: None)
    times = iter([NOW+timedelta(seconds=2), NOW+timedelta(seconds=1)])
    with pytest.raises(ValueError, match="completion_clock_reversed"):
        cr.capture_snapshot(s, before, now_fn=lambda: next(times), refresh_started_at=NOW)
    assert s == before


def test_oracle_refusal_remains_enabled_under_optimization():
    code = "from moex_data import rub_cr_futoi_dated_context as c; c.verify_projection({}, {'futoi_context': {'futoi_live_cr': {}}}, now=None)"
    result = subprocess.run([sys.executable, "-O", "-c", code], text=True, capture_output=True)
    assert result.returncode != 0 and "CR canonical refusal" in result.stderr


@pytest.mark.parametrize("index", [-6, -1])
def test_new_latest_invalid_pair_never_restores_old_available(monkeypatch, tmp_path, index):
    import pandas as pd
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    previous = snapshot(); day = previous[cr.STORE_KEY]["evidence"]["records"][index]["trade_date"]
    path = tmp_path / "raw.parquet"
    monkeypatch.setattr(engine.raw_materializer, "_partition_path", lambda *a: path)
    monkeypatch.setattr(source, "source_identity", lambda *a: {"source_ticker": "cr", "secid": "CRU6"})
    rows = [dict(trade_date=day, ts=day+" 23:50:00", systime=day+" 23:55:00",
        availability_ts_utc=day+"T20:56:00+00:00", ingest_ts=day+"T20:57:00+00:00",
        sess_id=1, seqnum=1, clgroup=side, pos=net, pos_long=long, pos_short=short,
        pos_long_num=10, pos_short_num=11, source_id=cr.SOURCE,
        instrument_id=cr.INSTRUMENT, source_ticker="cr", secid="CRU6")
        for side, net, long, short in (("FIZ",20,100,-80),("YUR",-20,80,-100))]
    pd.DataFrame(rows).to_parquet(path)
    previous[cr.STORE_KEY]["evidence"]["records"][index] = cr._load_record(tmp_path, day, None, None, NOW)
    assert previous[cr.STORE_KEY]["evidence"]["records"][index]["status"] == "AVAILABLE"
    rehash(previous); candidate = deepcopy(previous[cr.STORE_KEY]["evidence"])
    newer = deepcopy(rows)
    for row in newer: row.update(ts=day+" 23:51:00", seqnum=2)
    newer[1].update(pos_long=81, pos=-19)
    pd.DataFrame(rows+newer).to_parquet(path)
    candidate["records"][index] = cr._load_record(tmp_path, day, None, None, NOW)
    assert candidate["records"][index]["status"] == "UNAVAILABLE"
    monkeypatch.setattr(cr, "_capture", lambda *a: deepcopy(candidate))
    monkeypatch.setattr(cr, "_capture_current", lambda *a: (_ for _ in ()).throw(ValueError("current unavailable")))
    current = deepcopy(previous); times = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    completed = cr.capture_snapshot(current, previous, now_fn=lambda: next(times), refresh_started_at=NOW)
    out = cr.describe(current, now=completed)
    if index == -6:
        assert out["status"] == "AVAILABLE" and out["dated"]["changes"]["5"]["status"] == "UNAVAILABLE"
        assert out["dated"]["changes"]["5"]["values"] is None
    else:
        assert out["status"] == "UNAVAILABLE" and "latest_anchor_source_rejected" in out["reason"]
        assert current[cr.STORE_KEY]["evidence"] == previous[cr.STORE_KEY]["evidence"]
        assert current[cr.STORE_KEY]["evidence"]["accepted_at_utc"] == NOW.isoformat()
        assert cr.describe(current, now=NOW)["status"] == "AVAILABLE"
    verify(current, now=completed)


@pytest.mark.parametrize("error_class", ["PermissionError", "OSError", "FileNotFoundError"])
def test_read_error_class_is_explicitly_transient(monkeypatch, error_class):
    from moex_data.futures import futoi_delta_statistics_context as engine
    monkeypatch.setattr(engine, "_raw_factual", lambda *a, **k: {"status": "UNAVAILABLE",
        "reason": "canonical_raw_partition_failed_factual_validation", "error_class": error_class, "error": "read failed"})
    result = cr._load_record(Path("."), "2026-09-11", None, None, NOW)
    assert result["status"] == "UNAVAILABLE" and result["reason"].startswith(cr.TRANSIENT_READ)


def test_transient_anchor_failure_retains_own_first_acceptance(monkeypatch):
    previous = snapshot(); candidate = deepcopy(previous[cr.STORE_KEY]["evidence"])
    candidate["records"][-1].update(status="UNAVAILABLE", factual=None, provenance=None, source_kind=None,
        reason=cr.TRANSIENT_READ + "PermissionError: cannot read")
    monkeypatch.setattr(cr, "_capture", lambda *a: deepcopy(candidate))
    monkeypatch.setattr(cr, "_capture_current", lambda *a: (_ for _ in ()).throw(OSError("cannot read")))
    current = deepcopy(previous); times = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    completed = cr.capture_snapshot(current, previous, now_fn=lambda: next(times), refresh_started_at=NOW)
    assert current[cr.STORE_KEY]["evidence_sha256"] == previous[cr.STORE_KEY]["evidence_sha256"]
    assert current[cr.STORE_KEY]["latest_source_rejection"] is None
    assert cr.describe(current, now=completed)["status"] == "AVAILABLE"
    assert current[cr.STORE_KEY]["last_capture_error"]
    verify(current, now=completed)


@pytest.mark.parametrize("invalid", [True, {}, {"checked_at_utc": NOW.isoformat(), "trade_date": "invalid", "reason": "bad"},
    {"checked_at_utc": NOW.isoformat(), "trade_date": "2026-09-11", "reason": True},
    {"checked_at_utc": (NOW-timedelta(seconds=1)).isoformat(), "trade_date": "2026-09-11", "reason": "bad"}])
def test_latest_rejection_metadata_must_be_valid(invalid):
    s = snapshot(); s[cr.STORE_KEY]["latest_source_rejection"] = invalid
    assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("path,value", [
    (("task_id",), "alien_task"), (("instrument_id",), "si_futures_family"),
    (("admission", "source_selection"), "older_valid_pair_fallback"),
    (("admission", "earlier_invalid_pairs_do_not_prove_latest_pair_invalid"), False),
    (("admission", "dated_factual_use_allowed"), 1), (("admission", "lags"), [True, 5, 20]),
    (("admission", "maximum_retained_observed_dates"), 21.0),
    (("admission", "invented_authority"), True), (("invented_scope",), "trading"),
    (("limitations",), []), (("deployment_gate",), None)])
def test_every_contract_identity_policy_inventory_and_type_is_bound(path, value):
    s = snapshot(); doc = json.loads(s[cr.ADMISSION_KEY]["artifact_text"])
    target = doc
    for key in path[:-1]: target = target[key]
    target[path[-1]] = value
    text = json.dumps(doc); s[cr.ADMISSION_KEY].update(artifact_text=text, artifact_sha256=sha256(text.encode()).hexdigest())
    assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("key", list(cr._expected_contract()["admission"]))
def test_contract_missing_admission_field_revokes(key):
    s = snapshot(); doc = json.loads(s[cr.ADMISSION_KEY]["artifact_text"]); doc["admission"].pop(key)
    text = json.dumps(doc); s[cr.ADMISSION_KEY].update(artifact_text=text, artifact_sha256=sha256(text.encode()).hexdigest())
    assert cr.describe(s, now=NOW)["status"] == "UNAVAILABLE"; verify(s)


@pytest.mark.parametrize("next_reason", ["ValueError: latest pair balance failed", "ValueError: latest source identity failed", cr.TRANSIENT_READ + "OSError: temporary read failure"])
def test_first_anchor_rejection_survives_retries_until_valid_recovery(monkeypatch, next_reason):
    original = snapshot()
    valid = deepcopy(original[cr.STORE_KEY]["evidence"])
    first_reason = "ValueError: latest pair balance failed"
    candidate = deepcopy(valid)
    candidate["records"][-1].update(status="UNAVAILABLE", factual=None, provenance=None, source_kind=None, reason=first_reason)
    monkeypatch.setattr(cr, "_capture", lambda *a: deepcopy(candidate))
    monkeypatch.setattr(cr, "_capture_current", lambda *a: (_ for _ in ()).throw(OSError("current read unavailable")))
    def refresh(previous, offset):
        current = deepcopy(previous)
        times = iter([NOW+timedelta(seconds=offset), NOW+timedelta(seconds=offset+1)])
        cr.capture_snapshot(current, previous, now_fn=lambda: next(times), refresh_started_at=NOW)
        return current
    first = refresh(original, 1)
    first_rejection = deepcopy(first[cr.STORE_KEY]["latest_source_rejection"])
    between = NOW+timedelta(seconds=3)
    assert cr.describe(first, now=between)["status"] == "UNAVAILABLE"
    candidate["records"][-1]["reason"] = next_reason
    second = refresh(first, 4)
    assert second[cr.STORE_KEY]["latest_source_rejection"] == first_rejection
    assert first_rejection["checked_at_utc"] == (NOW+timedelta(seconds=2)).isoformat()
    assert first_rejection["reason"] == first_reason
    assert cr.describe(second, now=between)["status"] == "UNAVAILABLE"
    assert second[cr.STORE_KEY]["evidence_sha256"] == original[cr.STORE_KEY]["evidence_sha256"]
    assert second[cr.STORE_KEY]["last_capture_error"] == (
        "cr_latest_anchor_source_rejected: " + candidate["records"][-1]["trade_date"] + ": " + next_reason)
    latest = cr.describe(second, now=NOW+timedelta(seconds=5))
    assert latest["status"] == "UNAVAILABLE" and first_reason in latest["reason"]
    assert latest["latest_capture_diagnostics"]["dated_error"] == second[cr.STORE_KEY]["last_capture_error"]
    assert cr.describe(second, now=between)["latest_capture_diagnostics"] is None
    verify(second, now=between)
    verify(second, now=NOW+timedelta(seconds=5))
    candidate = deepcopy(valid)
    recovered = refresh(second, 6)
    assert recovered[cr.STORE_KEY]["latest_source_rejection"] is None
    assert recovered[cr.STORE_KEY]["last_capture_error"] is None
    assert recovered[cr.STORE_KEY]["evidence"]["accepted_at_utc"] == (NOW+timedelta(seconds=7)).isoformat()
    assert cr.describe(recovered, now=NOW+timedelta(seconds=7))["status"] == "AVAILABLE"
    verify(recovered, now=NOW+timedelta(seconds=7))


@pytest.mark.parametrize("first_capture", [False, True])
def test_capture_failure_causes_are_portable_without_fabricated_acceptance(monkeypatch, first_capture):
    s = snapshot(); previous = deepcopy(s); install_current(s, monkeypatch)
    if first_capture:
        s.pop(cr.STORE_KEY); previous = {}
        def dated_fail(*a): raise OSError("first dated source read permission denied")
        monkeypatch.setattr(cr, "_capture", dated_fail)
    else:
        monkeypatch.setattr(cr, "_capture", lambda *a: deepcopy(previous[cr.STORE_KEY]["evidence"]))
    def current_fail(*a): raise OSError("additional current frozen-byte read failed")
    monkeypatch.setattr(cr, "_capture_current", current_fail)
    times = iter([NOW+timedelta(seconds=1), NOW+timedelta(seconds=2)])
    completed = cr.capture_snapshot(s, previous, now_fn=lambda: next(times), refresh_started_at=NOW)
    out = cr.describe(s, now=completed)
    diagnostics = out["latest_capture_diagnostics"]
    assert diagnostics["checked_at_utc"] == completed.isoformat()
    assert diagnostics["current_error"] == "OSError: additional current frozen-byte read failed"
    assert cr.CURRENT_KEY not in s
    if first_capture:
        assert cr.STORE_KEY not in s and out["status"] == "UNAVAILABLE"
        assert "first dated source read permission denied" in out["reason"]
        assert diagnostics["dated_error"] == "OSError: first dated source read permission denied"
    else:
        assert out["dated"]["status"] == "AVAILABLE" and out["current"]["status"] == "UNAVAILABLE"
        assert "additional current frozen-byte read failed" in out["current"]["reason"]
        assert s[cr.STORE_KEY]["evidence_sha256"] == previous[cr.STORE_KEY]["evidence_sha256"]
    verify(s, now=completed)
    before = cr.describe(s, now=NOW)
    assert before["latest_capture_diagnostics"] is None
    assert "read permission denied" not in json.dumps(before) and "frozen-byte read failed" not in json.dumps(before)
    verify(s, now=NOW)


@pytest.mark.parametrize("invalid", [True, {}, {"checked_at_utc": NOW.isoformat(), "dated_error": {}, "current_error": None},
    {"checked_at_utc": "2026-09-14T17:30:00", "dated_error": None, "current_error": "bad"},
    {"checked_at_utc": NOW.isoformat(), "dated_error": None, "current_error": True},
    {"checked_at_utc": NOW.isoformat(), "dated_error": None, "current_error": None, "invented_fact": 1}])
def test_capture_diagnostics_shape_types_and_clocks_fail_closed(invalid):
    s = snapshot(); s[cr.DIAGNOSTICS_KEY] = invalid
    out = cr.describe(s, now=NOW)
    assert out["status"] == "UNAVAILABLE" and out["latest_capture_diagnostics"] is None
    verify(s)


@pytest.mark.parametrize("read_at", [None, "invalid", "2026-09-14T17:30:00"])
def test_invalid_read_clock_never_leaks_diagnostics(read_at):
    s = snapshot(); s[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": NOW.isoformat(), "dated_error": "private source failure", "current_error": None}
    out = cr.describe(s, now=read_at)
    assert out["latest_capture_diagnostics"] is None and "private source failure" not in json.dumps(out)
    verify(s, now=read_at)


def test_refused_diagnostic_tampering_is_rejected():
    s = snapshot(); s.pop(cr.STORE_KEY)
    s[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": NOW.isoformat(), "dated_error": "first source failure", "current_error": "current source failure"}
    r = release(s); out = r["futoi_context"]["futoi_live_cr"]["scoped_observed_comparisons"]
    out["latest_capture_diagnostics"]["dated_error"] = "invented different failure"
    with pytest.raises(AssertionError): verify(s, r)


@pytest.mark.parametrize("mutation", ["ancient", "dated_error", "current_error_with_proof", "different_proof_clock", "missing_store", "missing_proof", "before_refresh", "after_refresh"])
def test_diagnostic_producer_coherence_required(monkeypatch, mutation):
    s = snapshot(); install_current(s, monkeypatch)
    s[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": NOW.isoformat(), "dated_error": None, "current_error": None}
    assert cr.describe(s, now=NOW)["status"] == "AVAILABLE"
    if mutation == "ancient": s[cr.DIAGNOSTICS_KEY]["checked_at_utc"] = "1900-01-01T00:00:00+00:00"
    elif mutation == "dated_error": s[cr.DIAGNOSTICS_KEY]["dated_error"] = "contradictory dated failure"
    elif mutation == "current_error_with_proof": s[cr.DIAGNOSTICS_KEY]["current_error"] = "additional current byte validation failed"
    elif mutation == "different_proof_clock": s[cr.CURRENT_KEY]["evidence"]["captured_at_utc"] = (NOW-timedelta(seconds=1)).isoformat()
    elif mutation == "missing_store": s.pop(cr.STORE_KEY)
    elif mutation == "missing_proof": s.pop(cr.CURRENT_KEY)
    elif mutation == "before_refresh": s["identity"] = {"refresh_started_at_utc": (NOW+timedelta(seconds=1)).isoformat(), "refresh_completed_at_utc": (NOW+timedelta(seconds=2)).isoformat()}
    elif mutation == "after_refresh": s["identity"] = {"refresh_started_at_utc": (NOW-timedelta(seconds=2)).isoformat(), "refresh_completed_at_utc": (NOW-timedelta(seconds=1)).isoformat()}
    out = cr.describe(s, now=NOW)
    assert out["status"] == "UNAVAILABLE" and out["latest_capture_diagnostics"] is None
    verify(s)


def test_coherent_future_diagnostic_is_hidden():
    s = snapshot(); later = NOW+timedelta(seconds=10)
    s[cr.STORE_KEY].update(last_capture_attempt_at_utc=later.isoformat(), last_capture_error="future dated read failed")
    s[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": later.isoformat(), "dated_error": "future dated read failed", "current_error": "future current read failed"}
    out = cr.describe(s, now=NOW)
    assert out["status"] == "AVAILABLE" and out["latest_capture_diagnostics"] is None
    assert "future dated read failed" not in json.dumps(out) and "future current read failed" not in json.dumps(out)
    verify(s)


def test_first_failure_diagnostic_clock_prevents_backward_next_capture(monkeypatch):
    previous = snapshot(); previous.pop(cr.STORE_KEY)
    previous[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": (NOW+timedelta(seconds=10)).isoformat(),
        "dated_error": "first dated read failed", "current_error": "first current read failed"}
    current = snapshot(); before = deepcopy(current)
    monkeypatch.setattr(cr, "_capture", lambda *a: pytest.fail("clock reversal must stop before source reads"))
    with pytest.raises(ValueError, match="precedes_refresh_or_prior_capture"):
        cr.capture_snapshot(current, previous, now_fn=lambda: NOW+timedelta(seconds=1), refresh_started_at=NOW)
    assert current == before


@pytest.mark.parametrize("mutation", ["current_digest", "current_shape", "current_fact", "current_audit", "current_proof", "dated_digest", "dated_shape", "dated_fact"])
def test_success_diagnostics_require_valid_capture_evidence(monkeypatch, mutation):
    s = snapshot(); install_current(s, monkeypatch)
    s[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": NOW.isoformat(), "dated_error": None, "current_error": None}
    current = s[cr.CURRENT_KEY]
    if mutation == "current_digest": current["evidence_sha256"] = "0"*64
    elif mutation == "current_shape": current["evidence"].pop("publication_audit")
    elif mutation == "current_fact": current["evidence"]["record"]["factual"]["total_open_interest"] = 1
    elif mutation == "current_audit": current["evidence"]["publication_audit"]["text"] = "{}"
    elif mutation == "current_proof": current["evidence"]["record"]["provenance"]["raw_partition_sha256"] = "0"*64
    elif mutation == "dated_digest": s[cr.STORE_KEY]["evidence_sha256"] = "0"*64
    elif mutation == "dated_shape": s[cr.STORE_KEY]["evidence"].pop("records")
    elif mutation == "dated_fact": s[cr.STORE_KEY]["evidence"]["records"][-1]["factual"]["total_open_interest"] = 1
    if mutation.startswith("current_") and mutation != "current_digest": current["evidence_sha256"] = cr.common._digest(current["evidence"])
    if mutation.startswith("dated_") and mutation != "dated_digest": rehash(s)
    out = cr.describe(s, now=NOW)
    assert out["status"] == "UNAVAILABLE" and out["latest_capture_diagnostics"] is None
    verify(s)


def test_successful_capture_diagnostics_survive_later_live_and_dated_expiry(monkeypatch):
    from moex_data import rub_snapshot_read_freshness as freshness
    s = snapshot(); install_current(s, monkeypatch)
    s[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": NOW.isoformat(), "dated_error": None, "current_error": None}
    def expired(value, now):
        result = deepcopy(value)
        if now > NOW+timedelta(minutes=20): result["components"]["futoi_live_cr"]["status"] = "UNAVAILABLE"
        return result
    monkeypatch.setattr(freshness, "apply_read_freshness", expired)
    later = NOW+timedelta(minutes=21)
    out = cr.describe(s, now=later)
    assert out["status"] == "AVAILABLE" and out["current"]["status"] == "UNAVAILABLE"
    assert out["latest_capture_diagnostics"] == s[cr.DIAGNOSTICS_KEY]
    assert out["latest_capture_diagnostics"]["current_error"] is None
    verify(s, now=later)
    later = NOW+timedelta(days=5)
    out = cr.describe(s, now=later)
    assert out["status"] == "UNAVAILABLE" and out["latest_capture_diagnostics"] == s[cr.DIAGNOSTICS_KEY]
    verify(s, now=later)


@pytest.mark.parametrize("seconds,allowed", [(1170, True), (1170.000001, False), (86400, False)])
def test_current_capture_requires_existing_source_ttl_at_original_cutoff(monkeypatch, seconds, allowed):
    s = snapshot(); install_current(s, monkeypatch)
    carrier = s[cr.CURRENT_KEY]; cutoff = NOW+timedelta(seconds=seconds)
    # Fixture event is NOW-30 seconds; 1170 therefore reaches the exact1200 boundary.
    carrier["evidence"].update(causal_cutoff_at_utc=cutoff.isoformat(), captured_at_utc=cutoff.isoformat())
    carrier["evidence_sha256"] = cr.common._digest(carrier["evidence"])
    if allowed:
        assert cr._validated_current_capture(carrier)["causal_cutoff_at_utc"] == cutoff.isoformat()
    else:
        with pytest.raises(ValueError, match="expired_at_capture"):
            cr._validated_current_capture(carrier)
        s[cr.STORE_KEY]["last_capture_attempt_at_utc"] = cutoff.isoformat()
        s[cr.DIAGNOSTICS_KEY] = {"checked_at_utc": cutoff.isoformat(), "dated_error": None, "current_error": None}
        out = cr.describe(s, now=cutoff)
        assert out["status"] == "UNAVAILABLE" and out["latest_capture_diagnostics"] is None
        verify(s, now=cutoff)


def test_later_capture_completion_does_not_redate_original_source_freshness(monkeypatch):
    s = snapshot(); install_current(s, monkeypatch)
    carrier = s[cr.CURRENT_KEY]
    carrier["evidence"]["captured_at_utc"] = (NOW+timedelta(hours=1)).isoformat()
    carrier["evidence_sha256"] = cr.common._digest(carrier["evidence"])
    assert cr._validated_current_capture(carrier)["causal_cutoff_at_utc"] == NOW.isoformat()
