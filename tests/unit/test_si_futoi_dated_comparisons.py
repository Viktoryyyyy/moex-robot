"""Synthetic portable-witness tests; no source availability or production claim."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path

import pytest

# Normal repository import; direct loading keeps arithmetic tests source-independent.
path = Path(__file__).resolve().parents[2] / "src/moex_data/rub_si_futoi_dated_context.py"
spec = importlib.util.spec_from_file_location("si_dated_unit", path)
dated = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dated)

NOW = datetime(2026, 9, 14, 17, 30, tzinfo=timezone.utc)
GOV = {
    "instrument_id": dated.INSTRUMENT,
    "factual_use_allowed": True, "all_required_gates_pass": True,
    "instrument_local_acceptance_pass": True, "canonical_live_smoke_accepted": True,
    "directional_authority": False, "action_authority": False,
    "standalone_buy_sell_authority": False,
}


def proof(prefixes):
    result = {}
    for i, prefix in enumerate(prefixes):
        result[prefix + "_ref"] = "${MOEX_DATA_ROOT}/synthetic/" + prefix + ".json"
        result[prefix + "_sha256"] = str(i + 1) * 64
    return result


def factual(day, net=100, oi=10000, available=None):
    return {
        "trade_date": day, "snapshot_ts": day + "T20:50:00+00:00",
        "source_publication_time": day + "T20:50:10+00:00",
        "availability_ts_utc": (available or NOW - timedelta(seconds=1)).isoformat(),
        "ingest_ts_utc": (available or NOW - timedelta(seconds=1)).isoformat(),
        "total_open_interest": oi,
        "fiz": {"long": 4000, "short": 4000-net, "net": net,
                "long_participants": 40, "short_participants": 30},
        "yur": {"long": oi-4000, "short": oi-4000+net, "net": -net,
                "long_participants": 20, "short_participants": 25},
    }


def candidate():
    dates = [(datetime(2026, 8, 22) + timedelta(days=i)).date().isoformat() for i in range(21)]
    anchor = {
        "instrument_id": dated.INSTRUMENT, "source_id": dated.SOURCE,
        "source_kind": "previous_observed", "status": "AVAILABLE",
        "factual": factual(dates[-1], 400),
        "provenance": {**proof(("raw_partition", "raw_quality_report", "raw_refresh_manifest")),
                       "accepted_state_kind": "source_native_exact_date_raw_quality_pass"},
    }
    baselines = {}
    for lag in dated.LAGS:
        day = dates[-1-lag]
        baselines[str(lag)] = {
            "instrument_id": dated.INSTRUMENT, "source_id": dated.SOURCE,
            "source_kind": "canonical_raw", "status": "AVAILABLE", "target_trade_date": day,
            "factual": factual(day, 400-lag),
            "provenance": {**proof(("raw_partition",)), "source_id": dated.SOURCE,
                           "factual_validation": "PASS"},
        }
    return {
        "schema_version": dated.SCHEMA, "contract_ref": dated.CONTRACT,
        "instrument_id": dated.INSTRUMENT, "source_id": dated.SOURCE,
        "accepted_at_utc": NOW.isoformat(), "causal_cutoff_at_utc": NOW.isoformat(),
        "governance_at_acceptance": deepcopy(GOV),
        "anchor": anchor, "baselines": baselines,
        "observed_date_witness": {
            "status": "PASS", "authority_source_id": "moex_algopack_fo_tradestats_5m",
            "previous_observed_trade_date": dates[-1], "observed_trade_dates": dates,
            "provenance": {**proof(("partition", "manifest", "quality_report")),
                           "acceptance_contract_id": "step7_rub_native_d1_w1_technical_acceptance.v1"},
        },
        **dated.FLAGS,
    }


def store(evidence=None):
    e = candidate() if evidence is None else evidence
    return {"schema_version": dated.SCHEMA, "evidence": e, "evidence_sha256": dated._digest(e)}


@pytest.mark.parametrize("minutes", [0, 21, 720])
def test_dated_survives_later_read_without_live_permission(minutes):
    s = store()
    original = deepcopy(s)
    r = dated.describe(s, now=NOW + timedelta(minutes=minutes), governance=GOV)
    assert r["status"] == "AVAILABLE"
    assert r["anchor"]["factual"]["trade_date"] == "2026-09-11"
    assert r["current_usable"] is False
    assert r["deltas"]["delta_1d"]["values"]["fiz.net"] == 1
    assert r["deltas"]["delta_5d"]["values"]["fiz.net"] == 5
    assert r["deltas"]["delta_20d"]["values"]["fiz.net"] == 20
    assert r["deltas"]["delta_20d"]["target_trade_date"] == "2026-08-22"
    assert r["accepted_at_utc"] == NOW.isoformat()
    assert s == original


@pytest.mark.parametrize("seconds", [0, 0.000001])
def test_exact_anchor_source_expiry_boundary(seconds):
    s = store()
    deadline = datetime(2026, 9, 15, 20, 50, tzinfo=timezone.utc)
    r = dated.describe(s, now=deadline + timedelta(seconds=seconds), governance=GOV)
    assert r["status"] == ("AVAILABLE" if seconds == 0 else "UNAVAILABLE")


@pytest.mark.parametrize("key", ["factual_use_allowed", "all_required_gates_pass",
                                "instrument_local_acceptance_pass", "canonical_live_smoke_accepted"])
def test_governance_revocation_blocks_dated(key):
    gov = deepcopy(GOV)
    gov[key] = False
    r = dated.describe(store(), now=NOW, governance=gov)
    assert r["status"] == "UNAVAILABLE" and "anchor" not in r


def test_cr_is_not_admitted():
    gov = {**GOV, "instrument_id": "cr_futures_family"}
    assert dated.describe(store(), now=NOW, governance=gov)["status"] == "UNAVAILABLE"


@pytest.mark.parametrize("field", ["long", "short", "net", "long_participants", "short_participants"])
@pytest.mark.parametrize("invalid", [None, True, 1.5, float("inf")])
def test_invalid_counts_fail_closed(field, invalid):
    e = candidate()
    e["baselines"]["1"]["factual"]["fiz"][field] = invalid
    if invalid == float("inf"):
        # A non-finite value cannot even be serialized as accepted evidence.
        with pytest.raises(ValueError):
            store(e)
    else:
        assert dated.describe(store(e), now=NOW, governance=GOV)["status"] == "UNAVAILABLE"


@pytest.mark.parametrize("target", ["anchor", "baseline"])
@pytest.mark.parametrize("field", ["snapshot_ts", "availability_ts_utc", "ingest_ts_utc"])
def test_future_source_or_receipt_not_admitted(target, field):
    e = candidate()
    fact = e["anchor"]["factual"] if target == "anchor" else e["baselines"]["20"]["factual"]
    fact[field] = (NOW + timedelta(microseconds=1)).isoformat()
    assert dated.describe(store(e), now=NOW, governance=GOV)["status"] == "UNAVAILABLE"


def test_baseline_source_age_not_limited_to_anchor_ttl():
    r = dated.describe(store(), now=NOW, governance=GOV)
    assert r["deltas"]["delta_20d"]["status"] == "AVAILABLE"


@pytest.mark.parametrize("field", ["snapshot_ts", "availability_ts_utc"])
@pytest.mark.parametrize("invalid", [None, "invalid", "2026-09-11T20:50:00"])
def test_bad_clocks_fail_closed(field, invalid):
    e = candidate()
    e["anchor"]["factual"][field] = invalid
    assert dated.describe(store(e), now=NOW, governance=GOV)["status"] == "UNAVAILABLE"


def test_missing_baseline_is_local_and_not_zero():
    e = candidate()
    e["baselines"]["20"] = {"status": "UNAVAILABLE", "target_trade_date": "2026-08-22",
                           "factual": None, "reason": "canonical_raw_partition_missing"}
    r = dated.describe(store(e), now=NOW, governance=GOV)
    assert r["status"] == "PARTIAL"
    assert r["deltas"]["delta_20d"]["values"] is None
    assert r["deltas"]["delta_1d"]["status"] == "AVAILABLE"
    assert r["deltas"]["delta_20d"]["reason"] == "canonical_raw_partition_missing"


@pytest.mark.parametrize("defect", ["duplicate", "order", "remove", "shift"])
def test_observed_date_corruption_does_not_shift_lags(defect):
    e = candidate()
    dates = e["observed_date_witness"]["observed_trade_dates"]
    if defect == "duplicate":
        dates[1] = dates[0]
    elif defect == "order":
        dates.reverse()
    elif defect == "remove":
        dates.pop(3)
    else:
        e["baselines"]["1"]["target_trade_date"] = dates[-3]
    assert dated.describe(store(e), now=NOW, governance=GOV)["status"] == "UNAVAILABLE"


def test_digest_tamper_refuses_without_reading_files():
    s = store()
    s["evidence"]["anchor"]["factual"]["fiz"]["net"] += 1
    r = dated.describe(s, now=NOW, governance=GOV)
    assert r["status"] == "UNAVAILABLE"
    assert r["reason"] == "dated_evidence_digest_mismatch"


@pytest.mark.parametrize("bad_ref", ["../x", "${MOEX_DATA_ROOT}/../x", "${MOEX_DATA_ROOT}//x"])
def test_bad_provenance_path(bad_ref):
    e = candidate()
    e["anchor"]["provenance"]["raw_partition_ref"] = bad_ref
    assert dated.describe(store(e), now=NOW, governance=GOV)["status"] == "UNAVAILABLE"


def test_repeat_receipt_does_not_renew_first_acceptance():
    first = dated.retain(None, candidate(), now=NOW, governance=GOV)
    second = candidate()
    later = NOW + timedelta(minutes=3)
    second["accepted_at_utc"] = later.isoformat()
    second["causal_cutoff_at_utc"] = later.isoformat()
    second["anchor"]["factual"]["availability_ts_utc"] = later.isoformat()
    second["anchor"]["factual"]["ingest_ts_utc"] = later.isoformat()
    second["anchor"]["provenance"]["raw_partition_sha256"] = "a"*64
    actual = dated.retain(first, second, now=later, governance=GOV)
    assert actual["evidence"] == first["evidence"]
    assert actual["evidence_sha256"] == first["evidence_sha256"]


def test_new_revision_preserves_old_store_and_records_new_acceptance():
    first = dated.retain(None, candidate(), now=NOW, governance=GOV)
    original = deepcopy(first)
    second = candidate()
    later = NOW + timedelta(minutes=3)
    second["accepted_at_utc"] = later.isoformat()
    second["baselines"]["1"]["factual"] = factual("2026-09-10", 398)
    actual = dated.retain(first, second, now=later, governance=GOV)
    assert actual["evidence"]["accepted_at_utc"] == later.isoformat()
    assert actual["evidence_sha256"] != first["evidence_sha256"]
    assert first == original


def test_capture_failure_retains_witness_but_exposes_failure():
    first = dated.retain(None, candidate(), now=NOW, governance=GOV)
    later = NOW + timedelta(minutes=5)
    actual = dated.retain(first, None, now=later, governance=GOV, error="source_error")
    assert actual["evidence"] == first["evidence"]
    r = dated.describe(actual, now=later, governance=GOV)
    assert r["last_capture_error"] == "source_error"
    assert r["status"] == "AVAILABLE"


def test_future_acceptance_refuses_before_accepted_time():
    r = dated.describe(store(), now=NOW-timedelta(microseconds=1), governance=GOV)
    assert r["status"] == "UNAVAILABLE"


def test_consumer_does_not_change_live_state_or_cr():
    snapshot = {"components": {"futoi_live": {"status": "UNAVAILABLE",
                  "data": {"governance": deepcopy(GOV)}}}, dated.STORE_KEY: store()}
    consumers = {"futoi_context": {
        "futoi_live": {"current_usable": False, "comparisons": None, "reason": "expired"},
        "futoi_live_cr": {"scope": "current_pair_only", "current_usable": False}}}
    original = deepcopy(snapshot)
    cr = deepcopy(consumers["futoi_context"]["futoi_live_cr"])
    dated.attach_consumer(snapshot, consumers, now=NOW+timedelta(minutes=21))
    si = consumers["futoi_context"]["futoi_live"]
    assert si["dated_comparisons"]["status"] == "AVAILABLE"
    assert si["current_usable"] is False and si["comparisons"] is None and si["reason"] == "expired"
    assert consumers["futoi_context"]["futoi_live_cr"] == cr
    assert snapshot == original


@pytest.mark.parametrize("flag", list(dated.FLAGS))
def test_authority_tampering_refuses(flag):
    e = candidate()
    e[flag] = True
    assert dated.describe(store(e), now=NOW, governance=GOV)["status"] == "UNAVAILABLE"


def test_frozen_roundtrip_equal_without_network():
    s = store()
    frozen = json.loads(json.dumps(s))
    assert dated.describe(s, now=NOW, governance=GOV) == dated.describe(frozen, now=NOW, governance=GOV)


def test_share_of_oi_and_count_arithmetic_independently():
    s = store()
    r = dated.describe(s, now=NOW, governance=GOV)
    anchor = s["evidence"]["anchor"]["factual"]
    for lag in dated.LAGS:
        before = s["evidence"]["baselines"][str(lag)]["factual"]
        actual = r["deltas"]["delta_" + str(lag) + "d"]["values"]
        for side in ("fiz", "yur"):
            for field in ("long", "short", "net", "long_participants", "short_participants"):
                assert actual[side+"."+field] == anchor[side][field]-before[side][field]
            assert actual[side+".net_share_of_oi"] == pytest.approx(
                anchor[side]["net"]/anchor["total_open_interest"]-before[side]["net"]/before["total_open_interest"],
                abs=1e-14, rel=0)
        assert actual["total_open_interest"] == anchor["total_open_interest"]-before["total_open_interest"]


def test_reverse_oracle_catches_omission_and_changed_delta():
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}},
                dated.STORE_KEY: store()}
    output = {"futoi_context": {"futoi_live": {}}}
    dated.attach_consumer(snapshot, output, now=NOW)
    dated.verify_projection(snapshot, output, now=NOW)
    absent = deepcopy(output)
    absent["futoi_context"]["futoi_live"].pop("dated_comparisons")
    with pytest.raises(AssertionError, match="omitted"):
        dated.verify_projection(snapshot, absent, now=NOW)
    changed = deepcopy(output)
    changed["futoi_context"]["futoi_live"]["dated_comparisons"]["deltas"]["delta_20d"]["values"]["fiz.net"] += 1
    with pytest.raises(AssertionError, match="arithmetic"):
        dated.verify_projection(snapshot, changed, now=NOW)


def test_reverse_oracle_rejects_values_after_expiry():
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}},
                dated.STORE_KEY: store()}
    output = {"futoi_context": {"futoi_live": {}}}
    dated.attach_consumer(snapshot, output, now=NOW)
    with pytest.raises(AssertionError, match="refusal"):
        dated.verify_projection(snapshot, output, now=NOW+timedelta(days=5))


@pytest.mark.parametrize("minutes", [0, 21, 720])
def test_integration_release_and_compact_preserve_dated_without_current(minutes):
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness

    snapshot = {
        "identity": {"generated_at_utc": NOW.isoformat()},
        "components": {"futoi_live": {"status": "UNAVAILABLE", "data": {
            "instrument_id": dated.INSTRUMENT, "source_id": dated.SOURCE,
            "governance": deepcopy(GOV), "consumer_factual_use_allowed": False,
            "factual_authority": False}}},
        dated.STORE_KEY: store(),
    }
    original = deepcopy(snapshot)
    now = NOW + timedelta(minutes=minutes)
    result = release.build(snapshot, now=now, code_revision="a"*40)
    si = result["futoi_context"]["futoi_live"]
    assert si["current_usable"] is False and si["comparisons"] is None
    assert si["dated_comparisons"]["status"] == "AVAILABLE"
    projection_completeness(snapshot, result, now=now)
    compact = release.compact(snapshot, now=now, code_revision="a"*40)
    output = compact["futoi_context"]["futoi_live"]["dated_comparisons"]
    assert output["anchor"]["factual"]["trade_date"] == "2026-09-11"
    assert output["accepted_at_utc"] == NOW.isoformat()
    assert output["deltas"]["delta_20d"]["values"]["fiz.net"] == 20
    row = next(x for x in compact["factual_coverage"]["requirements"] if x["requirement_id"] == "futoi_live")
    assert row["usable"] is False and row["dated_preparation_available"] is True
    assert compact["futoi_context"]["futoi_live_cr"]["comparisons"] is None
    assert snapshot == original


def test_capture_acceptance_uses_post_validation_clock(monkeypatch):
    events = []
    completed = NOW + timedelta(seconds=9, microseconds=125)
    clocks = iter((NOW, completed))
    original = candidate()

    def clock():
        value = next(clocks)
        events.append(("clock", value))
        return value

    def checked(component, *, now):
        events.append(("source_validation", now))
        return original

    monkeypatch.setattr(dated, "_capture_candidate", checked)
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}}}
    dated.capture_snapshot(snapshot, None, now_fn=clock, refresh_started_at=NOW)
    assert events == [("clock", NOW), ("source_validation", NOW), ("clock", completed)]
    stored = snapshot[dated.STORE_KEY]
    assert stored["evidence"]["accepted_at_utc"] == completed.isoformat()
    assert stored["evidence"]["causal_cutoff_at_utc"] == NOW.isoformat()
    assert stored["last_capture_attempt_at_utc"] == completed.isoformat()
    assert original == candidate()
    assert dated.describe(stored, now=completed-timedelta(microseconds=1), governance=GOV)["status"] == "UNAVAILABLE"
    assert dated.describe(stored, now=completed, governance=GOV)["status"] == "AVAILABLE"


@pytest.mark.parametrize("field", ["availability_ts_utc", "ingest_ts_utc"])
def test_later_acceptance_cannot_admit_post_cutoff_baseline(monkeypatch, field):
    value = candidate()
    value["baselines"]["20"]["factual"][field] = (NOW+timedelta(seconds=1)).isoformat()
    monkeypatch.setattr(dated, "_capture_candidate", lambda *args, **kwargs: value)
    clocks = iter((NOW, NOW+timedelta(seconds=2)))
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}}}
    dated.capture_snapshot(snapshot, None, now_fn=lambda: next(clocks), refresh_started_at=NOW)
    assert "evidence" not in snapshot[dated.STORE_KEY]
    assert snapshot[dated.STORE_KEY]["last_capture_error"]


def test_capture_clock_reversal_cannot_publish_new_evidence(monkeypatch):
    monkeypatch.setattr(dated, "_capture_candidate", lambda *args, **kwargs: candidate())
    clocks = iter((NOW, NOW-timedelta(microseconds=1)))
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}}}
    before = deepcopy(snapshot)
    with pytest.raises(ValueError, match="clock_went_backwards"):
        dated.capture_snapshot(snapshot, None, now_fn=lambda: next(clocks), refresh_started_at=NOW)
    assert snapshot == before


@pytest.mark.parametrize("invalid", [None, "bad", "2026-09-14T17:30:00", "2026-09-14T17:30:01+00:00"])
def test_cutoff_is_required_aware_and_not_after_acceptance(invalid):
    value = candidate()
    if invalid is None:
        value.pop("causal_cutoff_at_utc")
    else:
        value["causal_cutoff_at_utc"] = invalid
    assert dated.describe(store(value), now=NOW, governance=GOV)["status"] == "UNAVAILABLE"


def test_failed_capture_records_completion_and_preserves_old_witness(monkeypatch):
    first = dated.retain(None, candidate(), now=NOW, governance=GOV)

    def failed(*args, **kwargs):
        raise ValueError("actual_capture_failure")

    monkeypatch.setattr(dated, "_capture_candidate", failed)
    clocks = iter((NOW+timedelta(seconds=1), NOW+timedelta(seconds=4)))
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}}}
    dated.capture_snapshot(snapshot, {dated.STORE_KEY: first}, now_fn=lambda: next(clocks), refresh_started_at=NOW)
    actual = snapshot[dated.STORE_KEY]
    assert actual["evidence"] == first["evidence"]
    assert actual["evidence_sha256"] == first["evidence_sha256"]
    assert actual["last_capture_attempt_at_utc"] == (NOW+timedelta(seconds=4)).isoformat()
    assert "actual_capture_failure" in actual["last_capture_error"]


def test_changed_refusal_preserves_identity_but_exposes_latest_diagnostic():
    e = candidate()
    e["baselines"]["20"] = {"status": "UNAVAILABLE", "target_trade_date": "2026-08-22",
                           "factual": None, "reason": "raw_partition_missing"}
    first = dated.retain(None, e, now=NOW, governance=GOV)
    original = deepcopy(first)
    changed = deepcopy(e)
    later = NOW+timedelta(minutes=3)
    changed["accepted_at_utc"] = later.isoformat()
    changed["causal_cutoff_at_utc"] = later.isoformat()
    changed["baselines"]["20"]["reason"] = "parquet_read_failed"
    second = dated.retain(first, changed, now=later, governance=GOV)
    assert second["evidence"] == first["evidence"]
    assert second["evidence_sha256"] == first["evidence_sha256"]
    assert first == original
    result = dated.describe(second, now=later, governance=GOV)
    assert result["status"] == "PARTIAL"
    assert result["accepted_at_utc"] == NOW.isoformat()
    assert result["deltas"]["delta_20d"]["reason"] == "raw_partition_missing"
    assert result["latest_baseline_diagnostics"]["baselines"]["20"]["reason"] == "parquet_read_failed"
    assert result["latest_baseline_diagnostics"]["checked_at_utc"] == later.isoformat()
    assert result["deltas"]["delta_1d"]["values"]["fiz.net"] == 1
    before_diagnostic = dated.describe(second, now=NOW, governance=GOV)
    assert before_diagnostic["latest_baseline_diagnostics"] is None
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}},
                dated.STORE_KEY: second}
    output = {"futoi_context": {"futoi_live": {"dated_comparisons": result}}}
    dated.verify_projection(snapshot, output, now=later)
    result["latest_baseline_diagnostics"]["baselines"]["20"]["reason"] = "forged"
    with pytest.raises(AssertionError, match="diagnostics"):
        dated.verify_projection(snapshot, output, now=later)


def test_slow_runner_final_clock_follows_si_source_capture():
    import ast
    runner = Path(__file__).resolve().parents[2] / "src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_live_market_oi.py"
    tree = ast.parse(runner.read_text(encoding="utf-8"))
    refresh = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "refresh_snapshot")
    calls = [n for n in ast.walk(refresh) if isinstance(n, ast.Call)]
    captures = [n for n in calls if isinstance(n.func, ast.Name) and n.func.id == "capture_si_dated"]
    finals = [n for n in calls if isinstance(n.func, ast.Attribute) and n.func.attr == "finalize_snapshot_timing"]
    assert len(captures) == len(finals) == 1
    assert captures[0].lineno < finals[0].lineno
    assert any(k.arg == "now_fn" and isinstance(k.value, ast.Name) and k.value.id == "now_fn" for k in captures[0].keywords)
    assert not any(k.arg == "now" for k in captures[0].keywords)


@pytest.mark.parametrize("boundary", ["refresh", "accepted", "cutoff", "attempt", "diagnostic"])
def test_capture_rejects_global_clock_rollback_before_source_access(monkeypatch, boundary):
    previous = {dated.STORE_KEY: dated.retain(None, candidate(), now=NOW, governance=GOV)}
    future = (NOW + timedelta(seconds=1)).isoformat()
    start = NOW
    prior = previous[dated.STORE_KEY]
    if boundary == "refresh":
        start = future
    elif boundary in ("accepted", "cutoff"):
        prior["evidence"]["accepted_at_utc" if boundary == "accepted" else "causal_cutoff_at_utc"] = future
    elif boundary == "attempt":
        prior["last_capture_attempt_at_utc"] = future
    else:
        prior["latest_baseline_diagnostics"] = {"checked_at_utc": future}
    monkeypatch.setattr(dated, "_capture_candidate", lambda *a, **k: pytest.fail("source read before clock admission"))
    snapshot = {"components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}}}
    before = deepcopy(snapshot)
    with pytest.raises(ValueError, match="precedes_refresh_or_prior_timeline"):
        dated.capture_snapshot(snapshot, previous, now_fn=lambda: NOW, refresh_started_at=start)
    assert snapshot == before


def test_transient_baseline_refusal_keeps_valid_accepted_fact_and_clock():
    first = dated.retain(None, candidate(), now=NOW, governance=GOV)
    later = NOW + timedelta(minutes=2)
    retry = candidate()
    retry["accepted_at_utc"] = retry["causal_cutoff_at_utc"] = later.isoformat()
    retry["baselines"]["5"] = {"status": "UNAVAILABLE", "target_trade_date": retry["baselines"]["5"]["target_trade_date"], "factual": None, "reason": "temporary_read_error"}
    second = dated.retain(first, retry, now=later, governance=GOV)
    assert second["evidence"] == first["evidence"]
    assert second["evidence_sha256"] == first["evidence_sha256"]
    output = dated.describe(second, now=later, governance=GOV)
    assert output["status"] == "AVAILABLE"
    assert output["accepted_at_utc"] == NOW.isoformat()
    assert output["deltas"]["delta_5d"]["values"]["fiz.net"] == 5
    assert output["latest_baseline_diagnostics"]["baselines"]["5"]["reason"] == "temporary_read_error"


def test_units_and_family_scope_are_verified():
    snapshot = {dated.STORE_KEY: store(candidate()), "components": {"futoi_live": {"data": {"governance": GOV}}}}
    output = dated.describe(snapshot[dated.STORE_KEY], now=NOW, governance=GOV)
    release = {"futoi_context": {"futoi_live": {"dated_comparisons": output}}}
    assert output["units"]["net_share_of_oi"] == "fraction_of_total_open_interest"
    dated.verify_projection(snapshot, release, now=NOW)
    output["units"]["participant_fields"] = "unique_people"
    with pytest.raises(AssertionError, match="units"):
        dated.verify_projection(snapshot, release, now=NOW)


@pytest.mark.parametrize("state", ["missing", "expired", "malformed", "governance"])
@pytest.mark.parametrize("tamper", [None, "omitted", "reason", "scope", "authority", "facts"])
def test_refused_projection_requires_complete_canonical_refusal(state, tamper):
    snapshot = {dated.STORE_KEY: store(candidate()), "components": {"futoi_live": {"data": {"governance": deepcopy(GOV)}}}}
    at = NOW
    if state == "missing":
        snapshot.pop(dated.STORE_KEY)
    elif state == "expired":
        at += timedelta(days=5)
    elif state == "malformed":
        snapshot[dated.STORE_KEY]["evidence_sha256"] = "bad"
    else:
        snapshot["components"]["futoi_live"]["data"]["governance"]["factual_use_allowed"] = False
    output = dated.describe(snapshot.get(dated.STORE_KEY), now=at, governance=snapshot["components"]["futoi_live"]["data"]["governance"])
    assert output["status"] == "UNAVAILABLE"
    if tamper == "omitted": output = None
    elif tamper == "reason": output["reason"] = "invented"
    elif tamper == "scope": output["scope"] = "CURRENT_USABLE"
    elif tamper == "authority": output["model_usable"] = True
    elif tamper == "facts": output["anchor"] = candidate()["anchor"]
    release = {"futoi_context": {"futoi_live": {"dated_comparisons": output}}}
    if tamper is None:
        dated.verify_projection(snapshot, release, now=at)
    else:
        with pytest.raises(AssertionError, match="Si dated refusal"):
            dated.verify_projection(snapshot, release, now=at)


@pytest.mark.parametrize("defect", ["arithmetic", "refusal_omitted", "refusal_authority", "refusal_facts"])
def test_projection_checks_survive_optimized_python(tmp_path, defect):
    import subprocess
    import sys
    snapshot = {dated.STORE_KEY: store(candidate()), "components": {"futoi_live": {"data": {"governance": GOV}}}}
    at = NOW if defect == "arithmetic" else NOW + timedelta(days=5)
    output = dated.describe(snapshot[dated.STORE_KEY], now=at, governance=GOV)
    if defect == "arithmetic": output["deltas"]["delta_5d"]["values"]["fiz.net"] = 999
    elif defect == "refusal_omitted": output = None
    elif defect == "refusal_authority": output["action_authority"] = True
    else: output["anchor"] = candidate()["anchor"]
    fixture = tmp_path / "witness.json"
    fixture.write_text(json.dumps([snapshot, {"futoi_context": {"futoi_live": {"dated_comparisons": output}}}]))
    script = """import runpy,json,sys
from datetime import datetime
m=runpy.run_path(sys.argv[1]); snapshot,release=json.load(open(sys.argv[2]))
try: m['verify_projection'](snapshot,release,now=datetime.fromisoformat(sys.argv[3]))
except AssertionError: sys.exit(0)
sys.exit(7)
"""
    result = subprocess.run([sys.executable, "-O", "-c", script, str(path), str(fixture), at.isoformat()], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_verified_frame_decodes_checked_buffer_despite_path_replacement(tmp_path, monkeypatch):
    import pandas as pd
    from hashlib import sha256
    from moex_data.futures import futoi_delta_statistics_context as engine
    partition = tmp_path / "source.parquet"
    pd.DataFrame({"value": [1]}).to_parquet(partition)
    provenance = {"partition_ref": "${MOEX_DATA_ROOT}/source.parquet", "partition_sha256": sha256(partition.read_bytes()).hexdigest()}
    read = engine.pd.read_parquet
    def replace_then_decode(buffer):
        pd.DataFrame({"value": [999]}).to_parquet(partition)
        return read(buffer)
    monkeypatch.setattr(engine.pd, "read_parquet", replace_then_decode)
    assert dated._verified_frame(tmp_path, provenance, "partition")["value"].tolist() == [1]
    with pytest.raises(ValueError, match="digest_mismatch"):
        dated._verified_frame(tmp_path, provenance, "partition")


@pytest.mark.parametrize("capture_fails", [False, True])
def test_runner_refuses_generation_before_completed_si_attempt(tmp_path, monkeypatch, capture_fails):
    from moex_data import rub_si_futoi_dated_context as integrated
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
    base = live.base
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(base, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(base, "install_timestamp_policy", lambda: None)
    monkeypatch.setattr(base, "bind_futures_calendar_clock", lambda *a, **k: {})
    monkeypatch.setattr(base, "bind_oil_history", lambda *a, **k: {})
    monkeypatch.setattr(live.current_context.current, "current_producers", lambda: {})
    monkeypatch.setattr(live.current_context.context, "run_refresh_all", lambda **k: {})
    monkeypatch.setattr(live.current_context.delta_context, "build_all", lambda **k: {})
    monkeypatch.setattr(live.parallel_prefetch, "prefetch_producers", lambda *a, **k: {})
    monkeypatch.setattr(live.current_context, "_attach_futoi_context", lambda *a: None)
    monkeypatch.setattr(live, "attach_live_market_oi_context", lambda *a, **k: None)
    monkeypatch.setattr(live, "attach_live_basis_carry_context", lambda *a, **k: None)
    monkeypatch.setattr(live.user_position, "attach_user_position_context", lambda *a, **k: None)
    monkeypatch.setattr("moex_data.rub_dated_hour_source.acquire", lambda **k: {})
    previous = {dated.STORE_KEY: dated.retain(None, candidate(), now=NOW, governance=GOV)} if capture_fails else {}
    monkeypatch.setattr(base, "_load_previous", lambda *a: previous)
    snapshot = {"identity": {}, "components": {"futoi_live": {"data": {"governance": GOV}}}}
    monkeypatch.setattr(live.futoi, "build_snapshot", lambda **k: snapshot)
    def collect(*a, **k):
        if capture_fails:
            raise ValueError("temporary_source_failure")
        return candidate()
    monkeypatch.setattr(integrated, "_capture_candidate", collect)
    monkeypatch.setattr(base, "_atomic_write", lambda *a: pytest.fail("regressed generation must not publish"))
    ticks = iter(NOW + timedelta(seconds=n) for n in (0, 1, 3, 2))
    with pytest.raises(base.ChatAnalysisSnapshotError, match="precedes Si capture completion"):
        live.refresh_snapshot(now_fn=lambda: next(ticks), live_loader=lambda: {})
    assert snapshot[dated.STORE_KEY]["last_capture_attempt_at_utc"] == (NOW+timedelta(seconds=3)).isoformat()
    if capture_fails:
        assert snapshot[dated.STORE_KEY]["evidence"] == previous[dated.STORE_KEY]["evidence"]
    else:
        assert snapshot[dated.STORE_KEY]["evidence"]["accepted_at_utc"] == (NOW+timedelta(seconds=3)).isoformat()
    assert "generated_at_utc" not in snapshot["identity"]


@pytest.mark.parametrize("corruption", [None, "raw", "eod", "witness"])
def test_capture_facts_and_dates_must_match_verified_partition_bytes(tmp_path, monkeypatch, corruption):
    import pandas as pd
    from hashlib import sha256
    from moex_data import rub_temporal_applicability as temporal
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    e = candidate()
    dates = e["observed_date_witness"]["observed_trade_dates"]
    def write_frame(name, frame, prefix="partition"):
        filename = name + ".parquet"
        frame.to_parquet(tmp_path / filename)
        return {prefix + "_ref": "${MOEX_DATA_ROOT}/" + filename,
                prefix + "_sha256": sha256((tmp_path / filename).read_bytes()).hexdigest()}
    def raw(day):
        return pd.DataFrame([dict(trade_date=day, ts=day+" 23:50:00", systime=day+" 23:55:00",
            availability_ts_utc=day+"T20:56:00+00:00", ingest_ts=day+"T20:57:00+00:00",
            sess_id=1, seqnum=1, clgroup=side, pos=net, pos_long=long, pos_short=short,
            pos_long_num=10, pos_short_num=11, source_id=dated.SOURCE,
            instrument_id=dated.INSTRUMENT, source_ticker="si", secid="SiU6")
            for side, net, long, short in (("FIZ",20,100,-80),("YUR",-20,80,-100))])
    identity = {"source_ticker": "si", "secid": "SiU6"}
    monkeypatch.setattr(source, "source_identity", lambda *a: identity)
    monkeypatch.setattr(source, "_data_root", lambda: tmp_path)
    monkeypatch.setattr(temporal, "_previous_witness", lambda *a: dates[-1])
    monkeypatch.setattr(temporal, "_previous_reason", lambda *a, **k: None)
    anchor_frame = raw(dates[-1])
    anchor_fact = source.latest_aligned_factual(anchor_frame, expected_trade_date=dates[-1], expected_instrument_id=dated.INSTRUMENT, expected_source_ticker="si", expected_secid="SiU6")
    anchor_proof = write_frame("anchor", anchor_frame, "raw_partition")
    for prefix in ("raw_quality_report", "raw_refresh_manifest"):
        (tmp_path / (prefix+".json")).write_text("{}")
        anchor_proof.update({prefix+"_ref": "${MOEX_DATA_ROOT}/"+prefix+".json", prefix+"_sha256": sha256(b"{}").hexdigest()})
    anchor_proof["accepted_state_kind"] = "source_native_exact_date_raw_quality_pass"
    witness = deepcopy(e["observed_date_witness"])
    witness["provenance"].update(write_frame("dates", pd.DataFrame({"trade_date": dates})))
    for prefix in ("manifest", "quality_report"):
        witness["provenance"].update({prefix+"_ref": anchor_proof["raw_quality_report_ref"], prefix+"_sha256": anchor_proof["raw_quality_report_sha256"]})
    if corruption == "witness":
        witness["observed_trade_dates"].pop(1)
    monkeypatch.setattr(engine, "_observed_witness", lambda *a, **k: deepcopy(witness))
    monkeypatch.setattr(engine, "_accepted_eod", lambda *a, **k: (pd.DataFrame(), {}))
    def baseline(*args, trade_date, **kwargs):
        frame = raw(trade_date)
        fact = source.latest_aligned_factual(frame, expected_trade_date=trade_date, expected_instrument_id=dated.INSTRUMENT, expected_source_ticker="si", expected_secid="SiU6")
        fact = engine._normalized_factual(fact, field="test")
        proof = write_frame(trade_date, frame, "raw_partition")
        proof.update(source_id=dated.SOURCE, factual_validation="PASS")
        if corruption == "raw":
            fact["fiz"]["long_participants"] += 1
        if corruption == "eod":
            pointer = write_frame("eod_"+trade_date, pd.DataFrame({"trade_date": [trade_date], "net": [20]}))
            for prefix in ("manifest", "quality_report"):
                pointer.update({prefix+"_ref": anchor_proof["raw_quality_report_ref"], prefix+"_sha256": anchor_proof["raw_quality_report_sha256"]})
            return {"status": "AVAILABLE", "source_kind": "accepted_stage5_eod_historical_context_only", "factual": fact, "provenance": {"accepted_pointer": pointer}}
        return {"status": "AVAILABLE", "factual": fact, "provenance": proof}
    monkeypatch.setattr(engine, "_factual_for_date", baseline)
    monkeypatch.setattr(engine, "_eod_factual", lambda row, **kwargs: {"different_verified_fact": int(row["net"])})
    component = {"data": {"instrument_id": dated.INSTRUMENT, "source_id": dated.SOURCE, "governance": GOV,
        "previous_completed_session": {"factual": anchor_fact, "provenance": anchor_proof}, "context_refresh": {}}}
    if corruption == "witness":
        with pytest.raises(ValueError, match="dates_differ"):
            dated._capture_candidate(component, now=NOW)
    else:
        captured = dated._capture_candidate(component, now=NOW)
        assert {row["status"] for row in captured["baselines"].values()} == ({"AVAILABLE"} if corruption is None else {"UNAVAILABLE"})
        if corruption:
            assert all("differs_from" in row["reason"] for row in captured["baselines"].values())
