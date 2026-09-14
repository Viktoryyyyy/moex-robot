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
        "accepted_at_utc": NOW.isoformat(), "governance_at_acceptance": deepcopy(GOV),
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
