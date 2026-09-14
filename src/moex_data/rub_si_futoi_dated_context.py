"""Si-only dated FUTOI comparisons, independent of the current-pair TTL.

The producer validates original accepted sources. The portable read view checks
retained identity, causality, arithmetic and the content digest; that digest is
not a signature or proof against coordinated replacement of all source evidence.
"""
from copy import deepcopy
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from hashlib import sha256
import json
from math import isfinite
import re

SCHEMA = "si_futoi_dated_comparisons.v1"
CONTRACT = "contracts/intelligence/si_futoi_dated_comparisons_v1.json"
STORE_KEY = "accepted_dated_futoi_si"
INSTRUMENT = "si_futures_family"
SOURCE = "moex_algopack_futoi"
MAX_AGE = 96 * 3600
LAGS = (1, 5, 20)
SIDE_FIELDS = ("long", "short", "net", "long_participants", "short_participants")
FLAGS = {
    "current_usable": False, "session_completion_proven": False,
    "historical_pit_usable": False, "model_usable": False,
    "directional_authority": False, "action_authority": False,
    "standalone_buy_sell_authority": False, "stage5_full_mode_ready": False,
    "stage5_pointer_promotion_performed": False,
}


def _stamp(value):
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.utcoffset() is None:
        raise ValueError("aware_timestamp_required")
    return parsed.astimezone(timezone.utc)


def _day(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("canonical_source_date_required")
    return value


def _digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _bounded(value, now):
    age = (now - _stamp(value)).total_seconds()
    if not 0 <= age <= MAX_AGE:
        raise ValueError("dated_context_outside_96h")
    return age


def _governance(value):
    if not isinstance(value, dict) or value.get("instrument_id") != INSTRUMENT:
        raise ValueError("si_governance_identity_not_admitted")
    for key in ("factual_use_allowed", "all_required_gates_pass",
                "instrument_local_acceptance_pass", "canonical_live_smoke_accepted"):
        if value.get(key) is not True:
            raise ValueError("si_governance_not_admitted")
    for key in ("directional_authority", "action_authority", "standalone_buy_sell_authority"):
        if value.get(key) is not False:
            raise ValueError("si_governance_authority_mismatch")


def _integer(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError("finite_integer_required")
    if not isfinite(value) or int(value) != value:
        raise ValueError("finite_integer_required")
    return int(value)


def _fact(record, expected, at):
    """Independent retained-fact arithmetic, not the engine's normalizer."""
    if not isinstance(record, dict) or record.get("instrument_id") != INSTRUMENT:
        raise ValueError("factual_instrument_mismatch")
    if record.get("source_id") != SOURCE:
        raise ValueError("factual_source_mismatch")
    fact = record["factual"]
    if not isinstance(fact, dict) or _day(fact.get("trade_date")) != expected:
        raise ValueError("exact_target_date_mismatch")
    event = _stamp(fact["snapshot_ts"])
    if event.astimezone(ZoneInfo("Europe/Moscow")).date().isoformat() != expected:
        raise ValueError("source_event_date_mismatch")
    available = _stamp(fact["availability_ts_utc"])
    if not event <= available <= at:
        raise ValueError("factual_not_available_at_capture")
    for key in ("source_publication_time", "ingest_ts_utc"):
        if fact.get(key) is not None:
            t = _stamp(fact[key])
            if key == "source_publication_time" and not event <= t <= available:
                raise ValueError("source_publication_chronology_mismatch")
            if key == "ingest_ts_utc" and not available <= t <= at:
                raise ValueError("source_ingest_chronology_mismatch")
    oi = _integer(fact["total_open_interest"])
    if oi <= 0:
        raise ValueError("positive_open_interest_required")
    values = {"total_open_interest": oi}
    for side in ("fiz", "yur"):
        raw = fact[side]
        for field in SIDE_FIELDS:
            number = _integer(raw[field])
            if field != "net" and number < 0:
                raise ValueError("negative_absolute_position_or_count")
            values[side + "." + field] = number
        if raw["long"] - raw["short"] != raw["net"]:
            raise ValueError("side_net_identity_failed")
        share = raw["net"] / oi
        if "net_share_of_oi" in raw and raw["net_share_of_oi"] != share:
            raise ValueError("net_share_identity_failed")
        values[side + ".net_share_of_oi"] = share
    if values["fiz.net"] + values["yur.net"] != 0:
        raise ValueError("net_balance_failed")
    if any(values["fiz." + k] + values["yur." + k] != oi for k in ("long", "short")):
        raise ValueError("total_open_interest_identity_failed")
    return values


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch("[0-9a-f]{64}", value):
        raise ValueError("source_digest_required")


def _ref(value):
    if not isinstance(value, str) or not value.startswith("${MOEX_DATA_ROOT}/"):
        raise ValueError("canonical_evidence_reference_required")
    tail = value[len("${MOEX_DATA_ROOT}/"):]
    if not tail or any(part in ("", ".", "..") for part in tail.split("/")) or "\\" in tail:
        raise ValueError("unsafe_evidence_reference")


def _proof(record):
    proof = record["provenance"]
    if not isinstance(proof, dict):
        raise ValueError("source_provenance_required")
    if record["source_kind"] == "accepted_eod":
        if proof.get("source_kind") != "accepted_stage5_eod_historical_context_only":
            raise ValueError("eod_scope_mismatch")
        proof = proof["accepted_pointer"]
        if proof.get("acceptance_contract_id") != "step5_futoi_positioning_acceptance.v1":
            raise ValueError("eod_acceptance_required")
        for prefix in ("partition", "manifest", "quality_report"):
            _ref(proof[prefix + "_ref"])
            _hash(proof[prefix + "_sha256"])
    elif record["source_kind"] in ("previous_observed", "canonical_raw"):
        _ref(proof["raw_partition_ref"])
        _hash(proof["raw_partition_sha256"])
        if record["source_kind"] == "previous_observed":
            if proof.get("accepted_state_kind") != "source_native_exact_date_raw_quality_pass":
                raise ValueError("previous_source_acceptance_required")
            for prefix in ("raw_quality_report", "raw_refresh_manifest"):
                _ref(proof[prefix + "_ref"])
                _hash(proof[prefix + "_sha256"])
        elif proof.get("factual_validation") != "PASS" or proof.get("source_id") != SOURCE:
            raise ValueError("raw_factual_validation_required")
    else:
        raise ValueError("unknown_factual_source_kind")


def _validated(evidence, now):
    if evidence.get("schema_version") != SCHEMA or evidence.get("contract_ref") != CONTRACT:
        raise ValueError("dated_schema_or_contract_mismatch")
    if evidence.get("instrument_id") != INSTRUMENT or evidence.get("source_id") != SOURCE:
        raise ValueError("dated_source_identity_mismatch")
    if any(evidence.get(k) is not False for k in FLAGS):
        raise ValueError("dated_authority_mismatch")
    accepted = _stamp(evidence["accepted_at_utc"])
    _bounded(accepted, now)
    _governance(evidence["governance_at_acceptance"])
    anchor = evidence["anchor"]
    if anchor.get("source_kind") != "previous_observed":
        raise ValueError("previous_observed_anchor_required")
    day = _day(anchor["factual"]["trade_date"])
    anchor_values = _fact(anchor, day, accepted)
    _proof(anchor)
    event_age = _bounded(anchor["factual"]["snapshot_ts"], now)
    witness = evidence["observed_date_witness"]
    if witness.get("authority_source_id") != "moex_algopack_fo_tradestats_5m":
        raise ValueError("observed_date_authority_mismatch")
    if witness.get("previous_observed_trade_date") != day:
        raise ValueError("anchor_does_not_match_observed_witness")
    if witness.get("status") != "PASS":
        raise ValueError("observed_date_witness_not_accepted")
    dates = witness["observed_trade_dates"]
    if not isinstance(dates, list) or not dates or len(dates) > 21:
        raise ValueError("bounded_observed_dates_required")
    if dates != sorted(set(dates)) or any(_day(d) > day for d in dates) or dates[-1] != day:
        raise ValueError("observed_dates_not_canonical")
    provenance = witness["provenance"]
    if provenance.get("acceptance_contract_id") != "step7_rub_native_d1_w1_technical_acceptance.v1":
        raise ValueError("observed_witness_acceptance_required")
    for prefix in ("partition", "manifest", "quality_report"):
        _ref(provenance[prefix + "_ref"])
        _hash(provenance[prefix + "_sha256"])
    baselines = evidence["baselines"]
    if not isinstance(baselines, dict) or set(baselines) != {str(lag) for lag in LAGS}:
        raise ValueError("exact_baseline_slots_required")
    result = {}
    for lag in LAGS:
        target = dates[-1-lag] if len(dates) > lag else None
        baseline = baselines[str(lag)]
        if not isinstance(baseline, dict) or baseline.get("target_trade_date") != target:
            raise ValueError("ordinal_target_mismatch")
        item = {"session_lag": lag, "anchor_trade_date": day, "target_trade_date": target}
        if baseline.get("status") == "AVAILABLE":
            if target is None:
                raise ValueError("baseline_without_observed_target")
            _proof(baseline)
            values = _fact(baseline, target, accepted)
            item.update(status="AVAILABLE",
                        values={k: anchor_values[k] - values[k] for k in anchor_values},
                        baseline=deepcopy(baseline))
        else:
            if baseline.get("status") != "UNAVAILABLE" or not isinstance(baseline.get("reason"), str) or not baseline["reason"]:
                raise ValueError("explicit_baseline_refusal_required")
            if baseline.get("factual") is not None:
                raise ValueError("unavailable_baseline_must_not_expose_values")
            item.update(status="UNAVAILABLE", values=None, reason=baseline["reason"])
        result["delta_" + str(lag) + "d"] = item
    return {
        "status": "AVAILABLE" if all(v["status"] == "AVAILABLE" for v in result.values()) else "PARTIAL",
        "scope": "SI_ACCEPTED_DATED_PREPARATION_ONLY",
        "anchor": deepcopy(anchor), "deltas": result,
        "accepted_at_utc": evidence["accepted_at_utc"],
        "source_event_age_seconds": event_age,
        "maximum_anchor_source_age_seconds": MAX_AGE,
        "observed_date_witness": deepcopy(witness),
        "participant_count_semantics": "side_counts_not_unique_participants",
        **FLAGS,
    }


def describe(store, *, now, governance):
    """No file reads, clock substitution, source fetch or implicit live promotion."""
    try:
        now = _stamp(now)
        _governance(governance)
        if not isinstance(store, dict) or store.get("schema_version") != SCHEMA:
            raise ValueError("dated_si_evidence_not_captured")
        evidence = store["evidence"]
        if _digest(evidence) != store["evidence_sha256"]:
            raise ValueError("dated_evidence_digest_mismatch")
        result = _validated(evidence, now)
        result.update(checked_at_utc=now.isoformat(),
                      evidence_sha256=store["evidence_sha256"],
                      last_capture_error=store.get("last_capture_error"))
        return result
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        return {"status": "UNAVAILABLE", "scope": "SI_ACCEPTED_DATED_PREPARATION_ONLY",
                "reason": str(exc), **FLAGS}


def _semantic(evidence):
    """New receipts alone are not a new economic observation or first acceptance."""
    def observation(record):
        fact = deepcopy(record.get("factual"))
        if isinstance(fact, dict):
            fact.pop("availability_ts_utc", None)
            fact.pop("ingest_ts_utc", None)
        return {"status": record.get("status"), "target_trade_date": record.get("target_trade_date"),
                "factual": fact, "reason": record.get("reason")}
    return {"anchor": observation(evidence["anchor"]),
            "dates": evidence["observed_date_witness"]["observed_trade_dates"],
            "baselines": {key: observation(value) for key, value in evidence["baselines"].items()}}


def retain(previous, candidate, *, now, governance, error=None):
    """Only the producer calls this, after source verification of a candidate."""
    original = deepcopy(previous) if isinstance(previous, dict) else {}
    try:
        now = _stamp(now)
        _governance(governance)
        if candidate is None:
            raise ValueError(error or "no_new_admitted_previous_observation")
        _validated(candidate, now)
        old = describe(original, now=now, governance=governance)
        if old["status"] in ("AVAILABLE", "PARTIAL") and _semantic(original["evidence"]) == _semantic(candidate):
            chosen = original["evidence"]
        else:
            chosen = deepcopy(candidate)
        return {"schema_version": SCHEMA, "evidence": chosen, "evidence_sha256": _digest(chosen),
                "last_capture_attempt_at_utc": now.isoformat(), "last_capture_error": None}
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        original["last_capture_error"] = str(exc)
        original["last_capture_attempt_at_utc"] = _stamp(now).isoformat()
        return original


def _check_source_refs(root, provenance, prefixes):
    """Validate original archived bytes at capture, not at a fictitious old time."""
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    for prefix in prefixes:
        ref, expected = provenance[prefix + "_ref"], provenance[prefix + "_sha256"]
        _ref(ref)
        _hash(expected)
        path = root / ref[len("${MOEX_DATA_ROOT}/"):]
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("source_evidence_missing_or_escaped_root")
        if source._sha256_file(path) != expected:
            raise ValueError("source_evidence_digest_mismatch")


def _capture_candidate(component, *, now):
    """Reuse the canonical source/observed-date/accepted-EOD loaders; no network."""
    from moex_data import rub_temporal_applicability as temporal
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    data = component["data"]
    _governance(data["governance"])
    if data.get("instrument_id") != INSTRUMENT or data.get("source_id") != SOURCE:
        raise ValueError("si_component_identity_mismatch")
    previous, context = data["previous_completed_session"], data["context_refresh"]
    if previous.get("consumer_factual_use_allowed") is False:
        raise ValueError("previous_factual_use_explicitly_denied")
    expected = temporal._previous_witness(context, now)
    reason = temporal._previous_reason(previous, context, expected=expected,
                                      instrument=INSTRUMENT, data=data, now=now)
    if reason is not None:
        raise ValueError(reason)
    root = source._data_root()
    proof = deepcopy(previous["provenance"])
    _check_source_refs(root, proof, ("raw_partition", "raw_quality_report", "raw_refresh_manifest"))
    path = root / proof["raw_partition_ref"][len("${MOEX_DATA_ROOT}/"):]
    identity = source.source_identity(INSTRUMENT)
    factual = source.latest_aligned_factual(
        engine.pd.read_parquet(path), expected_trade_date=expected,
        expected_instrument_id=INSTRUMENT,
        expected_source_ticker=identity["source_ticker"], expected_secid=identity["secid"])
    if factual != previous["factual"]:
        raise ValueError("previous_fact_differs_from_frozen_raw")
    _check_source_refs(root, proof, ("raw_partition",))
    anchor = {"instrument_id": INSTRUMENT, "source_id": SOURCE,
              "status": "AVAILABLE", "source_kind": "previous_observed",
              "factual": deepcopy(factual), "provenance": proof}
    raw_context = {**context, engine.session_context.PREVIOUS_ROLE: previous,
                   engine.session_context.CURRENT_ROLE: data.get("current_intraday")}
    witness = engine._observed_witness(root, as_of=now, raw_context=raw_context)
    if witness["previous_observed_trade_date"] != expected:
        raise ValueError("independent_observed_witness_disagrees")
    _check_source_refs(root, witness["provenance"], ("partition", "manifest", "quality_report"))
    i = witness["observed_trade_dates"].index(expected)
    witness["observed_trade_dates"] = witness["observed_trade_dates"][max(0, i-20):i+1]
    witness["source_current_observed_trade_date"] = witness.pop("current_observed_trade_date", None)
    witness["retained_scope"] = "anchor_and_exact_20_prior_observed_dates"
    dates = witness["observed_trade_dates"]
    eod_error = None
    try:
        eod, eod_proof = engine._accepted_eod(root, instrument_id=INSTRUMENT, as_of=now)
    except Exception as exc:
        # Missing EOD must not suppress exact validated recent raw baselines.
        eod, eod_proof = None, None
        eod_error = type(exc).__name__ + ": " + str(exc)
    baselines = {}
    for lag in LAGS:
        target = dates[-1-lag] if len(dates) > lag else None
        record = {"status": "UNAVAILABLE", "target_trade_date": target,
                  "factual": None, "reason": "insufficient_observed_date_history"}
        if target is not None:
            try:
                loaded = (engine._factual_for_date(
                    root, instrument_id=INSTRUMENT, trade_date=target, previous={},
                    eod=eod, eod_provenance=eod_proof)
                    if eod is not None else engine._raw_factual(root, instrument_id=INSTRUMENT, trade_date=target))
                if loaded.get("status") != "AVAILABLE":
                    raise ValueError(loaded.get("reason") or "exact_target_not_admitted")
                fact, provenance = deepcopy(loaded["factual"]), deepcopy(loaded["provenance"])
                if loaded.get("source_kind") == "accepted_stage5_eod_historical_context_only":
                    kind = "accepted_eod"
                    _check_source_refs(root, provenance["accepted_pointer"], ("partition", "manifest", "quality_report"))
                else:
                    kind = "canonical_raw"
                    _check_source_refs(root, provenance, ("raw_partition",))
                    path = root / provenance["raw_partition_ref"][len("${MOEX_DATA_ROOT}/"):]
                    frozen = source._freeze_artifact(root, path, provenance["raw_partition_sha256"])
                    provenance["raw_partition_ref"] = source._rooted_ref(root, frozen)
                record = {"instrument_id": INSTRUMENT, "source_id": SOURCE,
                          "source_kind": kind, "status": "AVAILABLE",
                          "target_trade_date": target, "factual": fact, "provenance": provenance}
                _fact(record, target, now)
                _proof(record)
            except Exception as exc:
                record = {"status": "UNAVAILABLE", "target_trade_date": target,
                          "factual": None, "reason": type(exc).__name__ + ": " + str(exc)}
        baselines[str(lag)] = record
    candidate = {
        "schema_version": SCHEMA, "contract_ref": CONTRACT,
        "instrument_id": INSTRUMENT, "source_id": SOURCE,
        "accepted_at_utc": now.isoformat(), "anchor": anchor, "baselines": baselines,
        "observed_date_witness": witness, "governance_at_acceptance": deepcopy(data["governance"]),
        "accepted_eod_load_error": eod_error, **FLAGS,
    }
    _validated(candidate, now)
    return candidate


def capture_snapshot(snapshot, previous, *, now):
    """Attach one bounded Si witness to the existing canonical slow snapshot."""
    component = snapshot.get("components", {}).get("futoi_live")
    if not isinstance(component, dict):
        return
    data = component.get("data")
    governance = data.get("governance") if isinstance(data, dict) else None
    candidate, error = None, None
    try:
        candidate = _capture_candidate(component, now=_stamp(now))
    except Exception as exc:
        error = type(exc).__name__ + ": " + str(exc)
    snapshot[STORE_KEY] = retain((previous or {}).get(STORE_KEY), candidate,
                                now=now, governance=governance, error=error)


def attach_consumer(snapshot, consumers, *, now):
    component = snapshot.get("components", {}).get("futoi_live") or {}
    data = component.get("data") or {}
    consumers["futoi_context"]["futoi_live"]["dated_comparisons"] = describe(
        snapshot.get(STORE_KEY), now=now, governance=data.get("governance"))


def verify_projection(snapshot, release, *, now):
    """Reverse completeness plus arithmetic from input facts, not exported deltas.

    Source admission is shared with the portable reader; arithmetic below does
    not call describe or reuse its computed delta values. Original raw/evidence
    authenticity is a separate capture/export audit boundary.
    """
    from decimal import Decimal
    from math import isclose
    component = snapshot.get("components", {}).get("futoi_live") or {}
    data = component.get("data") or {}
    stored = snapshot.get(STORE_KEY)
    output = ((release.get("futoi_context") or {}).get("futoi_live") or {}).get("dated_comparisons")
    try:
        _governance(data.get("governance"))
        if not isinstance(stored, dict) or stored.get("schema_version") != SCHEMA:
            raise ValueError("no_evidence")
        evidence = stored["evidence"]
        if _digest(evidence) != stored.get("evidence_sha256"):
            raise ValueError("bad_digest")
        admission = _validated(evidence, _stamp(now))
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError):
        if output is not None:
            assert output.get("status") == "UNAVAILABLE", "Si dated refusal completeness"
            assert "anchor" not in output and "deltas" not in output, "Si dated refused values leaked"
        return
    assert isinstance(output, dict), "Si dated admitted context omitted"
    assert output["status"] == admission["status"], "Si dated status completeness"
    assert output["anchor"] == evidence["anchor"], "Si dated anchor changed"
    assert output["accepted_at_utc"] == evidence["accepted_at_utc"], "Si dated acceptance time changed"
    assert output["evidence_sha256"] == stored["evidence_sha256"], "Si dated identity changed"
    assert output["observed_date_witness"] == evidence["observed_date_witness"], "Si dated witness changed"
    assert all(output.get(k) is False for k in FLAGS), "Si dated authority expanded"
    dates = evidence["observed_date_witness"]["observed_trade_dates"]
    anchor = evidence["anchor"]["factual"]
    deltas = output["deltas"]
    assert set(deltas) == {"delta_" + str(lag) + "d" for lag in (1, 5, 20)}, "Si dated missing lag"
    for lag in (1, 5, 20):
        d = deltas["delta_" + str(lag) + "d"]
        original = evidence["baselines"][str(lag)]
        expected = dates[len(dates)-1-lag] if len(dates) > lag else None
        assert d["target_trade_date"] == expected and d["anchor_trade_date"] == anchor["trade_date"], "Si dated target shifted"
        assert d["session_lag"] == lag, "Si dated lag changed"
        if original["status"] != "AVAILABLE":
            assert d["status"] == "UNAVAILABLE" and d["values"] is None, "Si dated missing target became value"
            assert d["reason"] == original["reason"], "Si dated target reason lost"
            continue
        assert d["status"] == "AVAILABLE" and d["baseline"] == original, "Si dated baseline omitted or changed"
        baseline, values = original["factual"], d["values"]
        expected_fields = {"total_open_interest"}
        for side in ("fiz", "yur"):
            for field in SIDE_FIELDS:
                key = side + "." + field
                expected_fields.add(key)
                assert type(values[key]) is int, "Si dated noninteger position delta"
                assert values[key] == int(anchor[side][field]) - int(baseline[side][field]), "Si dated position arithmetic"
            key = side + ".net_share_of_oi"
            expected_fields.add(key)
            ratio = (Decimal(str(anchor[side]["net"])) / Decimal(str(anchor["total_open_interest"]))
                     - Decimal(str(baseline[side]["net"])) / Decimal(str(baseline["total_open_interest"])))
            assert type(values[key]) in (int, float) and isclose(values[key], float(ratio), abs_tol=1e-12, rel_tol=0), "Si dated share arithmetic"
        assert set(values) == expected_fields, "Si dated field coverage"
        assert type(values["total_open_interest"]) is int, "Si dated noninteger OI delta"
        assert values["total_open_interest"] == anchor["total_open_interest"] - baseline["total_open_interest"], "Si dated OI arithmetic"
