"""Separately admitted CR observed-slot descriptive statistics."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path

from moex_data import rub_cr_futoi_dated_context as dated
from moex_data import rub_futoi_observed_statistics_core as core

common = dated.common
PROFILE = core.StatisticsProfile("cr_futures_family", "cr_futoi_observed_statistics.v1", "accepted_cr_observed_statistics",
    "CR_FAMILY_DESCRIPTIVE_OBSERVED_SUBSETS_NOT_STATISTICAL_CONFIDENCE", "cr_statistics_consumer_semantics.v1")
SCHEMA, STORE_KEY = PROFILE.schema, PROFILE.store_key
CONTRACT = "contracts/intelligence/cr_futoi_observed_statistics_v1.json"
GRANT_KEY = "cr_observed_statistics_admission"
DIAGNOSTICS_KEY = "cr_observed_statistics_capture_failure"
POLICY = "observed_slots_admitted_subset_min2_descriptive.v1"
FLAGS = deepcopy(dated.FLAGS)
SEMANTICS = {"version": PROFILE.semantics_version,
    "sample": "admitted_subset_within_exact_observed_slots_no_older_gap_substitution", "anchor_included_in_sample": True,
    "percentile_formula": "count(sample_value <= anchor_value) / sample_count",
    "percentile_ties": "all_sample_values_equal_to_anchor_are_included_in_numerator",
    "zscore_formula": "(anchor_value - population_mean) / population_std_ddof_0", "standard_deviation_ddof": 0,
    "minimum_sample_count": 2, "below_minimum_reason": "minimum_two_admitted_observations_required",
    "zero_variance_zscore": None, "zero_variance_reason": "zero_population_variance"}
UNITS = {"positions_and_open_interest": "contracts", "participant_fields": "side_counts_not_unique_people",
    "long_short_net_shares": "fraction_of_one_sided_open_interest", "gross_share_of_two_sided_oi": "gross_divided_by_2_times_open_interest",
    "share_changes": "fraction_difference_not_percentage_points", "percentile": "fraction", "zscore": "dimensionless"}
CONTRACT_DOCUMENT = {"schema_version": "cr_futoi_observed_statistics_admission.v1", "task_id": "cr_futoi_observed_statistics_v1",
    "instrument_id": PROFILE.instrument_id, "source_id": dated.SOURCE, "scope": PROFILE.scope, "policy": POLICY,
    "admission": {"descriptive_statistics_allowed": True, "linked_cr_dated_admission_required": True,
        "current_requires_existing_current_pair_and_original_proof": True, "maximum_retained_observed_slots": 504,
        "windows": [252, 504], "lags": [1, 5, 20], "maximum_age_seconds": 345600, "minimum_sample_count": 2,
        "source_selection": "exact_raw_first_invalid_latest_excluded_no_eod_or_older_slot_substitution",
        "wrapped_read_errors_preserve_transient_classification": True,
        "legacy_cr_governance_and_aliases_unchanged": True, **FLAGS}, "statistical_semantics": SEMANTICS, "units": UNITS}


def _grant(value):
    if not isinstance(value, dict) or set(value) != {"contract_ref", "artifact_text", "artifact_sha256"} or value["contract_ref"] != CONTRACT:
        raise ValueError("cr_statistics_grant_required")
    if not isinstance(value["artifact_text"], str) or sha256(value["artifact_text"].encode()).hexdigest() != value["artifact_sha256"]:
        raise ValueError("cr_statistics_grant_digest")
    if common._digest(json.loads(value["artifact_text"])) != common._digest(CONTRACT_DOCUMENT):
        raise ValueError("cr_statistics_grant_policy_or_identity")


def _proof(value):
    if not isinstance(value, dict) or set(value) != {"source_kind", "provenance"}:
        raise ValueError("cr_statistics_proof_shape")
    if value["source_kind"] == "excluded_raw":
        p = value["provenance"]
        if set(p) != {"raw_partition_ref", "raw_partition_sha256"}: raise ValueError("cr_statistics_excluded_proof_shape")
        common._ref(p["raw_partition_ref"]); common._hash(p["raw_partition_sha256"])
    elif value["source_kind"] in ("canonical_raw", "accepted_eod"):
        common._proof(value)
    else: raise ValueError("cr_statistics_proof_source_kind")


def _validate_rows(e, cutoff):
    slots, rows, proofs = e["slots"], e["rows"], e["proofs"]
    if not isinstance(proofs, dict) or len(proofs) > 504: raise ValueError("cr_statistics_proof_bound")
    for key, p in proofs.items():
        if common._digest(p) != key: raise ValueError("cr_statistics_proof_digest")
        _proof(p)
    if not isinstance(rows, list) or len(rows) != len(slots): raise ValueError("cr_statistics_slot_row_cardinality")
    facts, used = {}, set()
    for day, row in zip(slots, rows):
        if not isinstance(row, dict) or set(row) != {"trade_date", "status", "values", "clocks", "proof_id", "reason"} or row["trade_date"] != day:
            raise ValueError("cr_statistics_row_identity_shape")
        p = None
        if row["proof_id"] is not None:
            used.add(row["proof_id"]); p = proofs[row["proof_id"]]
        if row["status"] == "UNAVAILABLE":
            if not isinstance(row["reason"], str) or not row["reason"] or row["values"] is not None or row["clocks"] is not None or (p is not None and p["source_kind"] != "excluded_raw"):
                raise ValueError("cr_statistics_exclusion_shape")
            continue
        if row["status"] != "AVAILABLE" or row["reason"] is not None or p is None or p["source_kind"] == "excluded_raw":
            raise ValueError("cr_statistics_row_status")
        if not isinstance(row["values"], list) or len(row["values"]) != len(core.COLUMNS) or any(type(v) is not int for v in row["values"]):
            raise ValueError("cr_statistics_exact_integer_columns")
        if not isinstance(row["clocks"], dict) or set(row["clocks"]) != set(core.CLOCKS): raise ValueError("cr_statistics_clock_inventory")
        fact = core._decode_row(row)
        dated._valid_record(dated._record(fact, p["source_kind"], p["provenance"], day), day, cutoff)
        facts[day] = fact
    if used != set(proofs): raise ValueError("cr_statistics_unused_proof")
    return facts


def _admit(snapshot, now):
    now = common._stamp(now); _grant(snapshot.get(GRANT_KEY))
    linked = dated.describe(snapshot, now=now)
    if linked["status"] != "AVAILABLE": raise ValueError("cr_statistics_linked_dated_unavailable: " + linked["reason"])
    store = snapshot[STORE_KEY]
    if not isinstance(store, dict) or set(store) != {"schema_version", "evidence", "evidence_sha256", "last_capture_attempt_at_utc", "last_capture_error", "latest_sample_failures", "first_anchor_rejection"}:
        raise ValueError("cr_statistics_store_shape")
    e = store["evidence"]
    if store["schema_version"] != SCHEMA or common._digest(e) != store["evidence_sha256"]: raise ValueError("cr_statistics_evidence_digest")
    if set(e) != {"schema_version", "instrument_id", "source_id", "policy", "admission_at_acceptance", "linked_dated_evidence_sha256",
        "accepted_at_utc", "causal_cutoff_at_utc", "columns", "slots", "rows", "proofs", "observed_date_witness"}:
        raise ValueError("cr_statistics_evidence_shape")
    if e["schema_version"] != SCHEMA or e["instrument_id"] != PROFILE.instrument_id or e["source_id"] != dated.SOURCE or e["policy"] != POLICY or e["columns"] != list(core.COLUMNS):
        raise ValueError("cr_statistics_identity_policy_columns")
    _grant(e["admission_at_acceptance"])
    if e["linked_dated_evidence_sha256"] != linked["evidence_sha256"]: raise ValueError("cr_statistics_linked_source_version_changed")
    cutoff, accepted = common._stamp(e["causal_cutoff_at_utc"]), common._stamp(e["accepted_at_utc"])
    if not common._stamp(linked["accepted_at_utc"]) <= cutoff <= accepted <= now or (now-accepted).total_seconds() > 345600:
        raise ValueError("cr_statistics_capture_or_acceptance_clock")
    attempt = common._stamp(store["last_capture_attempt_at_utc"])
    if attempt < accepted: raise ValueError("cr_statistics_attempt_precedes_acceptance")
    rejection = store["first_anchor_rejection"]
    if rejection is not None:
        if not isinstance(rejection, dict) or set(rejection) != {"checked_at_utc", "trade_date", "reason"}:
            raise ValueError("cr_statistics_anchor_rejection_shape")
        checked = common._stamp(rejection["checked_at_utc"]); common._day(rejection["trade_date"])
        if not accepted <= checked <= attempt or not isinstance(rejection["reason"], str) or not rejection["reason"] or rejection["reason"].startswith(dated.TRANSIENT_READ):
            raise ValueError("cr_statistics_anchor_rejection_metadata")
        if checked <= now: raise ValueError("cr_statistics_latest_anchor_rejected: "+rejection["trade_date"]+": "+rejection["reason"])
    error = common._capture_error(store["last_capture_error"]) if attempt <= now else None
    common._capture_error(store["last_capture_error"])
    slots = e["slots"]
    if not isinstance(slots, list) or not 1 <= len(slots) <= 504 or slots != sorted(set(slots)): raise ValueError("cr_statistics_exact_slot_order_or_bound")
    for day in slots: common._day(day)
    witness = e["observed_date_witness"]
    if set(witness) != {"observed_trade_dates", "previous_observed_trade_date", "current_observed_trade_date", "provenance"} or witness["observed_trade_dates"] != slots:
        raise ValueError("cr_statistics_witness_inventory")
    if witness["previous_observed_trade_date"] != slots[-1] or slots[-1] != linked["dated"]["anchor"]["trade_date"] or witness["current_observed_trade_date"] != linked["witness"]["current_observed_trade_date"]:
        raise ValueError("cr_statistics_dated_witness_mismatch")
    p = witness["provenance"]
    if p.get("acceptance_contract_id") != "step7_rub_native_d1_w1_technical_acceptance.v1": raise ValueError("cr_statistics_observed_date_authority")
    for prefix in ("partition", "manifest", "quality_report"):
        common._ref(p[prefix+"_ref"]); common._hash(p[prefix+"_sha256"])
    facts = _validate_rows(e, cutoff)
    failures = store["latest_sample_failures"]
    if not isinstance(failures, list) or len(failures) > 504: raise ValueError("cr_statistics_sample_failure_bound")
    failure_dates = []
    rows_by_day = {r["trade_date"]: r for r in e["rows"]}
    for item in failures:
        if not isinstance(item, dict) or set(item) != {"trade_date", "reason"} or item["trade_date"] not in rows_by_day or not isinstance(item["reason"], str) or not item["reason"]:
            raise ValueError("cr_statistics_sample_failure_shape")
        failure_dates.append(item["trade_date"]); row = rows_by_day[item["trade_date"]]
        if row["status"] == "AVAILABLE" and not item["reason"].startswith(dated.TRANSIENT_READ): raise ValueError("cr_statistics_definitive_failure_cannot_restore_fact")
        # Latest diagnostic wording is operational; retained evidence keeps its
        # original reason without turning message changes into fact versions.
    if failure_dates != sorted(set(failure_dates)): raise ValueError("cr_statistics_sample_failure_date_order")
    if slots[-1] not in facts or core._values(facts[slots[-1]]) != core._values(linked["dated"]["anchor"]["factual"]):
        raise ValueError("cr_statistics_anchor_facts_mismatch")
    if any(facts[slots[-1]].get(key) != linked["dated"]["anchor"]["factual"].get(key) for key in ("snapshot_ts", "source_publication_time")):
        raise ValueError("cr_statistics_anchor_source_version_mismatch")
    return e, facts, linked, error


def _failure(snapshot, now):
    value = snapshot.get(DIAGNOSTICS_KEY)
    if value is None: return None
    if not isinstance(value, dict) or set(value) != {"checked_at_utc", "error"} or not isinstance(value["error"], str) or not value["error"]:
        raise ValueError("cr_statistics_capture_failure_shape")
    checked = common._stamp(value["checked_at_utc"])
    store = snapshot.get(STORE_KEY)
    if store is not None and (checked != common._stamp(store["last_capture_attempt_at_utc"]) or value["error"] != store["last_capture_error"]):
        raise ValueError("cr_statistics_capture_failure_attempt_mismatch")
    identity = snapshot.get("identity") or {}
    if identity.get("refresh_completed_at_utc") is not None and not common._stamp(identity["refresh_started_at_utc"]) <= checked <= common._stamp(identity["refresh_completed_at_utc"]):
        raise ValueError("cr_statistics_failure_outside_refresh")
    return deepcopy(value) if checked <= now else None


def _current(snapshot, e, now):
    original, _, _ = dated._read_admit(snapshot, now)
    record = dated._current(snapshot, original, now)
    fact = record["factual"]
    if fact["trade_date"] != e["observed_date_witness"]["current_observed_trade_date"] or original["witness"]["previous_observed_trade_date"] != e["slots"][-1]:
        raise ValueError("cr_statistics_current_witness_mismatch")
    return fact


def _view_metadata(role, fact):
    return {"anchor_role": role, "current_pair_usable_at_read": role == "current_intraday" and fact is not None,
        "source_anchor_clocks": {key: fact.get(key) for key in core.CLOCKS} if fact is not None else None,
        "statistical_semantics": deepcopy(SEMANTICS)}


def _summary(slots, facts, anchor, excluded):
    return core._summary(slots, facts, anchor, excluded)


def _metadata(snapshot, e, linked, error, failure, now):
    return {"schema_version": SCHEMA, "policy": POLICY, "status": "AVAILABLE", "scope": PROFILE.scope,
        "instrument_id": PROFILE.instrument_id, "source_id": dated.SOURCE, "minimum_sample_count": 2,
        "units": deepcopy(UNITS), "checked_at_utc": now.isoformat(), "accepted_at_utc": e["accepted_at_utc"],
        "causal_cutoff_at_utc": e["causal_cutoff_at_utc"], "linked_dated_evidence_sha256": linked["evidence_sha256"],
        "statistics_evidence_sha256": snapshot[STORE_KEY]["evidence_sha256"], "last_capture_error": error,
        "latest_sample_failures": deepcopy(snapshot[STORE_KEY]["latest_sample_failures"]) if common._stamp(snapshot[STORE_KEY]["last_capture_attempt_at_utc"]) <= now else [],
        "latest_capture_failure": failure, "audit_reference": "input_snapshot.json#/"+STORE_KEY+"/evidence", **FLAGS}


def describe(snapshot, *, now):
    failure = None
    try:
        now = common._stamp(now); failure = _failure(snapshot, now)
        if snapshot.get(STORE_KEY) is None and failure: raise ValueError(failure["error"])
        e, facts, linked, error = _admit(snapshot, now)
        result = _metadata(snapshot, e, linked, error, failure, now)
        excluded = {r["trade_date"]: r["reason"] for r in e["rows"] if r["status"] == "UNAVAILABLE"}
        result["dated"] = {**_summary(e["slots"], facts, facts[e["slots"][-1]], excluded), **_view_metadata("accepted_previous_observed", facts[e["slots"][-1]])}
        try:
            current = _current(snapshot, e, now); slots = (e["slots"]+[current["trade_date"]])[-504:]
            result["current"] = {**_summary(slots, {**facts, current["trade_date"]: current}, current, excluded), **_view_metadata("current_intraday", current)}
        except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
            result["current"] = {"status": "UNAVAILABLE", "reason": str(exc), **_view_metadata("current_intraday", None)}
        return result
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        return {"schema_version": SCHEMA, "status": "UNAVAILABLE", "scope": PROFILE.scope, "reason": str(exc), "latest_capture_failure": failure, **FLAGS}


def attach_consumer(snapshot, consumers, *, now):
    consumers["futoi_context"]["futoi_live_cr"]["observed_statistics"] = describe(snapshot, now=now)


def _source_row(root, day, eod, eod_proof, eod_error, cutoff):
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    row = {"trade_date": day, "status": "UNAVAILABLE", "values": None, "clocks": None, "proof_id": None, "reason": None}
    proof = None; source_rejection = None
    try:
        loaded = engine._raw_factual(root, instrument_id=PROFILE.instrument_id, trade_date=day)
        if loaded.get("reason") == "canonical_raw_partition_missing":
            if eod_error is not None: raise eod_error
            selected = eod.loc[eod["trade_date"].astype(str).eq(day)] if eod is not None else []
            if len(selected) != 1: raise ValueError("exact_cr_raw_and_eod_date_missing_or_duplicate")
            fact = engine._eod_factual(selected.iloc[0], instrument_id=PROFILE.instrument_id)
            proof = {"source_kind": "accepted_eod", "provenance": {"source_kind": "accepted_stage5_eod_historical_context_only", "accepted_pointer": eod_proof}}
        else:
            if loaded.get("status") != "AVAILABLE":
                if loaded.get("error_class") in ("OSError", "IOError", "PermissionError", "FileNotFoundError"): raise OSError(loaded.get("error") or "raw source read failed")
                p = loaded.get("provenance")
                source_rejection = loaded.get("reason") or "invalid_latest_cr_raw_no_fallback"
                if not p: raise ValueError("cr_statistics_raw_rejection_proof_missing")
                common._check_source_refs(root, p, ("raw_partition",))
                frozen = source._freeze_artifact(root, root/p["raw_partition_ref"][len("${MOEX_DATA_ROOT}/"):], p["raw_partition_sha256"])
                p = {"raw_partition_ref": source._rooted_ref(root, frozen), "raw_partition_sha256": p["raw_partition_sha256"]}
                frame = common._verified_frame(root, p, "raw_partition")
                identity = source.source_identity(PROFILE.instrument_id)
                try:
                    fact = source.latest_aligned_factual(frame, expected_trade_date=day,
                        expected_instrument_id=PROFILE.instrument_id, expected_source_ticker=identity["source_ticker"], expected_secid=identity["secid"])
                    fact = engine._normalized_factual(fact, field="frozen_raw."+day)
                except (ValueError, TypeError, KeyError) as exc:
                    source_rejection = "canonical_raw_partition_failed_factual_validation"
                    proof = {"source_kind": "excluded_raw", "provenance": p}
                    raise ValueError(str(exc)) from exc
                p.update(source_state_kind="validated_existing_canonical_raw_partition", source_id=dated.SOURCE,
                    source_ticker=identity["source_ticker"], secid=identity["secid"], factual_validation="PASS")
                source_rejection = None
                proof = {"source_kind": "canonical_raw", "provenance": p}
            else:
                fact, p = common._freeze_raw_fact(root, loaded["provenance"], day, normalized=True, instrument_id=PROFILE.instrument_id)
                if fact != loaded["factual"]: raise ValueError("cr_statistics_frozen_raw_fact_mismatch")
                proof = {"source_kind": "canonical_raw", "provenance": p}
        dated._valid_record(dated._record(fact, proof["source_kind"], proof["provenance"], day), day, cutoff)
        _proof(proof); row = core._encode_row(fact, common._digest(proof))
    except Exception as exc:
        row["reason"] = (source_rejection+": " if source_rejection else dated.TRANSIENT_READ if dated._transient_read_failure(exc) else "") + type(exc).__name__+": "+str(exc)
        if proof is not None and proof["source_kind"] != "excluded_raw": proof = None
    if proof is not None: row["proof_id"] = common._digest(proof)
    return row, proof


def _capture(snapshot, cutoff):
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    _grant(snapshot.get(GRANT_KEY)); linked = dated.describe(snapshot, now=cutoff)
    if linked["status"] != "AVAILABLE": raise ValueError("cr_statistics_linked_dated_unavailable: "+linked["reason"])
    body = snapshot["components"]["futoi_live_cr"]["data"]
    if body.get("instrument_id") != PROFILE.instrument_id or body.get("source_id") != dated.SOURCE: raise ValueError("cr_statistics_source_identity")
    context = {**body["context_refresh"], engine.session_context.PREVIOUS_ROLE: body.get("previous_completed_session"), engine.session_context.CURRENT_ROLE: body.get("current_intraday")}
    root = source._data_root(); witness = engine._observed_witness(root, as_of=cutoff, raw_context=context)
    common._check_source_refs(root, witness["provenance"], ("partition", "manifest", "quality_report"))
    frame = common._verified_frame(root, witness["provenance"], "partition")
    anchor = linked["dated"]["anchor"]["trade_date"]
    slots = sorted({common._day(str(day)) for day in frame["trade_date"] if str(day) <= anchor})[-504:]
    if not slots or slots[-1] != anchor or slots != [day for day in witness["observed_trade_dates"] if day <= anchor][-504:]: raise ValueError("cr_statistics_verified_date_witness_mismatch")
    eod = eod_proof = eod_error = None
    try:
        _, eod_proof = engine._accepted_eod(root, instrument_id=PROFILE.instrument_id, as_of=cutoff)
        common._check_source_refs(root, eod_proof, ("partition", "manifest", "quality_report")); eod = common._verified_frame(root, eod_proof, "partition")
    except Exception as exc: eod_error = exc
    rows, proofs = [], {}
    for day in slots:
        row, proof = _source_row(root, day, eod, eod_proof, eod_error, cutoff); rows.append(row)
        if proof is not None: proofs[row["proof_id"]] = proof
    return {"schema_version": SCHEMA, "instrument_id": PROFILE.instrument_id, "source_id": dated.SOURCE, "policy": POLICY,
        "admission_at_acceptance": deepcopy(snapshot[GRANT_KEY]), "linked_dated_evidence_sha256": linked["evidence_sha256"],
        "accepted_at_utc": cutoff.isoformat(), "causal_cutoff_at_utc": cutoff.isoformat(), "columns": list(core.COLUMNS), "slots": slots, "rows": rows, "proofs": proofs,
        "observed_date_witness": {"observed_trade_dates": slots, "previous_observed_trade_date": witness["previous_observed_trade_date"],
            "current_observed_trade_date": witness.get("current_observed_trade_date"), "provenance": deepcopy(witness["provenance"])}}


def _semantic(e):
    return {"linked": e["linked_dated_evidence_sha256"], "slots": e["slots"], "grant": e["admission_at_acceptance"]["artifact_sha256"],
        "current_date": e["observed_date_witness"]["current_observed_trade_date"],
        "rows": [{"trade_date": r["trade_date"], "status": r["status"], "values": r["values"],
            "excluded_source_sha256": e["proofs"][r["proof_id"]]["provenance"].get("raw_partition_sha256") if r["status"] == "UNAVAILABLE" and r["proof_id"] is not None else None,
            "source_clocks": {key: r["clocks"][key] for key in ("snapshot_ts", "source_publication_time")} if r["clocks"] else None} for r in e["rows"]]}


def capture_snapshot(snapshot, previous, *, now_fn, refresh_started_at, previous_capture_completed=None):
    if dated.STORE_KEY not in snapshot and not (snapshot.get("components", {}).get("futoi_live_cr") or {}).get("data"): return
    cutoff = common._stamp(now_fn()); old = deepcopy((previous or {}).get(STORE_KEY))
    floors = [common._stamp(refresh_started_at)]
    if previous_capture_completed is not None: floors.append(common._stamp(previous_capture_completed))
    if dated.STORE_KEY in snapshot: floors.append(common._stamp(snapshot[dated.STORE_KEY]["last_capture_attempt_at_utc"]))
    if old is not None: floors.append(common._stamp(old["last_capture_attempt_at_utc"]))
    prior_failure = (previous or {}).get(DIAGNOSTICS_KEY)
    if prior_failure is not None:
        _failure(previous, cutoff); floors.append(common._stamp(prior_failure["checked_at_utc"]))
    if cutoff < max(floors): raise ValueError("cr_statistics_capture_clock_reversed")
    working = deepcopy(snapshot); working.pop(GRANT_KEY, None); candidate = None; error = None
    try:
        raw = (Path(__file__).resolve().parents[2]/CONTRACT).read_bytes()
        working[GRANT_KEY] = {"contract_ref": CONTRACT, "artifact_text": raw.decode(), "artifact_sha256": sha256(raw).hexdigest()}
        candidate = _capture(working, cutoff)
    except Exception as exc: error = type(exc).__name__+": "+str(exc)
    completed = common._stamp(now_fn())
    if completed < cutoff: raise ValueError("cr_statistics_completion_clock_reversed")
    snapshot[GRANT_KEY] = working.get(GRANT_KEY)
    if candidate is not None:
        candidate["accepted_at_utc"] = completed.isoformat()
        sample_failures = [{"trade_date": r["trade_date"], "reason": r["reason"]} for r in candidate["rows"] if r["status"] == "UNAVAILABLE"]
        try:
            if old is not None:
                try:
                    previous_e, _, _, _ = _admit({**snapshot, STORE_KEY: old}, completed)
                    if previous_e["slots"] == candidate["slots"] and previous_e["linked_dated_evidence_sha256"] == candidate["linked_dated_evidence_sha256"]:
                        for index, row in enumerate(candidate["rows"]):
                            if row["status"] == "UNAVAILABLE" and row["reason"].startswith(dated.TRANSIENT_READ):
                                prior = previous_e["rows"][index]
                                if prior["status"] == "AVAILABLE":
                                    candidate["rows"][index] = deepcopy(prior)
                                    candidate["proofs"][prior["proof_id"]] = deepcopy(previous_e["proofs"][prior["proof_id"]])
                        used = {r["proof_id"] for r in candidate["rows"] if r["proof_id"] is not None}
                        candidate["proofs"] = {k: v for k, v in candidate["proofs"].items() if k in used}
                except (KeyError, TypeError, ValueError, OverflowError, AttributeError): pass
            store = {"schema_version": SCHEMA, "evidence": candidate, "evidence_sha256": common._digest(candidate), "last_capture_attempt_at_utc": completed.isoformat(), "last_capture_error": None, "latest_sample_failures": sample_failures, "first_anchor_rejection": None}
            _admit({**snapshot, STORE_KEY: store}, completed)
            if old is not None:
                try:
                    prior, _, _, _ = _admit({**snapshot, STORE_KEY: old}, completed)
                    if _semantic(prior) == _semantic(candidate): store["evidence"], store["evidence_sha256"] = prior, old["evidence_sha256"]
                except (KeyError, TypeError, ValueError, OverflowError, AttributeError): pass
            snapshot[STORE_KEY] = store; snapshot[DIAGNOSTICS_KEY] = None
            return completed
        except Exception as exc: error = type(exc).__name__+": "+str(exc)
    rejected = candidate["rows"][-1] if candidate is not None and candidate["rows"][-1]["status"] == "UNAVAILABLE" else None
    if rejected is not None: error = "cr_statistics_latest_anchor_rejected: "+rejected["trade_date"]+": "+rejected["reason"]
    if old is not None:
        if rejected is not None and not rejected["reason"].startswith(dated.TRANSIENT_READ) and old["first_anchor_rejection"] is None:
            old["first_anchor_rejection"] = {"checked_at_utc": completed.isoformat(), "trade_date": rejected["trade_date"], "reason": rejected["reason"]}
        old.update(last_capture_attempt_at_utc=completed.isoformat(), last_capture_error=error, latest_sample_failures=[]); snapshot[STORE_KEY] = old
    snapshot[DIAGNOSTICS_KEY] = {"checked_at_utc": completed.isoformat(), "error": error}
    return completed


def verify_projection(snapshot, release, *, now):
    out = release["futoi_context"]["futoi_live_cr"].get("observed_statistics"); failure = None
    try:
        now = common._stamp(now); failure = _failure(snapshot, now)
        if snapshot.get(STORE_KEY) is None and failure: raise ValueError(failure["error"])
        e, facts, linked, error = _admit(snapshot, now)
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        expected = {"schema_version": SCHEMA, "status": "UNAVAILABLE", "scope": PROFILE.scope, "reason": str(exc), "latest_capture_failure": failure, **FLAGS}
        common._require(common._digest(out) == common._digest(expected), "CR statistics canonical refusal")
        return
    metadata = {"schema_version": SCHEMA, "policy": POLICY, "status": "AVAILABLE", "scope": PROFILE.scope, "instrument_id": PROFILE.instrument_id,
        "source_id": dated.SOURCE, "minimum_sample_count": 2, "units": UNITS, "checked_at_utc": now.isoformat(),
        "accepted_at_utc": e["accepted_at_utc"], "causal_cutoff_at_utc": e["causal_cutoff_at_utc"], "linked_dated_evidence_sha256": linked["evidence_sha256"],
        "statistics_evidence_sha256": snapshot[STORE_KEY]["evidence_sha256"], "last_capture_error": error, "latest_capture_failure": failure,
        "latest_sample_failures": snapshot[STORE_KEY]["latest_sample_failures"] if common._stamp(snapshot[STORE_KEY]["last_capture_attempt_at_utc"]) <= now else [],
        "audit_reference": "input_snapshot.json#/"+STORE_KEY+"/evidence", **FLAGS}
    common._require(isinstance(out, dict) and set(out) == set(metadata)|{"dated", "current"} and common._digest({k: out[k] for k in metadata}) == common._digest(metadata), "CR statistics independent metadata")
    # Reconcile eligible source rows independently of the producer admission map.
    facts = {}
    for row in e["rows"]:
        if row["status"] == "AVAILABLE":
            values = dict(zip(core.COLUMNS, row["values"]))
            facts[row["trade_date"]] = {"trade_date": row["trade_date"], **row["clocks"], "total_open_interest": values["total_open_interest"],
                **{side: {field: values[side+"."+field] for field in ("long", "short", "net", "long_participants", "short_participants")} for side in ("fiz", "yur")}}
    views = {"dated": (e["slots"], facts)}
    try:
        original, _, _ = dated._read_admit(snapshot, now)
        current = dated._current(snapshot, original, now)["factual"]
        if current["trade_date"] != e["observed_date_witness"]["current_observed_trade_date"] or original["witness"]["previous_observed_trade_date"] != e["slots"][-1]:
            raise ValueError("cr_statistics_current_witness_mismatch")
        views["current"] = ((e["slots"]+[current["trade_date"]])[-504:], {**facts, current["trade_date"]: current})
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        expected = {"status": "UNAVAILABLE", "reason": str(exc), "anchor_role": "current_intraday", "current_pair_usable_at_read": False,
            "source_anchor_clocks": None, "statistical_semantics": SEMANTICS}
        common._require(common._digest(out["current"]) == common._digest(expected), "CR statistics current refusal")
    for name, (slots, source) in views.items():
        metadata = {"anchor_role": "current_intraday" if name == "current" else "accepted_previous_observed", "current_pair_usable_at_read": name == "current",
            "source_anchor_clocks": {k: source[slots[-1]].get(k) for k in core.CLOCKS}, "statistical_semantics": SEMANTICS}
        core.verify_view(out[name], slots, source, e["rows"], metadata, label="CR")
