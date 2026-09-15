"""CR-specific observed-pair admission, isolated from legacy current-pair authority."""
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json
from math import isclose
from pathlib import Path

from moex_data import rub_si_futoi_dated_context as common
from moex_data.rub_si_futoi_observed_statistics import _values as derived_values, FIELDS

INSTRUMENT = "cr_futures_family"
SOURCE = "moex_algopack_futoi"
SCHEMA = "cr_futoi_dated_comparisons.v1"
CONTRACT = "contracts/intelligence/cr_futoi_dated_comparisons_v1.json"
STORE_KEY = "accepted_dated_futoi_cr"
ADMISSION_KEY = "cr_dated_scope_admission"
CURRENT_KEY = "cr_scoped_current_pair_evidence"
SCOPE = "CR_OBSERVED_PAIR_COMPARISONS_ONLY"
FLAGS = {k: v for k, v in common.FLAGS.items() if k != "current_usable"}
CLOCKS = ("snapshot_ts", "source_publication_time", "availability_ts_utc", "ingest_ts_utc")
UNITS = {"positions_and_open_interest": "contracts", "participants": "side_counts_not_unique_people",
    "long_short_net_shares": "fraction_of_open_interest", "gross_share": "fraction_of_two_sided_open_interest",
    "share_changes": "fraction_difference_not_percentage_points"}
TRANSIENT_READ = "transient_source_read_failure: "


def _expected_contract():
    return {"schema_version": "cr_futoi_dated_admission.v1", "task_id": "cr_futoi_dated_comparisons_v1",
        "instrument_id": INSTRUMENT, "source_id": SOURCE, "scope": SCOPE,
        "admission": {
            **{key: True for key in ("dated_factual_use_allowed", "current_comparisons_require_existing_current_pair_admission",
                "legacy_current_pair_governance_unchanged", "earlier_invalid_pairs_do_not_prove_latest_pair_invalid",
                "source_byte_binding_required", "raw_publication_and_ingest_clocks_required",
                "first_acceptance_after_source_validation", "exact_missing_dates_never_substituted")},
            **{key: False for key in ("whole_partition_or_session_acceptance", "session_completion_proven", "historical_pit_usable",
                "model_usable", "directional_authority", "action_authority", "stage5_pointer_promotion_performed", "statistics_authority")},
            "maximum_dated_age_seconds": 345600, "maximum_retained_observed_dates": 21, "lags": [1, 5, 20],
            "source_selection": "latest aligned FIZ/YUR pair on exact witnessed date; invalid latest pair refuses without older pair or EOD fallback"},
        "deployment_gate": "scoped PM_L1 authorization; code review, tests, merge and separate runtime acceptance required",
        "limitations": "Current-pair-only legacy fields remain unchanged. A4 statistics and whole-history backfill are outside this contract. A valid latest pair does not admit earlier invalid intraday pairs."}


def _admission(artifact):
    if not isinstance(artifact, dict) or set(artifact) != {"contract_ref", "artifact_text", "artifact_sha256"} or artifact["contract_ref"] != CONTRACT:
        raise ValueError("cr_scoped_admission_artifact_required")
    if not isinstance(artifact["artifact_text"], str) or sha256(artifact["artifact_text"].encode()).hexdigest() != artifact["artifact_sha256"]:
        raise ValueError("cr_admission_artifact_digest")
    doc = json.loads(artifact["artifact_text"])
    if common._digest(doc) != common._digest(_expected_contract()):
        raise ValueError("cr_scoped_contract_policy_or_inventory_mismatch")
    if doc.get("schema_version") != "cr_futoi_dated_admission.v1" or doc.get("instrument_id") != INSTRUMENT or doc.get("source_id") != SOURCE or doc.get("scope") != SCOPE:
        raise ValueError("cr_dated_scope_not_admitted")
    grant = doc["admission"]
    required = ("dated_factual_use_allowed", "current_comparisons_require_existing_current_pair_admission",
        "legacy_current_pair_governance_unchanged", "source_byte_binding_required", "raw_publication_and_ingest_clocks_required",
        "first_acceptance_after_source_validation", "exact_missing_dates_never_substituted")
    if any(grant.get(k) is not True for k in required) or grant.get("maximum_dated_age_seconds") != common.MAX_AGE or grant.get("maximum_retained_observed_dates") != 21 or grant.get("lags") != [1, 5, 20]:
        raise ValueError("cr_dated_admission_bounds")
    if any(grant.get(k) is not False for k in ("whole_partition_or_session_acceptance", "session_completion_proven", "historical_pit_usable", "model_usable", "directional_authority", "action_authority", "stage5_pointer_promotion_performed", "statistics_authority")):
        raise ValueError("cr_dated_admission_authority_expanded")


def _record(fact, kind, proof, day):
    return {"instrument_id": INSTRUMENT, "source_id": SOURCE, "trade_date": day, "status": "AVAILABLE",
        "source_kind": kind, "factual": fact, "provenance": proof, "reason": None}


def _valid_record(record, day, at):
    if not isinstance(record, dict) or set(record) != {"instrument_id", "source_id", "trade_date", "status", "source_kind", "factual", "provenance", "reason"}:
        raise ValueError("cr_record_shape")
    if record["instrument_id"] != INSTRUMENT or record["source_id"] != SOURCE or record["trade_date"] != day:
        raise ValueError("cr_record_identity")
    if record["status"] == "UNAVAILABLE":
        if not isinstance(record["reason"], str) or not record["reason"] or any(record[k] is not None for k in ("source_kind", "factual", "provenance")):
            raise ValueError("cr_explicit_refusal_required")
        return
    if record["status"] != "AVAILABLE" or record["reason"] is not None or record["source_kind"] not in ("canonical_raw", "accepted_eod"):
        raise ValueError("cr_record_status_or_scope")
    common._fact(record, day, at, instrument_id=INSTRUMENT)
    if type(record["factual"]["total_open_interest"]) is not int or any(type(record["factual"][side][field]) is not int for side in ("fiz", "yur") for field in common.SIDE_FIELDS):
        raise ValueError("cr_exact_integer_factual_counts_required")
    common._proof(record)
    if record["source_kind"] == "canonical_raw":
        for key in CLOCKS: common._stamp(record["factual"][key])
    elif any(record["factual"].get(key) is not None for key in ("source_publication_time", "ingest_ts_utc")):
        raise ValueError("cr_eod_original_clock_semantics")


def _admit(store, now, *, current_admission):
    now = common._stamp(now)
    _admission(current_admission)
    if not isinstance(store, dict) or set(store) != {"schema_version", "evidence", "evidence_sha256", "last_capture_attempt_at_utc", "last_capture_error", "latest_diagnostics", "latest_source_rejection"}:
        raise ValueError("cr_dated_store_not_captured_or_malformed")
    e = store["evidence"]
    if store["schema_version"] != SCHEMA or common._digest(e) != store["evidence_sha256"]:
        raise ValueError("cr_dated_evidence_digest")
    if set(e) != {"schema_version", "instrument_id", "source_id", "admission", "accepted_at_utc", "causal_cutoff_at_utc", "witness", "records"} or e["schema_version"] != SCHEMA or e["instrument_id"] != INSTRUMENT or e["source_id"] != SOURCE:
        raise ValueError("cr_dated_evidence_identity_or_shape")
    _admission(e["admission"])
    accepted, cutoff = common._stamp(e["accepted_at_utc"]), common._stamp(e["causal_cutoff_at_utc"])
    if not cutoff <= accepted <= now or (now-accepted).total_seconds() > common.MAX_AGE:
        raise ValueError("cr_dated_acceptance_future_or_expired")
    attempted = common._stamp(store["last_capture_attempt_at_utc"])
    if attempted < accepted: raise ValueError("cr_attempt_precedes_acceptance")
    rejection = store["latest_source_rejection"]
    if rejection is not None:
        if not isinstance(rejection, dict) or set(rejection) != {"checked_at_utc", "trade_date", "reason"}:
            raise ValueError("cr_latest_source_rejection_shape")
        checked = common._stamp(rejection["checked_at_utc"])
        common._day(rejection["trade_date"])
        if not accepted <= checked <= attempted or not isinstance(rejection["reason"], str) or not rejection["reason"] or rejection["reason"].startswith(TRANSIENT_READ):
            raise ValueError("cr_latest_source_rejection_metadata")
        if checked <= now:
            raise ValueError("cr_latest_anchor_source_rejected: " + rejection["trade_date"] + ": " + rejection["reason"])
    error = common._capture_error(store["last_capture_error"])
    if attempted > now: error = None
    witness = e["witness"]
    if set(witness) != {"dates", "current_observed_trade_date", "previous_observed_trade_date", "provenance"}:
        raise ValueError("cr_observed_witness_shape")
    dates = witness["dates"]
    if not isinstance(dates, list) or not 1 <= len(dates) <= 21 or dates != sorted(set(dates)):
        raise ValueError("cr_observed_date_order_or_bound")
    for day in dates: common._day(day)
    if witness["previous_observed_trade_date"] != dates[-1]: raise ValueError("cr_previous_anchor_witness")
    if witness["current_observed_trade_date"] is not None and common._day(witness["current_observed_trade_date"]) <= dates[-1]:
        raise ValueError("cr_current_witness_order")
    p = witness["provenance"]
    if p.get("acceptance_contract_id") != "step7_rub_native_d1_w1_technical_acceptance.v1": raise ValueError("cr_observed_date_authority")
    for key in ("partition", "manifest", "quality_report"):
        common._ref(p[key+"_ref"]); common._hash(p[key+"_sha256"])
    if not isinstance(e["records"], list) or len(e["records"]) != len(dates): raise ValueError("cr_date_record_cardinality")
    for day, record in zip(dates, e["records"]): _valid_record(record, day, cutoff)
    anchor = e["records"][-1]
    if anchor["status"] != "AVAILABLE": raise ValueError("cr_anchor_latest_pair_not_admitted")
    if not 0 <= (now-common._stamp(anchor["factual"]["snapshot_ts"])).total_seconds() <= common.MAX_AGE:
        raise ValueError("cr_dated_anchor_future_or_expired")
    diagnostics = store["latest_diagnostics"]
    if diagnostics is not None:
        try:
            if set(diagnostics) != {"checked_at_utc", "records"} or not accepted <= common._stamp(diagnostics["checked_at_utc"]) <= now or set(diagnostics["records"]) != set(dates): raise ValueError()
            for record in e["records"]:
                diag = diagnostics["records"][record["trade_date"]]
                if set(diag) != {"status", "reason"} or diag["status"] not in ("AVAILABLE", "UNAVAILABLE"): raise ValueError()
                if diag["status"] == "AVAILABLE":
                    if record["status"] != "AVAILABLE" or diag["reason"] is not None: raise ValueError()
                elif not isinstance(diag["reason"], str) or not diag["reason"]: raise ValueError()
        except (KeyError, TypeError, ValueError, AttributeError): diagnostics = None
    return e, error, deepcopy(diagnostics)


def _public_record(record):
    """Project only fields whose meaning and values the admission validates."""
    result = {key: deepcopy(record[key]) for key in ("instrument_id", "source_id", "trade_date", "status", "source_kind", "reason")}
    fact = record["factual"]
    result["factual"] = {key: fact[key] for key in ("trade_date", *CLOCKS, "total_open_interest")}
    for side in ("fiz", "yur"):
        result["factual"][side] = {field: fact[side][field] for field in common.SIDE_FIELDS}
    proof = record["provenance"]
    if record["source_kind"] == "accepted_eod":
        p = proof["accepted_pointer"]
        result["provenance"] = {"source_kind": proof["source_kind"], "accepted_pointer": {
            key: p[key] for key in ("acceptance_contract_id", *(prefix+suffix for prefix in ("partition", "manifest", "quality_report") for suffix in ("_ref", "_sha256")))}}
    else:
        result["provenance"] = {key: proof[key] for key in ("raw_partition_ref", "raw_partition_sha256", "source_id", "factual_validation")}
    return result


def _public_witness(witness):
    return {**{key: deepcopy(witness[key]) for key in ("dates", "previous_observed_trade_date", "current_observed_trade_date")},
        "provenance": {key: witness["provenance"][key] for key in ("acceptance_contract_id", *(prefix+suffix for prefix in ("partition", "manifest", "quality_report") for suffix in ("_ref", "_sha256")))}}


def _view(records, current=None):
    records = records + [current] if current is not None else records
    anchor = records[-1]
    values = derived_values(anchor["factual"])
    changes = {}
    for lag in (1, 5, 20):
        baseline = records[-1-lag] if len(records) > lag else None
        admitted = baseline is not None and baseline["status"] == "AVAILABLE"
        changes[str(lag)] = {"target_trade_date": baseline["trade_date"] if baseline else None,
            "status": "AVAILABLE" if admitted else "UNAVAILABLE", "baseline": _public_record(baseline) if admitted else None,
            "reason": None if admitted else baseline["reason"] if baseline else "insufficient_exact_observed_dates", "values": None}
        if admitted:
            base = derived_values(baseline["factual"])
            changes[str(lag)]["values"] = {key: values[key]-base[key] for key in FIELDS}
    return {"status": "AVAILABLE", "anchor_role": "current_intraday" if current else "accepted_previous_observed",
        "current_pair_usable_at_read": current is not None, "anchor": _public_record(anchor), "anchor_values": values, "changes": changes}


def _current(snapshot, e, now):
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    data = (apply_read_freshness(snapshot, now=now).get("components", {}).get("futoi_live_cr") or {})
    body = data.get("data") or {}
    if data.get("status") != "READY" or body.get("consumer_factual_use_allowed") is not True or body.get("factual_authority") is not True or body.get("instrument_id") != INSTRUMENT or body.get("source_id") != SOURCE:
        raise ValueError("existing_cr_current_pair_not_admitted")
    record = body["current_intraday"]
    fact = record["factual"]
    if fact["trade_date"] != e["witness"]["current_observed_trade_date"]: raise ValueError("cr_current_observed_witness_mismatch")
    stored = snapshot[CURRENT_KEY]
    if set(stored) != {"evidence", "evidence_sha256"} or common._digest(stored["evidence"]) != stored["evidence_sha256"]:
        raise ValueError("cr_current_byte_witness_digest")
    proof = stored["evidence"]
    if set(proof) != {"record", "captured_at_utc", "causal_cutoff_at_utc", "publication_audit", "original_provenance"} or not common._stamp(proof["causal_cutoff_at_utc"]) <= common._stamp(proof["captured_at_utc"]) <= now:
        raise ValueError("cr_current_byte_witness_shape_or_clock")
    pair = body.get("current_pair_admission") or {}
    if pair.get("allowed") is not True or pair.get("scope") != "current_intraday_latest_pair_only": raise ValueError("cr_existing_current_scope_not_admitted")
    audit = proof["publication_audit"]
    if set(audit) != {"text", "sha256"} or sha256(audit["text"].encode()).hexdigest() != audit["sha256"] or audit["sha256"] != pair.get("audit_sha256"):
        raise ValueError("cr_current_publication_audit_digest")
    report = json.loads(audit["text"])
    from moex_data.futures import futoi_publication_audit as audit_source
    if report.get("schema_version") != audit_source.SCHEMA or report.get("policy") != audit_source.POLICY or report.get("instrument_id") != INSTRUMENT or report.get("latest_status") != "PASS" or report.get("latest_factual") != fact:
        raise ValueError("cr_current_publication_audit_fact_mismatch")
    if proof["record"]["factual"] != fact or proof["original_provenance"] != record.get("provenance"):
        raise ValueError("cr_current_frozen_source_fact_mismatch")
    receipt = proof["original_provenance"]["publication_audit"]
    common._ref(receipt["ref"])
    if receipt["sha256"] != audit["sha256"]:
        raise ValueError("cr_original_audit_receipt_mismatch")
    for key in ("raw_partition", "raw_quality_report", "raw_refresh_manifest"):
        for suffix in ("_ref", "_sha256"):
            if report["provenance"][key+suffix] != proof["original_provenance"][key+suffix]: raise ValueError("cr_current_audit_provenance_mismatch")
        common._ref(proof["original_provenance"][key+"_ref"]); common._hash(proof["original_provenance"][key+"_sha256"])
    result = proof["record"]
    _valid_record(result, fact["trade_date"], common._stamp(proof["causal_cutoff_at_utc"]))
    if result["provenance"]["raw_partition_sha256"] != proof["original_provenance"]["raw_partition_sha256"]:
        raise ValueError("cr_current_frozen_raw_digest_mismatch")
    common._fact(result, fact["trade_date"], now, instrument_id=INSTRUMENT)
    for key in CLOCKS: common._stamp(fact[key])
    return result


def _current_refusal(reason):
    return {"status": "UNAVAILABLE", "reason": reason, "anchor_role": "current_intraday", "current_pair_usable_at_read": False}


def _metadata(store, e, error, diagnostics, now):
    return {"schema_version": SCHEMA, "status": "AVAILABLE", "scope": SCOPE, "instrument_id": INSTRUMENT, "source_id": SOURCE,
        "accepted_at_utc": e["accepted_at_utc"], "causal_cutoff_at_utc": e["causal_cutoff_at_utc"], "checked_at_utc": now.isoformat(),
        "evidence_sha256": store["evidence_sha256"], "maximum_dated_age_seconds": common.MAX_AGE,
        "units": deepcopy(UNITS), "witness": _public_witness(e["witness"]), "latest_diagnostics": diagnostics, "last_capture_error": error,
        "admission_artifact_sha256": e["admission"]["artifact_sha256"], "legacy_cr_current_pair_fields_unchanged": True, **FLAGS}


def describe(snapshot, *, now):
    try:
        now = common._stamp(now)
        store = snapshot.get(STORE_KEY)
        e, error, diagnostics = _admit(store, now, current_admission=snapshot.get(ADMISSION_KEY))
        result = _metadata(store, e, error, diagnostics, now)
        result["dated"] = _view(e["records"])
        try: result["current"] = _view(e["records"], _current(snapshot, e, now))
        except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc: result["current"] = _current_refusal(str(exc))
        return result
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        return {"schema_version": SCHEMA, "status": "UNAVAILABLE", "scope": SCOPE, "reason": str(exc), **FLAGS}


def _load_record(root, day, eod, eod_proof, cutoff):
    from moex_data.futures import futoi_delta_statistics_context as engine
    try:
        loaded = engine._raw_factual(root, instrument_id=INSTRUMENT, trade_date=day)
        if loaded.get("reason") == "canonical_raw_partition_missing":
            rows = eod.loc[eod["trade_date"].astype(str).eq(day)] if eod is not None else []
            if len(rows) != 1: raise ValueError("exact_cr_raw_and_eod_date_missing_or_duplicate")
            fact = engine._eod_factual(rows.iloc[0], instrument_id=INSTRUMENT)
            result = _record(fact, "accepted_eod", {"source_kind": "accepted_stage5_eod_historical_context_only", "accepted_pointer": eod_proof}, day)
        else:
            if loaded.get("status") != "AVAILABLE":
                if loaded.get("error_class") in ("OSError", "IOError", "PermissionError", "FileNotFoundError"):
                    raise OSError(loaded.get("error") or "raw_source_read_failed")
                raise ValueError(loaded.get("reason") or "cr_latest_raw_pair_invalid_no_fallback")
            fact, proof = common._freeze_raw_fact(root, loaded["provenance"], day, normalized=True, instrument_id=INSTRUMENT)
            if fact != loaded["factual"]: raise ValueError("cr_frozen_raw_fact_mismatch")
            result = _record(fact, "canonical_raw", proof, day)
        _valid_record(result, day, cutoff)
        return result
    except Exception as exc:
        return {"instrument_id": INSTRUMENT, "source_id": SOURCE, "trade_date": day, "status": "UNAVAILABLE", "source_kind": None,
                "factual": None, "provenance": None, "reason": (TRANSIENT_READ if isinstance(exc, OSError) else "") + type(exc).__name__ + ": " + str(exc)}


def _capture(snapshot, cutoff):
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    from moex_data import rub_temporal_applicability as temporal
    artifact = snapshot[ADMISSION_KEY]
    _admission(artifact)
    body = snapshot["components"]["futoi_live_cr"]["data"]
    if body.get("instrument_id") != INSTRUMENT or body.get("source_id") != SOURCE: raise ValueError("cr_capture_source_identity")
    expected = temporal._previous_witness(body["context_refresh"], cutoff)
    if expected is None: raise ValueError("cr_independent_previous_date_witness_unavailable")
    context = {**body["context_refresh"], engine.session_context.PREVIOUS_ROLE: body.get("previous_completed_session"),
               engine.session_context.CURRENT_ROLE: body.get("current_intraday")}
    root = source._data_root()
    witness = engine._observed_witness(root, as_of=cutoff, raw_context=context)
    common._check_source_refs(root, witness["provenance"], ("partition", "manifest", "quality_report"))
    frame = common._verified_frame(root, witness["provenance"], "partition")
    dates = sorted({common._day(str(day)) for day in frame["trade_date"] if str(day) <= expected})[-21:]
    if not dates or dates[-1] != expected or dates != [day for day in witness["observed_trade_dates"] if day <= expected][-21:] or witness["previous_observed_trade_date"] != expected:
        raise ValueError("cr_verified_observed_dates_mismatch")
    try:
        _, eod_proof = engine._accepted_eod(root, instrument_id=INSTRUMENT, as_of=cutoff)
        common._check_source_refs(root, eod_proof, ("partition", "manifest", "quality_report"))
        eod = common._verified_frame(root, eod_proof, "partition")
    except Exception: eod, eod_proof = None, None
    return {"schema_version": SCHEMA, "instrument_id": INSTRUMENT, "source_id": SOURCE, "admission": artifact,
        "accepted_at_utc": cutoff.isoformat(), "causal_cutoff_at_utc": cutoff.isoformat(),
        "witness": {"dates": dates, "previous_observed_trade_date": expected, "current_observed_trade_date": witness.get("current_observed_trade_date"), "provenance": deepcopy(witness["provenance"])},
        "records": [_load_record(root, day, eod, eod_proof, cutoff) for day in dates]}


def _capture_current(snapshot, cutoff):
    from moex_data.futures import futoi_current_pair_authority as authority
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    root = source._data_root(); repo = Path(__file__).resolve().parents[2]
    body = snapshot["components"]["futoi_live_cr"]["data"]
    record = body["current_intraday"]
    governance = json.loads((repo / "contracts/intelligence/usdrubf_futoi_live_acceptance_governance_v1.json").read_text())
    allowed = authority.admit(governance, record, root=root, repo_root=repo, now=cutoff)
    if allowed.get("allowed") is not True: raise ValueError(allowed.get("error") or "cr_current_source_audit_refused")
    original = record["provenance"]
    fact, proof = common._freeze_raw_fact(root, original, record["factual"]["trade_date"], normalized=False, instrument_id=INSTRUMENT)
    if fact != record["factual"]: raise ValueError("cr_current_frozen_byte_fact_mismatch")
    proof.update(source_id=SOURCE, factual_validation="PASS")
    ref = original["publication_audit"]
    common._ref(ref["ref"]); common._hash(ref["sha256"])
    raw = (root / ref["ref"][len("${MOEX_DATA_ROOT}/"):]).read_bytes()
    if sha256(raw).hexdigest() != ref["sha256"]: raise ValueError("cr_current_audit_byte_mismatch")
    evidence = {"record": _record(fact, "canonical_raw", proof, fact["trade_date"]), "captured_at_utc": cutoff.isoformat(), "causal_cutoff_at_utc": cutoff.isoformat(),
        "publication_audit": {"text": raw.decode(), "sha256": ref["sha256"]}, "original_provenance": deepcopy(original)}
    return {"evidence": evidence, "evidence_sha256": common._digest(evidence)}


def _semantic(e):
    records = deepcopy(e["records"])
    for record in records:
        record.pop("reason"); record.pop("provenance")
        if record["factual"]:
            record["factual"].pop("availability_ts_utc", None); record["factual"].pop("ingest_ts_utc", None)
    return {"dates": e["witness"]["dates"], "current_date": e["witness"]["current_observed_trade_date"], "records": records,
            "admission_sha256": e["admission"]["artifact_sha256"]}


def capture_snapshot(snapshot, previous, *, now_fn, refresh_started_at, previous_capture_completed=None):
    body = (snapshot.get("components", {}).get("futoi_live_cr") or {}).get("data")
    if not isinstance(body, dict) or (not body and not (previous or {}).get(STORE_KEY)): return
    cutoff = common._stamp(now_fn())
    old = deepcopy((previous or {}).get(STORE_KEY))
    floor = [common._stamp(refresh_started_at)]
    if previous_capture_completed is not None: floor.append(common._stamp(previous_capture_completed))
    if old is not None: floor.append(common._stamp(old["last_capture_attempt_at_utc"]))
    if cutoff < max(floor): raise ValueError("cr_capture_clock_precedes_refresh_or_prior_capture")
    working = deepcopy(snapshot)
    working.pop(ADMISSION_KEY, None)
    candidate, error = None, None
    try:
        raw = (Path(__file__).resolve().parents[2] / CONTRACT).read_bytes()
        working[ADMISSION_KEY] = {"contract_ref": CONTRACT, "artifact_text": raw.decode(), "artifact_sha256": sha256(raw).hexdigest()}
        _admission(working[ADMISSION_KEY])
        candidate = _capture(working, cutoff)
    except Exception as exc: error = type(exc).__name__ + ": " + str(exc)
    working.pop(CURRENT_KEY, None)
    try: working[CURRENT_KEY] = _capture_current(working, cutoff)
    except Exception: pass
    completed = common._stamp(now_fn())
    if completed < cutoff: raise ValueError("cr_capture_completion_clock_reversed")
    snapshot[ADMISSION_KEY] = working.get(ADMISSION_KEY)
    snapshot.pop(CURRENT_KEY, None)
    if CURRENT_KEY in working:
        current_proof = working[CURRENT_KEY]
        current_proof["evidence"]["captured_at_utc"] = completed.isoformat()
        current_proof["evidence_sha256"] = common._digest(current_proof["evidence"])
        snapshot[CURRENT_KEY] = current_proof
    if candidate is not None:
        candidate["accepted_at_utc"] = completed.isoformat()
        store = {"schema_version": SCHEMA, "evidence": candidate, "evidence_sha256": common._digest(candidate),
            "last_capture_attempt_at_utc": completed.isoformat(), "last_capture_error": None, "latest_source_rejection": None,
            "latest_diagnostics": {"checked_at_utc": completed.isoformat(), "records": {r["trade_date"]: {"status": r["status"], "reason": r["reason"]} for r in candidate["records"]}}}
        try:
            _admit(store, completed, current_admission=snapshot.get(ADMISSION_KEY))
            if old is not None:
                try:
                    prior, _, _ = _admit(old, completed, current_admission=snapshot.get(ADMISSION_KEY))
                    a, b = _semantic(prior), _semantic(candidate)
                    if a == b or (a["dates"] == b["dates"] and a["current_date"] == b["current_date"] and a["admission_sha256"] == b["admission_sha256"]
                        and a["records"][-1] == b["records"][-1] and all(left == right or (candidate["records"][index]["status"] == "UNAVAILABLE" and candidate["records"][index]["reason"].startswith(TRANSIENT_READ)) for index, (left, right) in enumerate(zip(a["records"], b["records"])))):
                        store["evidence"] = prior; store["evidence_sha256"] = old["evidence_sha256"]
                except (KeyError, TypeError, ValueError, OverflowError, AttributeError): pass
            snapshot[STORE_KEY] = store
            return completed
        except Exception as exc: error = type(exc).__name__ + ": " + str(exc)
    if old is not None:
        old.update(last_capture_attempt_at_utc=completed.isoformat(), last_capture_error=error)
        if candidate is not None and candidate["records"][-1]["status"] == "UNAVAILABLE" and not candidate["records"][-1]["reason"].startswith(TRANSIENT_READ):
            old["latest_source_rejection"] = {"checked_at_utc": completed.isoformat(), "trade_date": candidate["records"][-1]["trade_date"], "reason": candidate["records"][-1]["reason"]}
        snapshot[STORE_KEY] = old
    return completed


def attach_consumer(snapshot, consumers, *, now):
    consumers["futoi_context"]["futoi_live_cr"]["scoped_observed_comparisons"] = describe(snapshot, now=now)


def verify_projection(snapshot, release, *, now):
    output = release["futoi_context"]["futoi_live_cr"].get("scoped_observed_comparisons")
    try:
        now = common._stamp(now); store = snapshot.get(STORE_KEY); e, error, diagnostics = _admit(store, now, current_admission=snapshot.get(ADMISSION_KEY))
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        expected = {"schema_version": SCHEMA, "status": "UNAVAILABLE", "scope": SCOPE, "reason": str(exc), **FLAGS}
        common._require(isinstance(output, dict) and common._digest(output) == common._digest(expected), "CR canonical refusal")
        return
    witness = {key: e["witness"][key] for key in ("dates", "previous_observed_trade_date", "current_observed_trade_date")}
    witness["provenance"] = {key: e["witness"]["provenance"][key] for key in ("acceptance_contract_id", "partition_ref", "partition_sha256", "manifest_ref", "manifest_sha256", "quality_report_ref", "quality_report_sha256")}
    def exposed(record):
        result = {key: record[key] for key in ("instrument_id", "source_id", "trade_date", "status", "source_kind", "reason")}
        fact = record["factual"]
        result["factual"] = {key: fact[key] for key in ("trade_date", "snapshot_ts", "source_publication_time", "availability_ts_utc", "ingest_ts_utc", "total_open_interest")}
        for side in ("fiz", "yur"):
            result["factual"][side] = {key: fact[side][key] for key in ("long", "short", "net", "long_participants", "short_participants")}
        proof = record["provenance"]
        if record["source_kind"] == "accepted_eod":
            result["provenance"] = {"source_kind": "accepted_stage5_eod_historical_context_only", "accepted_pointer": {
                key: proof["accepted_pointer"][key] for key in ("acceptance_contract_id", "partition_ref", "partition_sha256", "manifest_ref", "manifest_sha256", "quality_report_ref", "quality_report_sha256")}}
        else:
            result["provenance"] = {key: proof[key] for key in ("raw_partition_ref", "raw_partition_sha256", "source_id", "factual_validation")}
        return result
    metadata = {"schema_version": SCHEMA, "status": "AVAILABLE", "scope": SCOPE, "instrument_id": INSTRUMENT, "source_id": SOURCE,
        "accepted_at_utc": e["accepted_at_utc"], "causal_cutoff_at_utc": e["causal_cutoff_at_utc"], "checked_at_utc": now.isoformat(),
        "evidence_sha256": store["evidence_sha256"], "maximum_dated_age_seconds": 345600, "units": UNITS,
        "witness": witness, "latest_diagnostics": diagnostics, "last_capture_error": error,
        "admission_artifact_sha256": e["admission"]["artifact_sha256"], "legacy_cr_current_pair_fields_unchanged": True, **FLAGS}
    common._require(isinstance(output, dict) and set(output) == set(metadata) | {"dated", "current"}
        and common._digest({k: output[k] for k in metadata}) == common._digest(metadata), "CR independent metadata")
    sources = {"dated": e["records"]}
    try: sources["current"] = e["records"] + [_current(snapshot, e, now)]
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        common._require(common._digest(output["current"]) == common._digest({"status": "UNAVAILABLE", "reason": str(exc), "anchor_role": "current_intraday", "current_pair_usable_at_read": False}), "CR current refusal")
    def numbers(fact):
        oi = Decimal(fact["total_open_interest"]); result = {"total_open_interest": oi}
        for side in ("fiz", "yur"):
            for field in common.SIDE_FIELDS: result[side+"."+field] = Decimal(fact[side][field])
            result[side+".gross"] = Decimal(fact[side]["long"])+Decimal(fact[side]["short"])
            for field in ("long", "short", "net"): result[side+"."+field+"_share_of_oi"] = Decimal(fact[side][field])/oi
            result[side+".gross_share_of_two_sided_oi"] = result[side+".gross"]/(2*oi)
        return result
    def compare(actual, expected):
        common._require(set(actual) == set(expected), "CR derived field coverage")
        for field, value in expected.items():
            common._require(type(actual[field]) in (int, float) and ("share" in field or type(actual[field]) is int)
                and isclose(actual[field], float(value), rel_tol=1e-12, abs_tol=1e-12), "CR independent arithmetic")
    for name, records in sources.items():
        view = output[name]; anchor = records[-1]; values = numbers(anchor["factual"])
        expected_metadata = {"status": "AVAILABLE", "anchor_role": "current_intraday" if name == "current" else "accepted_previous_observed",
            "current_pair_usable_at_read": name == "current", "anchor": exposed(anchor)}
        common._require(set(view) == set(expected_metadata) | {"anchor_values", "changes"}
            and common._digest({k: view[k] for k in expected_metadata}) == common._digest(expected_metadata), "CR view inventory")
        compare(view["anchor_values"], values)
        common._require(set(view["changes"]) == {"1", "5", "20"}, "CR exact lag inventory")
        for lag in (1, 5, 20):
            baseline = records[-1-lag] if len(records) > lag else None; item = view["changes"][str(lag)]
            available = baseline is not None and baseline["status"] == "AVAILABLE"
            expected = {"target_trade_date": baseline["trade_date"] if baseline else None,
                "status": "AVAILABLE" if available else "UNAVAILABLE", "baseline": exposed(baseline) if available else None,
                "reason": None if available else baseline["reason"] if baseline else "insufficient_exact_observed_dates"}
            common._require(set(item) == set(expected) | {"values"} and common._digest({k: item[k] for k in expected}) == common._digest(expected), "CR exact baseline and refusal")
            if available:
                base = numbers(baseline["factual"]); compare(item["values"], {k: values[k]-base[k] for k in values})
            else: common._require(item["values"] is None, "CR refused delta leaked")
