"""Bounded Si observed-slot descriptive statistics; no live or model promotion."""
from copy import deepcopy
from decimal import Decimal, localcontext
from math import fsum, sqrt, isclose

from moex_data import rub_si_futoi_dated_context as dated

SCHEMA = "si_futoi_observed_statistics.v1"
POLICY = "observed_slots_admitted_subset_min2_descriptive.v1"
STORE_KEY = "accepted_si_observed_statistics"
WINDOWS = (252, 504)
COLUMNS = ("total_open_interest",) + tuple(side + "." + field for side in ("fiz", "yur") for field in dated.SIDE_FIELDS)
CLOCKS = ("snapshot_ts", "availability_ts_utc", "source_publication_time", "ingest_ts_utc")
FIELDS = ("total_open_interest",) + tuple(side + "." + field for side in ("fiz", "yur") for field in (
    "long", "short", "net", "gross", "long_participants", "short_participants",
    "long_share_of_oi", "short_share_of_oi", "net_share_of_oi", "gross_share_of_two_sided_oi"))
STAT_FIELDS = tuple(field for field in FIELDS if not field.endswith("participants"))
UNITS = {"positions_and_open_interest": "contracts", "participant_fields": "side_counts_not_unique_people",
         "long_short_net_shares": "fraction_of_one_sided_open_interest",
         "gross_share_of_two_sided_oi": "gross_divided_by_2_times_open_interest",
         "share_changes": "fraction_difference_not_percentage_points", "percentile": "fraction", "zscore": "dimensionless"}


def _text(value):
    if not isinstance(value, str) or not value:
        raise ValueError("nonempty_text_required")
    return value


def _record(fact, kind, proof):
    return {"instrument_id": dated.INSTRUMENT, "source_id": dated.SOURCE,
            "source_kind": kind, "status": "AVAILABLE", "factual": fact, "provenance": proof}


def _values(fact):
    oi = fact["total_open_interest"]
    result = {"total_open_interest": oi}
    for side in ("fiz", "yur"):
        value = fact[side]
        result.update({side + "." + key: value[key] for key in dated.SIDE_FIELDS})
        result[side + ".gross"] = value["long"] + value["short"]
        for key in ("long", "short", "net"):
            result[side + "." + key + "_share_of_oi"] = value[key] / oi
        result[side + ".gross_share_of_two_sided_oi"] = (value["long"] + value["short"]) / (2 * oi)
    return result


def _encode_row(fact, proof_id):
    numbers = [fact["total_open_interest"]] + [fact[s][k] for s in ("fiz", "yur") for k in dated.SIDE_FIELDS]
    return {"trade_date": fact["trade_date"], "status": "AVAILABLE", "values": numbers,
            "clocks": {key: fact.get(key) for key in CLOCKS}, "proof_id": proof_id, "reason": None}


def _decode_row(row):
    values = dict(zip(COLUMNS, row["values"]))
    return {"trade_date": row["trade_date"], **row["clocks"], "total_open_interest": values["total_open_interest"],
            **{side: {key: values[side + "." + key] for key in dated.SIDE_FIELDS} for side in ("fiz", "yur")}}


def _proof(value):
    if not isinstance(value, dict) or set(value) != {"source_kind", "provenance"}:
        raise ValueError("statistics_proof_shape")
    if value["source_kind"] == "excluded_raw":
        p = value["provenance"]
        if set(p) != {"raw_partition_ref", "raw_partition_sha256"}:
            raise ValueError("excluded_source_proof_shape")
        dated._ref(p["raw_partition_ref"])
        dated._hash(p["raw_partition_sha256"])
    elif value["source_kind"] in ("canonical_raw", "accepted_eod"):
        dated._proof(value)
    else:
        raise ValueError("statistics_source_kind_not_supported")


def _admit(snapshot, now):
    now = dated._stamp(now)
    data = snapshot.get("components", {}).get("futoi_live", {}).get("data") or {}
    anchor = dated.describe(snapshot.get(dated.STORE_KEY), now=now, governance=data.get("governance"))
    if anchor["status"] not in ("AVAILABLE", "PARTIAL"):
        raise ValueError("linked_dated_anchor_unavailable: " + anchor["reason"])
    store = snapshot[STORE_KEY]
    if not isinstance(store, dict) or set(store) != {"schema_version", "evidence", "evidence_sha256", "last_capture_attempt_at_utc", "last_capture_error"}:
        raise ValueError("statistics_store_shape")
    e = store["evidence"]
    if store["schema_version"] != SCHEMA or dated._digest(e) != store["evidence_sha256"]:
        raise ValueError("statistics_evidence_digest")
    if set(e) != {"schema_version", "policy", "linked_dated_evidence_sha256", "accepted_at_utc", "causal_cutoff_at_utc",
                  "columns", "slots", "rows", "proofs", "observed_date_witness", "governance_at_acceptance"}:
        raise ValueError("statistics_evidence_shape")
    if e["schema_version"] != SCHEMA or e["policy"] != POLICY or e["columns"] != list(COLUMNS):
        raise ValueError("statistics_schema_policy_columns")
    if e["linked_dated_evidence_sha256"] != anchor["evidence_sha256"]:
        raise ValueError("statistics_anchor_changed")
    cutoff, accepted = dated._stamp(e["causal_cutoff_at_utc"]), dated._stamp(e["accepted_at_utc"])
    if not dated._stamp(anchor["accepted_at_utc"]) <= cutoff <= accepted <= now or (now-accepted).total_seconds() > dated.MAX_AGE:
        raise ValueError("statistics_capture_or_acceptance_clock")
    attempted = dated._stamp(store["last_capture_attempt_at_utc"])
    if attempted < accepted:
        raise ValueError("statistics_attempt_precedes_acceptance")
    error = dated._capture_error(store["last_capture_error"]) if attempted <= now else None
    dated._capture_error(store["last_capture_error"])
    dated._governance(e["governance_at_acceptance"])
    slots = e["slots"]
    if not isinstance(slots, list) or not 1 <= len(slots) <= 504 or slots != sorted(set(slots)):
        raise ValueError("statistics_slot_order_or_bound")
    for day in slots: dated._day(day)
    if slots[-1] != anchor["anchor"]["factual"]["trade_date"]:
        raise ValueError("statistics_last_slot_not_anchor")
    witness = e["observed_date_witness"]
    if set(witness) != {"provenance", "observed_trade_dates", "current_observed_trade_date", "previous_observed_trade_date"} or witness["observed_trade_dates"] != slots:
        raise ValueError("statistics_date_witness_shape")
    if witness["previous_observed_trade_date"] is not None: dated._day(witness["previous_observed_trade_date"])
    if witness["current_observed_trade_date"] is not None:
        if dated._day(witness["current_observed_trade_date"]) <= slots[-1]:
            raise ValueError("statistics_current_date_order")
    p = witness["provenance"]
    if p.get("acceptance_contract_id") != "step7_rub_native_d1_w1_technical_acceptance.v1":
        raise ValueError("statistics_observed_date_authority")
    for key in ("partition", "manifest", "quality_report"):
        dated._ref(p[key + "_ref"]); dated._hash(p[key + "_sha256"])
    if not isinstance(e["proofs"], dict) or len(e["proofs"]) > 504:
        raise ValueError("statistics_proof_bound")
    for key, proof in e["proofs"].items():
        if key != dated._digest(proof): raise ValueError("statistics_proof_digest")
        _proof(proof)
    if not isinstance(e["rows"], list) or len(e["rows"]) != len(slots):
        raise ValueError("statistics_row_slot_cardinality")
    facts, used = {}, set()
    for day, row in zip(slots, e["rows"]):
        if not isinstance(row, dict) or set(row) != {"trade_date", "status", "values", "clocks", "proof_id", "reason"} or row["trade_date"] != day:
            raise ValueError("statistics_row_identity_shape")
        if row["proof_id"] is not None:
            used.add(row["proof_id"])
            proof = e["proofs"][row["proof_id"]]
        if row["status"] == "UNAVAILABLE":
            _text(row["reason"])
            if row["values"] is not None or row["clocks"] is not None:
                raise ValueError("excluded_statistics_fact_leak")
            if row["proof_id"] is not None and proof["source_kind"] != "excluded_raw":
                raise ValueError("excluded_proof_kind")
            continue
        if row["status"] != "AVAILABLE" or row["reason"] is not None or row["proof_id"] is None:
            raise ValueError("statistics_row_status")
        if not isinstance(row["values"], list) or len(row["values"]) != len(COLUMNS) or any(type(v) is not int for v in row["values"]):
            raise ValueError("statistics_integer_columns")
        if not isinstance(row["clocks"], dict) or set(row["clocks"]) != set(CLOCKS) or proof["source_kind"] == "excluded_raw":
            raise ValueError("statistics_row_clocks_or_proof")
        if proof["source_kind"] == "canonical_raw":
            for field in CLOCKS: dated._stamp(row["clocks"][field])
        elif any(row["clocks"][field] is not None for field in ("source_publication_time", "ingest_ts_utc")):
            raise ValueError("accepted_eod_original_clock_semantics")
        fact = _decode_row(row)
        dated._fact(_record(fact, proof["source_kind"], proof["provenance"]), day, cutoff)
        facts[day] = fact
    if used != set(e["proofs"]): raise ValueError("statistics_unused_proof")
    if slots[-1] not in facts or _values(facts[slots[-1]]) != _values(anchor["anchor"]["factual"]):
        raise ValueError("statistics_anchor_facts_mismatch")
    return e, facts, anchor, error


def _summary(slots, facts, anchor, excluded):
    values = {day: _values(facts[day]) for day in slots if day in facts}
    anchor_day = anchor["trade_date"]
    if not slots or slots[-1] != anchor_day or anchor_day not in values:
        raise ValueError("statistics_summary_anchor")
    anchor_values = values[anchor_day]
    windows = {}
    for window in WINDOWS:
        selected = slots[-window:]
        days = [day for day in selected if day in values]
        missing = [{"trade_date": day, "reason": excluded[day]} for day in selected if day not in values]
        variables = {}
        for field in STAT_FIELDS:
            sample = [values[day][field] for day in days]
            result = {"status": "UNAVAILABLE", "reason": "minimum_two_admitted_observations_required",
                      "population_mean": None, "population_std_ddof_0": None, "percentile": None, "zscore": None, "zscore_reason": None}
            if len(sample) >= 2:
                mean = fsum(sample) / len(sample)
                std = sqrt(fsum((v-mean)**2 for v in sample) / len(sample)) if len(set(sample)) > 1 else 0.0
                result.update(status="AVAILABLE", reason=None, population_mean=mean, population_std_ddof_0=std,
                    percentile=sum(v <= anchor_values[field] for v in sample)/len(sample),
                    zscore=(anchor_values[field]-mean)/std if std else None,
                    zscore_reason="zero_population_variance" if not std else None)
            variables[field] = result
        windows[str(window)] = {"expected_window_slots": window, "observed_slot_count": len(selected),
            "sample_count": len(days), "coverage_status": "COMPLETE" if len(days) == window else "PARTIAL",
            "sample_dates": days, "excluded_dates": missing, "missing_observed_history_slots": max(0, window-len(selected)),
            "slot_start_date": selected[0], "slot_end_date": selected[-1], "variables": variables}
    changes = {}
    for lag in dated.LAGS:
        target = slots[-1-lag] if len(slots) > lag else None
        changes[str(lag)] = {"target_trade_date": target, "anchor_trade_date": anchor_day,
            "status": "AVAILABLE" if target in values else "UNAVAILABLE", "values": None,
            "reason": None if target in values else (excluded.get(target) or "insufficient_exact_observed_slots")}
        if target in values:
            changes[str(lag)]["values"] = {field: anchor_values[field]-values[target][field] for field in FIELDS}
    return {"status": "AVAILABLE", "anchor_trade_date": anchor_day, "anchor_values": anchor_values,
            "changes": changes, "windows": windows}


def _current(snapshot, e, facts, now):
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    view = apply_read_freshness(snapshot, now=now)
    component = view.get("components", {}).get("futoi_live") or {}
    data = component.get("data") or {}
    record = data.get("current_intraday") or {}
    if component.get("status") != "READY" or data.get("consumer_factual_use_allowed") is not True or data.get("factual_authority") is not True:
        raise ValueError("current_si_pair_not_admitted")
    if data.get("instrument_id") != dated.INSTRUMENT or data.get("source_id") != dated.SOURCE:
        raise ValueError("current_si_identity_mismatch")
    fact = record["factual"]
    witness = e["observed_date_witness"]
    if fact["trade_date"] != witness["current_observed_trade_date"] or witness["previous_observed_trade_date"] != e["slots"][-1]:
        raise ValueError("current_si_observed_date_witness_mismatch")
    dated._fact(_record(fact, "previous_observed", record.get("provenance")), fact["trade_date"], now)
    slots = (e["slots"] + [fact["trade_date"]])[-504:]
    return slots, {**facts, fact["trade_date"]: fact}, fact


def _render(snapshot, now, e, facts, linked, error):
        excluded = {row["trade_date"]: row["reason"] for row in e["rows"] if row["status"] == "UNAVAILABLE"}
        prior = _summary(e["slots"], facts, facts[e["slots"][-1]], excluded)
        try:
            slots, current_facts, current = _current(snapshot, e, facts, now)
            live = _summary(slots, current_facts, current, excluded)
        except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
            live = {"status": "UNAVAILABLE", "reason": str(exc)}
        return {"schema_version": SCHEMA, "policy": POLICY, "status": "AVAILABLE",
            "scope": "SI_FAMILY_DESCRIPTIVE_OBSERVED_SUBSETS_NOT_STATISTICAL_CONFIDENCE", "minimum_sample_count": 2,
            "units": deepcopy(UNITS), "checked_at_utc": now.isoformat(), "accepted_at_utc": e["accepted_at_utc"],
            "causal_cutoff_at_utc": e["causal_cutoff_at_utc"], "linked_dated_evidence_sha256": linked["evidence_sha256"],
            "statistics_evidence_sha256": snapshot[STORE_KEY]["evidence_sha256"], "last_capture_error": error,
            "audit_reference": "input_snapshot.json#/" + STORE_KEY + "/evidence", "dated": prior, "current": live, **dated.FLAGS}


def describe(snapshot, *, now):
    now = dated._stamp(now)
    try:
        return _render(snapshot, now, *_admit(snapshot, now))
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        return {"schema_version": SCHEMA, "status": "UNAVAILABLE", "reason": str(exc), **dated.FLAGS}


def attach_consumer(snapshot, consumers, *, now):
    context = consumers["futoi_context"]["futoi_live"]
    result = describe(snapshot, now=now)
    context["observed_statistics"] = result
    if context.get("comparisons") is not None:
        context["comparisons"]["statistics"] = deepcopy(result.get("current") or {
            "status": "UNAVAILABLE", "reason": result["reason"]})
        if context["comparisons"]["statistics"]["status"] == "UNAVAILABLE":
            context["comparisons"]["statistics"]["variables"] = None
        context["comparisons"]["statistics_policy"] = POLICY


def _capture(snapshot, *, cutoff):
    from moex_data.futures import futoi_delta_statistics_context as engine
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    data = snapshot["components"]["futoi_live"]["data"]
    if data.get("instrument_id") != dated.INSTRUMENT or data.get("source_id") != dated.SOURCE:
        raise ValueError("statistics_source_identity_mismatch")
    linked = dated.describe(snapshot.get(dated.STORE_KEY), now=cutoff, governance=data.get("governance"))
    if linked["status"] not in ("AVAILABLE", "PARTIAL"):
        raise ValueError("statistics_linked_anchor_not_available")
    root = source._data_root()
    context = {**data["context_refresh"], engine.session_context.PREVIOUS_ROLE: data["previous_completed_session"],
               engine.session_context.CURRENT_ROLE: data.get("current_intraday")}
    witness = engine._observed_witness(root, as_of=cutoff, raw_context=context)
    dated._check_source_refs(root, witness["provenance"], ("partition", "manifest", "quality_report"))
    verified = dated._verified_frame(root, witness["provenance"], "partition")
    anchor_day = linked["anchor"]["factual"]["trade_date"]
    slots = sorted({dated._day(str(day)) for day in verified["trade_date"] if str(day) <= anchor_day})[-504:]
    observed = [day for day in witness["observed_trade_dates"] if day <= anchor_day][-504:]
    if slots != observed or not slots or slots[-1] != anchor_day:
        raise ValueError("statistics_verified_observed_date_mismatch")
    try:
        _, eod_proof = engine._accepted_eod(root, instrument_id=dated.INSTRUMENT, as_of=cutoff)
        dated._check_source_refs(root, eod_proof, ("partition", "manifest", "quality_report"))
        eod = dated._verified_frame(root, eod_proof, "partition")
    except Exception:
        eod, eod_proof = None, None
    rows, proofs = [], {}
    for day in slots:
        row = {"trade_date": day, "status": "UNAVAILABLE", "values": None, "clocks": None, "proof_id": None, "reason": None}
        try:
            loaded = engine._raw_factual(root, instrument_id=dated.INSTRUMENT, trade_date=day)
            if loaded.get("reason") == "canonical_raw_partition_missing":
                matches = eod.loc[eod["trade_date"].astype(str).eq(day)] if eod is not None else []
                if len(matches) != 1:
                    raise ValueError("exact_raw_and_accepted_eod_date_missing_or_duplicate")
                fact = engine._eod_factual(matches.iloc[0], instrument_id=dated.INSTRUMENT)
                proof = {"source_kind": "accepted_eod", "provenance": {
                    "source_kind": "accepted_stage5_eod_historical_context_only", "accepted_pointer": eod_proof}}
            else:
                if loaded.get("status") != "AVAILABLE":
                    raw_proof = loaded.get("provenance")
                    if raw_proof:
                        proof = {"source_kind": "excluded_raw", "provenance": {key: raw_proof[key] for key in ("raw_partition_ref", "raw_partition_sha256")}}
                        dated._check_source_refs(root, proof["provenance"], ("raw_partition",))
                        raw_path = root / raw_proof["raw_partition_ref"][len("${MOEX_DATA_ROOT}/"):]
                        frozen = source._freeze_artifact(root, raw_path, raw_proof["raw_partition_sha256"])
                        proof["provenance"]["raw_partition_ref"] = source._rooted_ref(root, frozen)
                        key = dated._digest(proof); proofs[key] = proof; row["proof_id"] = key
                    raise ValueError(loaded.get("reason") or "latest_raw_not_admitted_no_revision_fallback")
                fact, raw_proof = dated._freeze_raw_fact(root, loaded["provenance"], day, normalized=True)
                if fact != loaded["factual"]: raise ValueError("statistics_frozen_raw_fact_mismatch")
                proof = {"source_kind": "canonical_raw", "provenance": raw_proof}
            dated._fact(_record(fact, proof["source_kind"], proof["provenance"]), day, cutoff)
            if proof["source_kind"] == "canonical_raw":
                for field in CLOCKS: dated._stamp(fact.get(field))
            _proof(proof)
            key = dated._digest(proof); proofs[key] = deepcopy(proof)
            row = _encode_row(fact, key)
        except Exception as exc:
            row["reason"] = type(exc).__name__ + ": " + str(exc)
        rows.append(row)
    return {"schema_version": SCHEMA, "policy": POLICY, "linked_dated_evidence_sha256": linked["evidence_sha256"],
        "accepted_at_utc": cutoff.isoformat(), "causal_cutoff_at_utc": cutoff.isoformat(), "columns": list(COLUMNS),
        "slots": slots, "rows": rows, "proofs": proofs, "governance_at_acceptance": deepcopy(data["governance"]),
        "observed_date_witness": {"provenance": deepcopy(witness["provenance"]), "observed_trade_dates": slots,
            "current_observed_trade_date": witness.get("current_observed_trade_date"),
            "previous_observed_trade_date": witness.get("previous_observed_trade_date")}}


def _semantic(e):
    return {"linked": e["linked_dated_evidence_sha256"], "slots": e["slots"],
        "rows": [{key: row[key] for key in ("trade_date", "status", "values")} for row in e["rows"]],
        "current_date": e["observed_date_witness"]["current_observed_trade_date"],
        "previous_date": e["observed_date_witness"]["previous_observed_trade_date"]}


def capture_snapshot(snapshot, previous, *, now_fn, refresh_started_at):
    if dated.STORE_KEY not in snapshot:
        return
    cutoff = dated._stamp(now_fn())
    prior = deepcopy((previous or {}).get(STORE_KEY))
    floors = [dated._stamp(refresh_started_at), dated._stamp(snapshot[dated.STORE_KEY]["last_capture_attempt_at_utc"])]
    if isinstance(prior, dict): floors.append(dated._stamp(prior["last_capture_attempt_at_utc"]))
    if cutoff < max(floors): raise ValueError("statistics_cutoff_precedes_previous_capture")
    candidate, error = None, None
    try: candidate = _capture(snapshot, cutoff=cutoff)
    except Exception as exc: error = type(exc).__name__ + ": " + str(exc)
    completed = dated._stamp(now_fn())
    if completed < cutoff: raise ValueError("statistics_capture_clock_reversed")
    if candidate is not None:
        candidate["accepted_at_utc"] = completed.isoformat()
        new = {"schema_version": SCHEMA, "evidence": candidate, "evidence_sha256": dated._digest(candidate),
            "last_capture_attempt_at_utc": completed.isoformat(), "last_capture_error": None}
        try:
            _admit({**snapshot, STORE_KEY: new}, completed)
            if prior is not None:
                try:
                    old, _, _, _ = _admit({**snapshot, STORE_KEY: prior}, completed)
                    if _semantic(old) == _semantic(candidate):
                        new["evidence"] = old; new["evidence_sha256"] = prior["evidence_sha256"]
                except (KeyError, TypeError, ValueError, OverflowError, AttributeError): pass
            snapshot[STORE_KEY] = new
            return completed
        except Exception as exc: error = type(exc).__name__ + ": " + str(exc)
    if prior is not None:
        prior.update(last_capture_attempt_at_utc=completed.isoformat(), last_capture_error=error)
        snapshot[STORE_KEY] = prior
    return completed


def verify_projection(snapshot, release, *, now):
    """Canonical inventory plus separately derived Decimal arithmetic below."""
    output = release["futoi_context"]["futoi_live"].get("observed_statistics")
    now = dated._stamp(now)
    try: e, facts, linked, error = _admit(snapshot, now)
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        expected = {"schema_version": SCHEMA, "status": "UNAVAILABLE", "reason": str(exc), **dated.FLAGS}
        dated._require(isinstance(output, dict) and dated._digest(output) == dated._digest(expected), "Si statistics canonical refusal")
        comparisons = release["futoi_context"]["futoi_live"].get("comparisons")
        if comparisons is not None:
            dated._require(comparisons.get("statistics") == {"status": "UNAVAILABLE", "reason": str(exc), "variables": None}
                and comparisons.get("statistics_policy") == POLICY, "Si statistics refused legacy selection")
        return
    expected = _render(snapshot, now, e, facts, linked, error)
    dated._require(isinstance(output, dict) and dated._digest(output) == dated._digest(expected), "Si statistics canonical projection")
    metadata = {"schema_version": SCHEMA, "policy": POLICY, "status": "AVAILABLE",
        "scope": "SI_FAMILY_DESCRIPTIVE_OBSERVED_SUBSETS_NOT_STATISTICAL_CONFIDENCE", "minimum_sample_count": 2,
        "units": UNITS, "checked_at_utc": now.isoformat(), "accepted_at_utc": e["accepted_at_utc"],
        "causal_cutoff_at_utc": e["causal_cutoff_at_utc"], "linked_dated_evidence_sha256": linked["evidence_sha256"],
        "statistics_evidence_sha256": snapshot[STORE_KEY]["evidence_sha256"], "last_capture_error": error,
        "audit_reference": "input_snapshot.json#/" + STORE_KEY + "/evidence", **dated.FLAGS}
    dated._require(set(output) == set(metadata) | {"current", "dated"}
        and dated._digest({key: output[key] for key in metadata}) == dated._digest(metadata), "Si statistics independent metadata")
    comparisons = release["futoi_context"]["futoi_live"].get("comparisons")
    if comparisons is not None:
        alias = deepcopy(output["current"])
        if alias["status"] == "UNAVAILABLE": alias["variables"] = None
        dated._require(dated._digest(comparisons.get("statistics")) == dated._digest(alias) and comparisons.get("statistics_policy") == POLICY,
                       "Si statistics legacy current selection")
    views = {"dated": (e["slots"], facts)}
    try:
        slots, live_facts, _ = _current(snapshot, e, facts, dated._stamp(now))
        views["current"] = (slots, live_facts)
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        dated._require(output["current"] == {"status": "UNAVAILABLE", "reason": str(exc)}, "Si statistics current refusal completeness")
    for name, (slots, source) in views.items():
        result = output[name]
        dated._require(set(result) == {"status", "anchor_trade_date", "anchor_values", "changes", "windows"}
            and result["status"] == "AVAILABLE" and result["anchor_trade_date"] == slots[-1], "Si statistics admitted view inventory")
        dated._require(set(result["changes"]) == {str(lag) for lag in dated.LAGS}
            and set(result["windows"]) == {str(window) for window in WINDOWS}, "Si statistics window and lag inventory")
        def numbers(fact):
            oi = Decimal(fact["total_open_interest"])
            result = {"total_open_interest": oi}
            for side in ("fiz", "yur"):
                raw = fact[side]
                for field in dated.SIDE_FIELDS: result[side + "." + field] = Decimal(raw[field])
                result[side + ".gross"] = Decimal(raw["long"]) + Decimal(raw["short"])
                for field in ("long", "short", "net"):
                    result[side + "." + field + "_share_of_oi"] = Decimal(raw[field]) / oi
                result[side + ".gross_share_of_two_sided_oi"] = (Decimal(raw["long"]) + Decimal(raw["short"])) / (2*oi)
            return result
        decimal_values = {day: numbers(source[day]) for day in slots if day in source}
        last = decimal_values[slots[-1]]
        def equal(actual, calculated):
            dated._require(type(actual) in (int, float) and isclose(actual, float(calculated), rel_tol=1e-11, abs_tol=1e-11), "Si statistics independent arithmetic")
        dated._require(set(result["anchor_values"]) == set(FIELDS), "Si statistics derived field coverage")
        for key in FIELDS: equal(result["anchor_values"][key], last[key])
        for lag in dated.LAGS:
            target = slots[-1-lag] if len(slots) > lag else None
            item = result["changes"][str(lag)]
            dated._require(set(item) == {"target_trade_date", "anchor_trade_date", "status", "values", "reason"}
                and item["target_trade_date"] == target and item["anchor_trade_date"] == slots[-1], "Si statistics exact lag date")
            if target in decimal_values:
                dated._require(item["status"] == "AVAILABLE" and set(item["values"]) == set(FIELDS), "Si statistics lag coverage")
                dated._require(item["reason"] is None, "Si statistics lag reason")
                for key in FIELDS:
                    if "share" not in key: dated._require(type(item["values"][key]) is int, "Si statistics integer delta")
                    equal(item["values"][key], last[key]-decimal_values[target][key])
            else:
                excluded = {row["trade_date"]: row["reason"] for row in e["rows"] if row["status"] == "UNAVAILABLE"}
                dated._require(item["status"] == "UNAVAILABLE" and item["values"] is None
                    and item["reason"] == (excluded.get(target) or "insufficient_exact_observed_slots"), "Si statistics missing lag")
        for window in WINDOWS:
            item = result["windows"][str(window)]
            selected = slots[-window:]; days = [day for day in selected if day in source]
            excluded = {row["trade_date"]: row["reason"] for row in e["rows"] if row["status"] == "UNAVAILABLE"}
            expected_excluded = [{"trade_date": day, "reason": excluded[day]} for day in selected if day not in source]
            window_metadata = {"expected_window_slots": window, "observed_slot_count": len(selected), "sample_count": len(days),
                "coverage_status": "COMPLETE" if len(days) == window else "PARTIAL", "sample_dates": days,
                "excluded_dates": expected_excluded, "missing_observed_history_slots": max(0, window-len(selected)),
                "slot_start_date": selected[0], "slot_end_date": selected[-1]}
            dated._require(dated._digest({key: val for key, val in item.items() if key != "variables"}) == dated._digest(window_metadata),
                           "Si statistics independent window metadata")
            dated._require(set(item) == {"expected_window_slots", "observed_slot_count", "sample_count", "coverage_status", "sample_dates",
                "excluded_dates", "missing_observed_history_slots", "slot_start_date", "slot_end_date", "variables"}
                and item["excluded_dates"] == expected_excluded and item["missing_observed_history_slots"] == max(0, window-len(selected))
                and item["slot_start_date"] == selected[0] and item["slot_end_date"] == selected[-1], "Si statistics exact excluded slot inventory")
            dated._require(item["sample_dates"] == days and item["sample_count"] == len(days)
                and item["expected_window_slots"] == window and item["observed_slot_count"] == len(selected)
                and item["coverage_status"] == ("COMPLETE" if len(days) == window else "PARTIAL"), "Si statistics exact subset coverage")
            dated._require(set(item["variables"]) == set(STAT_FIELDS), "Si statistics variable coverage")
            for key in STAT_FIELDS:
                metric = item["variables"][key]
                dated._require(set(metric) == {"status", "reason", "population_mean", "population_std_ddof_0", "percentile", "zscore", "zscore_reason"}, "Si statistics metric shape")
                if len(days) < 2:
                    dated._require(metric["status"] == "UNAVAILABLE" and metric["reason"] == "minimum_two_admitted_observations_required"
                        and all(metric[k] is None for k in ("population_mean", "population_std_ddof_0", "percentile", "zscore", "zscore_reason")), "Si statistics minimum sample")
                    continue
                with localcontext() as ctx:
                    ctx.prec = 40
                    values = [decimal_values[day][key] for day in days]
                    mean = sum(values) / len(values)
                    variance = sum((v-mean)**2 for v in values) / len(values)
                    std = variance.sqrt()
                    dated._require(metric["status"] == "AVAILABLE", "Si statistics metric omitted")
                    dated._require(metric["reason"] is None, "Si statistics metric reason")
                    equal(metric["population_mean"], mean); equal(metric["population_std_ddof_0"], std)
                    equal(metric["percentile"], Decimal(sum(v <= last[key] for v in values))/len(values))
                    if len(set(values)) == 1:
                        dated._require(metric["zscore"] is None and metric["zscore_reason"] == "zero_population_variance", "Si statistics constant sample")
                    else:
                        dated._require(metric["zscore_reason"] is None, "Si statistics nonconstant zscore reason")
                        equal(metric["zscore"], (last[key]-mean)/std)
