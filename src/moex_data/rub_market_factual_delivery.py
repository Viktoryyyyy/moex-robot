"""Bounded final presentation of the canonical Price/OI, FUTOI and basis view.

This is a projection of existing admissions, never a collector or an admission
engine. The final clock may only revoke an expired result of the canonical read.
"""
from copy import deepcopy
from datetime import datetime, timedelta
from hashlib import sha256
import json
import re

SCHEMA = "rub_market_factual_delivery.v1"
MAX_BYTES = 131072
MARKETS = ("usdrubf", "si_front", "si_next", "cnyrubf", "cr_front", "cr_next", "cnyrub_tom")
IDENTITY = ("instrument_id", "source_id", "source_ticker", "source_identity_scope", "raw_schema_version")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def pick(value, fields):
    return {key: deepcopy(value[key]) for key in fields if key in value}


def _freshness(stamp, now, ttl):
    reason = None
    try:
        source = datetime.fromisoformat(stamp)
        if source.utcoffset() is None:
            raise ValueError("timezone required")
        age = (now-source).total_seconds()
        deadline = (source+timedelta(seconds=ttl)).isoformat()
    except (TypeError, ValueError, OverflowError):
        source = None
        age, deadline = None, None
        reason = "source_timestamp_missing_or_invalid"
    return {"source_timestamp": stamp, "checked_at_utc": now.isoformat(), "ttl_seconds": ttl,
        "reason": reason,
        "age_seconds": age, "valid_until_utc": deadline}


def _proof(record):
    provenance = record.get("provenance") or {}
    return {"record_sha256": sha256(encoded(record)).hexdigest(),
        "source_identity": pick(record, IDENTITY),
        "provenance": pick(provenance, (*IDENTITY, "publication_run_id", "publication_audit",
            "raw_partition_ref", "raw_partition_sha256", "raw_quality_report_ref", "raw_quality_report_sha256",
            "raw_refresh_manifest_ref", "raw_refresh_manifest_sha256"))}


def project(snapshot, *, now, code_revision):
    from moex_data.rub_factual_projection import market_data, fresh, basis_metrics
    from moex_data.futures.futoi_current_pair_authority import check_time
    if not re.fullmatch("[0-9a-f]{40}", code_revision or "") or now.utcoffset() is None:
        raise ValueError("exact code revision and aware delivery time required")
    release = snapshot["factual_release"]
    read_at = snapshot["live_read_freshness"]["read_at_utc"]
    if release.get("as_of_utc") != read_at or datetime.fromisoformat(read_at) > now:
        raise ValueError("canonical admission clock mismatch")
    bundle = release["analysis_bundles"]["daily"]
    if bundle.get("schema_version") != "rub_analysis_bundle.v2":
        raise ValueError("canonical Stage9 v2 admission required")
    selected = bundle["sections"]["current_market"]["items"]
    facts = {row["factor"]: row for row in release["facts"]}
    if len(facts) != len(release["facts"]):
        raise ValueError("duplicate canonical fact identity")
    market = market_data(snapshot).get("instruments", {})
    prices = {}
    for key in MARKETS:
        row, admitted = market.get(key, {}), release["market_usability"][key]
        allowed = key in facts and selected[key]["status"] == "AVAILABLE" and fresh(row, now)
        quote_allowed = admitted["quote_usable"] is True and fresh(row, now)
        prices[key] = {"status": "AVAILABLE" if allowed else "UNAVAILABLE",
            "scope": "current_spot_price" if key == "cnyrub_tom" else "current_price_oi",
            "reason": None if allowed else admitted.get("missing_reason") or "source_expired_before_delivery",
            "values": deepcopy(facts[key]["values"]) if allowed else None,
            "source_identity": pick(row, ("logical_id", "secid", "source_id", "asset_type", "source_trade_date",
                "timestamp", "source_update_timestamp_utc", "received_at_utc", "timestamp_semantics")),
            "freshness": _freshness(row.get("timestamp"), now, 60),
            "contract_metadata": deepcopy(admitted["contract_metadata"]),
            "missing_metadata": deepcopy(admitted["missing_metadata"]),
            "quote": deepcopy(admitted["quote"]) if quote_allowed else None,
            "quote_usable": quote_allowed,
            "cross_market_comparison_usable": allowed and admitted["cross_market_comparison_usable"],
            "evidence_ref": "components.synchronized_live_market_oi.data.instruments."+key,
            "source_record_sha256": sha256(encoded(row)).hexdigest()}
    futoi = {}
    for ticker, name in (("si", "futoi_live"), ("cr", "futoi_live_cr")):
        item = selected[name]
        component = snapshot["components"].get(name, {})
        data = component.get("data") or {}
        record = data.get("current_intraday") or {}
        fact = record.get("factual") or {}
        reason = (record.get("refresh_error") or component.get("refresh_error") or item.get("reason"))
        allowed = item["status"] == "AVAILABLE" and name in facts
        if allowed:
            try:
                check_time(record, now)
            except (ValueError, KeyError, TypeError) as exc:
                allowed, reason = False, str(exc)
        previous = release["futoi_context"][name].get("previous_observation")
        previous_record = data.get("previous_completed_session") or {}
        previous_reason = "previous_observation_not_admitted_in_existing_consumer_scope"
        if previous is not None:
            from moex_data.rub_temporal_applicability import _previous_witness, _previous_reason
            context = data.get("context_refresh") or {}
            previous_reason = _previous_reason(previous_record, context,
                expected=_previous_witness(context, now), instrument=ticker+"_futures_family", data=data, now=now)
            if previous_reason is not None:
                previous = None
        futoi[ticker] = {"status": "AVAILABLE" if allowed else "UNAVAILABLE",
            "scope": item.get("scope", "current_intraday_latest_pair_only" if ticker == "cr" else "current_source_pair_only"),
            "source_identity": deepcopy(item["source_identity"]), "reason": None if allowed else reason or "current_pair_not_admitted",
            "requested_trade_date": record.get("expected_trade_date"), "source_trade_date": fact.get("trade_date"),
            "values": deepcopy(fact) if allowed else None,
            "freshness": {"source": _freshness(fact.get("snapshot_ts"), now, 1200),
                "receipt": _freshness(fact.get("availability_ts_utc"), now, 1200)},
            "evidence_verification": deepcopy(item.get("evidence_verification")),
            "evidence": _proof(record), "evidence_ref": "components."+name+".data.current_intraday",
            "failure": pick(record, ("refresh_error", "refresh_error_class", "failed_attempt_at", "failure_stage", "date_witness_observations")),
            "previous_dated_observation": {"status": "AVAILABLE_DATED" if previous is not None else "UNAVAILABLE",
                "scope": "existing_independent_previous_admission_not_current_or_completed_session",
                "values": deepcopy(previous), "current_usable": False,
                "evidence": _proof(previous_record),
                "reason": None if previous is not None else previous_reason}}
    # Reuse the existing per-metric selector at the final clock. It performs no
    # I/O and cannot restore any metric absent from the already admitted release.
    final_view = dict(snapshot, live_read_freshness={"read_at_utc": now.isoformat()})
    allowed_metrics = dict(basis_metrics(final_view))
    original = facts.get("basis_carry", {}).get("values", {}).get("metrics", [])
    metrics = []
    for item in original:
        path, metric = item["snapshot_path"], item["values"]
        if path in allowed_metrics and allowed_metrics[path] == metric:
            value = deepcopy(metric)
            value["freshness"] = {"status": "FRESH", "threshold_seconds": 60, "age_reference_utc": now.isoformat(),
                "age_seconds_by_leg": {k: _freshness(market[k].get("timestamp"), now, 60)["age_seconds"] for k in metric["legs"]}}
            metrics.append({"evidence_ref": path, "source_metric_sha256": sha256(encoded(metric)).hexdigest(), "values": value})
    by_id = {item["values"]["metric_id"] for item in metrics}
    refusals = {}
    source_basis = (snapshot["components"].get("live_basis_carry", {}).get("data") or {})
    for pair in source_basis.get("pairs", {}).values():
        for metric in pair.get("metrics", []):
            identity = metric.get("metric_id")
            if identity and identity not in by_id:
                refusals[identity] = {"status": "UNAVAILABLE", "reason": metric.get("unavailable_reason") or "not_admitted_or_expired_before_delivery",
                    "legs": deepcopy(metric.get("legs")), "source_timestamps": deepcopy(metric.get("source_timestamps")),
                    "source_metric_sha256": sha256(encoded(metric)).hexdigest()}
    basis = {"status": "PARTIAL" if metrics and refusals else "AVAILABLE" if metrics else "UNAVAILABLE",
        "scope": "individual_existing_admitted_metrics_only", "metrics": metrics, "refusals": refusals,
        "admitted_metric_count": len(metrics), "unavailable_metric_count": len(refusals)}
    statuses = [v["status"] for v in prices.values()] + [v["status"] for v in futoi.values()] + [basis["status"]]
    result = {"project": "MOEX_Bot", "schema_version": SCHEMA,
        "status": "AVAILABLE" if all(v == "AVAILABLE" for v in statuses) else "PARTIAL" if any(v in ("AVAILABLE", "PARTIAL") for v in statuses) else "UNAVAILABLE",
        "scope": "current_price_oi_futoi_basis_carry_not_full_analysis_bundle",
        "as_of_utc": now.isoformat(), "canonical_read_at_utc": read_at, "code_revision": code_revision,
        "snapshot_generated_at_utc": snapshot["identity"]["generated_at_utc"],
        "fast_market_generation": deepcopy(snapshot.get("fast_market_read", {"status": "NOT_ENABLED"})),
        "selection_contract": deepcopy(bundle["selection_contract"]),
        "prices": prices, "futoi": futoi, "basis_carry": basis,
        "additional_context": {"full_factual_release_path": "/v1/rub/factual-release",
            "daily_weekly_status": {s: v["readiness"]["bundle_status"] for s,v in release["analysis_bundles"].items()},
            "historical_context_in_this_response": False, "external_context_in_this_response": False,
            "position_risk_in_this_response": False, "missing_source": "USD_TOM_not_in_existing_live_schema"},
        "authority": {"session_completion_proven": False, "analysis_bundle_complete": False,
            "model_ready": False, "forecast_generated": False, "action_authority": False, "broker_execution": False},
        "consumption_rule": "recheck_each_original_source_deadline_at_actual_use_no_TTL_extension_or_dated_fallback"}
    if len(encoded(result)) > MAX_BYTES:
        raise ValueError("market delivery exceeds explicit response bound; no silent truncation")
    return result
