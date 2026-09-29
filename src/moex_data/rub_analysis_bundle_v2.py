"""Stage9 v2 selects existing admitted evidence; it never acquires or promotes data."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
import base64
import json

SCHEMA = "rub_analysis_bundle.v2"
POLICY_ID = "rub_analysis_bundle_selection.v2"
CONTRACT = "contracts/intelligence/rub_analysis_bundle_selection_v2.json"
MOSCOW = timezone(timedelta(hours=3))
POLICY = {
    "project": "MOEX_Bot", "schema_version": POLICY_ID,
    "sections": ["current_market", "completed_periods", "historical_comparisons"],
    "market_source_ttl_seconds": 60, "futoi_current_ttl_seconds": 1200,
    "dated_maximum_lifetime_seconds": 345600,
    "period_policy": "accepted_elapsed_calendar_period_observations_no_session_completion_claim",
    "period_freshness": "explicit_source_period_age_not_live_freshness",
    "selection": "existing_admission_then_explicit_role_no_cross_role_fallback",
    "read": "replay_frozen_pointer_and_original_hash_bound_buffers_recheck_existing_admissions",
    "partial_is_ready": False, "new_collection": False, "historical_pointer_writes": False,
    "model_authority": False, "trading_authority": False,
}
MARKETS = ("usdrubf", "cnyrubf", "si_front", "si_next", "cr_front", "cr_next", "cnyrub_tom", "usd_tom")
FLAGS = {"session_completion_proven": False, "historical_pit_usable": False,
         "model_usable": False, "action_authority": False, "stage5_full_mode_ready": False}


def _stamp(value):
    from moex_data.step9_rub_analysis_bundle import _parse_as_of
    return _parse_as_of(value) if isinstance(value, str) else _parse_as_of(value.isoformat())


def _hash(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _contract():
    from moex_data.step9_rub_analysis_bundle import _load_json
    raw = _load_json(Path(__file__).resolve().parents[2] / CONTRACT, "Stage9 selection contract")
    if raw != POLICY:
        raise ValueError("Stage9 selection contract unsupported or changed")
    return {"contract_id": POLICY_ID, "contract_ref": CONTRACT, "contract_sha256": _hash(raw)}


def _refusal(block_id, reason, **evidence):
    return {"block_id": block_id, "status": "UNAVAILABLE", "reason": str(reason),
            "values": None, **evidence}


def _section(items, *, role, now, policy):
    states = [item.get("status") for item in items.values()]
    good = sum(state in ("AVAILABLE", "AVAILABLE_DATED") for state in states)
    usable = good + sum(state == "PARTIAL" for state in states)
    return {"role": role, "status": "AVAILABLE" if states and good == len(states) else
            "PARTIAL" if usable else "UNAVAILABLE", "selection_checked_at_utc": now.isoformat(),
            "freshness_policy": policy, "items": items,
            "unavailable_or_partial": sorted(key for key, item in items.items()
                                       if item.get("status") not in ("AVAILABLE", "AVAILABLE_DATED")),
            **FLAGS}


def seed(*, scope, now):
    """Capture only accepted Stage7 references once, inside the normal snapshot producer."""
    from moex_data import step9_rub_analysis_bundle as legacy
    contract = _contract()
    root = legacy._data_root()
    blocks = []
    for spec in legacy._stage7_specs(scope):
        try:
            block = legacy._read_pointer_block(root, spec, now, freeze_pointer=True)
        except Exception as exc:
            block = _refusal(spec.block_id, exc, stage=7, dataset_id=spec.dataset_id,
                             instrument_id=spec.instrument_id, timeframe=spec.timeframe)
        blocks.append(block)
    return {"schema_version": SCHEMA, "selection_contract": contract,
            "identity": {"project": "MOEX_Bot", "scope": scope, "as_of": now.isoformat()},
            "server_core": {"blocks": blocks, "block_count": len(blocks),
                            "status": "ASSEMBLY_PENDING"},
            "position_risk": {"status": "not_supplied", "reason": "explicit Stage8 input not supplied"},
            "readiness": {"bundle_status": "PARTIAL", "analysis_bundle_complete": False}}


def shared_seed_provider():
    """One refresh owns one immutable D1/W1 capture, including parallel producers."""
    from threading import Lock
    lock, cache = Lock(), {}
    def get(scope, now):
        with lock:
            if now not in cache:
                cache[now] = seed(scope="weekly", now=now)
            result = deepcopy(cache[now])
        result["identity"]["scope"] = scope
        if scope == "daily":
            core = result["server_core"]
            core["blocks"] = [b for b in core["blocks"] if b["timeframe"] == "1D"]
            core["block_count"] = len(core["blocks"])
        return result
    return get


def period_blocks(data, *, now):
    """Replay the selected immutable generation, never the current mutable pointer."""
    from moex_data import step9_rub_analysis_bundle as legacy
    _contract()
    scope = data.get("identity", {}).get("scope")
    if scope not in ("daily", "weekly"):
        return []
    raw = data.get("server_core", {}).get("blocks", [])
    specs = legacy._stage7_specs(scope)
    result = []
    for spec in specs:
        candidates = [item for item in raw if isinstance(item, dict) and item.get("block_id") == spec.block_id]
        try:
            if len(candidates) != 1:
                raise ValueError("missing_or_duplicate_selected_period_block")
            stored = candidates[0]
            if data.get("schema_version") != SCHEMA or data.get("identity", {}).get("project") != "MOEX_Bot":
                raise ValueError("selected_bundle_outer_identity_mismatch")
            if data.get("selection_contract") != _contract():
                raise ValueError("selected_bundle_contract_identity_mismatch")
            for key in ("instrument_id", "dataset_id", "timeframe"):
                if stored.get(key) != getattr(spec, key):
                    raise ValueError("selected_period_outer_identity_mismatch")
            proof = stored["source_envelope"]
            if proof.get("schema_version") != "stage9_accepted_pointer_evidence.v1":
                raise ValueError("selected_period_evidence_version_required")
            pointer = base64.b64decode(proof["pointer_bytes_base64"], validate=True)
            if sha256(pointer).hexdigest() != proof["pointer_sha256"]:
                raise ValueError("selected_period_pointer_digest_mismatch")
            cutoff = _stamp(proof["selection_as_of_utc"])
            if cutoff > now:
                raise ValueError("selected_period_capture_is_future")
            buffers = {key: base64.b64decode(value, validate=True)
                       for key, value in proof["buffers_base64"].items()}
            if set(buffers) != {"manifest_ref", "quality_report_ref", "partition_ref"}:
                raise ValueError("selected_period_full_evidence_required")
            block = legacy._read_pointer_block(legacy._data_root(), spec, cutoff,
                pointer_bytes=pointer, evidence_buffers=buffers, freeze_pointer=True)
            row = block["selected_observation"]
            if _stamp(row["build_ts_utc"]) > cutoff:
                raise ValueError("selected_period_build_is_future")
            start, end = date.fromisoformat(row["period_start_date"]), date.fromisoformat(row["period_end_date"])
            if end >= cutoff.astimezone(MOSCOW).date() or start > end:
                raise ValueError("period_has_not_elapsed")
            if spec.timeframe == "1D" and start != end:
                raise ValueError("D1_period_identity_mismatch")
            if spec.timeframe == "1W" and (start.weekday() != 0 or (end-start).days != 6):
                raise ValueError("W1_period_identity_mismatch")
            if row.get("secid") != {"usdrubf_futures_family": "USDRUBF", "cnyrubf_futures_family": "CNYRUBF"}[spec.instrument_id]:
                raise ValueError("selected_period_SECID_mismatch")
            # A valid proof cannot legitimize a changed outer selected observation.
            if stored.get("selected_observation") != row or stored.get("provenance") != block["provenance"]:
                raise ValueError("selected_period_outer_values_or_provenance_changed")
            block["age_seconds_at_as_of"] = (now-_stamp(block["selected_causal_ts_utc"])).total_seconds()
            block["freshness"] = {"status": "DATED_REFERENCE", "checked_at_utc": now.isoformat(),
                "source_start_date": start.isoformat(), "source_end_date": end.isoformat(),
                "calendar_days_since_period_end": (now.astimezone(MOSCOW).date()-end).days,
                "current_use_allowed": False, "calendar_period_elapsed": True,
                "session_completion_proven": False, "first_economic_acceptance_at_utc": None,
                "acceptance_time_limitation": "upstream_pointer_has_run_identity_not_first_acceptance_time"}
            from moex_data.rub_fx_observed_context import apply
            apply(block, now)
            if spec.dataset_id == "rub_native_ohlcv_htf" and block["observed_context"]["status"] != "AVAILABLE":
                raise ValueError("selected_period_invalid: " + block["observed_context"]["reason"])
            result.append(block)
        except Exception as exc:
            original = candidates[0] if len(candidates) == 1 else {}
            result.append(_refusal(spec.block_id, exc, stage=7, dataset_id=spec.dataset_id,
                instrument_id=spec.instrument_id, timeframe=spec.timeframe,
                source_envelope=deepcopy(original.get("source_envelope")),
                original_failure=original.get("reason")))
    # Technical values must belong to the exact OHLCV producer and period.
    by_id = {block["block_id"]: block for block in result}
    for block in result:
        if block.get("status") != "ready" or block["dataset_id"] != "rub_technical_features_htf":
            continue
        other = by_id[block["block_id"].replace(".technical.", ".ohlcv.")]
        row, price = block["selected_observation"], other.get("selected_observation", {})
        if (other.get("status") != "ready" or
            any(row.get(key) != price.get(key) for key in ("instrument_id", "secid", "timeframe", "period_start_date", "period_end_date", "close", "availability_ts_utc")) or
            row.get("source_ohlcv_run_id") != other.get("provenance", {}).get("run_id")):
            block.update(_refusal(other["block_id"].replace(".ohlcv.", ".technical."),
                                  "technical_OHLCV_generation_or_period_mismatch"))
    return result


def _current(snapshot, checks, *, now):
    from moex_data.rub_factual_projection import market_data, fresh, spot_usable, basis_metrics
    from moex_data.futures.futoi_current_pair_authority import check_time
    market = market_data(snapshot).get("instruments", {})
    component_allowed = snapshot.get("components", {}).get("synchronized_live_market_oi", {}).get("status") in ("READY", "PARTIAL")
    items = {}
    for key in MARKETS:
        row = market.get(key, {})
        usable = spot_usable(snapshot, key) if key in ("cnyrub_tom", "usd_tom") else row.get("price_oi_usable") is True and fresh(row, now)
        usable = usable and component_allowed
        items[key] = ({"block_id": key, "status": "AVAILABLE", "source_ref": "components.synchronized_live_market_oi.data.instruments."+key,
                       "values": deepcopy(row), "source_time_ages": deepcopy(row.get("source_time_ages"))}
                      if usable else _refusal(key, row.get("read_freshness_reason") or "source_missing_stale_or_not_admitted",
                          evidence=deepcopy(row), source_time_ages=deepcopy(row.get("source_time_ages"))))
        if key in ('cnyrub_tom','usd_tom'):
            from moex_data.rub_currency_market_state import describe as currency_state
            items[key]['market_state'] = currency_state(row, now=now, current_admitted=usable)
    basis = snapshot.get("components", {}).get("live_basis_carry", {})
    metrics = basis_metrics(snapshot)
    total = sum(len(pair.get("metrics", [])) for pair in (basis.get("data") or {}).get("pairs", {}).values())
    items["basis_carry"] = {"block_id": "basis_carry", "status": "AVAILABLE" if metrics and len(metrics)==total else "PARTIAL" if metrics else "UNAVAILABLE",
        "values": [{"source_ref": path, "metric": deepcopy(value)} for path, value in metrics],
        "coverage": {"admitted_metric_count": len(metrics), "requested_metric_count": total},
        "evidence": deepcopy(basis), "reason": None if metrics else "no_admitted_current_basis_metrics"}
    for name, root in (("futoi_live", "si"), ("futoi_live_cr", "cr")):
        component = snapshot.get("components", {}).get(name, {})
        data = component.get("data") or {}
        record = data.get("current_intraday") or {}
        expected = {"instrument_id": root+"_futures_family", "source_id": "moex_algopack_futoi",
                    "source_ticker": root, "source_identity_scope": "source_ticker_root", "raw_schema_version": "v2"}
        try:
            check_time(record, now)
            check = checks.get(name, {})
            if check.get("status") != "PASS":
                raise ValueError(check.get("reason", "full_FUTOI_evidence_not_verified"))
            if check.get("record_sha256") != _hash(record):
                raise ValueError("FUTOI_record_changed_after_evidence_verification")
            if not (component.get("status") == "READY" and data.get("consumer_factual_use_allowed") is True
                    and data.get("factual_authority") is True):
                raise ValueError("current_pair_not_admitted")
            if "secid" in data or "secid" in record or any(data.get(k)!=v or record.get(k)!=v for k,v in expected.items()):
                raise ValueError("current_FUTOI_outer_version_or_root_identity_mismatch")
            items[name] = {"block_id": name, "status": "AVAILABLE", "source_identity": expected,
                "scope": data.get("factual_authority_scope", "current_source_pair_only"),
                "values": deepcopy(record), "admission": deepcopy(data.get("current_pair_admission")),
                "evidence_verification": deepcopy(check),
                "source_ref": "components."+name+".data.current_intraday"}
        except (KeyError, TypeError, ValueError) as exc:
            items[name] = _refusal(name, exc, source_identity=expected, evidence=deepcopy(record))
    return _section(items, role="CURRENT_SOURCE_OBSERVATIONS", now=now,
                    policy={"market_source_ttl_seconds": 60, "FUTOI_source_and_receipt_ttl_seconds": 1200,
                            "generation_receipt_does_not_renew_source_event": True})


def _partial_paths(value, path=""):
    result = []
    if isinstance(value, dict):
        if value.get("status") in ("UNAVAILABLE", "PARTIAL"):
            result.append({"path": path, "status": value["status"], "reason": value.get("reason")})
        for key in sorted(value):
            if not key.startswith("latest_"):
                result.extend(_partial_paths(value[key], path+"/"+key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            result.extend(_partial_paths(item, path+"/"+str(index)))
    return result


def _dated_item(key, value, *, now, linked_accepted=None):
    value = deepcopy(value) if isinstance(value, dict) else {}
    # Current-anchored history is distinct from the independently admitted dated view.
    value.pop("current", None)
    accepted = value.get("accepted_at_utc")
    valid_until = value.get("valid_until_utc") or value.get("dated_valid_until_utc")
    dated = value.get("dated") or value
    anchor = (dated.get("anchor") or {}).get("factual") or dated.get("source_anchor_clocks") or {}
    deadlines = [_stamp(v)+timedelta(seconds=345600)
                 for v in (accepted, linked_accepted, anchor.get("snapshot_ts")) if v]
    if valid_until:
        deadlines.append(_stamp(valid_until))
    valid_until = min(deadlines).isoformat() if deadlines else None
    reason = value.get("reason")
    if valid_until and now > _stamp(valid_until):
        reason = "dated_admission_expired_before_publication"
    available = value.get("status") in ("AVAILABLE", "PARTIAL") and not reason
    partial = _partial_paths(value)
    return {"block_id": key, "status": ("PARTIAL" if partial else "AVAILABLE_DATED") if available else "UNAVAILABLE",
        "values": value if available else None, "reason": reason if reason else None if available else "not_admitted",
        "coverage_refusals": partial, "refused_evidence": None if available else value,
        "evidence_metadata": {k: deepcopy(value[k]) for k in ("schema_version", "scope", "accepted_at_utc",
            "causal_cutoff_at_utc", "evidence_sha256", "statistics_evidence_sha256", "linked_dated_evidence_sha256",
            "audit_reference", "last_capture_error", "latest_capture_diagnostics", "latest_sample_failures") if k in value},
        "freshness": {"status": "DATED_REFERENCE" if available else "UNAVAILABLE",
            "original_checked_at_utc": value.get("checked_at_utc"), "checked_at_utc": now.isoformat(),
            "valid_until_utc": valid_until, "current_use_allowed": False}, **FLAGS}


def _history(release, periods, *, scope, now):
    items = {}
    for component, root in (("futoi_live", "si"), ("futoi_live_cr", "cr")):
        ctx = release.get("futoi_context", {}).get(component, {})
        dated_key = "dated_comparisons" if root == "si" else "scoped_observed_comparisons"
        for field in (dated_key, "observed_statistics"):
            items[root+"_"+field] = _dated_item(root+"_"+field, ctx.get(field), now=now,
                linked_accepted=(ctx.get(dated_key) or {}).get("accepted_at_utc"))
    for key in ("contract_price_market_oi_context", "historical_basis_carry_context"):
        items[key] = _dated_item(key, release.get(key), now=now)
    for block in periods:
        if block.get("dataset_id") == "rub_native_ohlcv_htf":
            key = block["block_id"]+".observed_comparisons"
            items[key] = _dated_item(key, block.get("observed_context"), now=now)
    return _section(items, role="EXACT_OBSERVED_COMPARISONS_AND_DESCRIPTIVE_STATISTICS",
                    now=now, policy={"existing_dated_lifetime_seconds": 345600,
                    "observed_lags_are_not_calendar_or_completed_session_lags": True,
                    "coverage": "actual_retained_dates_counts_and_refusals_no_gap_fill"})


def _external(snapshot, release, *, now):
    """Project only facts surviving the native finalizer at the publication clock."""
    from moex_data.rub_news_read_view import project
    components = snapshot.get("components", {})
    admitted, refused = [], []
    for fact in release.get("facts", []):
        if fact.get("scope") != "latest_published_dated_reference":
            continue
        component = components.get(fact["factor"], {})
        if fact["factor"] == "oil":
            from moex_research.external_data.moex_brent_factual import reconcile_component
            component = reconcile_component(component, now=now)
        data = component.get("data") or {}
        if component.get("status") == "READY" and data.get("consumer_factual_use_allowed") is True:
            admitted.append(deepcopy(fact))
        else:
            refused.append({"factor": fact["factor"], "reason": data.get("read_freshness_reason")
                or "component_not_admitted_at_publication", "evidence": deepcopy(fact)})
    inventory = release.get("macro_evidence_inventory") or {}
    return {"admitted_dated_facts": admitted, "refused_dated_facts": refused,
        "selection_checked_at_utc": now.isoformat(),
        "news_context": project(components.get("official_news", {}), now=now),
        "macro_requirements_policy": deepcopy(inventory.get("requirements_policy")),
        "macro_policy_gaps": deepcopy(inventory.get("policy_gaps", [])),
        "macro_missing_evidence_at_admission": deepcopy(inventory.get("missing_evidence", [])),
        "macro_admission_checked_at_utc": inventory.get("as_of_utc"),
        "canonical_inventory_ref": "factual_release.macro_evidence_inventory"}


def _check_current(snapshot, *, now):
    """Existing governance and full publication replay remain the admission source."""
    from moex_data.futures import futoi_live_factual_refresh_source_native as source
    from moex_data.futures import futoi_current_pair_authority as pair
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as runner
    values = runner._load_governance()
    checks = {}
    for name, instrument in (("futoi_live", "si_futures_family"), ("futoi_live_cr", "cr_futures_family")):
        data = (snapshot.get("components", {}).get(name, {}).get("data") or {})
        record = data.get("current_intraday") or {}
        try:
            expected = {"instrument_id": instrument, "source_id": source.SOURCE_ID,
                "source_ticker": source.ROOT_TICKERS[instrument], "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
                "raw_schema_version": "v2"}
            if any("secid" in envelope or any(envelope.get(key) != value for key, value in expected.items())
                   for envelope in (data, record)):
                raise ValueError("current_FUTOI_outer_version_or_root_identity_mismatch")
            pair.check_time(record, now)
            replay = source.replay_root_factual(source._data_root(), record.get("provenance"),
                instrument_id=instrument, trade_date=record.get("expected_trade_date"))
            if replay != record.get("factual"):
                raise ValueError("FUTOI_current_fact_differs_from_full_evidence_replay")
            if instrument == "cr_futures_family":
                admission = pair.admit(values, record, root=source._data_root(), repo_root=runner.REPO_ROOT, now=now)
                allowed = admission["allowed"]
            else:
                admission = runner._governance_state(values, instrument)
                allowed = admission.get("factual_use_allowed") is True
            if not allowed:
                raise ValueError("existing_consumer_governance_does_not_admit_current_pair")
            checks[name] = {"status": "PASS", "record_sha256": _hash(record),
                "checked_at_utc": now.isoformat(), "admission": deepcopy(admission),
                "scope": "full_publication_and_raw_evidence_not_raw_only"}
        except (KeyError, ValueError, TypeError, OSError) as exc:
            checks[name] = {"status": "FAIL", "reason": str(exc), "checked_at_utc": now.isoformat()}
    return checks


def revoke_current(snapshot, checks, *, now):
    """A failed selected v2 proof revokes aliases too; dated evidence is independent."""
    from moex_data.futures.futoi_current_pair_authority import check_time
    for name, check in checks.items():
        component = snapshot.get("components", {}).get(name, {})
        data = component.get("data") or {}
        try:
            if check.get("status") != "PASS":
                raise ValueError(check.get("reason", "current_evidence_not_verified"))
            check_time(data.get("current_intraday"), now)
        except (ValueError, KeyError, TypeError) as exc:
            if not component:
                continue
            component["status"] = "UNAVAILABLE"
            component["stage9_current_refusal"] = str(exc)
            data["consumer_factual_use_allowed"] = data["factual_authority"] = False
            if isinstance(data.get("current_intraday"), dict):
                data["current_intraday"]["consumer_factual_use_allowed"] = False
            if isinstance(data.get("current_pair_admission"), dict):
                data["current_pair_admission"].update(allowed=False, error=str(exc))
            authority = snapshot.get("authority", {})
            instrument = "cr_futures_family" if name.endswith("_cr") else "si_futures_family"
            authority.get("futoi_by_instrument", {}).get(instrument, {})["factual_authority"] = False
            if name == "futoi_live":
                authority["futoi_factual_authority"] = False


def _periods(snapshot, *, now):
    result = {scope: period_blocks((snapshot["components"].get("stage9_"+scope, {}).get("data") or {}), now=now)
              for scope in ("daily", "weekly")}
    for scope, blocks in result.items():
        identity = (snapshot["components"].get("stage9_"+scope, {}).get("data") or {}).get("identity", {})
        if identity.get("scope") != scope:
            for block in blocks:
                block.update(status="UNAVAILABLE", reason="selected_bundle_scope_mismatch")
    # Also refuse inconsistent persisted envelopes supplied outside the shared producer.
    daily = {b["block_id"]: b for b in result["daily"]}
    for weekly in result["weekly"]:
        other = daily.get(weekly["block_id"])
        if other and other.get("status") == weekly.get("status") == "ready" and (
                other.get("source_envelope") != weekly.get("source_envelope")
                or other.get("selected_observation") != weekly.get("selected_observation")):
            for block in (other, weekly):
                block.update(status="UNAVAILABLE", reason="daily_weekly_D1_evidence_generation_mismatch")
    return result


def _bundles(snapshot, prepared, *, now):
    contract, release = prepared["contract"], prepared["release"]
    result = {}
    current = _current(snapshot, prepared["current_checks"], now=now)
    for scope in ("daily", "weekly"):
        key = "stage9_"+scope
        original = (snapshot.get("components", {}).get(key, {}).get("data") or {})
        if original.get("schema_version") != SCHEMA:
            continue
        periods = deepcopy(prepared["periods"][scope])
        from moex_data.rub_consumption_clock import block as period_clock
        for block in periods:
            if block.get("status") == "ready":
                period_clock(block, now)
                freshness = block["freshness"]
                freshness["checked_at_utc"] = now.isoformat()
                freshness["calendar_days_since_period_end"] = (now.astimezone(MOSCOW).date()
                    - date.fromisoformat(freshness["source_end_date"])).days
        period_items = {}
        for block in periods:
            good = block.get("status") == "ready"
            period_items[block["block_id"]] = {"block_id": block["block_id"],
                "status": "AVAILABLE_DATED" if good else "UNAVAILABLE",
                "values": deepcopy(block) if good else None, "reason": block.get("reason"),
                "freshness": deepcopy(block.get("freshness")), "refused_evidence": None if good else deepcopy(block)}
        completed = _section(period_items, role="ELAPSED_CALENDAR_PERIOD_OBSERVATIONS",
            now=now, policy=POLICY["period_policy"])
        history = _history(release, periods, scope=scope, now=now)
        sections = {"current_market": deepcopy(current), "completed_periods": completed,
                    "historical_comparisons": history}
        risk = deepcopy(original.get("position_risk") or {"status": "not_supplied"})
        risk["manual_position_context"] = deepcopy(release.get("user_position_context", {}))
        risk["manual_position_is_full_risk_state"] = False
        external = {"status": "PARTIAL", **_external(snapshot, release, now=now),
            "required_external_context": ["government_fx_operations_and_tax_cycle",
                "official_event_actuals_and_consensus", "verified_news_relevance_and_geopolitics",
                "Urals", "DXY", "UST"], "missing_does_not_mean_neutral": True}
        unavailable = [{"section": name, "block_id": item, "status": section["items"][item]["status"],
                        "reason": section["items"][item].get("reason")}
                       for name,section in sections.items() for item in section["unavailable_or_partial"]]
        limitations = ([] if scope=="daily" else ["si_cr_continuous_W1_not_admitted",
            "weekly_OI_not_admitted", "advanced_technical_features_without_upstream_policy_not_admitted"])
        source_times = [b.get("selected_causal_ts_utc") for b in periods if b.get("status")=="ready"]
        result[scope] = {"project": "MOEX_Bot", "schema_version": SCHEMA, "selection_contract": contract,
            "identity": {"project": "MOEX_Bot", "scope": scope, "as_of": now.isoformat(),
                "source_snapshot_generated_at_utc": snapshot.get("identity", {}).get("generated_at_utc")},
            "sections": sections,
            "server_core": {"status": completed["status"], "blocks": deepcopy(periods), "block_count": len(periods),
                "scope": "accepted_period_evidence_only_not_current_market",
                "freshness_alignment": {"status": "POLICY_DEFINED", "policy": POLICY_ID,
                    "oldest_selected_causal_ts_utc": min(source_times) if source_times else None,
                    "newest_selected_causal_ts_utc": max(source_times) if source_times else None,
                    "cross_section_timestamp_equality_required": False, "exact_trigger_generation_allowed": False}},
            "position_risk": risk, "external_context": external, "missing_sources": unavailable,
            "readiness": {"bundle_status": "PARTIAL", "analysis_bundle_complete": False,
                "server_core": completed["status"],
                "section_statuses": {name:s["status"] for name,s in sections.items()},
                "position_risk": risk.get("status"), "external_context": "PARTIAL",
                "unavailable_capabilities": limitations, "selection_policy_ready": True},
            "quality_gates": {"core_freshness_alignment_policy_ready": True,
                "exact_trigger_generation_allowed": False, "bundle_generates_trade_recommendation": False,
                "bundle_generates_position_size": False, "missing_position_risk_blocks_downstream_size_or_add_recommendation": risk.get("status")!="ready",
                **FLAGS}}
    return result


def prepare(snapshot, *, now):
    """Do expensive existing admission/replay before the final live publication clock."""
    if not any((snapshot.get("components", {}).get("stage9_"+s, {}).get("data") or {}).get("schema_version")==SCHEMA
               for s in ("daily", "weekly")):
        return None
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    from moex_data.rub_factual_release import describe
    view = apply_read_freshness(snapshot, now=now)
    current_checks = _check_current(view, now=now)
    revoke_current(view, current_checks, now=now)
    periods = _periods(view, now=now)
    release = describe(view, stage9_periods=periods, stage9_current_checks=current_checks, include_analysis_bundles=False)
    return {"release": release, "periods": periods, "checked_at_utc": now.isoformat(),
            "contract": _contract(), "current_checks": current_checks}


def finish(snapshot, prepared, *, now):
    if prepared is None:
        return
    if _stamp(prepared["checked_at_utc"]) > now:
        raise ValueError("Stage9 publication clock precedes admission")
    # Caller has run its native finalizer/read freshness. No evidence I/O here.
    revoke_current(snapshot, prepared["current_checks"], now=now)
    view = dict(snapshot, live_read_freshness={"read_at_utc": now.isoformat()})
    bundles = _bundles(view, prepared, now=now)
    for scope, bundle in bundles.items():
        component = snapshot["components"]["stage9_"+scope]
        component.update(data=bundle, status=bundle["readiness"]["bundle_status"])
    readiness = snapshot.get("readiness")
    if isinstance(readiness, dict):
        readiness["status"] = "PARTIAL"
        readiness["component_statuses"] = {k:v.get("status") for k,v in snapshot["components"].items()}
        readiness["partial_components"] = sorted(k for k,v in snapshot["components"].items() if v.get("status")=="PARTIAL")
    views = snapshot.setdefault("analysis_views", {})
    views["stage9_bundle_refs"] = {scope: "components.stage9_"+scope+".data.sections" for scope in bundles}
    views["carry"] = deepcopy(bundles.get("daily", {}).get("sections", {}).get("current_market", {}).get("items", {}).get("basis_carry", {}).get("values", []))
    views["cny_accepted_context_scope"] = "legacy_accepted_context_not_current_market"
    from moex_data.rub_snapshot_status_presentation import apply as present_status
    present_status(snapshot, now=now)


def reconcile(snapshot, *, now):
    """Canonical read replays the exact selected evidence, without new acquisition."""
    prepared = prepare(snapshot, now=now)
    if prepared is None:
        from moex_data.rub_factual_release import describe
        return describe(snapshot)
    finish(snapshot, prepared, now=now)
    release = prepared["release"]
    attach_release(snapshot, release)
    return release


def attach_release(snapshot, release):
    release['status_presentation'] = deepcopy(snapshot.get('status_presentation'))
    release["analysis_bundles"] = {}
    for scope,horizon in (("daily","D1"),("weekly","W1")):
        data = snapshot["components"].get("stage9_"+scope, {}).get("data") or {}
        if data.get("schema_version") == SCHEMA:
            release["horizons"][horizon].update(component_status="PARTIAL", source_readiness=deepcopy(data["readiness"]),
                analysis_bundle_schema=SCHEMA, section_statuses=deepcopy(data["readiness"]["section_statuses"]))
            release["analysis_bundles"][scope] = deepcopy(data)


def verify_projection(snapshot, release, *, now, periods):
    """Reverse inventory/value oracle over independently admitted source components."""
    from moex_data.rub_factual_projection import market_data, fresh, spot_usable, basis_metrics
    from moex_data.rub_consumption_clock import block as period_clock
    def require(condition, reason):
        if not condition:
            raise AssertionError("Stage9 projection: " + reason)
    components = snapshot.get("components", {})
    scopes = {scope for scope in ("daily", "weekly")
              if (components.get("stage9_"+scope, {}).get("data") or {}).get("schema_version") == SCHEMA}
    outputs = release.get("analysis_bundles")
    require(isinstance(outputs, dict) and set(outputs) == scopes, "scope inventory")
    market = market_data(snapshot).get("instruments", {})
    checks = _check_current(snapshot, now=now)
    for scope in scopes:
        output = outputs[scope]
        require(output.get("schema_version") == SCHEMA and output.get("project") == "MOEX_Bot"
            and output.get("selection_contract") == _contract(), "versioned contract identity")
        require(output.get("identity") == {"project": "MOEX_Bot", "scope": scope, "as_of": now.isoformat(),
            "source_snapshot_generated_at_utc": snapshot.get("identity", {}).get("generated_at_utc")}, "bundle identity")
        sections = output.get("sections")
        require(isinstance(sections, dict) and set(sections) == set(POLICY["sections"]), "section inventory")
        expected_periods = deepcopy(periods[scope])
        for block in expected_periods:
            if block.get("status") == "ready":
                period_clock(block, now)
        require(output.get("server_core", {}).get("blocks") == expected_periods, "full period values and evidence")
        usable_periods = sum(block.get("status") == "ready" for block in expected_periods)
        period_status = ("AVAILABLE" if expected_periods and usable_periods == len(expected_periods)
                         else "PARTIAL" if usable_periods else "UNAVAILABLE")
        require(output.get("server_core", {}).get("status") == period_status
            and output.get("readiness", {}).get("server_core") == period_status
            and sections["completed_periods"].get("status") == period_status, "period core availability")
        items = sections["completed_periods"]["items"]
        require(set(items) == {b["block_id"] for b in expected_periods}, "period inventory")
        history_sources = {}
        for block in expected_periods:
            item = items[block["block_id"]]
            if block.get("status") == "ready":
                require(item.get("status") == "AVAILABLE_DATED" and item.get("values") == block, "period projection")
            else:
                require(item.get("status") == "UNAVAILABLE" and item.get("values") is None
                    and item.get("refused_evidence") == block, "period refusal evidence")
            if block.get("dataset_id") == "rub_native_ohlcv_htf":
                history_sources[block["block_id"]+".observed_comparisons"] = block.get("observed_context") or {}
        current = sections["current_market"]["items"]
        require(set(current) == set(MARKETS) | {"usd_tom", "basis_carry", "futoi_live", "futoi_live_cr"}, "current inventory")
        for key in MARKETS:
            row = market.get(key, {})
            usable = spot_usable(snapshot, key) if key in ("cnyrub_tom", "usd_tom") else row.get("price_oi_usable") is True and fresh(row, now)
            usable = usable and components.get("synchronized_live_market_oi", {}).get("status") in ("READY", "PARTIAL")
            item = current[key]
            require(item.get("status") == ("AVAILABLE" if usable else "UNAVAILABLE"), "market admission")
            require(item.get("values") == (row if usable else None), "market values/identity/times")
            if not usable:
                require(item.get("evidence") == row and bool(item.get("reason")), "market refusal evidence")
        require(current["basis_carry"].get("values") == [{"source_ref": path, "metric": value}
                for path, value in basis_metrics(snapshot)], "basis metric inventory and values")
        for name, root in (("futoi_live", "si"), ("futoi_live_cr", "cr")):
            component = components.get(name, {}); data = component.get("data") or {}
            allowed = component.get("status") == "READY" and data.get("factual_authority") is True and data.get("consumer_factual_use_allowed") is True
            allowed = allowed and checks[name]["status"] == "PASS"
            item = current[name]
            require(item.get("status") == ("AVAILABLE" if allowed else "UNAVAILABLE"), "independent FUTOI admission")
            require(item.get("values") == (data.get("current_intraday") if allowed else None), "FUTOI full versioned envelope")
            if allowed:
                require(item.get("evidence_verification") == checks[name], "FUTOI full evidence verification")
            else:
                require(item.get("evidence") == (data.get("current_intraday") or {}) and bool(item.get("reason")), "FUTOI refusal evidence")
            ctx = release.get("futoi_context", {}).get(name, {})
            for field in (("dated_comparisons" if root == "si" else "scoped_observed_comparisons"), "observed_statistics"):
                history_sources[root+"_"+field] = ctx.get(field) or {}
        for key in ("contract_price_market_oi_context", "historical_basis_carry_context"):
            history_sources[key] = release.get(key) or {}
        history = sections["historical_comparisons"]["items"]
        require(set(history) == set(history_sources), "historical inventory")
        for key, source in history_sources.items():
            source = deepcopy(source); source.pop("current", None)
            item = history[key]
            if source.get("status") in ("AVAILABLE", "PARTIAL") and not source.get("reason"):
                require(item.get("values") == source and item.get("status") in ("AVAILABLE_DATED", "PARTIAL"), "historical values/identity/coverage")
            else:
                require(item.get("values") is None and item.get("status") == "UNAVAILABLE"
                    and item.get("refused_evidence") == source, "historical refusal evidence")
            require(item.get("coverage_refusals") == _partial_paths(source), "exact historical coverage refusals")
        for section in sections.values():
            require(section.get("selection_checked_at_utc") == now.isoformat(), "section consumption time")
            require(all(section.get(flag) is False for flag in FLAGS), "section authority")
            unavailable = sorted(key for key, item in section["items"].items()
                           if item.get("status") not in ("AVAILABLE", "AVAILABLE_DATED"))
            require(section.get("unavailable_or_partial") == unavailable, "section missing inventory")
        require(output.get("readiness", {}).get("bundle_status") == "PARTIAL"
                and output["readiness"].get("analysis_bundle_complete") is False, "no partial READY")


def build(*, scope, as_of, position_risk_input=None):
    if scope not in ("daily", "weekly"):
        raise ValueError("scope must be daily or weekly")
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
    from moex_data.step9_rub_analysis_bundle import _load_position_risk
    snapshot, _ = base.read_current_snapshot(now_fn=lambda: as_of)
    data = (snapshot.get("components", {}).get("stage9_"+scope, {}).get("data") or {})
    if data.get("schema_version") != SCHEMA:
        raise ValueError("selected Stage9 v2 snapshot missing; legacy v1 fallback forbidden")
    result = deepcopy(data)
    if position_risk_input is not None:
        result["position_risk"] = _load_position_risk(position_risk_input, as_of)
        result["readiness"]["position_risk"] = result["position_risk"]["status"]
        result["quality_gates"]["missing_position_risk_blocks_downstream_size_or_add_recommendation"] = False
    return result

