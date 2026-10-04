"""Downstream forecast workflow over the existing canonical factual consumer."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import re

from ..intelligence.usdrubf_forecast_journal import (
    ForecastJournal, ForecastJournalError, decode, digest, encode, fields,
    read_bytes, runtime_identity, text, timestamp,
)
from ..intelligence.usdrubf_forecast_evaluation import FORECAST_V2, FORECAST_V3, price


def pointer(document, path):
    text(path)
    if not path.startswith("/"):
        raise ForecastJournalError("absolute JSON pointer required")
    item = document
    try:
        for part in path[1:].split("/"):
            key = part.replace("~1", "/").replace("~0", "~")
            if key.replace("~", "~0").replace("/", "~1") != part:
                raise ForecastJournalError("invalid JSON pointer escape")
            item = item[int(key)] if isinstance(item, list) and key.isdigit() and str(int(key)) == key else item[key]
        return item
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise ForecastJournalError("missing JSON pointer: " + path) from exc


def canonical(raw: bytes, now: datetime) -> tuple[dict, dict]:
    from moex_data.rub_snapshot_serialization import expand
    from .usdrubf_chat_snapshot_consumer import validate_analysis_chat_snapshot
    document = expand(decode(raw))
    if not isinstance(document, dict):
        raise ForecastJournalError("canonical object required")
    schema = document.get("schema_version")
    limitations = ["source_level_PIT_not_proven", "download_time_not_publication_time"]
    if schema == "rub_chat_analysis_snapshot.v1":
        # Stored primary snapshots can predate reader enrichment. Apply the same
        # canonical freshness decorator, then validate the existing consumer.
        from moex_data.rub_snapshot_read_freshness import apply_read_freshness
        generated = timestamp(document["identity"]["generated_at_utc"])
        view = apply_read_freshness(document, now=now)
        threshold = document["refresh_policy"]["snapshot_stale_after_seconds"]
        if type(threshold) is not int or threshold < 0:
            raise ForecastJournalError("invalid canonical stale policy")
        age = (now - generated).total_seconds()
        view["read_freshness"] = {"read_at_utc": now.isoformat(),
            "snapshot_age_seconds": age, "status": "STALE" if age > threshold else "FRESH"}
        validate_analysis_chat_snapshot(view)
        status = view["readiness"]["status"]
        revision = "UNKNOWN_PRODUCER_REVISION"
        available = timestamp(document.get("read_freshness", {}).get("read_at_utc", generated.isoformat()))
        if view["read_freshness"]["status"] == "STALE":
            limitations.append("snapshot_stale_at_consumption")
    elif schema == "rub_factual_package.v1":
        if document.get("project") != "MOEX_Bot":
            raise ForecastJournalError("canonical project mismatch")
        revision = document.get("code_revision")
        if not isinstance(revision, str) or not re.fullmatch("[0-9a-f]{40}", revision):
            raise ForecastJournalError("canonical producer revision missing")
        authority = fields(document.get("authority"), {"model_ready", "forecast_generated",
            "training_authorized", "directional_authority", "action_authority", "broker_execution"})
        if any(value is not False for value in authority.values()):
            raise ForecastJournalError("canonical authority mismatch")
        generated = timestamp(document["generations"]["slow_snapshot_generated_at_utc"])
        available = timestamp(document["as_of_utc"])
        status = document["status"]
        if not isinstance(document.get("facts"), list) or not isinstance(document.get("market_usability"), dict):
            raise ForecastJournalError("canonical facts/usability missing")
        if any(not isinstance(fact, dict) for fact in document["facts"]):
            raise ForecastJournalError("canonical facts must contain objects")
        if document.get("presentation_integrity") != {"status": "VALIDATED_PROJECTION", "factual_only": True}:
            raise ForecastJournalError("canonical projection identity mismatch")
        # No global FRESH claim: each candidate baseline is checked separately.
        limitations.append("imported_projection_source_evidence_not_reaudited")
    else:
        raise ForecastJournalError("unsupported canonical schema")
    if status not in {"READY", "PARTIAL", "INCOMPLETE"} or not generated <= available <= now:
        raise ForecastJournalError("canonical status/timestamp mismatch")
    if status != "READY":
        limitations.append("canonical_" + status)
    return document, {"source_ref": "canonical:" + schema, "schema_version": schema,
        "code_revision": revision, "data_as_of": generated.isoformat(),
        "available_at": available.isoformat(), "received_at": now.isoformat(),
        "quality_limitations": limitations}


def capture_canonical(journal, identifier, raw):
    now = journal._now()
    document, source = canonical(raw, now)
    path = journal._path("input", identifier)
    if path.exists():
        data = read_bytes(path)
        ref = {"kind": "input", "id": identifier, "sha256": digest(data)}
        old = journal.read(ref)["payload"]
        if journal.object_bytes(old["object_sha256"]) != raw:
            raise ForecastJournalError("record ID already contains different content")
        journal.object_bytes(old["logical_sha256"])
        return journal.acknowledge(ref)
    transport = journal._object(raw)
    logical = journal._object(encode(document))
    return journal._put("input", identifier, {"source": source,
        "object_sha256": transport, "logical_sha256": logical,
        "logical_length": len(encode(document)), "schema_validation": "CANONICAL_METADATA_ONLY",
        "consumer_runtime": runtime_identity()}, journal._finished(now))


def capture_current(journal, identifier, *, reader=None):
    from .usdrubf_chat_snapshot_consumer import load_analysis_chat_snapshot
    path = journal._path("input", identifier)
    if path.exists():
        ref = {"kind": "input", "id": identifier, "sha256": digest(read_bytes(path))}
        existing = journal.verify_input(ref)
        if existing["payload"].get("schema_validation") != "CANONICAL_READER_OUTPUT":
            raise ForecastJournalError("record ID belongs to another capture mode")
        return journal.acknowledge(ref)
    now = journal._now()
    snapshot = (reader or load_analysis_chat_snapshot)(now_fn=lambda: now)
    # Existing canonical consumer performs actual read/admission. Freeze this
    # reader output, including original evidence, without rebuilding its history.
    raw = encode(snapshot)
    document, source = canonical(raw, now)
    return journal._put("input", identifier, {"source": source,
        "object_sha256": journal._object(raw), "logical_sha256": journal._object(encode(document)),
        "logical_length": len(encode(document)), "schema_validation": "CANONICAL_READER_OUTPUT",
        "consumer_runtime": runtime_identity()}, journal._finished(now))


def validate_baseline(journal, spec):
    binding = spec["context"]["baseline"]
    record = journal.read(binding["input"])["payload"]
    if binding["status"] == "CANONICAL_FIELD" and ("logical_sha256" not in record
            or record.get("schema_validation") != "CANONICAL_READER_OUTPUT"):
        raise ForecastJournalError("baseline canonical provenance missing")
    document = decode(journal.object_bytes(record.get("logical_sha256", record["object_sha256"])))
    value = pointer(document, binding["pointer"])
    if price(str(value)) != price(spec["reference_price"]) or timestamp(pointer(document, binding["timestamp_pointer"])) != timestamp(spec["reference_price_at"]):
        raise ForecastJournalError("reference price differs from frozen observation")
    if "logical_sha256" in record:
        expected = baseline(journal, binding["input"], timestamp(spec["issued_at"]))
        if expected != {"reference_price": spec["reference_price"], "reference_price_at": spec["reference_price_at"], "binding": binding}:
            raise ForecastJournalError("canonical baseline identity mismatch")


def dated_projection_baseline(document):
    """Bind the exported dated row; its acceptance digest is not source replay."""
    prefix = "/dated_context/observations/market:usdrubf"
    try:
        node = pointer(document, prefix)
    except ForecastJournalError as exc:
        raise ForecastJournalError("canonical USDRUBF price unavailable; dated row missing") from exc
    if (not isinstance(node, dict)
            or node.get("scope") != "LAST_ACCEPTED_DATED_PREPARATION_ONLY"
            or node.get("current_usable") is not False
            or not isinstance(node.get("acceptance_evidence_id"), str)
            or not re.fullmatch("[0-9a-f]{64}", node["acceptance_evidence_id"])):
        raise ForecastJournalError("dated baseline acceptance metadata missing")
    identity = pointer(node, "/source_identity")
    values = pointer(node, "/values")
    if (not isinstance(identity, dict) or not isinstance(values, dict)
            or identity.get("logical_id") != "usdrubf" or identity.get("asset_type") != "future"):
        raise ForecastJournalError("dated baseline identity missing")
    observed = timestamp(identity.get("timestamp"))
    received = timestamp(identity.get("received_at_utc"))
    accepted = timestamp(node.get("accepted_at_utc"))
    checked = timestamp(node.get("checked_at_utc"))
    if not (observed <= received <= timestamp(node.get("source_generation_at_utc"))
            <= accepted <= checked <= timestamp(document["as_of_utc"])):
        raise ForecastJournalError("dated baseline future/inconsistent acceptance timestamps")
    if (timestamp(pointer(node, "/source_times/source_observation_at_utc")) != observed
            or timestamp(pointer(node, "/source_times/received_at_utc")) != received):
        raise ForecastJournalError("dated baseline source time mismatch")
    metadata = pointer(node, "/contract_metadata")
    if (not isinstance(metadata, dict)
            or metadata.get("scope") != "exact_source_contract_metadata_independent_of_live_price"
            or metadata.get("secid") != "USDRUBF"
            or timestamp(metadata.get("applicable_source_timestamp_utc")) != observed
            or timestamp(metadata.get("received_at_utc")) != received
            or not received <= timestamp(metadata.get("checked_at_utc")) <= checked
            or pointer(metadata, "/values/normalized_unit") != "RUB_per_USD"
            or pointer(metadata, "/values/raw_unit") != "RUB_per_USD"
            or str(pointer(metadata, "/values/normalization_divisor")) not in {"1", "1.0"}):
        raise ForecastJournalError("dated baseline contract/unit metadata mismatch")
    return identity, values, prefix + "/values/last", prefix + "/source_identity/timestamp"


def baseline(journal, ref, now):
    payload = journal.read(ref)["payload"]
    document = decode(journal.object_bytes(payload["logical_sha256"]))
    schema = document["schema_version"]
    if schema == "rub_chat_analysis_snapshot.v1":
        # Registration binds the dated observation admitted in the frozen input.
        # Reapplying live-reader TTL here would impose an age limit on that input.
        node = document["components"]["synchronized_live_market_oi"]["data"]["instruments"]["usdrubf"]
        prefix = "/components/synchronized_live_market_oi/data/instruments/usdrubf"
        if node.get("price_oi_usable") is not True:
            raise ForecastJournalError("canonical USDRUBF price unavailable")
        value, at = node.get("last"), node.get("timestamp")
        field, timefield = prefix + "/last", prefix + "/timestamp"
        identity = node
        values = node
        available = timestamp(document.get("read_freshness", {}).get("read_at_utc", document["identity"]["generated_at_utc"]))
    else:
        candidates = [(i, fact) for i, fact in enumerate(document["facts"]) if fact.get("factor") == "usdrubf"]
        usable = document["market_usability"]["usdrubf"].get("price_oi_usable") is True
        if len(candidates) > 1 or (candidates and not usable) or (not candidates and usable):
            raise ForecastJournalError("canonical USDRUBF price unavailable/ambiguous")
        if candidates:
            index, fact = candidates[0]
            identity, values = fact["source_identity"], fact["values"]
            field, timefield = f"/facts/{index}/values/last", f"/facts/{index}/source_identity/timestamp"
        else:
            identity, values, field, timefield = dated_projection_baseline(document)
        value, at = values.get("last"), identity.get("timestamp")
        available = timestamp(document["as_of_utc"])
    from moex_data.synchronized_live_market_oi_context import FORTS_SOURCE_ID
    if identity.get("secid") != "USDRUBF" or identity.get("source_id") != FORTS_SOURCE_ID:
        raise ForecastJournalError("foreign canonical price identity")
    observed = timestamp(at)
    received = timestamp(identity.get("received_at_utc"))
    # Keep source chronology without expiring an already frozen observation.
    # Its original timestamp remains the forecast's reference_price_at.
    if not observed <= received <= available <= now:
        raise ForecastJournalError("canonical baseline future/inconsistent timestamps")
    # Unit values are preserved, not rescaled. A standalone imported projection
    # has no replay of original admission and must remain explicitly unverified.
    status = "CANONICAL_FIELD" if payload.get("schema_validation") == "CANONICAL_READER_OUTPUT" else "EXTERNAL_UNVERIFIED"
    accepted_units = {"RUB_PER_USD", "RUB/USD", "RUB per USD", "RUB_per_USD", "RUB per 1 USD", "RUB_per_1_USD"}
    for key in ("units", "price_unit", "quote_unit"):
        if values.get(key) is not None and values[key] not in accepted_units:
            raise ForecastJournalError("unsupported USDRUBF quote unit")
    # Source-native RFUD USDRUBF LAST uses the independently verified instrument
    # mapping (the contract file and source code are pinned in runtime_identity).
    mapping = decode(read_bytes(Path(__file__).resolve().parents[3] / "contracts/datasets/position_risk_scenarios.v1.json"))["supported_mapping"]
    if mapping["instrument"] != identity["secid"] or mapping["quote_unit"] != "RUB_PER_USD":
        raise ForecastJournalError("USDRUBF reference unit mapping unavailable")
    return {"reference_price": str(price(str(value))), "reference_price_at": at,
        "binding": {"input": ref, "pointer": field, "timestamp_pointer": timefield, "status": status}}


def register_forecast(journal, identifier, request, input_ref, *, defer_evaluation_reason=None):
    now = journal._now()
    request = deepcopy(request)
    # User supplies all hypotheses/grid/rules. Only technical provenance is filled.
    existing_path = journal._path("forecast", identifier)
    prior = None
    if existing_path.exists():
        prior = {"kind": "forecast", "id": identifier, "sha256": digest(read_bytes(existing_path))}
    issue_time = journal.read(prior)["payload"]["issued_at"] if prior else now.isoformat()
    request.setdefault("issued_at", issue_time)
    selected = baseline(journal, input_ref, timestamp(request["issued_at"]))
    request.update(schema_version=FORECAST_V2, instrument="USDRUBF", contract="USDRUBF",
        inputs=[input_ref] + request.pop("external_inputs", []),
        reference_price=selected["reference_price"], reference_price_at=selected["reference_price_at"])
    request["context"]["baseline"] = selected["binding"]
    if defer_evaluation_reason is not None:
        if "evaluation_policy" in request:
            raise ForecastJournalError("evaluation policy must be supplied by the explicit registration option")
        request["schema_version"] = FORECAST_V3
        request["evaluation_policy"] = {"mode": "DEFERRED", "reason": text(defer_evaluation_reason),
            "requires_new_forecast_revision": True}
    request.setdefault("supersedes", None)
    request.setdefault("revision_reason", None)
    return journal.register(identifier, request)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    commands = parser.add_subparsers(dest="action", required=True)
    sample = commands.add_parser("template")
    sample.add_argument("kind", choices=("forecast", "risk", "observation"))
    capture = commands.add_parser("capture")
    capture.add_argument("--id", required=True)
    source = capture.add_mutually_exclusive_group(required=True)
    source.add_argument("--source", type=Path)
    source.add_argument("--current", action="store_true")
    register = commands.add_parser("register")
    register.add_argument("--id", required=True)
    register.add_argument("--request", type=Path, required=True)
    register.add_argument("--input-ref", type=Path, required=True)
    register.add_argument("--defer-evaluation-reason",
        help="Explicitly register preserved analysis with automatic evaluation deferred until a new forecast revision")
    attach = commands.add_parser("risk")
    attach.add_argument("--id", required=True)
    attach.add_argument("--request", type=Path, required=True)
    attach.add_argument("--forecast-ref", type=Path, required=True)
    observe = commands.add_parser("observe")
    observe.add_argument("--request", type=Path, required=True)
    observe.add_argument("--forecast-ref", type=Path)
    report = commands.add_parser("report")
    reports = report.add_mutually_exclusive_group(required=True)
    reports.add_argument("--refs", type=Path)
    reports.add_argument("--run-result", type=Path)
    replay = commands.add_parser("reproduce")
    replay.add_argument("--evaluation-ref", type=Path, required=True)
    for name in ("research", "experiment"):
        command = commands.add_parser(name)
        command.add_argument("--id", required=True)
        command.add_argument("--request", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "template":
            print(encode(template(args.kind)).decode())
            return 0
        journal = ForecastJournal(args.root)
        load = lambda path: decode(read_bytes(path))
        if args.action == "capture":
            if args.current:
                result = capture_current(journal, args.id)
            else:
                raw = read_bytes(args.source)
                result = capture_canonical(journal, args.id, raw)
        elif args.action == "register":
            result = register_forecast(journal, args.id, load(args.request), load(args.input_ref),
                defer_evaluation_reason=args.defer_evaluation_reason)
        elif args.action == "risk":
            result = attach_risk(journal, args.id, load(args.forecast_ref), load(args.request))
        elif args.action == "observe":
            from ..runners.usdrubf_forecast_observation import run
            request = load(args.request)
            if args.forecast_ref:
                if request.get("forecasts"):
                    raise ForecastJournalError("use request forecasts or --forecast-ref, not both")
                request["forecasts"] = [load(args.forecast_ref)]
            result = run(journal, request)
        elif args.action == "report":
            from ..runners.usdrubf_forecast_observation import report
            if args.run_result:
                saved_run = load(args.run_result)
                result = report(journal, [item["observation"] for item in saved_run["items"] if "observation" in item])
                result["pending_or_unavailable"] = [item for item in saved_run["items"] if "observation" not in item]
            else:
                result = report(journal, load(args.refs))
        elif args.action in {"research", "experiment"}:
            from ..intelligence.usdrubf_forecast_research import capture_research, register_experiment
            function = capture_research if args.action == "research" else register_experiment
            result = function(journal, args.id, load(args.request))
        else:
            result = journal.reproduce(load(args.evaluation_ref))
        print(encode(result).decode())
        return 0
    except (ValueError, OSError, RuntimeError, KeyError, TypeError, AttributeError) as exc:
        parser.exit(2, "forecast cycle: " + str(exc) + "\n")


def template(kind):
    """Nulls are required owner inputs, never invented analysis or account data."""
    if kind == "forecast":
        return {"horizon_start": None, "horizon_end": None, "bias": None,
            "neutral_band_bps": None, "range": None, "method_version": None,
            "observation_grid": [], "scenarios": [], "external_inputs": [],
            "context": {"original_text": None, "interpretation": None, "external_context": [],
                "registration_class": "PROSPECTIVE_LOCAL", "horizon_label": None,
                "grid_provenance": {"source": None, "completeness_scope": None}}}
    if kind == "observation":
        return {"schema_version": "usdrubf.forecast_observation_request.v1", "limit": 1,
            "forecasts": [], "reader": {"mode": "accepted_current", "data_root": None}}
    return {"schema_version": "step8_forecast_risk_request.v1", "supersedes": None, "revision_reason": None,
        "position": {"schema_version": "step8_forecast_position.v1", "id": None, "version": 1,
            "source": {"mode": "manual", "reference": None}, "as_of": None, "received_at": None,
            "max_age_seconds": None, "explicit_empty": False, "stage8_supplied": None,
            "account": {"currency": "RUB", "free_funds_rub": None, "current_initial_margin_rub": None,
                "reserve_rub": None, "max_total_contracts": None, "max_loss_rub": None,
                "supplied_account_pnl_rub": None},
            "positions": [{"id": None, "instrument": "USDRUBF", "contracts": None, "mark_price": None, "tranches": []}]},
        "specification": {"instrument": "USDRUBF", "quote_unit": "RUB_PER_USD", "contract_size": "1000",
            "tick_size": "0.01", "tick_value_rub": "10", "settlement_currency": "RUB",
            "effective_from": None, "effective_until": None, "verified_at": None,
            "source_url": "https://www.moex.com/ru/derivatives/perpetual-futures/usdrubf"},
        "assumptions": {"gap_price": None, "commission_rub": None, "funding_roll_rub": None,
            "slippage_rub": None, "margin_per_contract_rub": None,
            "cost_scope": "total_for_each_scenario_including_all_assumed_tranches_and_exit",
            "margin_policy": "explicit_scenario_not_broker_guarantee",
            "gap_policy": "explicit_gap_price_no_stop_fill_guarantee"}}


def attach_risk(journal, identifier, forecast_ref, request):
    from moex_data.step8_position_risk_state import build_forecast_risk
    now = journal._now()
    forecast = journal.read(forecast_ref)
    if forecast["kind"] != "forecast":
        raise ForecastJournalError("forecast required")
    path = journal._path("risk", identifier)
    if path.exists():
        ref = {"kind": "risk", "id": identifier, "sha256": digest(read_bytes(path))}
        prior = journal.read(ref)["payload"]
        if prior["forecast"] != forecast_ref or encode(prior["request"]) != encode(request):
            raise ForecastJournalError("record ID already contains different content")
        journal.object_bytes(prior["request_sha256"])
        return journal.acknowledge(ref)
    result = build_forecast_risk(request, forecast["payload"], now=now)
    previous = request.get("supersedes")
    if previous is not None:
        old = journal.read(previous)
        if old["kind"] != "risk" or old["id"] == identifier or not request.get("revision_reason"):
            raise ForecastJournalError("risk revision requires previous risk and reason")
        prior_position = old["payload"]["request"]["position"]
        position = request["position"]
        if prior_position is not None and position is not None and encode(position) != encode(prior_position):
            if position["id"] != prior_position["id"] or position["version"] <= prior_position["version"]:
                raise ForecastJournalError("changed position requires higher version of same position identity")
    elif request.get("revision_reason") is not None:
        raise ForecastJournalError("risk revision reason without predecessor")
    position_ref = None
    if request["position"] is not None:
        position = request["position"]
        if previous is None and position["version"] != 1:
            raise ForecastJournalError("later position version requires prior risk reference")
        position_ref = journal._put("position", position["id"] + ".v" + str(position["version"]),
            position, journal._finished(now))
    return journal._put("risk", identifier, {"forecast": forecast_ref, "request": request,
        "position": position_ref,
        "request_sha256": journal._object(encode(request)), "result": result,
        "runtime": runtime_identity()}, journal._finished(now))


if __name__ == "__main__":
    from .usdrubf_forecast_cycle import main as entrypoint
    raise SystemExit(entrypoint())
