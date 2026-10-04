"""All fixtures are synthetic; no market or position acceptance is implied."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from moex_research.intelligence.usdrubf_forecast_journal import ForecastJournal, encode, decode, timestamp
from moex_research.consumers.usdrubf_forecast_cycle import capture_canonical, register_forecast, attach_risk
from moex_research.runners.usdrubf_forecast_observation import run, report
from moex_research.intelligence.usdrubf_forecast_research import register_experiment, capture_research
from moex_data.step8_position_risk_state import build_forecast_risk
from moex_data.synchronized_live_market_oi_context import FORTS_SOURCE_ID

BASE = datetime(2026, 9, 30, 9, tzinfo=timezone.utc)


def at(minutes):
    return (BASE + timedelta(minutes=minutes)).isoformat()


def package():
    return {"schema_version": "rub_factual_package.v1", "project": "MOEX_Bot",
        "as_of_utc": at(-1), "code_revision": "a" * 40, "status": "PARTIAL",
        "presentation_integrity": {"status": "VALIDATED_PROJECTION", "factual_only": True},
        "generations": {"slow_snapshot_generated_at_utc": at(-2)},
        "facts": [{"factor": "usdrubf", "scope": "current_source_row",
            "source_identity": {"secid": "USDRUBF", "source_id": FORTS_SOURCE_ID,
                "timestamp": at(-1), "received_at_utc": at(-1)}, "values": {"last": 100}}],
        "market_usability": {"usdrubf": {"price_oi_usable": True}},
        "authority": {k: False for k in ("model_ready", "forecast_generated", "training_authorized",
            "directional_authority", "action_authority", "broker_execution")}, "synthetic": True}


def request():
    return {"issued_at": at(0), "horizon_start": at(1), "horizon_end": at(16),
        "bias": "BULLISH_USD", "neutral_band_bps": "0", "range": {"lower": "90", "upper": "110"},
        "method_version": "synthetic-v1", "observation_grid": [[at(i), at(i+5)] for i in (1,6,11)],
        "scenarios": [{"id": "bull", "direction": "BULLISH_USD", "activation": None,
            "confirmation": None, "targets": ["103"], "invalidation": "97"}],
        "context": {"original_text": "Synthetic original analytical forecast", "interpretation": "Synthetic hypothesis",
            "external_context": [], "registration_class": "SYNTHETIC", "horizon_label": "DAY",
            "grid_provenance": {"source": "synthetic explicit grid", "completeness_scope": "synthetic intervals only"}}}


def dated_package():
    document = package()
    document["facts"] = []
    document["market_usability"]["usdrubf"]["price_oi_usable"] = False
    document["dated_context"] = {"observations": {"market:usdrubf": {
        "scope": "LAST_ACCEPTED_DATED_PREPARATION_ONLY", "current_usable": False,
        "acceptance_evidence_id": "b" * 64, "accepted_at_utc": at(-2998),
        "source_generation_at_utc": at(-2998), "checked_at_utc": at(-1),
        "source_identity": {"secid": "USDRUBF", "source_id": FORTS_SOURCE_ID,
            "logical_id": "usdrubf", "asset_type": "future", "timestamp": at(-3000),
            "received_at_utc": at(-2999)}, "values": {"last": 84.1},
        "source_times": {"source_observation_at_utc": at(-3000), "received_at_utc": at(-2999)},
        "contract_metadata": {"scope": "exact_source_contract_metadata_independent_of_live_price",
            "secid": "USDRUBF", "applicable_source_timestamp_utc": at(-3000),
            "received_at_utc": at(-2999), "checked_at_utc": at(-2998),
            "values": {"normalized_unit": "RUB_per_USD", "raw_unit": "RUB_per_USD",
                "normalization_divisor": 1.0}}}}}
    return document


def test_dated_projection_registration_preserves_bytes_and_never_claims_live(tmp_path):
    document = dated_package()
    raw = encode(document)
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    ref = capture_canonical(journal, "dated", raw)
    forecast = register_forecast(journal, "dated-forecast", request(), ref)
    spec = journal.read(forecast)["payload"]
    assert spec["reference_price"] == "84.1"
    assert spec["reference_price_at"] == at(-3000)
    assert spec["context"]["baseline"] == {"input": ref,
        "pointer": "/dated_context/observations/market:usdrubf/values/last",
        "timestamp_pointer": "/dated_context/observations/market:usdrubf/source_identity/timestamp",
        "status": "EXTERNAL_UNVERIFIED"}
    assert journal.object_bytes(journal.verify_input(ref)["payload"]["object_sha256"]) == raw
    assert document["market_usability"]["usdrubf"]["price_oi_usable"] is False
    assert register_forecast(journal, "dated-forecast", request(), ref) == forecast
    for field, value in [("reference_price", "84.2"), ("reference_price_at", at(-1))]:
        tampered = deepcopy(spec); tampered[field] = value
        with pytest.raises(ValueError): journal.register("tampered", tampered)


@pytest.mark.parametrize("path,value", [
    ("scope", "current_source_row"), ("current_usable", True),
    ("acceptance_evidence_id", None), ("acceptance_evidence_id", "unverified"),
    ("accepted_at_utc", at(1)), ("source_generation_at_utc", at(1)),
    ("checked_at_utc", at(1)), ("accepted_at_utc", at(-3001)),
    ("source_identity/secid", "SiU6"), ("source_identity/source_id", "foreign"),
    ("source_identity/logical_id", "si_front"), ("source_identity/asset_type", "spot"),
    ("source_identity/timestamp", at(1)), ("source_identity/received_at_utc", at(-3001)),
    ("source_times/source_observation_at_utc", at(-3001)), ("source_times/received_at_utc", at(-1)),
    ("values/last", None), ("values/last", 0),
    ("contract_metadata/scope", "unknown"), ("contract_metadata/secid", "SiU6"),
    ("contract_metadata/applicable_source_timestamp_utc", at(-1)),
    ("contract_metadata/received_at_utc", at(-1)), ("contract_metadata/checked_at_utc", at(1)),
    ("contract_metadata/values/normalized_unit", "points"),
    ("contract_metadata/values/raw_unit", "points"),
    ("contract_metadata/values/normalization_divisor", 1000),
    ("contract_metadata/values/normalization_divisor", True),
])
def test_dated_baseline_rejects_unbound_or_inconsistent_metadata(tmp_path, path, value):
    document = dated_package()
    target = document["dated_context"]["observations"]["market:usdrubf"]
    parts = path.split("/")
    for key in parts[:-1]: target = target[key]
    target[parts[-1]] = value
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    ref = capture_canonical(journal, "input", encode(document))
    with pytest.raises(ValueError): register_forecast(journal, "bad", request(), ref)
    assert not list((tmp_path / "records").glob("forecast.*"))


def test_dated_baseline_does_not_mask_conflicting_current_rows(tmp_path):
    document = dated_package()
    document["facts"] = package()["facts"]
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    ref = capture_canonical(journal, "inconsistent", encode(document))
    with pytest.raises(ValueError): register_forecast(journal, "bad", request(), ref)
    document["market_usability"]["usdrubf"]["price_oi_usable"] = True
    ref = capture_canonical(journal, "current", encode(document))
    forecast = register_forecast(journal, "current", request(), ref)
    assert journal.read(forecast)["payload"]["reference_price"] == "100"


def setup(tmp_path):
    clock = [BASE]
    journal = ForecastJournal(tmp_path / "journal", clock=lambda: clock[0])
    ref = capture_canonical(journal, "input", encode(package()))
    forecast = register_forecast(journal, "f1", request(), ref)
    return journal, ref, forecast, clock


def observation(ref):
    return {"schema_version": "usdrubf.forecast_observation_request.v1", "forecasts": [ref], "limit": 1,
        "reader": {"mode": "synthetic", "facts": {"schema_version": "usdrubf.forecast_facts.v1",
            "instrument": "USDRUBF", "contract": "USDRUBF", "bars": [
                {"open_at": at(i), "close_at": at(i+5), "open": "100", "high": "104", "low": "99", "close": "103"}
                for i in (1,6,11)]},
            "source": {"source_ref": "synthetic", "schema_version": "usdrubf.forecast_facts.v1", "code_revision": "synthetic",
                "data_as_of": at(16), "available_at": at(17), "received_at": at(17), "quality_limitations": []}}}


def risk_request():
    return {"schema_version": "step8_forecast_risk_request.v1", "supersedes": None, "revision_reason": None,
        "position": {"schema_version": "step8_forecast_position.v1", "id": "synthetic-position", "version": 1,
            "source": {"mode": "manual", "reference": "synthetic-fixture"}, "as_of": at(-1), "received_at": at(0),
            "max_age_seconds": 1200, "explicit_empty": False, "stage8_supplied": None,
            "account": {"currency": "RUB", "free_funds_rub": "10000", "current_initial_margin_rub": "2000",
                "reserve_rub": "1000", "max_total_contracts": 2, "max_loss_rub": "5000", "supplied_account_pnl_rub": None},
            "positions": [{"id": "p1", "instrument": "USDRUBF", "contracts": 2, "mark_price": "100",
                "tranches": [{"id": "reduce", "contracts_delta": -1, "assumed_fill_price": "101"}]}]},
        "specification": {"instrument": "USDRUBF", "quote_unit": "RUB_PER_USD", "contract_size": "1000",
            "tick_size": "0.01", "tick_value_rub": "10", "settlement_currency": "RUB", "effective_from": at(-60),
            "effective_until": at(100), "verified_at": at(-60),
            "source_url": "https://www.moex.com/ru/derivatives/perpetual-futures/usdrubf"},
        "assumptions": {"gap_price": "90", "commission_rub": "10", "funding_roll_rub": None,
            "slippage_rub": "20", "margin_per_contract_rub": "1000",
            "cost_scope": "total_for_each_scenario_including_all_assumed_tranches_and_exit",
            "margin_policy": "explicit_scenario_not_broker_guarantee",
            "gap_policy": "explicit_gap_price_no_stop_fill_guarantee"}}


def test_canonical_hashes_baseline_original_text_and_idempotence(tmp_path):
    journal, input_ref, forecast, clock = setup(tmp_path)
    payload = journal.read(input_ref)["payload"]
    assert journal.object_bytes(payload["object_sha256"]) == encode(package())
    assert journal.read(forecast)["payload"]["context"]["original_text"] == request()["context"]["original_text"]
    assert journal.read(forecast)["payload"]["reference_price"] == "100"
    clock[0] += timedelta(minutes=10)
    assert capture_canonical(journal, "input", encode(package())) == input_ref
    altered = package(); altered["facts"][0]["values"]["last"] = 101
    with pytest.raises(ValueError):
        capture_canonical(journal, "input", encode(altered))


@pytest.mark.parametrize("change", ["project", "authority", "future", "revision"])
def test_canonical_metadata_rejection(tmp_path, change):
    document = package()
    if change == "project": document["project"] = "other"
    if change == "authority": document["authority"]["broker_execution"] = True
    if change == "future": document["as_of_utc"] = at(1)
    if change == "revision": document["code_revision"] = "unknown"
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    with pytest.raises(ValueError): capture_canonical(journal, "bad", encode(document))
    assert not list((tmp_path / "records").iterdir())


@pytest.mark.parametrize("change", ["secid", "source_id", "timestamp", "received_at_utc", "price"])
def test_baseline_binding_rejects_foreign_future_and_tampered_price(tmp_path, change):
    journal, _, forecast, _ = setup(tmp_path)
    if change == "price":
        spec = deepcopy(journal.read(forecast)["payload"]); spec["reference_price"] = "101"
        with pytest.raises(ValueError): journal.register("bad", spec)
        return
    document = package()
    document["facts"][0]["source_identity"][change] = {"secid": "SiU6", "source_id": "foreign",
        "timestamp": at(1), "received_at_utc": at(10)}[change]
    ref = capture_canonical(journal, "bad-input", encode(document))
    with pytest.raises(ValueError): register_forecast(journal, "bad", request(), ref)


def test_reference_carrier_preserves_transport_and_expanded_hash(tmp_path):
    from moex_data.rub_snapshot_serialization import delivery
    value = package()
    value["large_history"] = [{"facts": "synthetic" * 500}] * 100
    carrier = delivery(value)
    assert carrier["schema_version"] == "rub_snapshot_references.v1"
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    ref = capture_canonical(journal, "carrier", encode(carrier))
    saved = journal.read(ref)["payload"]
    assert journal.object_bytes(saved["object_sha256"]) == encode(carrier)
    assert journal.object_bytes(saved["logical_sha256"]) == encode(value)
    assert saved["logical_sha256"] == carrier["expanded_sha256"]
    carrier["expanded_bytes"] += 1
    with pytest.raises(ValueError): capture_canonical(journal, "corrupt", encode(carrier))


def test_observation_restart_frozen_facts_report_and_missing_position(tmp_path):
    journal, _, forecast, clock = setup(tmp_path)
    req = observation(forecast)
    assert run(journal, req)["items"][0]["status"] == "PENDING"
    clock[0] = BASE + timedelta(minutes=20)
    first = run(journal, req)
    assert first["items"][0]["status"] == "COMPLETED"
    req["reader"] = {"mode": "not-a-reader"}
    assert run(journal, req) == first
    refs = [first["items"][0]["observation"]]
    summary = report(journal, refs)
    assert summary["groups"][0]["eligible_count"] == 1
    assert summary["groups"][0]["registration_class"] == "SYNTHETIC"
    assert summary["paper_pnl"] is None
    with pytest.raises(ValueError): report(journal, refs * 2)
    request_risk = risk_request(); request_risk["position"] = None
    risk = attach_risk(journal, "risk", forecast, request_risk)
    assert journal.read(risk)["payload"]["result"]["status"] == "POSITION_ABSENT"


@pytest.mark.parametrize("failure", ["middle", "terminal", "foreign", "duplicate", "outside"])
def test_factual_missing_and_invalid_bars(tmp_path, failure):
    journal, _, forecast, clock = setup(tmp_path)
    clock[0] = BASE + timedelta(minutes=20)
    req = observation(forecast); facts = req["reader"]["facts"]
    if failure == "middle": facts["bars"].pop(1)
    if failure == "terminal": facts["bars"].pop()
    if failure == "foreign": facts["instrument"] = "SiU6"
    if failure == "duplicate": facts["bars"].append(facts["bars"][0])
    if failure == "outside": facts["bars"][0]["open_at"] = at(-4)
    if failure in {"middle", "terminal"}:
        result = run(journal, req)
        assert result["items"][0]["status"] == "NOT_EVALUABLE"
    else:
        with pytest.raises(ValueError): run(journal, req)
        assert not list((journal.root / "records").glob("evaluation.*"))


def test_late_import_cannot_be_forward_and_synthetic_cannot_score_real(tmp_path):
    journal, ref, _, clock = setup(tmp_path)
    clock[0] += timedelta(minutes=20)
    req = request(); req["context"]["registration_class"] = "PROSPECTIVE_LOCAL"
    forecast = register_forecast(journal, "late", req, ref)
    with pytest.raises(ValueError, match="synthetic"):
        run(journal, observation(forecast))


def test_risk_decimal_long_reduction_unknown_cost_and_limits():
    req = risk_request()
    result = build_forecast_risk(req, request(), now=BASE)
    assert result["status"] == "PARTIAL"
    assert result["planned_conservative_gross_contracts"] == 3
    assert result["planned_conservative_contract_limit_breach"] is True
    gap = [r for r in result["money_risk"] if r["scenario"] == "gap"]
    assert [r["gross_pnl_change_rub"] for r in gap] == ["-20000", "-9000"]
    assert all(r["net_pnl_change_rub"] is None for r in gap)
    req["assumptions"]["funding_roll_rub"] = "-0.10"
    result = build_forecast_risk(req, request(), now=BASE)
    gap = result["money_risk"][0]
    assert gap["net_pnl_change_rub"] == "-20029.90"
    assert gap["loss_limit_breach"] is True
    assert gap["reserve_breach"] is True


@pytest.mark.parametrize("variant,status", [("absent","POSITION_ABSENT"),("stale","POSITION_STALE"),
    ("empty","EXPLICIT_EMPTY"),("foreign","UNSUPPORTED_INSTRUMENT_OR_UNIT_MAPPING"),("spec","SPECIFICATION_MISSING")])
def test_position_states(variant, status):
    req = risk_request()
    if variant == "absent": req["position"] = None
    if variant == "stale": req["position"]["as_of"] = at(-60)
    if variant == "empty": req["position"].update(positions=[], explicit_empty=True)
    if variant == "foreign": req["position"]["positions"][0]["instrument"] = "SiU6"
    if variant == "spec": req["specification"] = None
    assert build_forecast_risk(req, request(), now=BASE)["status"] == status


def test_short_and_ambient_decimal_context():
    from decimal import localcontext, Inexact
    req = risk_request(); req["position"]["positions"][0].update(contracts=-2, tranches=[])
    req["assumptions"]["funding_roll_rub"] = "-123.456"
    expected = build_forecast_risk(req, request(), now=BASE)
    assert expected["money_risk"][0]["gross_pnl_change_rub"] == "20000"
    with localcontext() as context:
        context.prec = 3; context.traps[Inexact] = True
        assert build_forecast_risk(req, request(), now=BASE) == expected


@pytest.mark.parametrize("target_count", [1, 10])
def test_risk_rejects_aggregate_compute_and_output_budgets(target_count):
    spec = request()
    spec["scenarios"][0]["targets"] = [str(103 + i) for i in range(target_count)]
    risk = risk_request()
    risk["position"]["positions"][0]["tranches"] = [
        {"id": str(i), "contracts_delta": 1, "assumed_fill_price": "100"} for i in range(1000)]
    with pytest.raises(ValueError, match="aggregate scenario risk resource bound"):
        build_forecast_risk(risk, spec, now=BASE)


def test_risk_version_retains_original(tmp_path):
    journal, _, forecast, _ = setup(tmp_path)
    req = risk_request()
    first = attach_risk(journal, "risk1", forecast, req)
    req = deepcopy(req); req.update(supersedes=first, revision_reason="explicit revised assumption")
    req["assumptions"]["funding_roll_rub"] = "20"
    second = attach_risk(journal, "risk2", forecast, req)
    assert journal.read(second)["payload"]["request"]["supersedes"] == first
    assert journal.read(first)["payload"]["result"]["status"] == "PARTIAL"


def test_real_subprocess_capture_register_observe_replay_and_error(tmp_path):
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(root), str(root / "src"))))
    prefix = [sys.executable, "-m", "moex_research.consumers.usdrubf_forecast_cycle", "--root", str(tmp_path / "journal")]
    def execute(*args):
        return subprocess.run(prefix + list(args), env=env, cwd=root, capture_output=True, text=True)
    source = tmp_path / "source.json"; source.write_bytes(encode(package()))
    captured = execute("capture", "--id", "input", "--source", str(source))
    assert captured.returncode == 0, captured.stderr
    input_file = tmp_path / "input.json"; input_file.write_text(captured.stdout)
    spec = request(); spec["context"]["registration_class"] = "SYNTHETIC"
    spec_file = tmp_path / "spec.json"; spec_file.write_bytes(encode(spec))
    registered = execute("register", "--id", "f", "--request", str(spec_file), "--input-ref", str(input_file))
    assert registered.returncode == 0, registered.stderr
    forecast = json.loads(registered.stdout)
    run_file = tmp_path / "run.json"; run_file.write_bytes(encode(observation(forecast)))
    observed = execute("observe", "--request", str(run_file))
    assert observed.returncode == 0, observed.stderr
    state_ref = json.loads(observed.stdout)["items"][0]["observation"]
    state = ForecastJournal(tmp_path / "journal").read(state_ref)
    eval_file = tmp_path / "eval.json"; eval_file.write_bytes(encode(state["payload"]["evaluation"]))
    replay = execute("reproduce", "--evaluation-ref", str(eval_file))
    assert replay.returncode == 0, replay.stderr
    spec_file.write_text('{"bad":true}')
    bad = execute("register", "--id", "bad", "--request", str(spec_file), "--input-ref", str(input_file))
    assert bad.returncode == 2 and "Traceback" not in bad.stderr
    assert not (tmp_path / "journal/records/forecast.bad.json").exists()


def test_missing_research_is_blocked_not_fabricated(tmp_path):
    journal = ForecastJournal(tmp_path / "j")
    ref = capture_research(journal, "missing", {"schema_version": "usdrubf.research_evidence_request.v1",
        "kind": "PHASE07", "directory": str(tmp_path / "absent"), "inputs": {}, "reproduction": None})
    assert journal.read(ref)["payload"]["status"] == "BLOCKED_MISSING_EVIDENCE"


def test_experiment_rejects_historical_and_overlapping_oos(tmp_path):
    journal = ForecastJournal(tmp_path / "j", clock=lambda: BASE)
    manifest = {"schema_version": "usdrubf.experiment.v1", "hypothesis": "synthetic", "method_version": "synthetic-v1",
        "rules": ["fixed"], "horizons": ["5m"], "baseline": "explicit", "metrics": ["direction"],
        "cost_model": "not trading", "exclusions": [], "prior_experiments": [],
        "samples": [{"id": "oos", "start": at(-20), "end": at(20), "role": "OOS", "previously_viewed": False}]}
    with pytest.raises(ValueError): register_experiment(journal, "past", manifest)
    manifest["samples"][0]["start"] = at(1)
    ref = register_experiment(journal, "first", manifest)
    manifest["prior_experiments"] = [ref]
    with pytest.raises(ValueError, match="overlap"): register_experiment(journal, "overlap", manifest)


def test_imported_package_baseline_is_explicitly_unverified(tmp_path):
    journal, _, forecast, _ = setup(tmp_path)
    assert journal.read(forecast)["payload"]["context"]["baseline"]["status"] == "EXTERNAL_UNVERIFIED"


@pytest.mark.parametrize("mutation", ["unit", "asof"])
def test_inconsistent_package_price_metadata_refuses(tmp_path, mutation):
    value = package()
    if mutation == "unit": value["facts"][0]["values"]["price_unit"] = "USD_PER_RUB"
    else: value["as_of_utc"] = at(-2)
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    ref = capture_canonical(journal, "input", encode(value))
    with pytest.raises(ValueError): register_forecast(journal, "bad", request(), ref)


def test_register_automatic_issued_time_retry_and_risk_retry_after_stale(tmp_path):
    journal, ref, forecast, clock = setup(tmp_path)
    req = request(); req.pop("issued_at")
    first = register_forecast(journal, "auto", req, ref)
    risk = attach_risk(journal, "risk", forecast, risk_request())
    clock[0] += timedelta(days=1)
    assert register_forecast(journal, "auto", req, ref) == first
    assert attach_risk(journal, "risk", forecast, risk_request()) == risk


def test_canonical_retry_reestablishes_durability(tmp_path, monkeypatch):
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    real = ForecastJournal._sync_directory
    def fail(path):
        if path == tmp_path / "records": raise OSError("synthetic fsync failure")
        real(path)
    monkeypatch.setattr(ForecastJournal, "_sync_directory", staticmethod(fail))
    with pytest.raises(OSError): capture_canonical(journal, "input", encode(package()))
    calls = []
    def good(path):
        calls.append(path); real(path)
    monkeypatch.setattr(ForecastJournal, "_sync_directory", staticmethod(good))
    capture_canonical(journal, "input", encode(package()))
    assert tmp_path / "records" in calls and tmp_path / "objects" in calls


def test_assessment_revision_keeps_original_and_does_not_double_count(tmp_path):
    journal, _, forecast, clock = setup(tmp_path)
    clock[0] += timedelta(minutes=20)
    req = observation(forecast); req["reader"]["facts"]["bars"].pop(1)
    first = run(journal, req)["items"][0]["observation"]
    req = observation(forecast); req["revision"] = {"supersedes": first, "reason": "later verified missing source bar"}
    second = run(journal, req)["items"][0]["observation"]
    assert journal.read(first)["payload"]["status"] == "NOT_EVALUABLE"
    assert journal.read(second)["payload"]["status"] == "COMPLETED"
    assert run(journal, req)["items"][0]["observation"] == second
    newer = journal.read(journal.read(second)["payload"]["evaluation"])["payload"]
    assert newer["supersedes"] == journal.read(first)["payload"]["evaluation"]
    with pytest.raises(ValueError): report(journal, [first, second])


def test_logical_object_tampering_blocks_evaluation(tmp_path):
    from moex_data.rub_snapshot_serialization import delivery
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    value = package(); value["large_history"] = [{"data": "synthetic" * 500}] * 100
    ref = capture_canonical(journal, "input", encode(delivery(value)))
    forecast = register_forecast(journal, "f", request(), ref)
    payload = journal.read(ref)["payload"]
    path = tmp_path / "objects" / payload["logical_sha256"]
    path.chmod(0o644); path.write_bytes(b"{}")
    journal.clock = lambda: BASE + timedelta(minutes=20)
    with pytest.raises(ValueError, match="hash mismatch"): run(journal, observation(forecast))


@pytest.mark.parametrize("field,value", [("commission_rub", "0." + "0"*129 + "1"), ("contracts", 10**121)])
def test_money_domain_rejects_inexact_input(field, value):
    req = risk_request()
    if field == "contracts": req["position"]["positions"][0][field] = value
    else: req["assumptions"][field] = value
    with pytest.raises(ValueError): build_forecast_risk(req, request(), now=BASE)


def test_generic_input_cannot_claim_canonical_baseline(tmp_path):
    journal, _, forecast, _ = setup(tmp_path)
    raw = {"price": 100, "at": at(-1)}
    metadata = observation(forecast)["reader"]["source"]
    metadata.update(data_as_of=at(-1), available_at=at(-1), received_at=at(-1))
    ref = journal.capture("external", encode(raw), metadata)
    spec = deepcopy(journal.read(forecast)["payload"])
    spec["inputs"] = [ref]
    spec["context"]["baseline"] = {"input": ref, "pointer": "/price", "timestamp_pointer": "/at", "status": "CANONICAL_FIELD"}
    with pytest.raises(ValueError, match="provenance"): journal.register("forged", spec)


def snapshot():
    return {"schema_version": "rub_chat_analysis_snapshot.v1",
        "identity": {"project": "MOEX_Bot", "generated_at_utc": at(-1)},
        "refresh_policy": {"snapshot_stale_after_seconds": 1200}, "readiness": {"status": "PARTIAL"},
        "read_freshness": {"read_at_utc": at(0), "snapshot_age_seconds": 60, "status": "FRESH"},
        "authority": {"data_only": True, **{k: False for k in ("server_generates_market_analysis", "server_generates_scenario",
            "server_generates_buy_sell_out", "server_generates_invalidation", "ema_standalone_directional_authority",
            "news_directional_action_authority", "broker_execution", "telegram_delivery")}},
        "components": {"synchronized_live_market_oi": {"status": "PARTIAL", "data": {"instruments": {"usdrubf": {
            "secid": "USDRUBF", "source_id": FORTS_SOURCE_ID, "timestamp": at(-1), "received_at_utc": at(-1),
            "price_oi_usable": True, "stale": False, "last": 100}}}}}}


def test_current_reader_capture_and_storage_carrier(tmp_path):
    from moex_research.consumers.usdrubf_forecast_cycle import capture_current
    from moex_data.rub_snapshot_serialization import storage
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    current = capture_current(journal, "current", reader=lambda **kwargs: snapshot())
    ref = register_forecast(journal, "canonical", request(), current)
    assert journal.read(ref)["payload"]["context"]["baseline"]["status"] == "CANONICAL_FIELD"
    assert capture_current(journal, "current", reader=lambda **kwargs: pytest.fail("must not refetch retry")) == current
    value = snapshot(); value["synthetic_history"] = ["synthetic" * 1000] * 40
    carrier = storage(value)
    assert carrier["schema_version"] == "rub_snapshot_storage.v1"
    captured = capture_canonical(journal, "storage", encode(carrier))
    assert decode(journal.object_bytes(journal.read(captured)["payload"]["logical_sha256"])) == value


@pytest.mark.parametrize("kind", ["projection", "snapshot"])
@pytest.mark.parametrize("age_seconds", [1200, 1201, 3600, 3 * 24 * 3600])
def test_registration_accepts_dated_frozen_baseline_without_age_limit(tmp_path, kind, age_seconds):
    document = package() if kind == "projection" else snapshot()
    observed_at = (BASE - timedelta(seconds=age_seconds)).isoformat()
    if kind == "projection":
        identity = document["facts"][0]["source_identity"]
    else:
        identity = document["components"]["synchronized_live_market_oi"]["data"]["instruments"]["usdrubf"]
    identity.update(timestamp=observed_at, received_at_utc=observed_at)
    raw = encode(document)
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    captured = capture_canonical(journal, "dated-input", raw)
    forecast = register_forecast(journal, "dated-forecast", request(), captured)
    saved = journal.read(forecast)["payload"]
    assert saved["reference_price"] == "100"
    assert saved["reference_price_at"] == observed_at
    assert saved["issued_at"] == request()["issued_at"]
    assert saved["context"]["baseline"]["status"] == "EXTERNAL_UNVERIFIED"
    assert journal.object_bytes(journal.read(captured)["payload"]["object_sha256"]) == raw
    assert register_forecast(journal, "dated-forecast", request(), captured) == forecast


@pytest.mark.parametrize("kind", ["projection", "snapshot", "omitted_projection_price"])
def test_age_policy_does_not_restore_unusable_or_omitted_baseline(tmp_path, kind):
    document = snapshot() if kind == "snapshot" else package()
    if kind == "snapshot":
        document["components"]["synchronized_live_market_oi"]["data"]["instruments"]["usdrubf"]["price_oi_usable"] = False
    else:
        document["market_usability"]["usdrubf"]["price_oi_usable"] = False
        if kind == "omitted_projection_price":
            document["facts"] = []
    journal = ForecastJournal(tmp_path, clock=lambda: BASE)
    captured = capture_canonical(journal, "unavailable-input", encode(document))
    with pytest.raises(ValueError, match="price unavailable"):
        register_forecast(journal, "unavailable-forecast", request(), captured)
    assert not list((journal.root / "records").glob("forecast.*"))


@pytest.mark.parametrize("mutation", [None, "hash", "foreign", "rows", "anchor", "duplicate"])
def test_accepted_stage2_adapter_with_real_partition_validation(tmp_path, monkeypatch, mutation):
    import pandas as pd
    from types import SimpleNamespace
    from hashlib import sha256
    from moex_data.futures import freeze_step7_accepted_raw_5m as layer
    from moex_research.runners.usdrubf_forecast_observation import accepted_facts
    from moex_research.intelligence.usdrubf_forecast_evaluation import evaluate
    root = tmp_path / "data"; root.mkdir()
    records = []
    for minute in (5, 10, 15):
        records.append({"instrument_id": "usdrubf_futures_family", "source_id": layer.SOURCE_ID,
            "secid": "USDRUBF", "board": "RFUD", "market": "forts", "engine": "futures",
            "source": layer.stage2.quote_core.SOURCE_CANDIDATE_APIM_TRADESTATS,
            "trade_date": "2026-09-30", "session_date": "2026-09-30", "ts": pd.Timestamp(f"2026-09-30 12:{minute:02d}:00"),
            "ingest_ts": at(16), "open": 100.0, "high": 104.0, "low": 99.0, "close": 103.0,
            "volume": 10.0, "value": 1000.0, "num_trades": 2})
    if mutation == "foreign": records[0]["secid"] = "SiU6"
    if mutation == "duplicate": records.append(records[0])
    path = root / "part.parquet"; pd.DataFrame(records).to_parquet(path)
    anchor = root / "anchor.json"; anchor.write_bytes(b"{}")
    actual_hash = sha256(path.read_bytes()).hexdigest()
    scope = SimpleNamespace(records=[{"trade_date": "2026-09-30", "snapshot_path": str(path),
        "sha256": "0"*64 if mutation == "hash" else actual_hash, "row_count": 99 if mutation == "rows" else len(records)}],
        pointer_ref="${MOEX_DATA_ROOT}/anchor.json", manifest_ref="${MOEX_DATA_ROOT}/anchor.json",
        marker_sha256="0"*64 if mutation == "anchor" else sha256(b"{}").hexdigest(), manifest_sha256=sha256(b"{}").hexdigest(),
        acceptance_run_id="synthetic-attested-generation")
    # Admission resolver is an explicit synthetic fixture; real physical source
    # validation, byte capture, timezone adapter, anchors and scoring execute.
    monkeypatch.setattr(layer, "accepted_quote_history", lambda *args, **kwargs: scope)
    journal, _, forecast, clock = setup(tmp_path)
    spec = deepcopy(journal.read(forecast)["payload"])
    spec.update(horizon_start=at(0), horizon_end=at(15), observation_grid=[[at(i),at(i+5)] for i in (0,5,10)])
    reader = {"mode": "accepted_stage2", "data_root": str(root), "accepted_start_date": "2026-09-30", "accepted_end_date": "2026-09-30"}
    def read():
        raw, source, provenance = accepted_facts(journal, spec, reader, BASE + timedelta(minutes=20))
        result = evaluate(spec, decode(raw), evaluated_at=BASE+timedelta(minutes=20), source_as_of=timestamp(source["data_as_of"]), limitations=[])
        return raw, provenance, result
    if mutation:
        with pytest.raises(ValueError): read()
    else:
        raw, provenance, result = read()
        assert result["direction"]["correct"] is True
        assert decode(raw)["bars"][0]["close_at"] == at(5)
        assert journal.object_bytes(provenance["partitions"][0]["object_sha256"]) == path.read_bytes()
