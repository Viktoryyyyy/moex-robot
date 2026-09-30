"""Freeze existing research evidence; never train, select stops or promote policy."""
from pathlib import Path

from .usdrubf_forecast_journal import ForecastJournalError, decode, digest, encode, fields, read_bytes, runtime_identity, text, timestamp


def register_experiment(journal, identifier, manifest):
    fields(manifest, {"schema_version", "hypothesis", "method_version", "rules", "horizons",
        "baseline", "metrics", "cost_model", "exclusions", "samples", "prior_experiments"})
    if manifest["schema_version"] != "usdrubf.experiment.v1":
        raise ForecastJournalError("unsupported experiment manifest")
    for key in ("hypothesis", "method_version", "baseline", "cost_model"):
        text(manifest[key])
    for key in ("rules", "horizons", "metrics", "exclusions", "samples", "prior_experiments"):
        if not isinstance(manifest[key], list) or len(manifest[key]) > 1000:
            raise ForecastJournalError("bounded experiment lists required")
    if not all(manifest[key] for key in ("rules", "horizons", "metrics", "samples")):
        raise ForecastJournalError("experiment definition incomplete")
    now = journal._now()
    intervals = []
    for sample in manifest["samples"]:
        fields(sample, {"id", "start", "end", "role", "previously_viewed"})
        text(sample["id"])
        a, b = timestamp(sample["start"]), timestamp(sample["end"])
        if a >= b or sample["role"] not in {"DEVELOPMENT", "OOS"} or type(sample["previously_viewed"]) is not bool:
            raise ForecastJournalError("invalid sample interval/role")
        if sample["role"] == "OOS" and (sample["previously_viewed"] or a <= now):
            raise ForecastJournalError("viewed or historical data cannot become fresh OOS")
        intervals.append((a, b, sample))
    all_samples = list(intervals)
    for ref in manifest["prior_experiments"]:
        prior = journal.read(ref)
        if prior["kind"] != "experiment":
            raise ForecastJournalError("prior experiment reference required")
        all_samples += [(timestamp(s["start"]), timestamp(s["end"]), s) for s in prior["payload"]["manifest"]["samples"]]
    overlap = []
    for index, (a, b, sample) in enumerate(intervals):
        for c, d, other in all_samples[index + 1:]:
            if max(a, c) < min(b, d):
                overlap.append([sample["id"], other["id"]])
                if "OOS" in {sample["role"], other["role"]}:
                    raise ForecastJournalError("OOS sample overlap")
    return journal._put("experiment", identifier, {"manifest": manifest,
        "manifest_sha256": journal._object(encode(manifest)), "overlaps": overlap,
        "coverage": "declared_samples_and_linked_prior_experiments_only", "runtime": runtime_identity()}, journal._finished(now))


def capture_research(journal, identifier, request):
    fields(request, {"schema_version", "kind", "directory", "inputs", "reproduction"})
    if request["schema_version"] != "usdrubf.research_evidence_request.v1":
        raise ForecastJournalError("unsupported research evidence request")
    now = journal._now()
    kind = request["kind"]
    if kind == "PHASE07":
        from ..runners.usdrubf_oil_fx_rub_v1_phase07_risk_model import DECLARED_OUTPUTS
        required = DECLARED_OUTPUTS
    elif kind == "S7.2":
        from ..runners.usdrubf_s7_2_empirical_stability_analysis import DECLARED_OUTPUTS
        required = DECLARED_OUTPUTS
    elif kind in {"PHASE06", "PHASE06A"}:
        from importlib import import_module
        suffix = "phase06_backtest" if kind == "PHASE06" else "phase06a_robustness_sensitivity"
        required = import_module("moex_research.runners.usdrubf_oil_fx_rub_v1_" + suffix).DECLARED_OUTPUTS
    else:
        raise ForecastJournalError("unsupported research family")
    directory = Path(request["directory"])
    frozen = {}
    missing = []
    documents = {}
    for name in required:
        path = directory / name
        if not path.is_file():
            missing.append(str(path))
            continue
        raw = read_bytes(path)
        frozen[name] = journal._object(raw)
        if path.suffix == ".json":
            documents[name] = decode(raw)
    if not isinstance(request["inputs"], dict) or len(request["inputs"]) > 100:
        raise ForecastJournalError("bounded exact research inputs required")
    inputs = {}
    manifest = documents.get("research_manifest.json", documents.get("run_metadata.json", {}))
    expected = manifest.get("input_sha256", {})
    identity_limitations = []
    if manifest and manifest.get("project") != "MOEX_Bot":
        raise ForecastJournalError("research project mismatch")
    if kind == "S7.2" and manifest:
        from ..runners.usdrubf_s7_2_empirical_stability_analysis import EXPERIMENT_ID, MODE
        if manifest.get("experiment_id") != EXPERIMENT_ID or manifest.get("mode") != MODE:
            raise ForecastJournalError("S7.2 identity mismatch")
        if (manifest.get("majority_reference_post_hoc_only") is not True
                or manifest.get("historical_sparse_min_constituents") != 1
                or manifest.get("historical_complete_only_min_constituents") != 3
                or type(manifest.get("min_group_sample")) is not int or manifest["min_group_sample"] <= 0):
            raise ForecastJournalError("S7.2 stability policy mismatch")
        provenance = manifest.get("source_provenance", {})
        if provenance.get("source_mode") == "explicit_csv":
            expected = {"source_dataset": provenance.get("source_dataset_sha256")}
        elif provenance.get("source_mode") == "phase3_panel_manifest":
            expected = {"panel_manifest": provenance.get("panel_manifest_sha256")}
            identity_limitations.append("S7.2_original_partition_byte_hashes_not_recorded_by_legacy_runner")
        else:
            identity_limitations.append("S7.2_source_provenance_unavailable")
    for key, raw_path in request["inputs"].items():
        raw = read_bytes(Path(raw_path))
        observed = digest(raw)
        if key in expected and expected[key] != observed:
            raise ForecastJournalError("research input hash mismatch: " + key)
        inputs[key] = journal._object(raw)
    missing_inputs = sorted(set(expected) - set(inputs))
    if kind == "PHASE07" and not missing:
        from ..runners.usdrubf_oil_fx_rub_v1_phase07_risk_model import _validate_contract
        contract = decode(read_bytes(Path(__file__).resolve().parents[3] / "contracts/experiments/usdrubf_oil_fx_rub_v1_phase07_risk_model.json"))
        _validate_contract(contract)
        expected_inputs = {key.removesuffix("_sha256"): value for key, value in contract["inputs"].items() if key.endswith("_sha256")}
        if expected != expected_inputs or manifest.get("independent_trade_count") != 3:
            raise ForecastJournalError("Phase07 frozen identity/sample mismatch")
        if (manifest.get("excursion_row_count"), manifest.get("stop_metric_variant_count"), manifest.get("sizing_variant_count")) != (9, 15, 45):
            raise ForecastJournalError("Phase07 full sensitivity grid missing")
        for flag in ("parameter_optimization_performed", "best_stop_selection_performed", "strategy_promotion_performed", "trading_action_performed"):
            if manifest.get(flag) is not False:
                raise ForecastJournalError("research authority violation")
    reproduction = request["reproduction"]
    if reproduction is not None:
        fields(reproduction, {"directory", "artifact_names"})
        names = reproduction["artifact_names"]
        if not isinstance(names, list) or not names or any(name not in frozen for name in names):
            raise ForecastJournalError("explicit comparable research artifacts required")
        comparisons = {}
        for name in names:
            raw = read_bytes(Path(reproduction["directory"]) / name)
            comparisons[name] = {"object_sha256": journal._object(raw), "exact_match": digest(raw) == frozen[name]}
    else:
        comparisons = None
    payload = {"kind": kind, "status": "BLOCKED_MISSING_EVIDENCE" if missing or missing_inputs or identity_limitations else "ARTIFACTS_CAPTURED",
        "missing_artifacts": missing, "missing_inputs": missing_inputs, "artifacts": frozen,
        "inputs": inputs, "documents": documents, "reproduction": comparisons,
        "runtime": runtime_identity(), "limitations": identity_limitations + ["historical_descriptive_not_new_OOS",
            "artifact_capture_is_not_research_acceptance", "reproduction_covers_only_explicit_compared_artifacts",
            "majority_class_reference_is_post_hoc_not_deployable"], "trading_authority": False}
    return journal._put("research", identifier, payload, journal._finished(now))
