#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.controlled_scope import CONTROLLED_SCOPE
from moex_data.futures.controlled_scope import DEFAULT_CONTROLLED_CONFIG
from moex_data.futures.controlled_scope import assert_raw_only_config
from moex_data.futures.controlled_scope import load_scope_config
from moex_data.futures.controlled_scope import select_controlled_instruments
from moex_data.futures.futoi_raw_loader import load_contract_values_extended
from moex_data.futures.futoi_raw_loader import load_inputs as load_futoi_inputs
from moex_data.futures.raw_5m_loader import load_inputs as load_raw_inputs
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_DIAGNOSTICS = "futures_controlled_batch_raw_only_diagnostics.v1"
RAW_ONLY_COMPONENTS = [
    {
        "component_id": "raw_5m_loader",
        "script": "src/moex_data/futures/raw_5m_loader.py",
        "manifest_rel": ["futures", "runs", "raw_5m_loader"],
        "verdict_field": "loader_result_verdict",
        "whitelist_field": "loader_whitelist_applied"
    },
    {
        "component_id": "futoi_raw_loader",
        "script": "src/moex_data/futures/futoi_raw_loader.py",
        "manifest_rel": ["futures", "runs", "futoi_raw_loader"],
        "verdict_field": "loader_result_verdict",
        "whitelist_field": "loader_whitelist_applied"
    },
    {
        "component_id": "derived_d1_ohlcv_builder",
        "script": "src/moex_data/futures/derived_d1_ohlcv_builder.py",
        "manifest_rel": ["futures", "runs", "derived_d1_ohlcv_builder"],
        "verdict_field": "builder_result_verdict",
        "whitelist_field": "builder_whitelist_applied"
    }
]
CONTINUOUS_ROOTS = [
    ["futures", "continuous_5m"],
    ["futures", "continuous_d1"],
    ["futures", "continuous", "roll_map"],
    ["futures", "runs", "continuous_series_builder"],
    ["futures", "quality", "continuous_series_builder"]
]


def output_paths(data_root, universe_scope, run_date):
    return {
        "diagnostics_manifest": str(data_root / "futures" / "runs" / "controlled_raw_pipeline" / ("universe_scope=" + universe_scope) / ("run_date=" + run_date) / "manifest.json")
    }


def child_manifest_path(data_root, component, run_date):
    path = data_root
    for part in component["manifest_rel"]:
        path = path / part
    return path / ("run_date=" + run_date) / "manifest.json"


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def continuous_snapshot(data_root):
    rows = []
    for parts in CONTINUOUS_ROOTS:
        path = data_root
        for part in parts:
            path = path / part
        parquet_count = 0
        json_count = 0
        if path.exists():
            parquet_count = len(list(path.rglob("*.parquet"))) if path.is_dir() else (1 if path.suffix == ".parquet" else 0)
            json_count = len(list(path.rglob("*.json"))) if path.is_dir() else (1 if path.suffix == ".json" else 0)
        rows.append({
            "path": str(path),
            "exists": path.exists(),
            "parquet_count": parquet_count,
            "json_count": json_count
        })
    return rows


def compare_continuous_absence(before, after):
    created = []
    for b, a in zip(before, after):
        if not b.get("exists") and a.get("exists"):
            created.append(a.get("path"))
        if int(a.get("parquet_count") or 0) > int(b.get("parquet_count") or 0):
            created.append(a.get("path") + ":parquet_count_increased")
        if int(a.get("json_count") or 0) > int(b.get("json_count") or 0):
            created.append(a.get("path") + ":json_count_increased")
    return {
        "continuous_build_executed": False,
        "continuous_roots_before": before,
        "continuous_roots_after": after,
        "new_or_changed_continuous_artifacts_detected": created,
        "status": "pass" if not created else "fail"
    }


def select_universe(root, data_root, snapshot_date, whitelist, excluded, config):
    raw_contracts = base.load_contract_values(root)
    _, normalized, liquidity, history = load_raw_inputs(data_root, raw_contracts, snapshot_date)
    futoi_contracts = load_contract_values_extended(root)
    _, _, _, _, futoi_availability = load_futoi_inputs(data_root, futoi_contracts, snapshot_date)
    selected, gate = select_controlled_instruments(normalized, liquidity, history, whitelist, excluded, config, futoi_availability=futoi_availability)
    return selected, gate


def component_command(root, component, args, whitelist, excluded):
    cmd = [sys.executable, str(root / component["script"])]
    if component["component_id"] in ["raw_5m_loader", "futoi_raw_loader"]:
        cmd.extend(["--snapshot-date", args.snapshot_date])
    cmd.extend(["--run-date", args.run_date])
    if args.from_date:
        cmd.extend(["--from", args.from_date])
    if args.till:
        cmd.extend(["--till", args.till])
    cmd.extend(["--data-root", str(args.data_root_resolved)])
    if component["component_id"] in ["raw_5m_loader", "futoi_raw_loader"]:
        cmd.extend(["--iss-base-url", args.iss_base_url])
        cmd.extend(["--apim-base-url", args.apim_base_url])
        cmd.extend(["--timeout", str(args.timeout)])
    cmd.extend(["--whitelist", ",".join(whitelist)])
    cmd.extend(["--excluded", ",".join(excluded)])
    return cmd


def validate_child_manifest(component, manifest, whitelist, excluded):
    verdict = str(manifest.get(component["verdict_field"]) or "")
    if verdict != "pass":
        raise RuntimeError(component["component_id"] + " verdict is not pass: " + verdict)
    observed = manifest.get(component["whitelist_field"])
    if observed != whitelist:
        raise RuntimeError(component["component_id"] + " whitelist mismatch")
    confirmed = manifest.get("excluded_instruments_confirmed") or []
    for secid in excluded:
        if secid not in confirmed:
            raise RuntimeError(component["component_id"] + " missing excluded confirmation: " + secid)
    summaries = manifest.get("instrument_summaries") or {}
    for secid in whitelist:
        if secid not in summaries:
            raise RuntimeError(component["component_id"] + " missing summary for " + secid)
    for secid in excluded:
        if secid in summaries:
            raise RuntimeError(component["component_id"] + " excluded instrument appeared: " + secid)
    quality = manifest.get("quality_status_counts") or {}
    if int(quality.get("fail") or 0) != 0:
        raise RuntimeError(component["component_id"] + " quality fail rows present")
    return "pass"


def run_component(root, data_root, component, args, whitelist, excluded):
    started = time.time()
    manifest_path = child_manifest_path(data_root, component, args.run_date)
    cmd = component_command(root, component, args, whitelist, excluded)
    proc = subprocess.run(cmd, cwd=str(root), text=True, capture_output=True)
    item = {
        "component_id": component["component_id"],
        "command": cmd,
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
        "manifest_path": str(manifest_path),
        "status": "fail",
        "validation_status": "not_validated"
    }
    if proc.returncode != 0:
        item["failure_reason"] = "component_returncode_nonzero"
        return item, None
    if not manifest_path.exists():
        item["failure_reason"] = "component_manifest_missing"
        return item, None
    if manifest_path.stat().st_mtime < started - 1.0:
        item["failure_reason"] = "component_manifest_stale"
        return item, None
    manifest = load_json(manifest_path)
    try:
        validate_child_manifest(component, manifest, whitelist, excluded)
        item["status"] = "pass"
        item["validation_status"] = "pass"
        item["child_run_id"] = manifest.get("run_id")
        item["child_verdict"] = manifest.get(component["verdict_field"])
        item["quality_status_counts"] = manifest.get("quality_status_counts")
        item["output_artifacts"] = manifest.get("output_artifacts")
        item["partition_count"] = len(manifest.get("partition_paths_created") or [])
        return item, manifest
    except Exception as exc:
        item["failure_reason"] = exc.__class__.__name__ + ": " + str(exc)
        item["validation_status"] = "fail"
        return item, manifest


def component_status(items, component_id):
    for item in items:
        if item.get("component_id") == component_id:
            return item
    return {"component_id": component_id, "status": "not_run"}


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe-scope", default="slice1")
    parser.add_argument("--config", default=DEFAULT_CONTROLLED_CONFIG)
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--whitelist", default="")
    parser.add_argument("--excluded", default="")
    args = parser.parse_args()

    if args.universe_scope != CONTROLLED_SCOPE:
        raise RuntimeError("controlled_raw_pipeline_runner only supports " + CONTROLLED_SCOPE)
    root = Path.cwd().resolve()
    args.data_root_resolved = base.resolve_data_root(args)
    data_root = args.data_root_resolved
    config = load_scope_config(root, args.config)
    raw_only_check = assert_raw_only_config(config)
    if raw_only_check.get("status") != "pass":
        raise RuntimeError("raw-only config check failed")
    whitelist_input = parse_list(args.whitelist, [])
    excluded = parse_list(args.excluded, [])
    selected, gate = select_universe(root, data_root, args.snapshot_date, whitelist_input, excluded, config)
    whitelist = sorted(selected["secid"].astype(str).tolist())
    run_id = "controlled_raw_pipeline_" + args.run_date + "_" + stable_id([args.snapshot_date, utc_now_iso(), ",".join(whitelist), args.from_date, args.till])
    outputs = output_paths(data_root, args.universe_scope, args.run_date)
    before = continuous_snapshot(data_root)
    items = []
    manifests = {}
    final_status = "pass"
    blockers = []
    for component in RAW_ONLY_COMPONENTS:
        item, manifest = run_component(root, data_root, component, args, whitelist, excluded)
        items.append(item)
        if item.get("status") != "pass":
            final_status = "fail"
            blockers.append(component["component_id"] + ":" + str(item.get("failure_reason") or "failed"))
            break
        manifests[component["component_id"]] = manifest
    after = continuous_snapshot(data_root)
    absence = compare_continuous_absence(before, after)
    if absence.get("status") != "pass":
        final_status = "fail"
        blockers.append("continuous_absence_check_failed")
    preservation = {
        "slice1_defaults_changed": False,
        "si_continuous_components_invoked": False,
        "roll_policy_changed": False,
        "status": "pass"
    }
    manifest = {
        "schema_version": SCHEMA_DIAGNOSTICS,
        "run_id": run_id,
        "universe_scope": args.universe_scope,
        "config_path": args.config,
        "run_date": args.run_date,
        "snapshot_date": args.snapshot_date,
        "refresh_from": args.from_date or None,
        "refresh_till": args.till or None,
        "selected_secids": whitelist,
        "classification_gate": gate,
        "raw_only_config_check": raw_only_check,
        "raw_5m_status": component_status(items, "raw_5m_loader"),
        "futoi_raw_status": component_status(items, "futoi_raw_loader"),
        "derived_d1_status": component_status(items, "derived_d1_ohlcv_builder"),
        "component_status": items,
        "continuous_absence_checks": absence,
        "preservation_checks": preservation,
        "child_manifest_references": {k: {"run_id": v.get("run_id"), "output_artifacts": v.get("output_artifacts")} for k, v in manifests.items()},
        "final_verdict": final_status,
        "blockers": blockers,
        "output_artifacts": outputs
    }
    path = Path(outputs["diagnostics_manifest"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print_json_line("controlled_raw_pipeline_manifest_path", str(path))
    print_json_line("selected_secids", whitelist)
    print_json_line("classification_gate", gate)
    print_json_line("raw_5m_status", manifest["raw_5m_status"])
    print_json_line("futoi_raw_status", manifest["futoi_raw_status"])
    print_json_line("derived_d1_status", manifest["derived_d1_status"])
    print_json_line("continuous_absence_checks", absence)
    print_json_line("preservation_checks", preservation)
    print_json_line("final_verdict", final_status)
    if blockers:
        print_json_line("blockers", blockers)
    return 0 if final_status == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
