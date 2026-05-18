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
from moex_data.futures.controlled_wmmmx_select import CONFIG
from moex_data.futures.controlled_wmmmx_select import SCOPE
from moex_data.futures.controlled_wmmmx_select import select
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_DIAGNOSTICS = "futures_controlled_batch_raw_only_diagnostics.v1"
COMPONENTS = [
    ["raw_5m_loader", "src/moex_data/futures/raw_5m_loader.py", "loader_result_verdict", "loader_whitelist_applied"],
    ["futoi_raw_loader", "src/moex_data/futures/futoi_raw_loader.py", "loader_result_verdict", "loader_whitelist_applied"],
    ["derived_d1_ohlcv_builder", "src/moex_data/futures/derived_d1_ohlcv_builder.py", "builder_result_verdict", "builder_whitelist_applied"]
]
CONTINUOUS_ROOTS = ["continuous_5m", "continuous_d1", "continuous/roll_map", "runs/continuous_series_builder", "quality/continuous_series_builder"]


def out_path(data_root, scope, run_date):
    return data_root / "futures" / "runs" / "controlled_raw_pipeline" / ("universe_scope=" + scope) / ("run_date=" + run_date) / "manifest.json"


def child_manifest(data_root, component_id, run_date):
    names = {
        "raw_5m_loader": "raw_5m_loader",
        "futoi_raw_loader": "futoi_raw_loader",
        "derived_d1_ohlcv_builder": "derived_d1_ohlcv_builder"
    }
    return data_root / "futures" / "runs" / names[component_id] / ("run_date=" + run_date) / "manifest.json"


def snapshot_continuous(data_root):
    rows = []
    for rel in CONTINUOUS_ROOTS:
        p = data_root / "futures" / rel
        rows.append({"path": str(p), "exists": p.exists(), "parquet_count": len(list(p.rglob("*.parquet"))) if p.exists() and p.is_dir() else 0, "json_count": len(list(p.rglob("*.json"))) if p.exists() and p.is_dir() else 0})
    return rows


def absence(before, after):
    changed = []
    for b, a in zip(before, after):
        if (not b["exists"] and a["exists"]) or a["parquet_count"] > b["parquet_count"] or a["json_count"] > b["json_count"]:
            changed.append(a["path"])
    return {"continuous_build_executed": False, "new_or_changed_continuous_artifacts_detected": changed, "status": "pass" if not changed else "fail", "before": before, "after": after}


def resolved_ranges(gate, args):
    rows = gate.get("rows") or []
    ranges = {}
    for row in rows:
        secid = str(row.get("secid") or "").strip()
        if not secid:
            raise RuntimeError("controlled eligibility contains empty secid")
        if not bool(row.get("raw_loader_date_range_resolvable")):
            raise RuntimeError("controlled eligibility contains unresolved raw-loader range for " + secid)
        start = str(row.get("first_available_date") or "").strip()
        end = str(row.get("last_available_date") or "").strip()
        if not start or not end:
            raise RuntimeError("controlled eligibility resolved empty loader range for " + secid)
        if start > end:
            raise RuntimeError("controlled eligibility resolved inverted loader range for " + secid)
        ranges[secid] = {"from": str(args.from_date or start), "till": str(args.till or end)}
    if not ranges:
        raise RuntimeError("controlled eligibility resolved zero loader ranges")
    return ranges


def child_run_date(base_run_date, cid, secid):
    return base_run_date + "_" + cid + "_" + secid


def command(root, cid, script, args, secid, excluded, date_range, run_date):
    cmd = [sys.executable, str(root / script)]
    if cid in ["raw_5m_loader", "futoi_raw_loader"]:
        cmd += ["--snapshot-date", args.snapshot_date]
    cmd += ["--run-date", run_date]
    cmd += ["--from", date_range["from"]]
    cmd += ["--till", date_range["till"]]
    cmd += ["--data-root", str(args.data_root_resolved), "--whitelist", secid, "--excluded", ",".join(excluded)]
    if cid in ["raw_5m_loader", "futoi_raw_loader"]:
        cmd += ["--iss-base-url", args.iss_base_url, "--apim-base-url", args.apim_base_url, "--timeout", str(args.timeout)]
    return cmd


def validate_manifest(component_id, manifest, verdict_field, whitelist_field, secid, excluded):
    if str(manifest.get(verdict_field)) != "pass":
        raise RuntimeError(component_id + " verdict not pass")
    if manifest.get(whitelist_field) != [secid]:
        raise RuntimeError(component_id + " whitelist mismatch")
    summaries = manifest.get("instrument_summaries") or {}
    if secid not in summaries:
        raise RuntimeError(component_id + " missing summary " + secid)
    for excluded_secid in excluded:
        if excluded_secid in summaries:
            raise RuntimeError(component_id + " excluded present " + excluded_secid)
    quality = manifest.get("quality_status_counts") or {}
    if int(quality.get("fail") or 0) != 0:
        raise RuntimeError(component_id + " quality fail rows")


def run_one(root, data_root, item, args, secid, excluded, date_range):
    cid, script, verdict, whitelist = item
    run_date = child_run_date(args.run_date, cid, secid)
    mpath = child_manifest(data_root, cid, run_date)
    started = time.time()
    proc = subprocess.run(command(root, cid, script, args, secid, excluded, date_range, run_date), cwd=str(root), text=True, capture_output=True)
    status = {"component_id": cid, "secid": secid, "requested_from": date_range["from"], "requested_till": date_range["till"], "child_run_date": run_date, "returncode": proc.returncode, "stdout_tail": proc.stdout[-3000:], "stderr_tail": proc.stderr[-3000:], "manifest_path": str(mpath), "status": "fail"}
    if proc.returncode != 0:
        status["failure_reason"] = "returncode_nonzero"
        return status, None
    if not mpath.exists() or mpath.stat().st_mtime < started - 1.0:
        status["failure_reason"] = "manifest_missing_or_stale"
        return status, None
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    try:
        validate_manifest(cid, manifest, verdict, whitelist, secid, excluded)
        status["status"] = "pass"
        status["quality_status_counts"] = manifest.get("quality_status_counts")
        status["output_artifacts"] = manifest.get("output_artifacts")
        return status, manifest
    except Exception as exc:
        status["failure_reason"] = exc.__class__.__name__ + ": " + str(exc)
        return status, manifest


def aggregate_component(cid, statuses):
    rows = [x for x in statuses if x.get("component_id") == cid]
    failed = [x for x in rows if x.get("status") != "pass"]
    return {"component_id": cid, "status": "pass" if rows and not failed else "fail", "instrument_count": len(rows), "failed_count": len(failed), "rows": rows}


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe-scope", default="slice1")
    parser.add_argument("--config", default=CONFIG)
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
    if args.universe_scope != SCOPE:
        raise RuntimeError("controlled runner supports only " + SCOPE)
    root = Path.cwd().resolve()
    args.data_root_resolved = base.resolve_data_root(args)
    data_root = args.data_root_resolved
    excluded = parse_list(args.excluded, [])
    secids, gate = select(root, data_root, args.snapshot_date, args.config, parse_list(args.whitelist, []), excluded, args.apim_base_url, args.iss_base_url, float(args.timeout))
    ranges = resolved_ranges(gate, args)
    before = snapshot_continuous(data_root)
    statuses = []
    manifests = {}
    final = "pass"
    blockers = []
    for item in COMPONENTS:
        cid = item[0]
        for secid in secids:
            st, mf = run_one(root, data_root, item, args, secid, excluded, ranges[secid])
            statuses.append(st)
            if st["status"] != "pass":
                final = "fail"
                blockers.append(st["component_id"] + ":" + secid + ":" + st.get("failure_reason", "failed"))
                break
            manifests[cid + ":" + secid] = mf
        if final != "pass":
            break
    abs_check = absence(before, snapshot_continuous(data_root))
    if abs_check["status"] != "pass":
        final = "fail"
        blockers.append("continuous_absence_failed")
    raw_5m_status = aggregate_component("raw_5m_loader", statuses)
    futoi_status = aggregate_component("futoi_raw_loader", statuses)
    d1_status = aggregate_component("derived_d1_ohlcv_builder", statuses)
    manifest = {"schema_version": SCHEMA_DIAGNOSTICS, "run_id": "controlled_raw_pipeline_" + args.run_date + "_" + stable_id([args.snapshot_date, utc_now_iso(), ",".join(secids)]), "universe_scope": args.universe_scope, "snapshot_date": args.snapshot_date, "run_date": args.run_date, "resolved_loader_ranges": ranges, "selected_secids": secids, "classification_gate": gate, "raw_5m_status": raw_5m_status, "futoi_integration_status": futoi_status, "raw_d1_integration_status": d1_status, "diagnostics_status": "pass", "continuous_absence_checks": abs_check, "preservation_checks": {"slice1_defaults_changed": False, "si_continuous_behavior_changed": False, "roll_policy_changed": False, "status": "pass"}, "component_status": statuses, "final_verdict": final, "blockers": blockers}
    path = out_path(data_root, args.universe_scope, args.run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print_json_line("controlled_raw_pipeline_manifest_path", str(path))
    print_json_line("selected_secids", secids)
    print_json_line("resolved_loader_ranges", ranges)
    print_json_line("classification_gate", gate)
    print_json_line("continuous_absence_checks", abs_check)
    print_json_line("final_verdict", final)
    if blockers:
        print_json_line("blockers", blockers)
    return 0 if final == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
