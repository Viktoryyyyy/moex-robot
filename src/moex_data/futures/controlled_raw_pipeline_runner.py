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


def command(root, cid, script, args, secids, excluded):
    cmd = [sys.executable, str(root / script)]
    if cid in ["raw_5m_loader", "futoi_raw_loader"]:
        cmd += ["--snapshot-date", args.snapshot_date]
    cmd += ["--run-date", args.run_date]
    if args.from_date:
        cmd += ["--from", args.from_date]
    if args.till:
        cmd += ["--till", args.till]
    cmd += ["--data-root", str(args.data_root_resolved), "--whitelist", ",".join(secids), "--excluded", ",".join(excluded)]
    if cid in ["raw_5m_loader", "futoi_raw_loader"]:
        cmd += ["--iss-base-url", args.iss_base_url, "--apim-base-url", args.apim_base_url, "--timeout", str(args.timeout)]
    return cmd


def validate_manifest(component_id, manifest, verdict_field, whitelist_field, secids, excluded):
    if str(manifest.get(verdict_field)) != "pass":
        raise RuntimeError(component_id + " verdict not pass")
    if manifest.get(whitelist_field) != secids:
        raise RuntimeError(component_id + " whitelist mismatch")
    summaries = manifest.get("instrument_summaries") or {}
    for secid in secids:
        if secid not in summaries:
            raise RuntimeError(component_id + " missing summary " + secid)
    for secid in excluded:
        if secid in summaries:
            raise RuntimeError(component_id + " excluded present " + secid)
    quality = manifest.get("quality_status_counts") or {}
    if int(quality.get("fail") or 0) != 0:
        raise RuntimeError(component_id + " quality fail rows")


def run_one(root, data_root, item, args, secids, excluded):
    cid, script, verdict, whitelist = item
    mpath = child_manifest(data_root, cid, args.run_date)
    started = time.time()
    proc = subprocess.run(command(root, cid, script, args, secids, excluded), cwd=str(root), text=True, capture_output=True)
    status = {"component_id": cid, "returncode": proc.returncode, "stdout_tail": proc.stdout[-3000:], "stderr_tail": proc.stderr[-3000:], "manifest_path": str(mpath), "status": "fail"}
    if proc.returncode != 0:
        status["failure_reason"] = "returncode_nonzero"
        return status, None
    if not mpath.exists() or mpath.stat().st_mtime < started - 1.0:
        status["failure_reason"] = "manifest_missing_or_stale"
        return status, None
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    try:
        validate_manifest(cid, manifest, verdict, whitelist, secids, excluded)
        status["status"] = "pass"
        status["quality_status_counts"] = manifest.get("quality_status_counts")
        status["output_artifacts"] = manifest.get("output_artifacts")
        return status, manifest
    except Exception as exc:
        status["failure_reason"] = exc.__class__.__name__ + ": " + str(exc)
        return status, manifest


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
    secids, gate = select(root, data_root, args.snapshot_date, args.config, parse_list(args.whitelist, []), excluded)
    before = snapshot_continuous(data_root)
    statuses = []
    manifests = {}
    final = "pass"
    blockers = []
    for item in COMPONENTS:
        st, mf = run_one(root, data_root, item, args, secids, excluded)
        statuses.append(st)
        if st["status"] != "pass":
            final = "fail"
            blockers.append(st["component_id"] + ":" + st.get("failure_reason", "failed"))
            break
        manifests[st["component_id"]] = mf
    abs_check = absence(before, snapshot_continuous(data_root))
    if abs_check["status"] != "pass":
        final = "fail"
        blockers.append("continuous_absence_failed")
    manifest = {"schema_version": SCHEMA_DIAGNOSTICS, "run_id": "controlled_raw_pipeline_" + args.run_date + "_" + stable_id([args.snapshot_date, utc_now_iso(), ",".join(secids)]), "universe_scope": args.universe_scope, "snapshot_date": args.snapshot_date, "run_date": args.run_date, "selected_secids": secids, "classification_gate": gate, "raw_5m_status": statuses[0] if len(statuses) > 0 else {"status": "not_run"}, "futoi_integration_status": statuses[1] if len(statuses) > 1 else {"status": "not_run"}, "raw_d1_integration_status": statuses[2] if len(statuses) > 2 else {"status": "not_run"}, "diagnostics_status": "pass", "continuous_absence_checks": abs_check, "preservation_checks": {"slice1_defaults_changed": False, "si_continuous_behavior_changed": False, "roll_policy_changed": False, "status": "pass"}, "component_status": statuses, "final_verdict": final, "blockers": blockers}
    path = out_path(data_root, args.universe_scope, args.run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print_json_line("controlled_raw_pipeline_manifest_path", str(path))
    print_json_line("selected_secids", secids)
    print_json_line("classification_gate", gate)
    print_json_line("continuous_absence_checks", abs_check)
    print_json_line("final_verdict", final)
    if blockers:
        print_json_line("blockers", blockers)
    return 0 if final == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
