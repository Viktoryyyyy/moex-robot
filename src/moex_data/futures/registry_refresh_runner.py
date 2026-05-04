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

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import SHORT_HISTORY_ALLOWED
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_MANIFEST = "futures_registry_refresh_manifest.v1"
REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_registry_snapshot_contract.md",
    "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "contracts/datasets/futures_futoi_availability_report_contract.md",
    "contracts/datasets/futures_obstats_availability_report_contract.md",
    "contracts/datasets/futures_hi2_availability_report_contract.md",
    "contracts/datasets/futures_liquidity_screen_contract.md",
    "contracts/datasets/futures_history_depth_screen_contract.md",
    "contracts/datasets/futures_registry_refresh_manifest_contract.md",
]
REQUIRED_CONFIGS = [
    "configs/datasets/futures_algopack_availability_sources_config.json",
    "configs/datasets/futures_slice1_universe_config.json",
    "configs/datasets/futures_liquidity_screen_thresholds_config.json",
    "configs/datasets/futures_history_depth_thresholds_config.json",
]
CONTRACTS = {
    "registry_snapshot": "contracts/datasets/futures_registry_snapshot_contract.md",
    "normalized_registry": "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "algopack_fo_tradestats": "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "moex_futoi": "contracts/datasets/futures_futoi_availability_report_contract.md",
    "algopack_fo_obstats": "contracts/datasets/futures_obstats_availability_report_contract.md",
    "algopack_fo_hi2": "contracts/datasets/futures_hi2_availability_report_contract.md",
    "liquidity_screen": "contracts/datasets/futures_liquidity_screen_contract.md",
    "history_depth_screen": "contracts/datasets/futures_history_depth_screen_contract.md",
}


def contract_value(root, rel, key):
    prefix = key + ":"
    for raw in (root / rel).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path(root, data_root, rel, snapshot_date):
    pattern = contract_value(root, rel, "path_pattern")
    if not pattern.startswith("${MOEX_DATA_ROOT}"):
        raise RuntimeError("Unsupported contract path_pattern: " + rel)
    tail = pattern[len("${MOEX_DATA_ROOT}"):].lstrip("/")
    tail = tail.replace("{snapshot_date}", snapshot_date).replace("YYYY-MM-DD", snapshot_date)
    return data_root / tail


def output_paths(root, data_root, snapshot_date, run_date):
    out = {key: str(contract_path(root, data_root, rel, snapshot_date)) for key, rel in CONTRACTS.items()}
    out["manifest"] = str(data_root / "futures" / "runs" / "registry_refresh" / ("run_date=" + run_date) / "manifest.json")
    return out


def run_child(root, component_id, command, expected):
    started_at = time.time()
    proc = subprocess.run(command, cwd=str(root), text=True, capture_output=True)
    item = {"component_id": component_id, "command": command, "returncode": int(proc.returncode), "stdout_tail": proc.stdout[-4000:], "stderr_tail": proc.stderr[-4000:], "status": "fail", "validation_status": "not_validated"}
    if proc.returncode != 0:
        item["failure_reason"] = "component_returncode_nonzero"
        return item
    missing = []
    stale = []
    for key, raw_path in expected.items():
        path = Path(str(raw_path))
        if not path.exists():
            missing.append(key + "=" + str(path))
        elif path.stat().st_mtime < started_at - 1.0:
            stale.append(key + "=" + str(path))
    if missing:
        item["failure_reason"] = "expected_output_missing: " + "; ".join(missing)
        item["validation_status"] = "fail"
        return item
    if stale:
        item["failure_reason"] = "expected_output_stale: " + "; ".join(stale)
        item["validation_status"] = "fail"
        return item
    item["status"] = "pass"
    item["validation_status"] = "pass"
    item["expected_outputs"] = expected
    return item


def availability_summary(path):
    frame = pd.read_parquet(path)
    field = "availability_status"
    if field not in frame.columns:
        return {"rows": int(len(frame)), "validation_status": "fail", "failure_reason": "missing " + field}
    counts = {str(k): int(v) for k, v in frame[field].astype(str).value_counts(dropna=False).to_dict().items()}
    expected_available = int(len(frame)) > 0 and int(counts.get("available") or 0) == int(len(frame))
    return {"rows": int(len(frame)), "status_counts": counts, "validation_status": "pass" if expected_available else "fail"}


def screen_summary(path, field, whitelist):
    frame = pd.read_parquet(path)
    if "secid" not in frame.columns or field not in frame.columns:
        return {"rows": int(len(frame)), "validation_status": "fail", "failure_reason": "missing required screen fields"}
    by_secid = {}
    status = "pass"
    for secid in whitelist:
        row = frame.loc[frame["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        if row.empty:
            by_secid[secid] = "missing"
            status = "fail"
            continue
        value = str(row.iloc[0].get(field, ""))
        by_secid[secid] = value
        if field == "history_depth_status" and secid in SHORT_HISTORY_ALLOWED:
            if value not in ["pass", "review_required"]:
                status = "fail"
        elif value != "pass":
            status = "fail"
    counts = {str(k): int(v) for k, v in frame[field].astype(str).value_counts(dropna=False).to_dict().items()}
    return {"rows": int(len(frame)), "status_counts": counts, "whitelist_status": by_secid, "validation_status": status}


def validate_outputs(outputs, whitelist):
    registry_rows = int(len(pd.read_parquet(outputs["registry_snapshot"])))
    normalized_rows = int(len(pd.read_parquet(outputs["normalized_registry"])))
    summaries = {
        "registry_snapshot": {"rows": registry_rows, "validation_status": "pass" if registry_rows > 0 else "fail"},
        "normalized_registry": {"rows": normalized_rows, "validation_status": "pass" if normalized_rows > 0 else "fail"},
        "algopack_fo_tradestats": availability_summary(outputs["algopack_fo_tradestats"]),
        "moex_futoi": availability_summary(outputs["moex_futoi"]),
        "algopack_fo_obstats": availability_summary(outputs["algopack_fo_obstats"]),
        "algopack_fo_hi2": availability_summary(outputs["algopack_fo_hi2"]),
        "liquidity_screen": screen_summary(outputs["liquidity_screen"], "liquidity_status", whitelist),
        "history_depth_screen": screen_summary(outputs["history_depth_screen"], "history_depth_status", whitelist),
    }
    blockers = [key + "_validation_failed" for key, value in summaries.items() if value.get("validation_status") != "pass"]
    return summaries, blockers


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--whitelist", default=",".join(DEFAULT_WHITELIST))
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()
    root = Path.cwd().resolve()
    data_root = base.resolve_data_root(args)
    whitelist = parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)
    base.assert_files_exist(root, REQUIRED_CONTRACTS + REQUIRED_CONFIGS)
    outputs = output_paths(root, data_root, args.snapshot_date, args.run_date)
    started_ts = utc_now_iso()
    run_id = "futures_registry_refresh_" + args.run_date + "_" + stable_id([args.snapshot_date, started_ts, ",".join(whitelist)])
    common = ["--snapshot-date", args.snapshot_date, "--data-root", str(data_root), "--timeout", str(args.timeout), "--iss-base-url", args.iss_base_url, "--apim-base-url", args.apim_base_url]
    if args.from_date:
        common += ["--from", args.from_date]
    if args.till:
        common += ["--till", args.till]
    child_items = []
    child_items.append(run_child(root, "algopack_availability_probe", [sys.executable, str(root / "src/moex_data/futures/algopack_availability_probe.py")] + common, {k: outputs[k] for k in ["registry_snapshot", "normalized_registry", "algopack_fo_tradestats", "moex_futoi", "algopack_fo_obstats", "algopack_fo_hi2"]}))
    if child_items[-1].get("status") == "pass":
        screen_cmd = [sys.executable, str(root / "src/moex_data/futures/liquidity_history_metrics_probe.py")] + common + ["--full-history-proven"]
        child_items.append(run_child(root, "liquidity_history_metrics_probe", screen_cmd, {"liquidity_screen": outputs["liquidity_screen"], "history_depth_screen": outputs["history_depth_screen"]}))
    final_status = "pass" if len(child_items) == 2 and all(x.get("status") == "pass" for x in child_items) else "fail"
    blockers = [str(x.get("component_id")) + ":" + str(x.get("failure_reason")) for x in child_items if x.get("status") != "pass"]
    output_summaries = {}
    if final_status == "pass":
        output_summaries, validation_blockers = validate_outputs(outputs, whitelist)
        blockers += validation_blockers
        if validation_blockers:
            final_status = "fail"
    manifest = {"schema_version": SCHEMA_MANIFEST, "run_id": run_id, "run_date": args.run_date, "snapshot_date": args.snapshot_date, "refresh_from": args.from_date or None, "refresh_till": args.till or None, "started_ts": started_ts, "completed_ts": utc_now_iso(), "runner_whitelist_applied": whitelist, "excluded_instruments_confirmed": excluded, "component_execution_order": ["algopack_availability_probe", "liquidity_history_metrics_probe"], "child_component_status": child_items, "child_output_references": {x["component_id"]: {"status": x.get("status"), "validation_status": x.get("validation_status"), "expected_outputs": x.get("expected_outputs")} for x in child_items}, "output_artifacts": outputs, "output_summaries": output_summaries, "artifact_validation_status": "pass" if final_status == "pass" else "fail", "registry_refresh_result_verdict": final_status, "blockers": blockers}
    path = Path(outputs["manifest"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print_json_line("registry_refresh_manifest_path", str(path))
    print_json_line("child_component_status", manifest["child_output_references"])
    print_json_line("artifact_validation_status", manifest["artifact_validation_status"])
    print_json_line("registry_refresh_result_verdict", final_status)
    if blockers:
        print_json_line("blockers", blockers)
    return 0 if final_status == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
