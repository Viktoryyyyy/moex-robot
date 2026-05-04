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

SCHEMA_DAILY_REFRESH_MANIFEST = "futures_daily_data_refresh_manifest.v1"
ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0
CALENDAR_STATUS = "canonical_apim_futures_xml"

REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_registry_refresh_manifest_contract.md",
    "contracts/datasets/futures_raw_5m_loader_manifest_contract.md",
    "contracts/datasets/futures_futoi_5m_raw_loader_manifest_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_manifest_contract.md",
    "contracts/datasets/futures_daily_data_refresh_manifest_contract.md",
    "contracts/datasets/futures_expiration_map_contract.md",
    "contracts/datasets/futures_continuous_roll_map_contract.md",
    "contracts/datasets/futures_continuous_5m_contract.md",
    "contracts/datasets/futures_continuous_d1_contract.md",
    "contracts/datasets/futures_continuous_builder_manifest_contract.md",
    "contracts/datasets/futures_continuous_quality_report_contract.md",
]

COMPONENTS = [
    {"component_id": "registry_refresh_runner", "script": "src/moex_data/futures/registry_refresh_runner.py", "kind": "slice_manifest", "manifest_rel": ["futures", "runs", "registry_refresh"], "schema": "futures_registry_refresh_manifest.v1", "verdict": "registry_refresh_result_verdict"},
    {"component_id": "raw_5m_loader", "script": "src/moex_data/futures/raw_5m_loader.py", "kind": "slice_manifest", "manifest_rel": ["futures", "runs", "raw_5m_loader"], "schema": "futures_raw_5m_loader_manifest.v1", "verdict": "loader_result_verdict", "whitelist_field": "loader_whitelist_applied", "short_history_container": "short_history_handling"},
    {"component_id": "futoi_raw_loader", "script": "src/moex_data/futures/futoi_raw_loader.py", "kind": "slice_manifest", "manifest_rel": ["futures", "runs", "futoi_raw_loader"], "schema": "futures_futoi_5m_raw_loader_manifest.v1", "verdict": "loader_result_verdict", "whitelist_field": "loader_whitelist_applied", "short_history_container": "short_history_handling"},
    {"component_id": "derived_d1_ohlcv_builder", "script": "src/moex_data/futures/derived_d1_ohlcv_builder.py", "kind": "slice_manifest", "manifest_rel": ["futures", "runs", "derived_d1_ohlcv_builder"], "schema": "futures_derived_d1_ohlcv_manifest.v1", "verdict": "builder_result_verdict", "whitelist_field": "builder_whitelist_applied", "short_history_container": "short_history_handling"},
    {"component_id": "expiration_map_builder", "script": "src/moex_data/futures/expiration_map_builder.py", "kind": "stdout_artifact", "artifact": "expiration_map"},
    {"component_id": "continuous_roll_map_builder", "script": "src/moex_data/futures/continuous_roll_map_builder.py", "kind": "stdout_artifact", "artifact": "roll_map"},
    {"component_id": "continuous_5m_builder", "script": "src/moex_data/futures/continuous_series_builder.py", "kind": "stdout_artifact", "artifact": "continuous_5m"},
    {"component_id": "continuous_d1_builder", "script": "src/moex_data/futures/continuous_d1_builder.py", "kind": "stdout_artifact", "artifact": "continuous_d1"},
    {"component_id": "continuous_builder_manifest", "script": "src/moex_data/futures/continuous_builder_manifest.py", "kind": "continuous_manifest"},
    {"component_id": "continuous_quality_report", "script": "", "kind": "continuous_quality_gate"},
]


def output_paths(data_root, run_date):
    return {"manifest": str(data_root / "futures" / "runs" / "daily_refresh" / ("run_date=" + run_date) / "manifest.json")}


def continuous_paths(data_root, snapshot_date, run_date):
    roll = "roll_policy=" + ROLL_POLICY_ID
    adj = "adjustment_policy=" + ADJUSTMENT_POLICY_ID
    return {
        "expiration_map": str(data_root / "futures" / "registry" / ("snapshot_date=" + snapshot_date) / "futures_expiration_map.parquet"),
        "continuous_roll_map": str(data_root / "futures" / "continuous" / "roll_map" / ("snapshot_date=" + snapshot_date) / roll / "futures_continuous_roll_map.parquet"),
        "continuous_5m_root": str(data_root / "futures" / "continuous_5m" / roll / adj),
        "continuous_d1_root": str(data_root / "futures" / "continuous_d1" / roll / adj),
        "continuous_builder_manifest": str(data_root / "futures" / "runs" / "continuous_series_builder" / ("run_date=" + run_date) / "manifest.json"),
        "continuous_quality_report": str(data_root / "futures" / "quality" / "continuous_series_builder" / ("run_date=" + run_date) / "futures_continuous_quality_report.parquet"),
    }


def child_manifest_path(data_root, component, run_date):
    path = data_root
    for part in component.get("manifest_rel", []):
        path = path / part
    return path / ("run_date=" + run_date) / "manifest.json"


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def parse_stdout(stdout):
    out = {}
    for line in str(stdout or "").splitlines():
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        try:
            out[key.strip()] = json.loads(value)
        except Exception:
            out[key.strip()] = value
    return out


def require_path(path, label):
    item = Path(str(path)).expanduser()
    if not item.exists():
        raise RuntimeError(label + " missing: " + str(path))
    return item


def read_parquet(path, schema, label):
    item = require_path(path, label)
    frame = pd.read_parquet(item)
    if frame.empty:
        raise RuntimeError(label + " is empty: " + str(path))
    if "schema_version" in frame.columns:
        observed = sorted([str(x) for x in frame["schema_version"].dropna().unique().tolist()])
        if observed != [schema]:
            raise RuntimeError(label + " schema_version mismatch: " + json.dumps(observed, ensure_ascii=False))
    return frame


def validate_slice_manifest(component, manifest, whitelist, excluded):
    component_id = component["component_id"]
    if str(manifest.get("schema_version") or "") != component["schema"]:
        raise RuntimeError(component_id + " schema_version mismatch")
    if str(manifest.get(component["verdict"]) or "") != "pass":
        raise RuntimeError(component_id + " verdict is not pass")
    if component_id == "registry_refresh_runner":
        outputs = manifest.get("output_artifacts") or {}
        for key, value in outputs.items():
            if key != "manifest":
                require_path(value, component_id + "." + key)
        return
    if manifest.get(component["whitelist_field"]) != whitelist:
        raise RuntimeError(component_id + " whitelist mismatch")
    confirmed = manifest.get("excluded_instruments_confirmed") or []
    for secid in excluded:
        if secid not in confirmed:
            raise RuntimeError(component_id + " missing excluded confirmation: " + secid)
    quality = manifest.get("quality_status_counts") or {}
    if int(quality.get("fail") or 0) != 0:
        raise RuntimeError(component_id + " quality fail rows present")
    summaries = manifest.get("instrument_summaries") or {}
    for secid in whitelist:
        if secid not in summaries:
            raise RuntimeError(component_id + " missing instrument summary: " + secid)
    for secid in excluded:
        if secid in summaries:
            raise RuntimeError(component_id + " excluded instrument present: " + secid)
    container = manifest.get(component["short_history_container"]) or {}
    for secid in SHORT_HISTORY_ALLOWED:
        item = container.get(secid) if isinstance(container, dict) else None
        if not isinstance(item, dict) or item.get("short_history_flag") is not True:
            raise RuntimeError(component_id + " short_history_flag missing for " + secid)
    outputs = manifest.get("output_artifacts") or {}
    for key, value in outputs.items():
        require_path(value, component_id + "." + key)
    for value in manifest.get("partition_paths_created") or []:
        text = str(value)
        for secid in excluded:
            if ("secid=" + secid) in text:
                raise RuntimeError(component_id + " excluded partition path: " + text)
        require_path(value, component_id + ".partition")


def validate_stdout_artifact(component, paths):
    kind = component["artifact"]
    if kind == "expiration_map":
        frame = read_parquet(paths["expiration_map"], "futures_expiration_map.v1", "expiration_map")
        return {"path": paths["expiration_map"], "rows": int(len(frame))}
    if kind == "roll_map":
        frame = read_parquet(paths["continuous_roll_map"], "futures_continuous_roll_map.v1", "continuous_roll_map")
        if "adjustment_factor" in frame.columns and int((pd.to_numeric(frame["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR).sum()) != 0:
            raise RuntimeError("continuous_roll_map adjustment_factor_not_1")
        return {"path": paths["continuous_roll_map"], "rows": int(len(frame))}
    if kind in {"continuous_5m", "continuous_d1"}:
        root_key = kind + "_root"
        schema = "futures_continuous_5m.v1" if kind == "continuous_5m" else "futures_continuous_d1.v1"
        root = require_path(paths[root_key], kind)
        parquet_paths = sorted(root.glob("family=*/trade_date=*/part.parquet"))
        if not parquet_paths:
            raise RuntimeError(kind + " has no partitions")
        sample = pd.read_parquet(parquet_paths[0])
        if "schema_version" in sample.columns:
            observed = sorted([str(x) for x in sample["schema_version"].dropna().unique().tolist()])
            if observed != [schema]:
                raise RuntimeError(kind + " sample schema_version mismatch: " + json.dumps(observed, ensure_ascii=False))
        return {"path": paths[root_key], "partition_count": int(len(parquet_paths))}
    raise RuntimeError("Unsupported artifact kind: " + str(kind))


def validate_continuous_manifest(paths, whitelist, excluded):
    manifest = load_json(paths["continuous_builder_manifest"])
    if str(manifest.get("schema_version") or "") != "futures_continuous_builder_manifest.v1":
        raise RuntimeError("continuous manifest schema mismatch")
    if str(manifest.get("builder_result_verdict") or "") != "pass":
        raise RuntimeError("continuous manifest verdict is not pass")
    if manifest.get("builder_whitelist_applied") != whitelist:
        raise RuntimeError("continuous manifest whitelist mismatch")
    confirmed = manifest.get("excluded_instruments_confirmed") or []
    for secid in excluded:
        if secid not in confirmed:
            raise RuntimeError("continuous manifest missing excluded confirmation: " + secid)
    if str(manifest.get("roll_policy_id") or "") != ROLL_POLICY_ID:
        raise RuntimeError("continuous manifest roll_policy_id mismatch")
    if str(manifest.get("adjustment_policy_id") or "") != ADJUSTMENT_POLICY_ID:
        raise RuntimeError("continuous manifest adjustment_policy_id mismatch")
    if str(manifest.get("calendar_status") or "") != CALENDAR_STATUS:
        raise RuntimeError("continuous manifest calendar_status mismatch")
    quality = manifest.get("quality_status_counts") or {}
    if int(quality.get("fail") or 0) != 0:
        raise RuntimeError("continuous manifest quality fail rows present")
    for key in ["usdrubf_identity_check", "source_lineage_check"]:
        item = manifest.get(key) or {}
        if str(item.get("status") or "") != "pass":
            raise RuntimeError("continuous manifest " + key + " is not pass")
    report = read_parquet(paths["continuous_quality_report"], "futures_continuous_quality_report.v1", "continuous_quality_report")
    if "check_status" not in report.columns:
        raise RuntimeError("continuous quality report missing check_status")
    fail_rows = int((report["check_status"].astype(str) == "fail").sum())
    if fail_rows != 0:
        raise RuntimeError("continuous quality report fail rows: " + str(fail_rows))
    return manifest, report


def component_command(root, component, args, whitelist, excluded):
    component_id = component["component_id"]
    cmd = [sys.executable, str(root / component["script"])]
    if component_id in ["registry_refresh_runner", "raw_5m_loader", "futoi_raw_loader", "expiration_map_builder", "continuous_roll_map_builder", "continuous_5m_builder", "continuous_builder_manifest"]:
        cmd.extend(["--snapshot-date", args.snapshot_date])
    cmd.extend(["--run-date", args.run_date])
    if component_id in ["raw_5m_loader", "futoi_raw_loader", "derived_d1_ohlcv_builder", "continuous_5m_builder", "continuous_d1_builder"]:
        if args.from_date:
            cmd.extend(["--from", args.from_date])
        if args.till:
            cmd.extend(["--till", args.till])
    cmd.extend(["--data-root", str(args.data_root_resolved)])
    if component_id in ["registry_refresh_runner", "raw_5m_loader", "futoi_raw_loader", "continuous_roll_map_builder"]:
        cmd.extend(["--timeout", str(args.timeout)])
    if component_id in ["continuous_5m_builder", "continuous_d1_builder", "continuous_builder_manifest"]:
        cmd.extend(["--roll-policy-id", ROLL_POLICY_ID])
        cmd.extend(["--adjustment-policy-id", ADJUSTMENT_POLICY_ID])
    if component_id != "continuous_d1_builder":
        cmd.extend(["--whitelist", ",".join(whitelist)])
    cmd.extend(["--excluded", ",".join(excluded)])
    if component_id in ["registry_refresh_runner", "raw_5m_loader", "futoi_raw_loader"]:
        cmd.extend(["--iss-base-url", args.iss_base_url])
        cmd.extend(["--apim-base-url", args.apim_base_url])
    if component_id == "continuous_roll_map_builder":
        cmd.extend(["--iss-base-url", args.iss_base_url])
    return cmd


def run_component(root, data_root, component, args, whitelist, excluded):
    paths = continuous_paths(data_root, args.snapshot_date, args.run_date)
    item = {"component_id": component["component_id"], "status": "fail", "validation_status": "not_validated"}
    try:
        if component["kind"] == "continuous_quality_gate":
            manifest, report = validate_continuous_manifest(paths, whitelist, excluded)
            item.update({"status": "pass", "validation_status": "pass", "manifest_path": paths["continuous_builder_manifest"], "quality_report_path": paths["continuous_quality_report"], "child_run_id": manifest.get("run_id"), "quality_rows": int(len(report))})
            return item, manifest
        started_at = time.time()
        cmd = component_command(root, component, args, whitelist, excluded)
        proc = subprocess.run(cmd, cwd=str(root), text=True, capture_output=True)
        item.update({"command": cmd, "returncode": int(proc.returncode), "stdout_tail": proc.stdout[-4000:], "stderr_tail": proc.stderr[-4000:]})
        if proc.returncode != 0:
            raise RuntimeError("component_returncode_nonzero")
        if component["kind"] == "slice_manifest":
            manifest_path = child_manifest_path(data_root, component, args.run_date)
            item["manifest_path"] = str(manifest_path)
            if not manifest_path.exists():
                raise RuntimeError("component_manifest_missing")
            if manifest_path.stat().st_mtime < started_at - 1.0:
                raise RuntimeError("component_manifest_stale")
            manifest = load_json(manifest_path)
            validate_slice_manifest(component, manifest, whitelist, excluded)
            item.update({"status": "pass", "validation_status": "pass", "child_run_id": manifest.get("run_id"), "child_verdict": manifest.get(component["verdict"]), "output_artifacts": manifest.get("output_artifacts")})
            return item, manifest
        if component["kind"] == "stdout_artifact":
            parsed = parse_stdout(proc.stdout)
            item["stdout_fields"] = parsed
            if str(parsed.get("builder_result_verdict") or "") != "pass":
                raise RuntimeError("stdout_builder_result_verdict_not_pass")
            artifact = validate_stdout_artifact(component, paths)
            item.update({"status": "pass", "validation_status": "pass", "child_verdict": "pass", "output_artifacts": artifact})
            return item, None
        if component["kind"] == "continuous_manifest":
            manifest, report = validate_continuous_manifest(paths, whitelist, excluded)
            manifest_path = Path(paths["continuous_builder_manifest"])
            if manifest_path.stat().st_mtime < started_at - 1.0:
                raise RuntimeError("continuous_manifest_stale")
            item.update({"status": "pass", "validation_status": "pass", "manifest_path": paths["continuous_builder_manifest"], "quality_report_path": paths["continuous_quality_report"], "child_run_id": manifest.get("run_id"), "child_verdict": manifest.get("builder_result_verdict"), "quality_rows": int(len(report)), "output_artifacts": manifest.get("output_artifacts")})
            return item, manifest
        raise RuntimeError("unsupported_component_kind")
    except Exception as exc:
        item["failure_reason"] = exc.__class__.__name__ + ": " + str(exc)
        item["validation_status"] = "fail"
        return item, None


def merge_per_instrument(child_manifests):
    out = {}
    for component_id, manifest in child_manifests.items():
        if component_id in {"registry_refresh_runner", "continuous_builder_manifest"}:
            continue
        for secid, summary in (manifest.get("instrument_summaries") or {}).items():
            out.setdefault(secid, {})[component_id] = summary if isinstance(summary, dict) else {"raw_summary": summary}
    return out


def validate_final_scope(per_instrument, whitelist, excluded):
    observed = sorted(per_instrument.keys())
    if sorted([x.upper() for x in observed]) != sorted([x.upper() for x in whitelist]):
        return {"status": "fail", "observed": observed, "expected": whitelist}
    hits = [x for x in observed if str(x).upper() in {y.upper() for y in excluded}]
    if hits:
        return {"status": "fail", "excluded_hits": hits}
    return {"status": "pass", "observed": observed, "expected": whitelist}


def validate_short_history_final(per_instrument):
    status = "pass"
    details = {}
    for secid in SHORT_HISTORY_ALLOWED:
        components = {}
        for component_id, summary in (per_instrument.get(secid) or {}).items():
            flag = summary.get("short_history_flag") if isinstance(summary, dict) else None
            components[component_id] = flag
            if flag is not True:
                status = "fail"
        details[secid] = components
    return {"status": status, "details": details}


def child_refs(items):
    out = {}
    for item in items:
        out[item["component_id"]] = {"manifest_path": item.get("manifest_path"), "quality_report_path": item.get("quality_report_path"), "child_run_id": item.get("child_run_id"), "status": item.get("status"), "validation_status": item.get("validation_status")}
    return out


def continuous_refs(data_root, snapshot_date, run_date):
    paths = continuous_paths(data_root, snapshot_date, run_date)
    return {
        "expiration_map": {"path": paths["expiration_map"], "schema_version": "futures_expiration_map.v1"},
        "continuous_roll_map": {"path": paths["continuous_roll_map"], "schema_version": "futures_continuous_roll_map.v1"},
        "continuous_5m_root": {"path": paths["continuous_5m_root"], "schema_version": "futures_continuous_5m.v1"},
        "continuous_d1_root": {"path": paths["continuous_d1_root"], "schema_version": "futures_continuous_d1.v1"},
        "continuous_builder_manifest": {"path": paths["continuous_builder_manifest"], "schema_version": "futures_continuous_builder_manifest.v1"},
        "continuous_quality_report": {"path": paths["continuous_quality_report"], "schema_version": "futures_continuous_quality_report.v1"},
    }


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
    args.data_root_resolved = base.resolve_data_root(args)
    data_root = args.data_root_resolved
    whitelist = parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)
    for secid in whitelist:
        if secid in excluded:
            raise RuntimeError("Whitelisted instrument is also excluded: " + secid)
    base.assert_files_exist(root, REQUIRED_CONTRACTS)
    started_ts = utc_now_iso()
    run_id = "futures_daily_data_refresh_" + args.run_date + "_" + stable_id([args.snapshot_date, started_ts, ",".join(whitelist), args.from_date, args.till, ROLL_POLICY_ID, ADJUSTMENT_POLICY_ID])
    items = []
    child_manifests = {}
    final_status = "pass"
    blockers = []
    for component in COMPONENTS:
        item, manifest = run_component(root, data_root, component, args, whitelist, excluded)
        items.append(item)
        if item.get("status") != "pass":
            final_status = "fail"
            blockers.append(component["component_id"] + ":" + str(item.get("failure_reason") or "failed"))
            break
        if manifest is not None:
            child_manifests[component["component_id"]] = manifest
    per_instrument = merge_per_instrument(child_manifests)
    scope_check = validate_final_scope(per_instrument, whitelist, excluded) if per_instrument else {"status": "not_computed"}
    short_history_check = validate_short_history_final(per_instrument) if per_instrument else {"status": "not_computed"}
    if scope_check.get("status") != "pass":
        final_status = "fail"
        blockers.append("final_scope_check_failed")
    if short_history_check.get("status") != "pass":
        final_status = "fail"
        blockers.append("final_short_history_check_failed")
    outputs = output_paths(data_root, args.run_date)
    manifest = {
        "schema_version": SCHEMA_DAILY_REFRESH_MANIFEST,
        "run_id": run_id,
        "run_date": args.run_date,
        "snapshot_date": args.snapshot_date,
        "refresh_from": args.from_date or None,
        "refresh_till": args.till or None,
        "started_ts": started_ts,
        "completed_ts": utc_now_iso(),
        "runner_whitelist_applied": whitelist,
        "excluded_instruments_confirmed": excluded,
        "roll_policy_id": ROLL_POLICY_ID,
        "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
        "adjustment_factor": ADJUSTMENT_FACTOR,
        "component_execution_order": [x["component_id"] for x in COMPONENTS],
        "child_component_status": items,
        "child_manifest_references": child_refs(items),
        "continuous_child_artifact_references": continuous_refs(data_root, args.snapshot_date, args.run_date),
        "per_instrument_status": per_instrument,
        "short_history_flag_check": short_history_check,
        "excluded_instruments_check": scope_check,
        "artifact_validation_status": "pass" if final_status == "pass" else "fail",
        "daily_refresh_result_verdict": final_status,
        "blockers": blockers,
        "output_artifacts": outputs,
    }
    path = Path(outputs["manifest"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print_json_line("daily_refresh_manifest_path", str(path))
    print_json_line("child_component_status", manifest["child_manifest_references"])
    print_json_line("continuous_child_artifact_references", manifest["continuous_child_artifact_references"])
    print_json_line("artifact_validation_status", manifest["artifact_validation_status"])
    print_json_line("per_instrument_status", per_instrument)
    print_json_line("short_history_flag_check", short_history_check)
    print_json_line("excluded_instruments_check", scope_check)
    print_json_line("daily_refresh_result_verdict", final_status)
    if blockers:
        print_json_line("blockers", blockers)
    return 0 if final_status == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
