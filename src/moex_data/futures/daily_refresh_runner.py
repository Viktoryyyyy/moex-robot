#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from moex_data.futures import liquidity_history_metrics_probe as base

TZ_MSK = ZoneInfo("Europe/Moscow")
SCHEMA_DAILY_REFRESH_MANIFEST = "futures_daily_data_refresh_manifest.v1"
DEFAULT_WHITELIST = ["SiM6", "SiU6", "SiU7", "SiZ6", "USDRUBF"]
DEFAULT_EXCLUDED = ["SiH7", "SiM7"]
SHORT_HISTORY_ALLOWED = {"SiU7"}
REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_raw_5m_loader_manifest_contract.md",
    "contracts/datasets/futures_futoi_5m_raw_loader_manifest_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_manifest_contract.md",
    "contracts/datasets/futures_daily_data_refresh_manifest_contract.md",
]
COMPONENTS = [
    {
        "component_id": "raw_5m_loader",
        "script": "src/moex_data/futures/raw_5m_loader.py",
        "manifest_rel": ["futures", "runs", "raw_5m_loader"],
        "manifest_schema": "futures_raw_5m_loader_manifest.v1",
        "verdict_field": "loader_result_verdict",
        "whitelist_field": "loader_whitelist_applied",
        "short_history_container": "short_history_handling",
        "quality_label": "raw_5m",
    },
    {
        "component_id": "futoi_raw_loader",
        "script": "src/moex_data/futures/futoi_raw_loader.py",
        "manifest_rel": ["futures", "runs", "futoi_raw_loader"],
        "manifest_schema": "futures_futoi_5m_raw_loader_manifest.v1",
        "verdict_field": "loader_result_verdict",
        "whitelist_field": "loader_whitelist_applied",
        "short_history_container": "short_history_handling",
        "quality_label": "futoi_raw",
    },
    {
        "component_id": "derived_d1_ohlcv_builder",
        "script": "src/moex_data/futures/derived_d1_ohlcv_builder.py",
        "manifest_rel": ["futures", "runs", "derived_d1_ohlcv_builder"],
        "manifest_schema": "futures_derived_d1_ohlcv_manifest.v1",
        "verdict_field": "builder_result_verdict",
        "whitelist_field": "builder_whitelist_applied",
        "short_history_container": "short_history_handling",
        "quality_label": "derived_d1",
    },
]


def today_msk():
    return datetime.now(TZ_MSK).date().isoformat()


def utc_now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def stable_id(parts):
    return hashlib.sha256("|".join([str(x) for x in parts]).encode("utf-8")).hexdigest()[:24]


def parse_list(value, default):
    text = str(value or "").strip()
    if not text:
        return list(default)
    return [x.strip() for x in text.split(",") if x.strip()]


def output_paths(data_root, run_date):
    return {
        "manifest": str(data_root / "futures" / "runs" / "daily_refresh" / ("run_date=" + run_date) / "manifest.json")
    }


def child_manifest_path(data_root, component, run_date):
    path = data_root
    for part in component["manifest_rel"]:
        path = path / part
    return path / ("run_date=" + run_date) / "manifest.json"


def print_json_line(key, value):
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def assert_exact_scope(name, manifest, component, whitelist, excluded):
    observed = manifest.get(component["whitelist_field"])
    if observed != whitelist:
        raise RuntimeError(name + " whitelist mismatch: " + json.dumps(observed, ensure_ascii=False))
    confirmed = manifest.get("excluded_instruments_confirmed")
    if not isinstance(confirmed, list):
        raise RuntimeError(name + " excluded_instruments_confirmed is not list")
    missing_excluded = [x for x in excluded if x not in confirmed]
    if missing_excluded:
        raise RuntimeError(name + " missing excluded confirmations: " + ",".join(missing_excluded))
    summaries = manifest.get("instrument_summaries") or {}
    if not isinstance(summaries, dict):
        raise RuntimeError(name + " instrument_summaries is not object")
    missing = [x for x in whitelist if x not in summaries]
    if missing:
        raise RuntimeError(name + " missing accepted whitelist instruments: " + ",".join(missing))
    excluded_upper = {x.upper() for x in excluded}
    summary_hits = [x for x in summaries.keys() if str(x).upper() in excluded_upper]
    if summary_hits:
        raise RuntimeError(name + " excluded instruments found in summaries: " + ",".join(summary_hits))
    for raw_path in manifest.get("partition_paths_created") or []:
        text = str(raw_path)
        for secid in excluded:
            if ("secid=" + secid) in text:
                raise RuntimeError(name + " excluded instrument path found: " + text)


def assert_short_history(name, manifest, component):
    container = manifest.get(component["short_history_container"]) or {}
    siu7 = container.get("SiU7") if isinstance(container, dict) else None
    if not isinstance(siu7, dict) or siu7.get("short_history_flag") is not True:
        raise RuntimeError(name + " SiU7 short_history_flag is not true")
    summaries = manifest.get("instrument_summaries") or {}
    for secid, summary in summaries.items():
        if str(secid) in SHORT_HISTORY_ALLOWED:
            if not isinstance(summary, dict) or summary.get("short_history_flag") is not True:
                raise RuntimeError(name + " " + str(secid) + " short_history_flag is not true")
        else:
            if isinstance(summary, dict) and summary.get("short_history_flag") is True:
                raise RuntimeError(name + " unexpected short_history_flag=true for " + str(secid))


def assert_child_artifacts(name, manifest):
    outputs = manifest.get("output_artifacts") or {}
    if not isinstance(outputs, dict):
        raise RuntimeError(name + " output_artifacts is not object")
    missing_outputs = []
    for key, value in outputs.items():
        if not value:
            missing_outputs.append(key)
            continue
        path = Path(str(value)).expanduser()
        if not path.exists():
            missing_outputs.append(key + "=" + str(value))
    if missing_outputs:
        raise RuntimeError(name + " missing output_artifacts: " + "; ".join(missing_outputs))
    missing_partitions = []
    for value in manifest.get("partition_paths_created") or []:
        path = Path(str(value)).expanduser()
        if not path.exists():
            missing_partitions.append(str(value))
            if len(missing_partitions) >= 10:
                break
    if missing_partitions:
        raise RuntimeError(name + " missing partition paths: " + "; ".join(missing_partitions))


def assert_child_verdict(name, manifest, component):
    schema = str(manifest.get("schema_version") or "")
    if schema != component["manifest_schema"]:
        raise RuntimeError(name + " schema_version mismatch: " + schema)
    verdict = str(manifest.get(component["verdict_field"]) or "")
    if verdict != "pass":
        raise RuntimeError(name + " verdict is not pass: " + verdict)
    quality = manifest.get("quality_status_counts") or {}
    if int(quality.get("fail") or 0) != 0:
        raise RuntimeError(name + " quality_status_counts.fail is not zero")
    calendar = manifest.get("calendar_validation_summary") or {}
    if str(calendar.get("calendar_denominator_status") or "") != "canonical_apim_futures_xml":
        raise RuntimeError(name + " calendar denominator is not canonical_apim_futures_xml")
    if component["component_id"] == "derived_d1_ohlcv_builder":
        check = manifest.get("source_to_output_row_check") or {}
        if int(check.get("missing_d1_row_count") or 0) != 0:
            raise RuntimeError(name + " missing_d1_row_count is not zero")


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
        cmd.extend(["--timeout", str(args.timeout)])
    cmd.extend(["--whitelist", ",".join(whitelist)])
    cmd.extend(["--excluded", ",".join(excluded)])
    if component["component_id"] in ["raw_5m_loader", "futoi_raw_loader"]:
        cmd.extend(["--iss-base-url", args.iss_base_url])
        cmd.extend(["--apim-base-url", args.apim_base_url])
    return cmd


def run_component(root, data_root, component, args, whitelist, excluded):
    expected_manifest = child_manifest_path(data_root, component, args.run_date)
    started_at = time.time()
    cmd = component_command(root, component, args, whitelist, excluded)
    proc = subprocess.run(cmd, cwd=str(root), text=True, capture_output=True)
    item = {
        "component_id": component["component_id"],
        "command": cmd,
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
        "manifest_path": str(expected_manifest),
        "status": "fail",
        "validation_status": "not_validated",
    }
    if proc.returncode != 0:
        item["failure_reason"] = "component_returncode_nonzero"
        return item, None
    if not expected_manifest.exists():
        item["failure_reason"] = "component_manifest_missing"
        return item, None
    if expected_manifest.stat().st_mtime < started_at - 1.0:
        item["failure_reason"] = "component_manifest_stale"
        return item, None
    manifest = load_json(expected_manifest)
    try:
        assert_child_verdict(component["component_id"], manifest, component)
        assert_exact_scope(component["component_id"], manifest, component, whitelist, excluded)
        assert_short_history(component["component_id"], manifest, component)
        assert_child_artifacts(component["component_id"], manifest)
        item["status"] = "pass"
        item["validation_status"] = "pass"
        item["child_run_id"] = manifest.get("run_id")
        item["child_verdict"] = manifest.get(component["verdict_field"])
        item["quality_status_counts"] = manifest.get("quality_status_counts")
        item["output_artifacts"] = manifest.get("output_artifacts")
        item["partition_count"] = len(manifest.get("partition_paths_created") or [])
    except Exception as exc:
        item["failure_reason"] = exc.__class__.__name__ + ": " + str(exc)
        item["validation_status"] = "fail"
    return item, manifest


def merge_per_instrument(child_manifests):
    out = {}
    for component_id, manifest in child_manifests.items():
        summaries = manifest.get("instrument_summaries") or {}
        for secid, summary in summaries.items():
            if secid not in out:
                out[secid] = {}
            item = summary if isinstance(summary, dict) else {"raw_summary": summary}
            out[secid][component_id] = item
    return out


def manifest_references(child_items):
    out = {}
    for item in child_items:
        out[item["component_id"]] = {
            "manifest_path": item.get("manifest_path"),
            "child_run_id": item.get("child_run_id"),
            "status": item.get("status"),
            "validation_status": item.get("validation_status"),
        }
    return out


def validate_final_scope(per_instrument, whitelist, excluded):
    observed = sorted(per_instrument.keys())
    if sorted([x.upper() for x in observed]) != sorted([x.upper() for x in whitelist]):
        return {"status": "fail", "observed": observed, "expected": whitelist}
    excluded_upper = {x.upper() for x in excluded}
    hits = [x for x in observed if str(x).upper() in excluded_upper]
    if hits:
        return {"status": "fail", "excluded_hits": hits}
    return {"status": "pass", "observed": observed, "expected": whitelist}


def validate_short_history_final(per_instrument):
    siu7 = per_instrument.get("SiU7") or {}
    components = {}
    status = "pass"
    for component_id, summary in siu7.items():
        value = summary.get("short_history_flag") if isinstance(summary, dict) else None
        components[component_id] = value
        if value is not True:
            status = "fail"
    return {"status": status, "SiU7": components}


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
    run_started_ts = utc_now_iso()
    run_id = "futures_daily_data_refresh_" + args.run_date + "_" + stable_id([args.snapshot_date, run_started_ts, ",".join(whitelist), args.from_date, args.till])
    outputs = output_paths(data_root, args.run_date)

    child_items = []
    child_manifests = {}
    final_status = "pass"
    blockers = []
    for component in COMPONENTS:
        item, manifest = run_component(root, data_root, component, args, whitelist, excluded)
        child_items.append(item)
        if item.get("status") != "pass":
            final_status = "fail"
            blockers.append(component["component_id"] + ":" + str(item.get("failure_reason") or "failed"))
            break
        child_manifests[component["component_id"]] = manifest

    per_instrument = merge_per_instrument(child_manifests)
    scope_check = validate_final_scope(per_instrument, whitelist, excluded) if child_manifests else {"status": "not_computed"}
    short_history_check = validate_short_history_final(per_instrument) if child_manifests else {"status": "not_computed"}
    if scope_check.get("status") != "pass":
        final_status = "fail"
        blockers.append("final_scope_check_failed")
    if short_history_check.get("status") != "pass":
        final_status = "fail"
        blockers.append("final_short_history_check_failed")

    manifest = {
        "schema_version": SCHEMA_DAILY_REFRESH_MANIFEST,
        "run_id": run_id,
        "run_date": args.run_date,
        "snapshot_date": args.snapshot_date,
        "refresh_from": args.from_date or None,
        "refresh_till": args.till or None,
        "started_ts": run_started_ts,
        "completed_ts": utc_now_iso(),
        "runner_whitelist_applied": whitelist,
        "excluded_instruments_confirmed": excluded,
        "component_execution_order": [x["component_id"] for x in COMPONENTS],
        "child_component_status": child_items,
        "child_manifest_references": manifest_references(child_items),
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
