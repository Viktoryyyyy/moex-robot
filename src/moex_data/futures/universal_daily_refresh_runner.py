#!/usr/bin/env python3
import argparse
import copy
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from moex_data.futures import liquidity_history_metrics_probe as base

PROJECT_ENV_PATH = Path(__file__).resolve().parents[4] / ".env"
SCHEMA_UNIVERSAL_DAILY_REFRESH_MANIFEST = "futures_universal_daily_refresh_manifest.v1"
ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0

CANONICAL_STAGE_IDS = [
    "registry_refresh",
    "all_universe_eligibility_snapshot",
    "raw_5m_refresh",
    "futoi_raw_refresh",
    "raw_d1_derivation",
    "continuous_eligibility_refinement",
    "expiration_map",
    "roll_map",
    "continuous_5m",
    "continuous_d1",
    "continuous_w1",
    "quality_reports",
    "unified_manifest",
]

REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_universal_daily_refresh_manifest_contract.md",
    "contracts/datasets/futures_daily_refresh_scheduler_contract.md",
    "contracts/datasets/futures_all_universe_snapshot_contract.md",
    "contracts/datasets/futures_all_universe_eligibility_contract.md",
    "contracts/datasets/futures_futoi_availability_report_contract.md",
    "configs/datasets/futures_all_universe_eligibility_config.json",
]

DATE_WINDOW_FORWARD_STAGE_IDS = {
    "futoi_raw_refresh",
    "raw_d1_derivation",
    "continuous_5m",
    "continuous_d1",
}

STAGES = {
    "registry_refresh": {
        "component_id": "registry_refresh_runner",
        "script": "src/moex_data/futures/registry_refresh_runner.py",
        "kind": "command",
    },
    "all_universe_eligibility_snapshot": {
        "component_id": "all_universe_eligibility_snapshot_runner",
        "script": "src/moex_data/futures/all_universe_eligibility_snapshot_runner.py",
        "kind": "command",
    },
    "raw_5m_refresh": {
        "component_id": "all_universe_raw_5m_backfill_slice",
        "script": "src/moex_data/futures/all_universe_raw_5m_backfill_slice.py",
        "kind": "command",
    },
    "futoi_raw_refresh": {
        "component_id": "canonical_all_universe_futoi_raw_refresh",
        "script": "src/moex_data/futures/all_universe_futoi_raw_backfill_slice.py",
        "kind": "command",
    },
    "raw_d1_derivation": {
        "component_id": "derived_d1_ohlcv_builder",
        "script": "src/moex_data/futures/derived_d1_ohlcv_builder.py",
        "kind": "command",
    },
    "continuous_eligibility_refinement": {
        "component_id": "canonical_continuous_eligibility_refinement",
        "script": "",
        "kind": "metadata_gate",
    },
    "expiration_map": {
        "component_id": "expiration_map_builder",
        "script": "src/moex_data/futures/expiration_map_builder.py",
        "kind": "command",
    },
    "roll_map": {
        "component_id": "continuous_roll_map_builder",
        "script": "src/moex_data/futures/continuous_roll_map_builder.py",
        "kind": "command",
    },
    "continuous_5m": {
        "component_id": "continuous_series_builder",
        "script": "src/moex_data/futures/continuous_series_builder.py",
        "kind": "command",
    },
    "continuous_d1": {
        "component_id": "continuous_d1_builder",
        "script": "src/moex_data/futures/continuous_d1_builder.py",
        "kind": "command",
    },
    "continuous_w1": {
        "component_id": "continuous_w1_builder",
        "script": "src/moex_data/futures/continuous_w1_builder.py",
        "kind": "family_command",
    },
    "quality_reports": {
        "component_id": "canonical_universal_quality_reports",
        "script": "",
        "kind": "metadata_gate",
    },
    "unified_manifest": {
        "component_id": "unified_manifest",
        "script": "",
        "kind": "manifest_write",
    },
}


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def today_msk():
    return base.today_msk()


def output_paths(data_root, run_date):
    return {
        "manifest": str(data_root / "futures" / "runs" / "universal_daily_refresh" / ("run_date=" + run_date) / "manifest.json")
    }


def prerequisite_paths(data_root, snapshot_date):
    return {
        "registry_snapshot": str(data_root / "futures" / "all_universe" / "registry_snapshot" / ("snapshot_date=" + snapshot_date) / "registry_snapshot.parquet"),
        "eligibility_snapshot": str(data_root / "futures" / "all_universe" / "eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"),
        "futoi_availability_report": str(data_root / "futures" / "availability" / ("snapshot_date=" + snapshot_date) / "futures_futoi_availability_report.parquet"),
    }


def parse_family_or_secid(value):
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


def validate_stage_name(value):
    text = str(value or "").strip()
    if text and text not in CANONICAL_STAGE_IDS:
        raise RuntimeError("Unsupported stage: " + text)
    return text


def command_for_stage(root, stage_id, args):
    stage = STAGES[stage_id]
    cmd = [sys.executable, str(root / stage["script"])]
    if stage_id in {"registry_refresh", "all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh", "raw_d1_derivation", "expiration_map", "roll_map", "continuous_5m"}:
        cmd.extend(["--snapshot-date", args.snapshot_date])
    if stage_id not in {"continuous_eligibility_refinement", "quality_reports", "unified_manifest"}:
        cmd.extend(["--run-date", args.run_date])
    if stage_id in DATE_WINDOW_FORWARD_STAGE_IDS:
        if args.from_date:
            cmd.extend(["--from", args.from_date])
        if args.till:
            cmd.extend(["--till", args.till])
    if stage_id == "continuous_w1":
        w1_from_date, w1_till = w1_date_window(args)
        cmd.extend(["--from", w1_from_date, "--till", w1_till])
    if stage_id in {"all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh"}:
        cmd.extend(["--selection-mode", "rfud_included_universe"])
    if stage_id == "futoi_raw_refresh":
        if args.family:
            cmd.extend(["--family", args.family])
        if args.secid:
            cmd.extend(["--secid", args.secid])
    if stage_id == "continuous_w1":
        if args.family:
            cmd.extend(["--family", args.family])
    if stage_id in {"registry_refresh", "all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh", "raw_d1_derivation", "expiration_map", "roll_map", "continuous_5m", "continuous_d1", "continuous_w1"}:
        cmd.extend(["--data-root", str(args.data_root_resolved)])
    if stage_id in {"registry_refresh", "all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh", "roll_map"}:
        cmd.extend(["--timeout", str(args.timeout)])
    if stage_id in {"registry_refresh", "all_universe_eligibility_snapshot", "raw_5m_refresh", "futoi_raw_refresh", "roll_map"}:
        cmd.extend(["--iss-base-url", args.iss_base_url])
    if stage_id == "registry_refresh":
        cmd.extend(["--availability-max-workers", str(args.availability_max_workers)])
    if stage_id in {"raw_5m_refresh", "futoi_raw_refresh"}:
        cmd.extend(["--apim-base-url", args.apim_base_url])
    if stage_id in {"continuous_5m", "continuous_d1", "continuous_w1"}:
        cmd.extend(["--roll-policy-id", ROLL_POLICY_ID, "--adjustment-policy-id", ADJUSTMENT_POLICY_ID])
    return cmd


def run_command_stage(root, stage_id, args):
    stage = STAGES[stage_id]
    item = {
        "stage_id": stage_id,
        "component_id": stage["component_id"],
        "status": "fail",
        "validation_status": "not_validated",
    }
    if not stage.get("script"):
        item["failure_reason"] = "missing script for command stage"
        item["validation_status"] = "fail"
        return item
    cmd = command_for_stage(root, stage_id, args)
    started_at = time.time()
    proc = subprocess.run(cmd, cwd=str(root), text=True, capture_output=True)
    completed_at = time.time()
    item.update({
        "command": cmd,
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
        "started_epoch": started_at,
        "completed_epoch": completed_at,
        "duration_sec": round(completed_at - started_at, 3),
    })
    if proc.returncode != 0:
        item["failure_reason"] = "component_returncode_nonzero"
        item["validation_status"] = "fail"
        return item
    item["status"] = "pass"
    item["validation_status"] = "pass"
    return item


def parse_json_line_output(text):
    parsed = {}
    for raw in str(text or "").splitlines():
        if ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if not value:
            continue
        try:
            parsed[key] = json.loads(value)
        except Exception:
            continue
    return parsed


def family_from_path(value):
    for part in Path(str(value)).parts:
        if part.startswith("family="):
            family = part[len("family="):].strip()
            if family:
                return family
    return ""


def ordered_unique(values):
    out = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def week_start_for_date(value):
    day = datetime.strptime(str(value), "%Y-%m-%d").date()
    return (day - timedelta(days=day.weekday())).isoformat()


def date_values(from_date, till):
    start = datetime.strptime(str(from_date), "%Y-%m-%d").date()
    end = datetime.strptime(str(till), "%Y-%m-%d").date()
    if start > end:
        raise RuntimeError("W1 --from is after --till")
    out = []
    day = start
    while day <= end:
        out.append(day.isoformat())
        day = day + timedelta(days=1)
    return out


def w1_date_window(args):
    return args.from_date or week_start_for_date(args.run_date), args.till or args.run_date


def continuous_d1_partition_path(data_root, family, trade_date):
    return Path(data_root) / "futures" / "continuous_d1" / ("roll_policy=" + ROLL_POLICY_ID) / ("adjustment_policy=" + ADJUSTMENT_POLICY_ID) / ("family=" + str(family)) / ("trade_date=" + str(trade_date)) / "part.parquet"


def filter_w1_families_by_existing_d1_partitions(families, args):
    from_date, till = w1_date_window(args)
    dates = date_values(from_date, till)
    kept = []
    skipped = []
    for family in families:
        if any(continuous_d1_partition_path(args.data_root_resolved, family, trade_date).is_file() for trade_date in dates):
            kept.append(family)
        else:
            skipped.append(family)
    if not kept:
        raise RuntimeError("accepted_continuous_d1_family_discovery_no_source_partitions_for_w1")
    return kept, skipped


def accepted_continuous_d1_item(items):
    for item in reversed(items):
        if item.get("stage_id") == "continuous_d1":
            if item.get("status") == "pass" and item.get("validation_status") == "pass" and int(item.get("returncode", 1)) == 0:
                return item
            return None
    return None


def discover_w1_families_from_accepted_d1(items):
    d1_item = accepted_continuous_d1_item(items)
    if d1_item is None:
        raise RuntimeError("accepted_continuous_d1_child_status_missing")
    parsed = parse_json_line_output(d1_item.get("stdout_tail", ""))
    families = []
    summary = parsed.get("continuous_d1_summary")
    if isinstance(summary, dict):
        families.extend(summary.get("families") or [])
    artifacts = parsed.get("output_artifacts_created")
    if isinstance(artifacts, dict):
        paths = artifacts.get("continuous_d1_partitions_created") or []
        if not isinstance(paths, list):
            paths = []
        families.extend([family_from_path(path) for path in paths])
    families = ordered_unique(families)
    if not families:
        raise RuntimeError("accepted_continuous_d1_family_discovery_empty")
    return families


def run_family_command_stage(root, stage_id, args, items):
    started_at = time.time()
    stage = STAGES[stage_id]
    item = {
        "stage_id": stage_id,
        "component_id": stage["component_id"],
        "status": "fail",
        "validation_status": "fail",
        "family_discovery_source": "accepted_continuous_d1_child_output",
        "family_results": [],
        "duration_sec": 0.0,
    }
    try:
        if args.family:
            explicit = parse_family_or_secid(args.family)
            families = explicit if explicit else discover_w1_families_from_accepted_d1(items)
            item["family_discovery_source"] = "debug_family_filter"
        else:
            families = discover_w1_families_from_accepted_d1(items)
        if stage_id == "continuous_w1":
            item["families_before_d1_partition_filter"] = families
            families, skipped = filter_w1_families_by_existing_d1_partitions(families, args)
            item["families_skipped_no_d1_source_partition"] = skipped
        item["families"] = families
    except Exception as exc:
        item["failure_reason"] = "canonical_family_discovery_from_accepted_continuous_d1_failed:" + str(exc)
        item["blocker_class"] = "canonical_family_discovery_failed"
        item["duration_sec"] = round(time.time() - started_at, 3)
        return item
    if not families:
        item["failure_reason"] = "canonical_family_discovery_from_accepted_continuous_d1_empty"
        item["blocker_class"] = "canonical_family_discovery_empty"
        item["duration_sec"] = round(time.time() - started_at, 3)
        return item
    family_failures = []
    for family in families:
        family_args = copy.copy(args)
        family_args.family = family
        child = run_command_stage(root, stage_id, family_args)
        child["family_code"] = family
        item["family_results"].append(child)
        if child.get("status") != "pass" or child.get("validation_status") != "pass":
            family_failures.append(family + ":" + str(child.get("failure_reason") or "failed"))
    if family_failures:
        item["failure_reason"] = "continuous_w1_family_execution_failed"
        item["blocker_class"] = "continuous_w1_family_execution_failed"
        item["family_failures"] = family_failures
        item["duration_sec"] = round(time.time() - started_at, 3)
        return item
    item["status"] = "pass"
    item["validation_status"] = "pass"
    item["family_count"] = len(families)
    item["duration_sec"] = round(time.time() - started_at, 3)
    return item


def metadata_gate(stage_id):
    stage = STAGES[stage_id]
    return {
        "stage_id": stage_id,
        "component_id": stage["component_id"],
        "status": "pass",
        "validation_status": "pass",
        "duration_sec": 0.0,
        "gate_note": "metadata gate only; no universe semantics are changed here",
    }


def missing_component(stage_id):
    stage = STAGES[stage_id]
    return {
        "stage_id": stage_id,
        "component_id": stage["component_id"],
        "status": "fail",
        "validation_status": "fail",
        "duration_sec": 0.0,
        "failure_reason": stage.get("blocker") or "missing_canonical_component",
        "blocker_class": "canonical_component_missing",
    }


def preflight_planned_stages(planned_stage_order):
    for stage_id in planned_stage_order:
        stage = STAGES[stage_id]
        if stage.get("kind") == "missing_canonical_component":
            return {
                "stage_id": "preflight",
                "component_id": "universal_daily_refresh_preflight",
                "status": "fail",
                "validation_status": "fail",
                "blocked_stage_id": stage_id,
                "blocked_component_id": stage["component_id"],
                "failure_reason": stage.get("blocker") or "missing_canonical_component",
                "blocker_class": "canonical_component_missing",
                "preflight_scope": "planned_stage_order",
                "preflight_result": "known_missing_canonical_component_detected",
            }
    return None


def blocker_for_item(item):
    stage_id = item.get("blocked_stage_id") or item.get("stage_id")
    return str(stage_id) + ":" + str(item.get("failure_reason") or "failed")


def executed_stage_ids(items):
    return [x.get("stage_id") for x in items if x.get("stage_id") != "preflight"]


def stages_to_run(args):
    if args.stage:
        return [args.stage]
    out = []
    for stage_id in CANONICAL_STAGE_IDS:
        out.append(stage_id)
        if args.stop_after and stage_id == args.stop_after:
            break
    return out


def write_manifest(path, manifest):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def main():
    if load_dotenv is not None:
        load_dotenv(PROJECT_ENV_PATH, override=False)
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--availability-max-workers", type=int, default=int(os.getenv("MOEX_AVAILABILITY_MAX_WORKERS", "4")))
    parser.add_argument("--reuse-prerequisites", action="store_true")
    parser.add_argument("--stop-after", default="")
    parser.add_argument("--stage", default="")
    parser.add_argument("--family", default="")
    parser.add_argument("--secid", default="")
    args = parser.parse_args()

    args.stop_after = validate_stage_name(args.stop_after)
    args.stage = validate_stage_name(args.stage)
    root = Path.cwd().resolve()
    args.data_root_resolved = base.resolve_data_root(args)
    data_root = args.data_root_resolved
    base.assert_files_exist(root, REQUIRED_CONTRACTS)

    family_filter = parse_family_or_secid(args.family)
    secid_filter = parse_family_or_secid(args.secid)
    debug_scope = {
        "reuse_prerequisites": bool(args.reuse_prerequisites),
        "stop_after": args.stop_after or None,
        "stage": args.stage or None,
        "family": family_filter,
        "secid": secid_filter,
        "from": args.from_date or None,
        "till": args.till or None,
        "availability_max_workers": int(args.availability_max_workers),
        "semantics_effect": "orchestration_only_no_universe_or_eligibility_redefinition",
    }

    started_ts = utc_now_iso()
    run_id = "futures_universal_daily_refresh_" + args.run_date + "_" + base.stable_id([
        args.snapshot_date,
        started_ts,
        args.from_date,
        args.till,
        args.stage,
        args.stop_after,
        ",".join(family_filter),
        ",".join(secid_filter),
    ])

    planned_stage_order = stages_to_run(args)
    run_started_epoch = time.time()
    items = []
    blockers = []
    final_status = "pass"

    preflight_item = None
    if not args.stage:
        preflight_item = preflight_planned_stages(planned_stage_order)
    if preflight_item is not None:
        items.append(preflight_item)
        final_status = "fail"
        blockers.append(blocker_for_item(preflight_item))
    else:
        for stage_id in planned_stage_order:
            kind = STAGES[stage_id]["kind"]
            if kind == "command":
                item = run_command_stage(root, stage_id, args)
            elif kind == "family_command":
                item = run_family_command_stage(root, stage_id, args, items)
            elif kind == "metadata_gate":
                item = metadata_gate(stage_id)
            elif kind == "missing_canonical_component":
                item = missing_component(stage_id)
            elif kind == "manifest_write":
                item = metadata_gate(stage_id)
            else:
                item = missing_component(stage_id)
                item["failure_reason"] = "unsupported_stage_kind:" + str(kind)
            items.append(item)
            if item.get("status") != "pass":
                final_status = "fail"
                blockers.append(blocker_for_item(item))
                break

    stage_duration_summary = {str(x.get("stage_id")): x.get("duration_sec") for x in items if x.get("stage_id")}
    registry_child_duration_summary = {}
    availability_probe_timing_summary = {}
    for item in items:
        if item.get("stage_id") == "registry_refresh":
            parsed = parse_json_line_output(item.get("stdout_tail", ""))
            registry_child_duration_summary = parsed.get("child_duration_summary") or {}
            availability_probe_timing_summary = parsed.get("availability_probe_timing_summary") or {}
            if not availability_probe_timing_summary:
                registry_manifest_path = args.data_root_resolved / "futures" / "runs" / "registry_refresh" / ("run_date=" + args.run_date) / "manifest.json"
                if registry_manifest_path.is_file():
                    registry_manifest = json.loads(registry_manifest_path.read_text(encoding="utf-8"))
                    registry_child_duration_summary = registry_child_duration_summary or registry_manifest.get("child_duration_summary") or {}
                    availability_probe_timing_summary = registry_manifest.get("availability_probe_timing_summary") or {}
    outputs = output_paths(data_root, args.run_date)
    manifest = {
        "schema_version": SCHEMA_UNIVERSAL_DAILY_REFRESH_MANIFEST,
        "run_id": run_id,
        "run_date": args.run_date,
        "snapshot_date": args.snapshot_date,
        "refresh_from": args.from_date or None,
        "refresh_till": args.till or None,
        "started_ts": started_ts,
        "completed_ts": utc_now_iso(),
        "total_duration_sec": round(time.time() - run_started_epoch, 3),
        "canonical_stage_order": CANONICAL_STAGE_IDS,
        "planned_stage_order": planned_stage_order,
        "executed_stage_order": executed_stage_ids(items),
        "stage_duration_summary": stage_duration_summary,
        "registry_child_duration_summary": registry_child_duration_summary,
        "availability_probe_timing_summary": availability_probe_timing_summary,
        "debug_controls": debug_scope,
        "selection_model": "eligibility_snapshot_driven",
        "futoi_selection_model": "eligibility_snapshot_driven_futoi_eligible_true",
        "slice1_whitelist_semantics": "forbidden_as_canonical_scope",
        "prerequisite_artifacts": prerequisite_paths(data_root, args.snapshot_date),
        "roll_policy_id": ROLL_POLICY_ID,
        "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
        "adjustment_factor": ADJUSTMENT_FACTOR,
        "child_component_status": items,
        "artifact_validation_status": "pass" if final_status == "pass" else "fail",
        "universal_daily_refresh_result_verdict": final_status,
        "blockers": blockers,
        "output_artifacts": outputs,
    }
    write_manifest(outputs["manifest"], manifest)
    print(json.dumps({
        "universal_daily_refresh_manifest_path": outputs["manifest"],
        "universal_daily_refresh_result_verdict": final_status,
        "blockers": blockers,
    }, ensure_ascii=False, sort_keys=True))
    return 0 if final_status == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)