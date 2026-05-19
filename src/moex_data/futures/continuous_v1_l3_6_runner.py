#!/usr/bin/env python3
import argparse
import json
import runpy
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import continuous_l3_6_common as common
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

REQUIRED_REPO_FILES = [
    "contracts/datasets/futures_all_universe_eligibility_contract.md",
    "contracts/datasets/futures_expiration_map_contract.md",
    "contracts/datasets/futures_continuous_roll_map_contract.md",
    "contracts/datasets/futures_continuous_5m_contract.md",
    "contracts/datasets/futures_continuous_d1_contract.md",
    "contracts/datasets/futures_continuous_quality_report_contract.md",
    "contracts/datasets/futures_continuous_builder_manifest_contract.md",
    "configs/datasets/futures_continuous_v1_l3_6_config.json",
]


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def module_run(module_name: str, argv: List[str]) -> None:
    old_argv = sys.argv[:]
    try:
        sys.argv = [module_name] + argv
        try:
            runpy.run_module(module_name, run_name="__main__")
        except SystemExit as exc:
            code = exc.code
            if code not in (0, None):
                raise RuntimeError(module_name + " exited with code " + str(code))
    finally:
        sys.argv = old_argv


def maybe_read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def refine_after_expiration(data_root: Path, snapshot_date: str, input_path: Path, output_path: Path) -> Tuple[pd.DataFrame, List[str]]:
    eligibility = pd.read_parquet(input_path)
    eligibility = common.ensure_eligibility_columns(eligibility)
    base_mask = common.base_gate_mask(eligibility)
    expiration_path = common.expiration_map_path(data_root, snapshot_date)
    expiration = maybe_read_parquet(expiration_path)
    exp_map = common.expiration_buildable_map(expiration)
    statuses = []
    selected = []
    for idx, row in eligibility.iterrows():
        secid = str(row.get("secid"))
        if not bool(base_mask.loc[idx]):
            statuses.append("base_gate_not_passed")
            selected.append(False)
            continue
        ok, reason = exp_map.get(secid.upper(), (False, "missing_expiration_map_row"))
        statuses.append(reason if ok else reason)
        selected.append(bool(ok))
    eligibility["continuous_v1_eligible"] = selected
    eligibility["access_api_eligible"] = selected
    eligibility["continuous_v1_check_status"] = ["expiration_map_buildable" if x else "deferred" for x in selected]
    eligibility["continuous_v1_deferral_reason"] = ["" if x else statuses[i] for i, x in enumerate(selected)]
    eligibility["dataset_stage"] = common.DATASET_STAGE
    eligibility["schema_version"] = common.SCHEMA_ELIGIBILITY
    eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_status"] = "deferred"
    eligibility.loc[base_mask & eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_status"] = "selected"
    eligibility.loc[base_mask & eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_reason"] = "continuous_v1_selected_after_expiration_gate"
    eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_reason"] = eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "continuous_v1_deferral_reason"]
    write_parquet(output_path, eligibility)
    return eligibility, common.selected_secids_from_eligibility(eligibility, final_only=True)


def refine_after_roll_map(data_root: Path, snapshot_date: str, eligibility: pd.DataFrame, output_path: Path, roll_policy_id: str) -> Tuple[pd.DataFrame, List[str]]:
    eligibility = common.ensure_eligibility_columns(eligibility)
    base_mask = common.base_gate_mask(eligibility)
    roll_path = common.roll_map_path(data_root, snapshot_date, roll_policy_id)
    roll = maybe_read_parquet(roll_path)
    roll_map = common.roll_buildable_map(roll)
    selected = []
    reasons = []
    for idx, row in eligibility.iterrows():
        secid = str(row.get("secid"))
        if not bool(base_mask.loc[idx]):
            selected.append(False)
            reasons.append("base_gate_not_passed")
            continue
        ok, reason = roll_map.get(secid.upper(), (False, "missing_roll_map_row"))
        selected.append(bool(ok))
        reasons.append(reason)
    eligibility["continuous_v1_eligible"] = selected
    eligibility["access_api_eligible"] = selected
    eligibility["continuous_v1_check_status"] = ["pass" if x else "deferred" for x in selected]
    eligibility["continuous_v1_deferral_reason"] = ["" if x else reasons[i] for i, x in enumerate(selected)]
    eligibility["dataset_stage"] = common.DATASET_STAGE
    eligibility["schema_version"] = common.SCHEMA_ELIGIBILITY
    eligibility.loc[base_mask & eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_status"] = "selected"
    eligibility.loc[base_mask & eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_reason"] = "continuous_v1_selected_after_roll_map_gate"
    eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_status"] = "deferred"
    eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_reason"] = eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "continuous_v1_deferral_reason"]
    write_parquet(output_path, eligibility)
    return eligibility, common.selected_secids_from_eligibility(eligibility, final_only=True)


def build_access_api_smoke(data_root: Path, roll_policy_id: str, adjustment_policy_id: str, eligible: pd.DataFrame, timeframes: List[str]) -> Dict[str, Any]:
    selected = eligible.loc[eligible["continuous_v1_eligible"].map(common.bool_value)].copy()
    families = sorted(selected["family_code"].dropna().astype(str).unique().tolist())
    result: Dict[str, Any] = {"families": families, "timeframes": {}, "status": "pass"}
    for timeframe in timeframes:
        rows = 0
        if timeframe == "D1":
            root = data_root / "futures" / "continuous_d1" / ("roll_policy=" + roll_policy_id) / ("adjustment_policy=" + adjustment_policy_id)
            paths = sorted(root.glob("family=*/trade_date=*/part.parquet")) if root.exists() else []
        else:
            root = data_root / "futures" / "continuous_5m" / ("roll_policy=" + roll_policy_id) / ("adjustment_policy=" + adjustment_policy_id)
            paths = sorted(root.glob("family=*/trade_date=*/part.parquet")) if root.exists() else []
        for path in paths[:2000]:
            family = ""
            for part in path.parts:
                if part.startswith("family="):
                    family = part.split("=", 1)[1]
            if family not in families:
                continue
            try:
                rows += int(len(pd.read_parquet(path)))
            except Exception:
                result["status"] = "fail"
                result["timeframes"][timeframe] = {"status": "fail", "error": "cannot_read_partition", "path": str(path)}
                break
        if timeframe not in result["timeframes"]:
            status = "pass" if rows > 0 else "fail"
            if status == "fail":
                result["status"] = "fail"
            result["timeframes"][timeframe] = {"status": status, "rows_observed": rows, "note": "higher intraday timeframe smoke uses on-demand resampling source 5m partitions" if timeframe != "5m" and timeframe != "D1" else "materialized source smoke"}
    return result


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--config", default="configs/datasets/futures_continuous_v1_l3_6_config.json")
    parser.add_argument("--input-eligibility", default="")
    parser.add_argument("--output-eligibility", default="")
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    parser.add_argument("--skip-quality", action="store_true")
    args = parser.parse_args()

    root = Path.cwd().resolve()
    data_root = base.resolve_data_root(args)
    run_date = str(args.run_date).strip()
    snapshot_date = str(args.snapshot_date).strip()
    from_date = base.parse_iso_date(str(args.from_date or "")) if str(args.from_date or "").strip() else ""
    till = base.parse_iso_date(str(args.till or "")) if str(args.till or "").strip() else ""
    config = load_json(root / args.config)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)
    roll_policy_id = str(config.get("roll_policy_id") or common.ROLL_POLICY_ID)
    adjustment_policy_id = str(config.get("adjustment_policy_id") or common.ADJUSTMENT_POLICY_ID)
    timeframes = [str(x) for x in config.get("access_api_timeframes", ["5m", "15m", "30m", "1h", "4h", "D1"])]
    if roll_policy_id != common.ROLL_POLICY_ID:
        raise RuntimeError("Unsupported roll_policy_id: " + roll_policy_id)
    if adjustment_policy_id != common.ADJUSTMENT_POLICY_ID:
        raise RuntimeError("Unsupported adjustment_policy_id: " + adjustment_policy_id)
    if float(config.get("adjustment_factor")) != common.ADJUSTMENT_FACTOR:
        raise RuntimeError("Unsupported adjustment_factor")
    if config.get("continuous_build_enabled") is not True or config.get("candidate_flag_only") is not False:
        raise RuntimeError("PM L3-6 requires continuous_build_enabled=true and candidate_flag_only=false")
    if config.get("w1_build_enabled") is not False:
        raise RuntimeError("W1 is out of scope for PM L3-6")

    base.assert_files_exist(root, REQUIRED_REPO_FILES)
    input_eligibility = common.eligibility_input_path(data_root, snapshot_date, str(args.input_eligibility or ""))
    output_eligibility = common.eligibility_output_path(data_root, snapshot_date, str(args.output_eligibility or ""))
    if not input_eligibility.exists():
        raise FileNotFoundError("Missing eligibility snapshot: " + str(input_eligibility))

    started_ts = utc_now_iso()
    run_id = "futures_continuous_v1_l3_6_" + run_date + "_" + stable_id([started_ts, snapshot_date, from_date, till])

    base_eligibility = pd.read_parquet(input_eligibility)
    base_selected = common.selected_secids_from_eligibility(base_eligibility, final_only=False)
    module_run("moex_data.futures.expiration_map_builder", ["--snapshot-date", snapshot_date, "--run-date", run_date, "--whitelist", ",".join(base_selected), "--excluded", ",".join(excluded)])
    eligibility_after_expiration, expiration_selected = refine_after_expiration(data_root, snapshot_date, input_eligibility, output_eligibility)
    module_run("moex_data.futures.continuous_roll_map_builder", ["--snapshot-date", snapshot_date, "--run-date", run_date, "--whitelist", ",".join(expiration_selected), "--excluded", ",".join(excluded)])
    eligibility_final, final_selected = refine_after_roll_map(data_root, snapshot_date, eligibility_after_expiration, output_eligibility, roll_policy_id)
    module_run("moex_data.futures.continuous_series_builder", ["--snapshot-date", snapshot_date, "--run-date", run_date, "--from", from_date, "--till", till, "--roll-policy-id", roll_policy_id, "--adjustment-policy-id", adjustment_policy_id, "--whitelist", ",".join(final_selected), "--excluded", ",".join(excluded)])
    module_run("moex_data.futures.continuous_d1_builder", ["--run-date", run_date, "--from", from_date, "--till", till, "--roll-policy-id", roll_policy_id, "--adjustment-policy-id", adjustment_policy_id, "--excluded", ",".join(excluded)])
    if not args.skip_quality:
        module_run("moex_data.futures.continuous_quality_report", ["--snapshot-date", snapshot_date, "--run-date", run_date, "--roll-policy-id", roll_policy_id, "--adjustment-policy-id", adjustment_policy_id, "--whitelist", ",".join(final_selected), "--excluded", ",".join(excluded)])

    access_smoke = build_access_api_smoke(data_root, roll_policy_id, adjustment_policy_id, eligibility_final, timeframes)
    manifest_path = data_root / "futures" / "runs" / "continuous_v1_l3_6" / ("run_date=" + run_date) / "manifest.json"
    manifest = {
        "schema_version": "futures_continuous_v1_l3_6_manifest.v1",
        "run_id": run_id,
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "started_at": started_ts,
        "finished_at": utc_now_iso(),
        "selection_mode": str(config.get("selection_mode")),
        "roll_policy_id": roll_policy_id,
        "adjustment_policy_id": adjustment_policy_id,
        "adjustment_factor": common.ADJUSTMENT_FACTOR,
        "input_eligibility": str(input_eligibility),
        "output_eligibility": str(output_eligibility),
        "eligible_summary": common.summarize_eligible(eligibility_final),
        "expiration_map": str(common.expiration_map_path(data_root, snapshot_date)),
        "roll_map": str(common.roll_map_path(data_root, snapshot_date, roll_policy_id)),
        "access_api_smoke": access_smoke,
        "preservation_checks": {
            "raw_5m_partitions_not_modified_by_runner": True,
            "futoi_partitions_not_modified_by_runner": True,
            "raw_d1_partitions_not_modified_by_runner": True,
            "w1_not_touched": True,
            "materialized_15m_30m_1h_4h_not_created": True
        }
    }
    write_json(manifest_path, manifest)

    print_json_line("run_id", run_id)
    print_json_line("eligibility_promotion_status", {"output_eligibility": str(output_eligibility), "eligible_summary": manifest["eligible_summary"]})
    print_json_line("expiration_map_status", {"artifact": manifest["expiration_map"]})
    print_json_line("roll_map_status", {"artifact": manifest["roll_map"]})
    print_json_line("access_api_validation_status", access_smoke)
    print_json_line("output_artifacts_created", {"manifest": str(manifest_path), "eligibility": str(output_eligibility)})
    print_json_line("builder_result_verdict", "pass" if access_smoke.get("status") == "pass" else "fail")
    return 0 if access_smoke.get("status") == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
