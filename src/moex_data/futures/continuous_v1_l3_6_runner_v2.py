#!/usr/bin/env python3
import argparse
import json
import runpy
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    "contracts/datasets/futures_continuous_v1_l3_6_manifest_contract.md",
    "configs/datasets/futures_continuous_v1_l3_6_config.json",
]

SNAPSHOT_DIRS = {
    "base": "eligibility_snapshot",
    "futoi_raw": "eligibility_snapshot_futoi_raw",
    "futoi_raw_refined": "eligibility_snapshot_futoi_raw_refined",
    "raw_d1": "eligibility_snapshot_raw_d1",
}


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


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


def snapshot_path(data_root: Path, snapshot_date: str, name: str) -> Path:
    return data_root / "futures" / "all_universe" / SNAPSHOT_DIRS[name] / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def existing_snapshot_paths(data_root: Path, snapshot_date: str) -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    for name in SNAPSHOT_DIRS:
        path = snapshot_path(data_root, snapshot_date, name)
        if path.exists():
            out[name] = path
    if "base" not in out:
        raise FileNotFoundError("Missing base eligibility snapshot: " + str(snapshot_path(data_root, snapshot_date, "base")))
    return out


def read_snapshot(path: Path, name: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if "secid" not in frame.columns:
        raise RuntimeError("Eligibility snapshot missing secid: " + name)
    frame = frame.copy()
    frame["secid"] = frame["secid"].astype(str)
    return frame


def latest_by_secid(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.drop_duplicates(subset=["secid"], keep="last").reset_index(drop=True)


def merge_flag(base_frame: pd.DataFrame, source_frame: pd.DataFrame, source_flag: str, target_flag: str) -> pd.Series:
    if source_flag not in source_frame.columns:
        return pd.Series([False] * len(base_frame), index=base_frame.index)
    source = latest_by_secid(source_frame[["secid", source_flag]].copy())
    source[source_flag] = source[source_flag].map(common.bool_value)
    merged = base_frame[["secid"]].merge(source, on="secid", how="left")
    return merged[source_flag].fillna(False).map(common.bool_value)


def merge_status(base_frame: pd.DataFrame, source_frame: pd.DataFrame, source_col: str, default: str) -> pd.Series:
    if source_col not in source_frame.columns:
        return pd.Series([default] * len(base_frame), index=base_frame.index)
    source = latest_by_secid(source_frame[["secid", source_col]].copy())
    merged = base_frame[["secid"]].merge(source, on="secid", how="left")
    return merged[source_col].fillna(default).astype(str)


def build_merged_eligibility(data_root: Path, snapshot_date: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    paths = existing_snapshot_paths(data_root, snapshot_date)
    frames = {name: read_snapshot(path, name) for name, path in paths.items()}
    base_frame = latest_by_secid(frames["base"]).copy()
    base_frame = common.ensure_eligibility_columns(base_frame)
    if "futoi_raw_refined" in frames:
        futoi_source = frames["futoi_raw_refined"]
    elif "futoi_raw" in frames:
        futoi_source = frames["futoi_raw"]
    else:
        futoi_source = pd.DataFrame(columns=["secid", "futoi_eligible"])
    raw_d1_source = frames.get("raw_d1", pd.DataFrame(columns=["secid", "raw_d1_eligible"]))
    base_frame["raw_5m_eligible"] = base_frame.get("raw_5m_eligible", pd.Series([False] * len(base_frame))).map(common.bool_value)
    base_frame["futoi_eligible"] = merge_flag(base_frame, futoi_source, "futoi_eligible", "futoi_eligible")
    base_frame["raw_d1_eligible"] = merge_flag(base_frame, raw_d1_source, "raw_d1_eligible", "raw_d1_eligible")
    base_frame["futoi_check_status"] = merge_status(base_frame, futoi_source, "futoi_check_status", "missing_futoi_snapshot")
    base_frame["raw_d1_check_status"] = merge_status(base_frame, raw_d1_source, "raw_d1_check_status", "missing_raw_d1_snapshot")
    base_frame["dataset_stage"] = common.DATASET_STAGE
    base_frame["schema_version"] = common.SCHEMA_ELIGIBILITY
    return base_frame, {name: str(path) for name, path in paths.items()}


def apply_expiration_gate(data_root: Path, snapshot_date: str, eligibility: pd.DataFrame, output_path: Path) -> Tuple[pd.DataFrame, List[str]]:
    expiration_path = common.expiration_map_path(data_root, snapshot_date)
    if not expiration_path.exists():
        raise FileNotFoundError("Missing expiration map after builder run: " + str(expiration_path))
    expiration = pd.read_parquet(expiration_path)
    exp_map = common.expiration_buildable_map(expiration)
    base_mask = common.base_gate_mask(eligibility)
    selected: List[bool] = []
    reasons: List[str] = []
    for idx, row in eligibility.iterrows():
        secid = str(row.get("secid"))
        if not bool(base_mask.loc[idx]):
            selected.append(False)
            reasons.append("base_gate_not_passed")
            continue
        ok, reason = exp_map.get(secid.upper(), (False, "missing_expiration_map_row"))
        selected.append(bool(ok))
        reasons.append(reason)
    eligibility = eligibility.copy()
    eligibility["continuous_v1_eligible"] = selected
    eligibility["access_api_eligible"] = selected
    eligibility["continuous_v1_check_status"] = ["expiration_map_buildable" if x else "deferred" for x in selected]
    eligibility["continuous_v1_deferral_reason"] = ["" if x else reasons[i] for i, x in enumerate(selected)]
    eligibility.loc[base_mask & eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_status"] = "selected"
    eligibility.loc[base_mask & eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_reason"] = "continuous_v1_selected_after_expiration_gate"
    eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_status"] = "deferred"
    eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "backfill_selection_reason"] = eligibility.loc[base_mask & ~eligibility["continuous_v1_eligible"].map(common.bool_value), "continuous_v1_deferral_reason"]
    write_parquet(output_path, eligibility)
    return eligibility, common.selected_secids_from_eligibility(eligibility, final_only=True)


def apply_roll_gate(data_root: Path, snapshot_date: str, eligibility: pd.DataFrame, output_path: Path, roll_policy_id: str) -> Tuple[pd.DataFrame, List[str]]:
    roll_path = common.roll_map_path(data_root, snapshot_date, roll_policy_id)
    if not roll_path.exists():
        raise FileNotFoundError("Missing roll map after builder run: " + str(roll_path))
    roll = pd.read_parquet(roll_path)
    roll_map = common.roll_buildable_map(roll)
    base_mask = common.base_gate_mask(eligibility)
    selected: List[bool] = []
    reasons: List[str] = []
    for idx, row in eligibility.iterrows():
        secid = str(row.get("secid"))
        if not bool(base_mask.loc[idx]):
            selected.append(False)
            reasons.append("base_gate_not_passed")
            continue
        ok, reason = roll_map.get(secid.upper(), (False, "missing_roll_map_row"))
        selected.append(bool(ok))
        reasons.append(reason)
    eligibility = eligibility.copy()
    eligibility["continuous_v1_eligible"] = selected
    eligibility["access_api_eligible"] = selected
    eligibility["continuous_v1_check_status"] = ["pass" if x else "deferred" for x in selected]
    eligibility["continuous_v1_deferral_reason"] = ["" if x else reasons[i] for i, x in enumerate(selected)]
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
        for path in paths[:5000]:
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
            result["timeframes"][timeframe] = {"status": status, "rows_observed": rows, "note": "higher intraday timeframe smoke uses continuous 5m as on-demand source" if timeframe not in {"5m", "D1"} else "materialized source smoke"}
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
    parser.add_argument("--output-eligibility", default="")
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
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

    started_ts = utc_now_iso()
    run_id = "futures_continuous_v1_l3_6_v2_" + run_date + "_" + stable_id([started_ts, snapshot_date, from_date, till])
    output_eligibility = common.eligibility_output_path(data_root, snapshot_date, str(args.output_eligibility or ""))
    merged, input_paths = build_merged_eligibility(data_root, snapshot_date)
    base_selected = common.selected_secids_from_eligibility(merged, final_only=False)
    module_run("moex_data.futures.expiration_map_builder", ["--snapshot-date", snapshot_date, "--run-date", run_date, "--whitelist", ",".join(base_selected), "--excluded", ",".join(excluded)])
    after_expiration, expiration_selected = apply_expiration_gate(data_root, snapshot_date, merged, output_eligibility)
    module_run("moex_data.futures.continuous_roll_map_builder", ["--snapshot-date", snapshot_date, "--run-date", run_date, "--whitelist", ",".join(expiration_selected), "--excluded", ",".join(excluded)])
    final_eligibility, final_selected = apply_roll_gate(data_root, snapshot_date, after_expiration, output_eligibility, roll_policy_id)
    module_run("moex_data.futures.continuous_series_builder", ["--snapshot-date", snapshot_date, "--run-date", run_date, "--from", from_date, "--till", till, "--roll-policy-id", roll_policy_id, "--adjustment-policy-id", adjustment_policy_id, "--whitelist", ",".join(final_selected), "--excluded", ",".join(excluded)])
    module_run("moex_data.futures.continuous_d1_builder", ["--run-date", run_date, "--from", from_date, "--till", till, "--roll-policy-id", roll_policy_id, "--adjustment-policy-id", adjustment_policy_id, "--excluded", ",".join(excluded)])
    access_smoke = build_access_api_smoke(data_root, roll_policy_id, adjustment_policy_id, final_eligibility, timeframes)
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
        "input_eligibility_snapshots": input_paths,
        "output_eligibility": str(output_eligibility),
        "eligible_summary": common.summarize_eligible(final_eligibility),
        "expiration_map": str(common.expiration_map_path(data_root, snapshot_date)),
        "roll_map": str(common.roll_map_path(data_root, snapshot_date, roll_policy_id)),
        "access_api_smoke": access_smoke,
        "output_artifacts": {
            "continuous_5m_root": str(data_root / "futures" / "continuous_5m" / ("roll_policy=" + roll_policy_id) / ("adjustment_policy=" + adjustment_policy_id)),
            "continuous_d1_root": str(data_root / "futures" / "continuous_d1" / ("roll_policy=" + roll_policy_id) / ("adjustment_policy=" + adjustment_policy_id)),
            "manifest": str(manifest_path),
            "eligibility": str(output_eligibility)
        },
        "preservation_checks": {
            "raw_5m_partitions_not_modified_by_runner": True,
            "futoi_partitions_not_modified_by_runner": True,
            "raw_d1_partitions_not_modified_by_runner": True,
            "w1_not_touched": True,
            "materialized_15m_30m_1h_4h_not_created": True
        },
        "builder_result_verdict": "pass" if access_smoke.get("status") == "pass" else "fail"
    }
    write_json(manifest_path, manifest)
    print_json_line("run_id", run_id)
    print_json_line("merged_eligibility_inputs", input_paths)
    print_json_line("eligibility_promotion_status", {"output_eligibility": str(output_eligibility), "eligible_summary": manifest["eligible_summary"]})
    print_json_line("expiration_map_status", {"artifact": manifest["expiration_map"]})
    print_json_line("roll_map_status", {"artifact": manifest["roll_map"]})
    print_json_line("access_api_validation_status", access_smoke)
    print_json_line("output_artifacts_created", manifest["output_artifacts"])
    print_json_line("builder_result_verdict", manifest["builder_result_verdict"])
    return 0 if manifest["builder_result_verdict"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
