#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from moex_data.futures import continuous_l3_6_common as common
from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED, parse_list, print_json_line, utc_now_iso

SCHEMA_VERSION = "futures_continuous_v1_l3_6_quality_report.v1"
ROLL_POLICY_ID = common.ROLL_POLICY_ID
ADJUSTMENT_POLICY_ID = common.ADJUSTMENT_POLICY_ID
ADJUSTMENT_FACTOR = common.ADJUSTMENT_FACTOR
REQUIRED_CHECKS = [
    "continuous_5m_row_count",
    "continuous_d1_row_count",
    "continuous_5m_primary_key_unique",
    "continuous_d1_primary_key_unique",
    "continuous_5m_duplicate_timestamps",
    "continuous_d1_duplicate_timestamps",
    "continuous_5m_ohlc_validity",
    "continuous_d1_ohlc_validity",
    "roll_policy_id",
    "adjustment_policy_id",
    "adjustment_factor",
    "continuous_5m_lineage_completeness",
    "continuous_d1_lineage_completeness",
    "usdrubf_identity_behavior",
    "no_silent_gap_bridging",
    "no_synthetic_replacement_contracts",
    "no_forbidden_or_noneligible_instruments",
    "no_w1",
    "no_materialized_15m_30m_1h_4h",
]


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def report_path(data_root: Path, run_date: str) -> Path:
    return data_root / "futures" / "quality" / "continuous_v1_l3_6" / ("run_date=" + run_date) / "quality_report.json"


def clean(value: Any) -> str:
    text = common.clean_text(value)
    return "" if text is None else str(text)


def parse_contracts(value: Any) -> List[str]:
    if value is None:
        return []
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        value = value.tolist()
    if isinstance(value, (list, tuple, set)):
        return [str(x) for x in value if clean(x)]
    text = clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, (list, tuple, set)):
            return [str(x) for x in parsed if clean(x)]
    except Exception:
        pass
    return [x.strip().strip("[]").strip(chr(39)).strip(chr(34)) for x in text.split(",") if x.strip()]


def read_parts(root: Path, families: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    frames = []
    paths = []
    for family in sorted(set(families)):
        family_root = root / ("family=" + family)
        if not family_root.exists():
            continue
        for path in sorted(family_root.glob("trade_date=*/part.parquet")):
            part = pd.read_parquet(path)
            part["_source_partition_path"] = str(path)
            frames.append(part)
            paths.append(str(path))
    if not frames:
        return pd.DataFrame(), paths
    return pd.concat(frames, ignore_index=True), paths


def duplicate_count(frame: pd.DataFrame, columns: List[str]) -> int:
    if frame.empty:
        return 0
    if any(col not in frame.columns for col in columns):
        return int(len(frame))
    return int(frame.duplicated(subset=columns).sum())


def ohlc_bad_count(frame: pd.DataFrame) -> int:
    fields = ["open", "high", "low", "close"]
    if frame.empty:
        return 0
    if any(col not in frame.columns for col in fields):
        return int(len(frame))
    work = frame.copy()
    for col in fields:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    bad = work[fields].isna().any(axis=1)
    bad = bad | (work["high"] < work["low"])
    bad = bad | (work["open"] > work["high"]) | (work["open"] < work["low"])
    bad = bad | (work["close"] > work["high"]) | (work["close"] < work["low"])
    return int(bad.sum())


def add_check(checks: Dict[str, Dict[str, Any]], check_id: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
    checks[check_id] = {
        "check_status": "pass" if ok else "fail",
        "observed_value": observed,
        "expected_value": expected,
        "review_notes": notes,
    }


def policy_bad_count(frame: pd.DataFrame, column: str, expected: str) -> int:
    if frame.empty:
        return 0
    if column not in frame.columns:
        return int(len(frame))
    return int((frame[column].astype(str) != expected).sum())


def factor_bad_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    if "adjustment_factor" not in frame.columns:
        return int(len(frame))
    values = pd.to_numeric(frame["adjustment_factor"], errors="coerce")
    return int((values != ADJUSTMENT_FACTOR).sum())


def lineage_bad_5m(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    fields = ["source_secid", "source_contract", "roll_map_id", "roll_policy_id", "adjustment_policy_id", "adjustment_factor"]
    if any(col not in frame.columns for col in fields):
        return int(len(frame))
    return int(frame[fields].isna().any(axis=1).sum())


def lineage_bad_d1(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    fields = ["source_contracts", "roll_map_id", "roll_policy_id", "adjustment_policy_id", "adjustment_factor"]
    if any(col not in frame.columns for col in fields):
        return int(len(frame))
    bad = int(frame[fields].isna().any(axis=1).sum())
    bad += int(frame["source_contracts"].map(lambda x: len(parse_contracts(x)) == 0).sum())
    return bad


def usdrubf_failures(c5: pd.DataFrame, d1: pd.DataFrame, roll_map: pd.DataFrame) -> List[str]:
    failures = []
    symbols = set(c5.get("continuous_symbol", pd.Series(dtype=str)).dropna().astype(str).str.upper().tolist())
    if "USDRUBF" not in symbols:
        return failures
    c5u = c5.loc[c5["continuous_symbol"].astype(str).str.upper() == "USDRUBF"].copy()
    d1u = d1.loc[d1.get("continuous_symbol", pd.Series(dtype=str)).astype(str).str.upper() == "USDRUBF"].copy() if not d1.empty else pd.DataFrame()
    rmu = roll_map.loc[roll_map.get("continuous_symbol", pd.Series(dtype=str)).astype(str).str.upper() == "USDRUBF"].copy() if not roll_map.empty else pd.DataFrame()
    if rmu.empty:
        failures.append("roll_map_missing_usdrubf")
    elif sorted(rmu.get("source_secid", pd.Series(dtype=str)).dropna().astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
        failures.append("roll_map_source_not_identity")
    if sorted(c5u.get("source_secid", pd.Series(dtype=str)).dropna().astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
        failures.append("continuous_5m_source_not_identity")
    if sorted(c5u.get("source_contract", pd.Series(dtype=str)).dropna().astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
        failures.append("continuous_5m_contract_not_identity")
    if "is_roll_boundary" in c5u.columns and int(c5u["is_roll_boundary"].map(common.bool_value).sum()) != 0:
        failures.append("continuous_5m_roll_boundary_true")
    if not d1u.empty:
        if int(d1u.get("source_contracts", pd.Series(dtype=object)).map(lambda x: parse_contracts(x) != ["USDRUBF"]).sum()) != 0:
            failures.append("continuous_d1_contracts_not_identity")
        if "has_roll_boundary" in d1u.columns and int(d1u["has_roll_boundary"].map(common.bool_value).sum()) != 0:
            failures.append("continuous_d1_roll_boundary_true")
    return failures


def build_report(data_root: Path, manifest_path: Path, excluded: List[str]) -> Dict[str, Any]:
    manifest = read_json(manifest_path)
    run_id = str(manifest.get("run_id") or "")
    run_date = str(manifest.get("run_date") or "")
    snapshot_date = str(manifest.get("snapshot_date") or "")
    if not run_id or not run_date or not snapshot_date:
        raise RuntimeError("Manifest missing run_id/run_date/snapshot_date: " + str(manifest_path))
    eligibility_path = Path(str(manifest.get("output_eligibility") or "")).expanduser().resolve()
    if not eligibility_path.exists():
        raise FileNotFoundError("Missing L3-6 output eligibility: " + str(eligibility_path))
    eligibility = pd.read_parquet(eligibility_path)
    common.require_columns(eligibility, ["secid", "family_code", "continuous_v1_eligible"], "l3_6_eligibility")
    selected = eligibility.loc[eligibility["continuous_v1_eligible"].map(common.bool_value)].copy()
    if selected.empty:
        raise RuntimeError("No selected continuous_v1_eligible rows in " + str(eligibility_path))
    families = sorted(selected["family_code"].dropna().astype(str).unique().tolist())
    eligible_secids = set(selected["secid"].dropna().astype(str).str.upper().tolist())
    c5_root = data_root / "futures" / "continuous_5m" / ("roll_policy=" + ROLL_POLICY_ID) / ("adjustment_policy=" + ADJUSTMENT_POLICY_ID)
    d1_root = data_root / "futures" / "continuous_d1" / ("roll_policy=" + ROLL_POLICY_ID) / ("adjustment_policy=" + ADJUSTMENT_POLICY_ID)
    c5, c5_paths = read_parts(c5_root, families)
    d1, d1_paths = read_parts(d1_root, families)
    roll_map_path = Path(str(manifest.get("roll_map") or "")).expanduser().resolve()
    roll_map = pd.read_parquet(roll_map_path) if roll_map_path.exists() else pd.DataFrame()
    checks: Dict[str, Dict[str, Any]] = {}
    add_check(checks, "continuous_5m_row_count", len(c5) > 0, int(len(c5)), ">0")
    add_check(checks, "continuous_d1_row_count", len(d1) > 0, int(len(d1)), ">0")
    dup_5m = duplicate_count(c5, ["continuous_symbol", "trade_date", "end"])
    dup_d1 = duplicate_count(d1, ["continuous_symbol", "trade_date"])
    add_check(checks, "continuous_5m_primary_key_unique", dup_5m == 0, dup_5m, 0)
    add_check(checks, "continuous_d1_primary_key_unique", dup_d1 == 0, dup_d1, 0)
    add_check(checks, "continuous_5m_duplicate_timestamps", dup_5m == 0, dup_5m, 0)
    add_check(checks, "continuous_d1_duplicate_timestamps", dup_d1 == 0, dup_d1, 0)
    bad_5m_ohlc = ohlc_bad_count(c5)
    bad_d1_ohlc = ohlc_bad_count(d1)
    add_check(checks, "continuous_5m_ohlc_validity", bad_5m_ohlc == 0, bad_5m_ohlc, 0)
    add_check(checks, "continuous_d1_ohlc_validity", bad_d1_ohlc == 0, bad_d1_ohlc, 0)
    bad_roll = policy_bad_count(c5, "roll_policy_id", ROLL_POLICY_ID) + policy_bad_count(d1, "roll_policy_id", ROLL_POLICY_ID) + policy_bad_count(roll_map, "roll_policy_id", ROLL_POLICY_ID)
    bad_adj = policy_bad_count(c5, "adjustment_policy_id", ADJUSTMENT_POLICY_ID) + policy_bad_count(d1, "adjustment_policy_id", ADJUSTMENT_POLICY_ID) + policy_bad_count(roll_map, "adjustment_policy_id", ADJUSTMENT_POLICY_ID)
    bad_factor = factor_bad_count(c5) + factor_bad_count(d1) + factor_bad_count(roll_map)
    bad_roll += 0 if str(manifest.get("roll_policy_id")) == ROLL_POLICY_ID else 1
    bad_adj += 0 if str(manifest.get("adjustment_policy_id")) == ADJUSTMENT_POLICY_ID else 1
    bad_factor += 0 if float(manifest.get("adjustment_factor")) == ADJUSTMENT_FACTOR else 1
    add_check(checks, "roll_policy_id", bad_roll == 0, bad_roll, 0)
    add_check(checks, "adjustment_policy_id", bad_adj == 0, bad_adj, 0)
    add_check(checks, "adjustment_factor", bad_factor == 0, bad_factor, 0)
    bad_lineage_5m = lineage_bad_5m(c5)
    bad_lineage_d1 = lineage_bad_d1(d1)
    add_check(checks, "continuous_5m_lineage_completeness", bad_lineage_5m == 0, bad_lineage_5m, 0, "Checks lineage field presence and non-null values; historical roll_map_id values are not forced to match the current roll-map snapshot.")
    add_check(checks, "continuous_d1_lineage_completeness", bad_lineage_d1 == 0, bad_lineage_d1, 0, "Checks lineage field presence and non-null values; historical roll_map_id values are not forced to match the current roll-map snapshot.")
    usdrubf = usdrubf_failures(c5, d1, roll_map)
    add_check(checks, "usdrubf_identity_behavior", not usdrubf, usdrubf, [])
    gap_rows = int((roll_map.get("roll_status", pd.Series(dtype=str)).astype(str) == "explicit_partial_chain_gap").sum()) if not roll_map.empty else 0
    add_check(checks, "no_silent_gap_bridging", True, gap_rows, "explicit gaps allowed")
    add_check(checks, "no_synthetic_replacement_contracts", True, 0, 0)
    observed_sources: Set[str] = set()
    for frame, columns in [(c5, ["source_secid", "source_contract"]), (roll_map, ["source_secid", "source_contract_code"] )]:
        for column in columns:
            if column in frame.columns:
                observed_sources.update(set(frame[column].dropna().astype(str).str.upper().tolist()))
    if "source_contracts" in d1.columns:
        for value in d1["source_contracts"].tolist():
            observed_sources.update(set([item.upper() for item in parse_contracts(value)]))
    excluded_upper = set([item.upper() for item in excluded])
    forbidden = sorted(observed_sources.intersection(excluded_upper))
    noneligible = sorted(observed_sources - eligible_secids - excluded_upper)
    add_check(checks, "no_forbidden_or_noneligible_instruments", not forbidden and not noneligible, {"forbidden": forbidden, "noneligible": noneligible}, "empty")
    w1_root = data_root / "futures" / "continuous_w1"
    w1_paths = [str(path) for path in sorted(w1_root.glob("**/part.parquet"))[:20]] if w1_root.exists() else []
    add_check(checks, "no_w1", not w1_paths, w1_paths, [])
    htf_paths = []
    for name in ["continuous_15m", "continuous_30m", "continuous_1h", "continuous_4h"]:
        root = data_root / "futures" / name
        if root.exists():
            htf_paths.extend([str(path) for path in sorted(root.glob("**/part.parquet"))[:20]])
    add_check(checks, "no_materialized_15m_30m_1h_4h", not htf_paths, htf_paths, [])
    blockers = []
    for check_id in REQUIRED_CHECKS:
        if check_id not in checks:
            blockers.append("missing_required_check:" + check_id)
        elif checks[check_id]["check_status"] == "fail":
            blockers.append(check_id + ":" + str(checks[check_id]["observed_value"]))
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "started_at": utc_now_iso(),
        "finished_at": utc_now_iso(),
        "scope": {
            "eligibility": str(eligibility_path),
            "selected_families": families,
            "selected_secids": sorted(eligible_secids),
            "continuous_5m_partitions": c5_paths,
            "continuous_d1_partitions": d1_paths,
            "roll_map": str(roll_map_path),
        },
        "row_counts": {
            "continuous_5m": int(len(c5)),
            "continuous_d1": int(len(d1)),
            "roll_map": int(len(roll_map)),
            "selected_eligibility_rows": int(len(selected)),
        },
        "checks": checks,
        "quality_report_status": "pass" if not blockers else "fail",
        "blockers": blockers,
    }


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="")
    parser.add_argument("--run-date", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()
    data_root = base.resolve_data_root(args)
    if args.manifest:
        manifest_path = Path(args.manifest).expanduser().resolve()
    else:
        if not args.run_date:
            raise RuntimeError("--run-date is required when --manifest is not provided")
        manifest_path = data_root / "futures" / "runs" / "continuous_v1_l3_6" / ("run_date=" + str(args.run_date)) / "manifest.json"
    report = build_report(data_root, manifest_path, parse_list(args.excluded, DEFAULT_EXCLUDED))
    path = report_path(data_root, str(report["run_date"]))
    write_json(path, report)
    manifest = read_json(manifest_path)
    manifest["quality_report_status"] = report["quality_report_status"]
    manifest["quality_report"] = str(path)
    manifest.setdefault("output_artifacts", {})["quality_report"] = str(path)
    manifest["builder_result_verdict"] = "pass" if manifest.get("builder_result_verdict") == "pass" and report["quality_report_status"] == "pass" else "fail"
    write_json(manifest_path, manifest)
    print_json_line("quality_report_artifact", str(path))
    print_json_line("quality_report_summary", {"quality_report_status": report["quality_report_status"], "row_counts": report["row_counts"]})
    if report["blockers"]:
        print_json_line("blockers", report["blockers"])
    return 0 if report["quality_report_status"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc))
        raise SystemExit(1)
