#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

TZ_MSK = ZoneInfo("Europe/Moscow")
TARGET_FAMILIES = ["W", "MM", "MX"]
UNIVERSE_SCOPE = "rfud_candidates"
OUTPUT_SCOPE = "controlled_batch_w_mm_mx"
SCHEMA_INSTRUMENT_DIAGNOSTICS = "futures_w_mm_mx_liquidity_history_diagnostics.v1"
SCHEMA_FAMILY_SUMMARY = "futures_w_mm_mx_liquidity_history_diagnostics_summary.v1"

REQUIRED_REPO_FILES = [
    "contracts/datasets/futures_rfud_candidates_evidence_contract.md",
    "configs/datasets/futures_evidence_universe_scope_config.json",
]

INPUT_PATHS = {
    "normalized_registry": "futures/registry/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_normalized_instrument_registry.parquet",
    "tradestats": "futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_algopack_tradestats_availability_report.parquet",
    "futoi": "futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_futoi_availability_report.parquet",
    "obstats": "futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_obstats_availability_report.parquet",
    "hi2": "futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_hi2_availability_report.parquet",
}

OUTPUT_DIAGNOSTICS_PATTERN = "futures/diagnostics/liquidity_history/universe_scope=controlled_batch_w_mm_mx/snapshot_date={snapshot_date}/futures_w_mm_mx_liquidity_history_diagnostics.parquet"
OUTPUT_SUMMARY_PATTERN = "futures/diagnostics/liquidity_history/universe_scope=controlled_batch_w_mm_mx/snapshot_date={snapshot_date}/futures_w_mm_mx_liquidity_history_diagnostics_summary.json"


def today_msk() -> str:
    return datetime.now(TZ_MSK).date().isoformat()


def repo_root() -> Path:
    return Path.cwd().resolve()


def resolve_data_root(args: argparse.Namespace) -> Path:
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT is required")
    path = Path(raw).expanduser().resolve()
    if not path.is_absolute():
        raise RuntimeError("MOEX_DATA_ROOT must resolve to an absolute path")
    return path


def assert_files_exist(root: Path, rel_paths: Iterable[str]) -> None:
    missing = []
    for rel in rel_paths:
        if not (root / rel).exists():
            missing.append(rel)
    if missing:
        raise FileNotFoundError("Missing required SoT files: " + ", ".join(missing))


def path_from_pattern(data_root: Path, pattern: str, snapshot_date: str) -> Path:
    value = pattern.replace("{snapshot_date}", snapshot_date)
    if "{" in value or "}" in value or "$" in value:
        raise RuntimeError("Unresolved path placeholder: " + pattern)
    return (data_root / value).resolve()


def read_parquet_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError("Missing required input artifact for " + label + ": " + str(path))
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError("Cannot read parquet for " + label + " at " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc
    if frame.empty:
        raise RuntimeError("Required input artifact is empty for " + label + ": " + str(path))
    return frame


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def write_json(payload: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def require_columns(frame: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise RuntimeError(label + " missing required columns: " + ", ".join(missing))


def as_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def as_int(value: Any) -> Optional[int]:
    text = as_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def parse_ts(value: Any) -> Optional[pd.Timestamp]:
    text = as_text(value)
    if not text:
        return None
    try:
        ts = pd.to_datetime(text, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return ts


def iso_ts(value: Optional[pd.Timestamp]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()


def inclusive_calendar_days(min_ts: Optional[pd.Timestamp], max_ts: Optional[pd.Timestamp]) -> Optional[int]:
    if min_ts is None or max_ts is None:
        return None
    left = min_ts.date()
    right = max_ts.date()
    if right < left:
        return None
    return int((right - left).days) + 1


def observed_active_days_from_bounds(min_ts: Optional[pd.Timestamp], max_ts: Optional[pd.Timestamp]) -> Optional[int]:
    return inclusive_calendar_days(min_ts, max_ts)


def build_lookup(frame: pd.DataFrame, label: str) -> Dict[str, Dict[str, Any]]:
    require_columns(frame, ["snapshot_date", "board", "secid", "family_code"], label)
    out = {}
    for _, row in frame.iterrows():
        board = as_text(row.get("board")) or "rfud"
        secid = as_text(row.get("secid"))
        key = board + "|" + secid
        if key in out:
            raise RuntimeError(label + " duplicate key: " + key)
        out[key] = row.to_dict()
    return out


def endpoint_row_status(lookup: Dict[str, Dict[str, Any]], board: str, secid: str) -> Dict[str, Any]:
    row = lookup.get(board + "|" + secid, {})
    return {
        "availability_status": as_text(row.get("availability_status")) or "missing",
        "observed_rows": as_int(row.get("observed_rows")),
        "observed_min_ts": as_text(row.get("observed_min_ts")) or None,
        "observed_max_ts": as_text(row.get("observed_max_ts")) or None,
    }


def candidate_status(raw_rows: Optional[int], coverage_ratio: Optional[float], endpoint_statuses: Dict[str, str]) -> str:
    if any(value in ["missing", "error", "unavailable"] for value in endpoint_statuses.values()):
        return "fail_candidate"
    if raw_rows is None or raw_rows <= 0:
        return "fail_candidate"
    if coverage_ratio is None:
        return "defer_candidate"
    if coverage_ratio >= 0.80:
        return "pass_candidate"
    return "defer_candidate"


def build_instrument_diagnostics(inputs: Dict[str, pd.DataFrame], snapshot_date: str) -> pd.DataFrame:
    registry = inputs["normalized_registry"].copy()
    require_columns(registry, ["snapshot_date", "board", "secid", "family_code"], "normalized_registry")
    registry = registry.loc[
        (registry["snapshot_date"].astype(str) == snapshot_date)
        & (registry["board"].astype(str).str.strip().str.lower() == "rfud")
        & (registry["family_code"].astype(str).str.strip().isin(TARGET_FAMILIES))
    ].copy()
    if registry.empty:
        raise RuntimeError("No W/MM/MX instruments found in rfud_candidates normalized registry for snapshot_date=" + snapshot_date)
    registry = registry.drop_duplicates(["board", "secid"]).sort_values(["family_code", "secid", "board"]).reset_index(drop=True)

    lookups = {
        "tradestats": build_lookup(inputs["tradestats"], "tradestats"),
        "futoi": build_lookup(inputs["futoi"], "futoi"),
        "obstats": build_lookup(inputs["obstats"], "obstats"),
        "hi2": build_lookup(inputs["hi2"], "hi2"),
    }

    rows: List[Dict[str, Any]] = []
    for _, item in registry.iterrows():
        board = as_text(item.get("board")) or "rfud"
        secid = as_text(item.get("secid"))
        family = as_text(item.get("family_code"))
        tradestats = endpoint_row_status(lookups["tradestats"], board, secid)
        futoi = endpoint_row_status(lookups["futoi"], board, secid)
        obstats = endpoint_row_status(lookups["obstats"], board, secid)
        hi2 = endpoint_row_status(lookups["hi2"], board, secid)
        min_ts = parse_ts(tradestats.get("observed_min_ts"))
        max_ts = parse_ts(tradestats.get("observed_max_ts"))
        expected_days = inclusive_calendar_days(min_ts, max_ts)
        active_days = observed_active_days_from_bounds(min_ts, max_ts)
        coverage_ratio = None
        if expected_days and active_days is not None:
            coverage_ratio = float(active_days) / float(expected_days)
        raw_rows = tradestats.get("observed_rows")
        median_daily_rows = None
        if raw_rows is not None and active_days and active_days > 0:
            median_daily_rows = float(raw_rows) / float(active_days)
        zero_row_days = None
        if expected_days is not None and active_days is not None:
            zero_row_days = int(max(expected_days - active_days, 0))
        endpoint_statuses = {
            "tradestats": str(tradestats.get("availability_status")),
            "futoi": str(futoi.get("availability_status")),
            "obstats": str(obstats.get("availability_status")),
            "hi2": str(hi2.get("availability_status")),
        }
        status = candidate_status(raw_rows, coverage_ratio, endpoint_statuses)
        rows.append({
            "snapshot_date": snapshot_date,
            "universe_scope": OUTPUT_SCOPE,
            "source_universe_scope": UNIVERSE_SCOPE,
            "board": board,
            "secid": secid,
            "family_code": family,
            "classification_status": "controlled_provisional",
            "continuous_eligibility_status": "not_accepted",
            "diagnostics_candidate_status": status,
            "raw_rows": raw_rows,
            "observed_min_ts": iso_ts(min_ts),
            "observed_max_ts": iso_ts(max_ts),
            "observed_active_days": active_days,
            "expected_calendar_days_in_window": expected_days,
            "coverage_ratio": coverage_ratio,
            "median_daily_rows": median_daily_rows,
            "zero_row_days": zero_row_days,
            "tradestats_availability_status": endpoint_statuses["tradestats"],
            "futoi_availability_status": endpoint_statuses["futoi"],
            "obstats_availability_status": endpoint_statuses["obstats"],
            "hi2_availability_status": endpoint_statuses["hi2"],
            "schema_version": SCHEMA_INSTRUMENT_DIAGNOSTICS,
            "notes": "Derived only from existing rfud_candidates evidence artifacts; no raw loader, backfill, continuous build, or PM promotion performed.",
        })
    diagnostics = pd.DataFrame(rows)
    unexpected = sorted(set(diagnostics["family_code"].astype(str)) - set(TARGET_FAMILIES))
    if unexpected:
        raise RuntimeError("Unexpected family_code in diagnostics: " + ", ".join(unexpected))
    return diagnostics


def weakest_instruments(frame: pd.DataFrame) -> List[Dict[str, Any]]:
    cols = ["secid", "diagnostics_candidate_status", "coverage_ratio", "raw_rows", "observed_active_days"]
    ordered = frame.copy()
    ordered["coverage_sort"] = pd.to_numeric(ordered["coverage_ratio"], errors="coerce").fillna(-1.0)
    ordered["rows_sort"] = pd.to_numeric(ordered["raw_rows"], errors="coerce").fillna(-1)
    ordered = ordered.sort_values(["coverage_sort", "rows_sort", "secid"], ascending=[True, True, True])
    return ordered[cols].head(5).to_dict("records")


def family_summary(diagnostics: pd.DataFrame, snapshot_date: str) -> List[Dict[str, Any]]:
    rows = []
    for family in TARGET_FAMILIES:
        sub = diagnostics.loc[diagnostics["family_code"].astype(str) == family].copy()
        counts = sub["diagnostics_candidate_status"].astype(str).value_counts().to_dict() if not sub.empty else {}
        coverage = pd.to_numeric(sub["coverage_ratio"], errors="coerce") if not sub.empty else pd.Series(dtype="float64")
        rows.append({
            "snapshot_date": snapshot_date,
            "universe_scope": OUTPUT_SCOPE,
            "family_code": family,
            "instrument_count": int(len(sub)),
            "pass_candidate_count": int(counts.get("pass_candidate", 0)),
            "defer_candidate_count": int(counts.get("defer_candidate", 0)),
            "fail_candidate_count": int(counts.get("fail_candidate", 0)),
            "min_coverage_ratio": None if coverage.dropna().empty else float(coverage.min()),
            "median_coverage_ratio": None if coverage.dropna().empty else float(coverage.median()),
            "weakest_instruments": weakest_instruments(sub) if not sub.empty else [],
            "schema_version": SCHEMA_FAMILY_SUMMARY,
        })
    return rows


def print_json_line(key: str, value: Any) -> None:
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    args = parser.parse_args()

    root = repo_root()
    snapshot_date = str(args.snapshot_date).strip()
    data_root = resolve_data_root(args)
    assert_files_exist(root, REQUIRED_REPO_FILES)

    input_paths = {name: path_from_pattern(data_root, pattern, snapshot_date) for name, pattern in INPUT_PATHS.items()}
    inputs = {name: read_parquet_required(path, name) for name, path in input_paths.items()}

    diagnostics = build_instrument_diagnostics(inputs, snapshot_date)
    summary_rows = family_summary(diagnostics, snapshot_date)
    diagnostics_path = path_from_pattern(data_root, OUTPUT_DIAGNOSTICS_PATTERN, snapshot_date)
    summary_path = path_from_pattern(data_root, OUTPUT_SUMMARY_PATTERN, snapshot_date)

    write_parquet(diagnostics, diagnostics_path)
    summary_payload = {
        "snapshot_date": snapshot_date,
        "universe_scope": OUTPUT_SCOPE,
        "source_universe_scope": UNIVERSE_SCOPE,
        "target_families": TARGET_FAMILIES,
        "schema_version": SCHEMA_FAMILY_SUMMARY,
        "diagnostics_artifact": str(diagnostics_path),
        "diagnostics_summary_artifact": str(summary_path),
        "input_artifacts_read": {name: str(path) for name, path in input_paths.items()},
        "instrument_diagnostics_summary": diagnostics[["family_code", "secid", "diagnostics_candidate_status", "raw_rows", "observed_min_ts", "observed_max_ts", "observed_active_days", "expected_calendar_days_in_window", "coverage_ratio", "median_daily_rows", "zero_row_days"]].sort_values(["family_code", "secid"]).to_dict("records"),
        "family_diagnostics_summary": summary_rows,
        "guardrails": {
            "only_w_mm_mx_analyzed": True,
            "derived_from_existing_rfud_candidates_artifacts": True,
            "slice1_untouched": True,
            "continuous_eligibility_status": "not_accepted",
            "classification_status": "controlled_provisional",
        },
    }
    write_json(summary_payload, summary_path)

    print_json_line("diagnostics_artifact", str(diagnostics_path))
    print_json_line("diagnostics_summary_artifact", str(summary_path))
    print_json_line("instrument_diagnostics_summary", summary_payload["instrument_diagnostics_summary"])
    print_json_line("family_diagnostics_summary", summary_rows)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
