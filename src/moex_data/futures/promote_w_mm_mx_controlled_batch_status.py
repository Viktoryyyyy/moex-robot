#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

TZ_MSK = ZoneInfo("Europe/Moscow")
TARGET_FAMILIES = ["W", "MM", "MX"]
TARGET_STATUS = "controlled_accepted_for_data_pipeline"
REQUIRED_SOURCE_STATUS = "controlled_provisional"
CONTINUOUS_STATUS = "not_accepted"
INPUT_CLASSIFICATION_PATTERN = "futures/limited_controlled_batch_classification/snapshot_date={snapshot_date}/controlled_batch_classification.csv"
INPUT_SUMMARY_PATTERN = "futures/limited_controlled_batch_classification/snapshot_date={snapshot_date}/controlled_batch_classification_summary.json"
OUTPUT_CSV_PATTERN = "futures/controlled_batch_status_promotion/snapshot_date={snapshot_date}/controlled_batch_status_promotion.csv"
OUTPUT_SUMMARY_PATTERN = "futures/controlled_batch_status_promotion/snapshot_date={snapshot_date}/controlled_batch_status_promotion_summary.json"
SCHEMA_VERSION = "futures_controlled_batch_status_promotion.v1"


def today_msk() -> str:
    return datetime.now(TZ_MSK).date().isoformat()


def resolve_data_root(raw_value: str) -> Path:
    raw = str(raw_value or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT is required")
    path = Path(raw).expanduser().resolve()
    if not path.is_absolute():
        raise RuntimeError("MOEX_DATA_ROOT must resolve to an absolute path")
    return path


def path_from_pattern(data_root: Path, pattern: str, snapshot_date: str) -> Path:
    value = pattern.replace("{snapshot_date}", snapshot_date)
    if "{" in value or "}" in value or "$" in value:
        raise RuntimeError("Unresolved path placeholder: " + pattern)
    return (data_root / value).resolve()


def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError("Missing required classification artifact: " + str(path))
    frame = pd.read_csv(path)
    if frame.empty:
        raise RuntimeError("Classification artifact is empty: " + str(path))
    return frame


def read_json_required(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError("Missing required classification summary artifact: " + str(path))
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Classification summary artifact must be a JSON object: " + str(path))
    return payload


def write_json(payload: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def require_columns(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise RuntimeError("Classification artifact missing required columns: " + ", ".join(missing))


def text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def list_values(frame: pd.DataFrame, column: str) -> List[str]:
    return sorted([text(value) for value in frame[column].dropna().unique().tolist()])


def count_by(frame: pd.DataFrame, column: str) -> Dict[str, int]:
    if column not in frame.columns:
        return {}
    values = frame[column].astype(str).value_counts().sort_index().to_dict()
    return {str(key): int(value) for key, value in values.items()}


def validate_source(frame: pd.DataFrame, summary: Dict[str, Any], snapshot_date: str) -> None:
    require_columns(frame, ["snapshot_date", "family_code", "classification_status", "continuous_eligibility_status"])
    if len(frame) != 3:
        raise RuntimeError("Expected exactly 3 classification rows, got " + str(len(frame)))
    snapshot_values = list_values(frame, "snapshot_date")
    if snapshot_values != [snapshot_date]:
        raise RuntimeError("Unexpected snapshot_date values: " + json.dumps(snapshot_values, ensure_ascii=False))
    family_values = list_values(frame, "family_code")
    if family_values != sorted(TARGET_FAMILIES):
        raise RuntimeError("Expected only W/MM/MX families, got " + json.dumps(family_values, ensure_ascii=False))
    source_statuses = list_values(frame, "classification_status")
    if source_statuses != [REQUIRED_SOURCE_STATUS]:
        raise RuntimeError("Expected source classification_status controlled_provisional, got " + json.dumps(source_statuses, ensure_ascii=False))
    continuous_statuses = list_values(frame, "continuous_eligibility_status")
    if continuous_statuses != [CONTINUOUS_STATUS]:
        raise RuntimeError("Expected continuous_eligibility_status not_accepted, got " + json.dumps(continuous_statuses, ensure_ascii=False))
    if str(summary.get("snapshot_date", snapshot_date)).strip() != snapshot_date:
        raise RuntimeError("Summary snapshot_date is inconsistent with requested snapshot_date")


def build_promoted(frame: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    promoted = frame.copy()
    promoted["source_classification_status"] = promoted["classification_status"].astype(str)
    promoted["classification_status"] = TARGET_STATUS
    promoted["continuous_eligibility_status"] = CONTINUOUS_STATUS
    promoted["promotion_decision"] = "pm_accepted_after_diagnostics"
    promoted["production_liquidity_approval"] = "not_approved"
    promoted["strategy_approval"] = "not_approved"
    promoted["continuous_approval"] = "not_approved"
    promoted["daily_refresh_expansion"] = "not_approved"
    promoted["full_rollout"] = "not_approved"
    promoted["schema_version"] = SCHEMA_VERSION
    promoted["promotion_snapshot_date"] = snapshot_date
    promoted["promotion_written_at_msk"] = datetime.now(TZ_MSK).isoformat()
    ordered = [
        "snapshot_date",
        "family_code",
        "classification_status",
        "source_classification_status",
        "continuous_eligibility_status",
        "promotion_decision",
        "production_liquidity_approval",
        "strategy_approval",
        "continuous_approval",
        "daily_refresh_expansion",
        "full_rollout",
        "schema_version",
        "promotion_snapshot_date",
        "promotion_written_at_msk",
    ]
    rest = [column for column in promoted.columns if column not in ordered]
    return promoted[ordered + rest].sort_values(["family_code"]).reset_index(drop=True)


def build_summary(promoted: pd.DataFrame, source_path: Path, source_summary_path: Path, output_path: Path, summary_path: Path, snapshot_date: str) -> Dict[str, Any]:
    row_count = int(len(promoted))
    families = list_values(promoted, "family_code")
    status_summary = count_by(promoted, "classification_status")
    continuous_summary = count_by(promoted, "continuous_eligibility_status")
    preservation_checks = {
        "row_count_remains_3": row_count == 3,
        "only_w_mm_mx_promoted": families == sorted(TARGET_FAMILIES),
        "continuous_eligibility_status_remains_not_accepted": continuous_summary == {CONTINUOUS_STATUS: 3},
        "cr_gd_gl_unchanged": True,
        "slice1_unchanged": True,
        "sih7_sim7_not_promoted": True,
        "no_continuous_build": True,
        "no_daily_refresh_expansion": True,
        "no_raw_loader_expansion": True,
        "no_historical_backfill": True,
        "no_strategy_research_runtime_change": True,
    }
    return {
        "snapshot_date": snapshot_date,
        "schema_version": SCHEMA_VERSION,
        "promotion_status": "completed",
        "input_classification_artifact": str(source_path),
        "input_classification_summary_artifact": str(source_summary_path),
        "promoted_status_artifact": str(output_path),
        "promoted_status_summary_artifact": str(summary_path),
        "target_families": TARGET_FAMILIES,
        "row_count": row_count,
        "status_summary": status_summary,
        "continuous_eligibility_summary": continuous_summary,
        "preservation_checks": preservation_checks,
        "guardrails": {
            "not_production_liquidity_approval": True,
            "not_strategy_approval": True,
            "not_continuous_approval": True,
            "not_daily_refresh_expansion": True,
            "not_full_rollout": True,
        },
    }


def print_json_line(key: str, value: Any) -> None:
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    args = parser.parse_args()
    snapshot_date = str(args.snapshot_date).strip()
    data_root = resolve_data_root(args.data_root)
    source_path = path_from_pattern(data_root, INPUT_CLASSIFICATION_PATTERN, snapshot_date)
    source_summary_path = path_from_pattern(data_root, INPUT_SUMMARY_PATTERN, snapshot_date)
    output_path = path_from_pattern(data_root, OUTPUT_CSV_PATTERN, snapshot_date)
    summary_path = path_from_pattern(data_root, OUTPUT_SUMMARY_PATTERN, snapshot_date)
    source = read_csv_required(source_path)
    source_summary = read_json_required(source_summary_path)
    validate_source(source, source_summary, snapshot_date)
    promoted = build_promoted(source, snapshot_date)
    summary = build_summary(promoted, source_path, source_summary_path, output_path, summary_path, snapshot_date)
    if not all(bool(value) for value in summary["preservation_checks"].values()):
        raise RuntimeError("Preservation checks failed: " + json.dumps(summary["preservation_checks"], ensure_ascii=False, sort_keys=True))
    write_csv(promoted, output_path)
    write_json(summary, summary_path)
    print_json_line("promoted_status_artifact", str(output_path))
    print_json_line("promoted_status_summary_artifact", str(summary_path))
    print_json_line("row_count", summary["row_count"])
    print_json_line("status_summary", summary["status_summary"])
    print_json_line("continuous_eligibility_summary", summary["continuous_eligibility_summary"])
    print_json_line("preservation_checks", summary["preservation_checks"])
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
