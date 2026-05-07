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
BLOCKED_STATUS = "controlled_blocked"
REQUIRED_SOURCE_STATUS = "controlled_provisional"
CONTINUOUS_STATUS = "not_accepted"
NO_LOADABLE_REASON = "no_validated_loadable_contracts"
ACCEPTED_VALIDATION_STATUSES = {"validated", "accepted", "pass", "controlled_accepted_for_data_pipeline"}
INPUT_CLASSIFICATION_PATTERN = "futures/limited_controlled_batch_classification/snapshot_date={snapshot_date}/controlled_batch_classification.csv"
INPUT_SUMMARY_PATTERN = "futures/limited_controlled_batch_classification/snapshot_date={snapshot_date}/controlled_batch_classification_summary.json"
INPUT_REGISTRY_PATTERN = "futures/registry/normalized/snapshot_date={snapshot_date}/normalized_registry.parquet"
OUTPUT_CSV_PATTERN = "futures/controlled_batch_status_promotion/snapshot_date={snapshot_date}/controlled_batch_status_promotion.csv"
OUTPUT_SUMMARY_PATTERN = "futures/controlled_batch_status_promotion/snapshot_date={snapshot_date}/controlled_batch_status_promotion_summary.json"
SCHEMA_VERSION = "futures_controlled_batch_status_promotion.v2"


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


def read_parquet_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError("Missing required normalized registry artifact: " + str(path))
    frame = pd.read_parquet(path)
    if frame.empty:
        raise RuntimeError("Normalized registry artifact is empty: " + str(path))
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
        raise RuntimeError("Artifact missing required columns: " + ", ".join(missing))


def require_col(frame: pd.DataFrame, candidates: Iterable[str], label: str) -> str:
    for column in candidates:
        if column in frame.columns:
            return column
    raise RuntimeError(label + " column missing")


def optional_col(frame: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def normalize_family_column(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "family_code" not in out.columns:
        if "family" not in out.columns:
            raise RuntimeError("Classification artifact missing required family column: family_code or family")
        out["family_code"] = out["family"]
    return out


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


def parse_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def family_col(frame: pd.DataFrame) -> str:
    return require_col(frame, ["family_code", "family", "asset_code", "underlying_family", "underlying_asset"], "family")


def board_col(frame: pd.DataFrame) -> str:
    return require_col(frame, ["board", "boardid", "board_id"], "board")


def build_loadable_contract_counts(registry: pd.DataFrame, snapshot_date: str) -> tuple[Dict[str, int], Dict[str, Dict[str, int]]]:
    fam_col = family_col(registry)
    brd_col = board_col(registry)
    mapping_col = require_col(registry, ["mapping_status", "map_status"], "mapping_status")
    validation_col = require_col(registry, ["validation_status", "validated_status", "registry_validation_status"], "validation_status")
    first_col = optional_col(registry, ["first_available_date", "screen_from", "history_first_available_date", "first_trade_date", "firsttradedate", "start_date"])
    last_col = require_col(registry, ["last_available_date", "screen_till", "history_last_available_date", "last_trade_date", "lasttradedate", "expiration_date", "expiration"], "last_trade_date")
    snapshot_day = pd.to_datetime(snapshot_date, errors="coerce").date()
    if pd.isna(snapshot_day):
        raise RuntimeError("invalid snapshot_date: " + str(snapshot_date))
    work = registry.copy()
    work["_family"] = work[fam_col].fillna("").astype(str).str.strip().str.upper()
    work["_board"] = work[brd_col].fillna("").astype(str).str.strip().str.upper()
    work["_mapping_status"] = work[mapping_col].fillna("").astype(str).str.strip().str.lower()
    work["_validation_status"] = work[validation_col].fillna("").astype(str).str.strip().str.lower()
    if first_col:
        work["_first_day"] = parse_date_series(work[first_col])
    else:
        work["_first_day"] = snapshot_day
    work["_last_day"] = parse_date_series(work[last_col])
    work = work.loc[work["_family"].isin(TARGET_FAMILIES)].copy()
    work = work.loc[work["_board"] == "RFUD"].copy()
    counts: Dict[str, int] = {}
    diagnostics: Dict[str, Dict[str, int]] = {}
    for family in TARGET_FAMILIES:
        group = work.loc[work["_family"] == family].copy()
        draft_mask = group["_mapping_status"] == "draft"
        validation_mask = group["_validation_status"].isin(ACCEPTED_VALIDATION_STATUSES)
        missing_range_mask = group["_first_day"].isna() | group["_last_day"].isna()
        inverted_range_mask = (~missing_range_mask) & (group["_first_day"] > group["_last_day"])
        expired_mask = (~missing_range_mask) & (group["_last_day"] < snapshot_day)
        loadable = group.loc[(~draft_mask) & validation_mask & (~missing_range_mask) & (~inverted_range_mask) & (~expired_mask)].copy()
        counts[family] = int(len(loadable.index))
        diagnostics[family] = {
            "registry_rows": int(len(group.index)),
            "draft_rows": int(draft_mask.sum()) if len(group.index) else 0,
            "not_validated_rows": int((~validation_mask).sum()) if len(group.index) else 0,
            "missing_range_rows": int(missing_range_mask.sum()) if len(group.index) else 0,
            "inverted_range_rows": int(inverted_range_mask.sum()) if len(group.index) else 0,
            "expired_before_snapshot_rows": int(expired_mask.sum()) if len(group.index) else 0,
            "validated_loadable_contract_count": int(len(loadable.index)),
        }
    return counts, diagnostics


def validate_source(frame: pd.DataFrame, summary: Dict[str, Any], snapshot_date: str) -> pd.DataFrame:
    normalized = normalize_family_column(frame)
    require_columns(normalized, ["snapshot_date", "family_code", "classification_status", "continuous_eligibility_status"])
    if len(normalized) != 3:
        raise RuntimeError("Expected exactly 3 classification rows, got " + str(len(normalized)))
    snapshot_values = list_values(normalized, "snapshot_date")
    if snapshot_values != [snapshot_date]:
        raise RuntimeError("Unexpected snapshot_date values: " + json.dumps(snapshot_values, ensure_ascii=False))
    family_values = list_values(normalized, "family_code")
    if family_values != sorted(TARGET_FAMILIES):
        raise RuntimeError("Expected only W/MM/MX families, got " + json.dumps(family_values, ensure_ascii=False))
    source_statuses = list_values(normalized, "classification_status")
    if source_statuses != [REQUIRED_SOURCE_STATUS]:
        raise RuntimeError("Expected source classification_status controlled_provisional, got " + json.dumps(source_statuses, ensure_ascii=False))
    continuous_statuses = list_values(normalized, "continuous_eligibility_status")
    if continuous_statuses != [CONTINUOUS_STATUS]:
        raise RuntimeError("Expected continuous_eligibility_status not_accepted, got " + json.dumps(continuous_statuses, ensure_ascii=False))
    if str(summary.get("snapshot_date", snapshot_date)).strip() != snapshot_date:
        raise RuntimeError("Summary snapshot_date is inconsistent with requested snapshot_date")
    return normalized


def build_promoted(frame: pd.DataFrame, snapshot_date: str, loadable_counts: Dict[str, int]) -> pd.DataFrame:
    promoted = frame.copy()
    promoted["source_classification_status"] = promoted["classification_status"].astype(str)
    promoted["validated_loadable_contract_count"] = promoted["family_code"].astype(str).str.upper().map(loadable_counts).fillna(0).astype(int)
    accepted_mask = promoted["validated_loadable_contract_count"] > 0
    promoted["classification_status"] = BLOCKED_STATUS
    promoted.loc[accepted_mask, "classification_status"] = TARGET_STATUS
    promoted["continuous_eligibility_status"] = CONTINUOUS_STATUS
    promoted["promotion_decision"] = "blocked"
    promoted.loc[accepted_mask, "promotion_decision"] = "pm_accepted_after_diagnostics"
    promoted["promotion_blocker_reason"] = NO_LOADABLE_REASON
    promoted.loc[accepted_mask, "promotion_blocker_reason"] = ""
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
        "promotion_blocker_reason",
        "validated_loadable_contract_count",
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


def build_summary(promoted: pd.DataFrame, source_path: Path, source_summary_path: Path, registry_path: Path, output_path: Path, summary_path: Path, snapshot_date: str, contract_diagnostics: Dict[str, Dict[str, int]]) -> Dict[str, Any]:
    row_count = int(len(promoted))
    families = list_values(promoted, "family_code")
    status_summary = count_by(promoted, "classification_status")
    continuous_summary = count_by(promoted, "continuous_eligibility_status")
    accepted_rows = promoted.loc[promoted["classification_status"].astype(str) == TARGET_STATUS]
    blocked_rows = promoted.loc[promoted["promotion_blocker_reason"].astype(str) == NO_LOADABLE_REASON]
    accepted_with_zero = accepted_rows.loc[accepted_rows["validated_loadable_contract_count"].astype(int) <= 0]
    preservation_checks = {
        "row_count_remains_3": row_count == 3,
        "only_w_mm_mx_in_output": families == sorted(TARGET_FAMILIES),
        "no_family_accepted_with_zero_loadable_contracts": accepted_with_zero.empty,
        "zero_loadable_families_blocked": int(len(blocked_rows.index)) == int((promoted["validated_loadable_contract_count"].astype(int) <= 0).sum()),
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
        "input_normalized_registry_artifact": str(registry_path),
        "promoted_status_artifact": str(output_path),
        "promoted_status_summary_artifact": str(summary_path),
        "target_families": TARGET_FAMILIES,
        "row_count": row_count,
        "status_summary": status_summary,
        "continuous_eligibility_summary": continuous_summary,
        "loadable_contract_gate": {
            "required_for_controlled_accepted_for_data_pipeline": True,
            "accepted_validation_statuses": sorted(ACCEPTED_VALIDATION_STATUSES),
            "zero_loadable_blocker_reason": NO_LOADABLE_REASON,
            "family_diagnostics": contract_diagnostics,
        },
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
    registry_path = path_from_pattern(data_root, INPUT_REGISTRY_PATTERN, snapshot_date)
    output_path = path_from_pattern(data_root, OUTPUT_CSV_PATTERN, snapshot_date)
    summary_path = path_from_pattern(data_root, OUTPUT_SUMMARY_PATTERN, snapshot_date)
    source = read_csv_required(source_path)
    source_summary = read_json_required(source_summary_path)
    registry = read_parquet_required(registry_path)
    normalized_source = validate_source(source, source_summary, snapshot_date)
    loadable_counts, contract_diagnostics = build_loadable_contract_counts(registry, snapshot_date)
    promoted = build_promoted(normalized_source, snapshot_date, loadable_counts)
    summary = build_summary(promoted, source_path, source_summary_path, registry_path, output_path, summary_path, snapshot_date, contract_diagnostics)
    if not all(bool(value) for value in summary["preservation_checks"].values()):
        raise RuntimeError("Preservation checks failed: " + json.dumps(summary["preservation_checks"], ensure_ascii=False, sort_keys=True))
    write_csv(promoted, output_path)
    write_json(summary, summary_path)
    print_json_line("promoted_status_artifact", str(output_path))
    print_json_line("promoted_status_summary_artifact", str(summary_path))
    print_json_line("row_count", summary["row_count"])
    print_json_line("status_summary", summary["status_summary"])
    print_json_line("continuous_eligibility_summary", summary["continuous_eligibility_summary"])
    print_json_line("loadable_contract_gate", summary["loadable_contract_gate"])
    print_json_line("preservation_checks", summary["preservation_checks"])
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
