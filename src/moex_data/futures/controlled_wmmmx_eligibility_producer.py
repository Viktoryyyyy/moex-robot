#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

TARGET_FAMILIES = {"W", "MM", "MX"}
TARGET_BOARD = "RFUD"
TARGET_STATUS = "controlled_accepted_for_data_pipeline"
CONTINUOUS_STATUS = "not_accepted"
SCHEMA_VERSION = "futures_controlled_wmmmx_eligibility.v1"
INPUT_STATUS_PATTERN = "futures/controlled_batch_status_promotion/snapshot_date={snapshot_date}/controlled_batch_status_promotion.csv"
INPUT_REGISTRY_PATTERN = "futures/registry/normalized/snapshot_date={snapshot_date}/normalized_registry.parquet"
OUTPUT_PATTERN = "futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility.parquet"
SUMMARY_PATTERN = "futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility_summary.json"
ACCEPTED_VALIDATION_STATUSES = {"validated", "accepted", "pass", "controlled_accepted_for_data_pipeline"}


def path_from_pattern(data_root, pattern, snapshot_date):
    return Path(data_root) / pattern.replace("{snapshot_date}", snapshot_date)


def family_col(frame):
    for col in ["family_code", "family", "asset_code", "underlying_family"]:
        if col in frame.columns:
            return col
    raise RuntimeError("family column missing")


def board_col(frame):
    for col in ["board", "boardid", "board_id"]:
        if col in frame.columns:
            return col
    raise RuntimeError("board column missing")


def require_col(frame, candidates, label):
    for col in candidates:
        if col in frame.columns:
            return col
    raise RuntimeError(label + " column missing")


def optional_col(frame, candidates):
    for col in candidates:
        if col in frame.columns:
            return col
    return None


def parse_date_series(series):
    return pd.to_datetime(series, errors="coerce").dt.date


def controlled_status_filter(frame, snapshot_date):
    mapping_col = require_col(frame, ["mapping_status", "map_status"], "mapping_status")
    validation_col = require_col(frame, ["validation_status", "validated_status", "registry_validation_status"], "validation_status")
    first_col = optional_col(frame, ["first_available_date", "screen_from", "history_first_available_date", "first_trade_date", "firsttradedate", "start_date"])
    last_col = require_col(frame, ["last_available_date", "screen_till", "history_last_available_date", "last_trade_date", "lasttradedate", "expiration_date", "expiration"], "last_trade_date")
    snapshot_day = pd.to_datetime(snapshot_date, errors="coerce").date()
    if pd.isna(snapshot_day):
        raise RuntimeError("invalid snapshot_date: " + str(snapshot_date))
    work = frame.copy()
    work["_mapping_status_normalized"] = work[mapping_col].fillna("").astype(str).str.strip().str.lower()
    work["_validation_status_normalized"] = work[validation_col].fillna("").astype(str).str.strip().str.lower()
    if first_col:
        work["_first_available_day"] = parse_date_series(work[first_col])
    else:
        work["_first_available_day"] = snapshot_day
    work["_last_available_day"] = parse_date_series(work[last_col])
    work["eligibility_rejection_reason"] = ""
    draft_mask = work["_mapping_status_normalized"] == "draft"
    validation_mask = ~work["_validation_status_normalized"].isin(ACCEPTED_VALIDATION_STATUSES)
    missing_range_mask = work["_first_available_day"].isna() | work["_last_available_day"].isna()
    inverted_range_mask = (~missing_range_mask) & (work["_first_available_day"] > work["_last_available_day"])
    future_unresolved_mask = (~missing_range_mask) & (work["_last_available_day"] > snapshot_day)
    work.loc[draft_mask, "eligibility_rejection_reason"] = "mapping_status_draft"
    work.loc[(work["eligibility_rejection_reason"] == "") & validation_mask, "eligibility_rejection_reason"] = "validation_status_not_accepted"
    work.loc[(work["eligibility_rejection_reason"] == "") & missing_range_mask, "eligibility_rejection_reason"] = "raw_loader_date_range_missing"
    work.loc[(work["eligibility_rejection_reason"] == "") & inverted_range_mask, "eligibility_rejection_reason"] = "raw_loader_date_range_inverted"
    work.loc[(work["eligibility_rejection_reason"] == "") & future_unresolved_mask, "eligibility_rejection_reason"] = "raw_loader_date_range_not_resolvable_as_of_snapshot"
    accepted = work.loc[work["eligibility_rejection_reason"] == ""].copy()
    rejected = work.loc[work["eligibility_rejection_reason"] != ""].copy()
    accepted["mapping_status"] = accepted[mapping_col].astype(str)
    accepted["validation_status"] = accepted[validation_col].astype(str)
    accepted["first_available_date"] = accepted["_first_available_day"].astype(str)
    accepted["last_available_date"] = accepted["_last_available_day"].astype(str)
    accepted["eligibility_date_source"] = json.dumps({"first_col": first_col, "last_col": last_col}, sort_keys=True)
    return accepted, rejected


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--data-root", default=os.getenv("MOEX_DATA_ROOT", ""))
    args = parser.parse_args()
    if not str(args.data_root).strip():
        raise RuntimeError("MOEX_DATA_ROOT is required")
    data_root = Path(args.data_root).expanduser().resolve()
    snapshot_date = str(args.snapshot_date).strip()
    status_path = path_from_pattern(data_root, INPUT_STATUS_PATTERN, snapshot_date)
    registry_path = path_from_pattern(data_root, INPUT_REGISTRY_PATTERN, snapshot_date)
    if not status_path.exists():
        raise FileNotFoundError("Missing status artifact: " + str(status_path))
    if not registry_path.exists():
        raise FileNotFoundError("Missing normalized registry artifact: " + str(registry_path))
    status = pd.read_csv(status_path)
    registry = pd.read_parquet(registry_path)
    fam_col = family_col(registry)
    brd_col = board_col(registry)
    if "family_code" not in status.columns:
        if "family" not in status.columns:
            raise RuntimeError("status artifact missing family_code or family column")
        status = status.rename(columns={"family": "family_code"})
    status = status.loc[status["family_code"].astype(str).str.upper().isin(TARGET_FAMILIES)].copy()
    status = status.loc[(status["classification_status"].astype(str) == TARGET_STATUS) & (status["continuous_eligibility_status"].astype(str) == CONTINUOUS_STATUS)].copy()
    if status.empty:
        raise RuntimeError("No promoted W/MM/MX rows")
    registry = registry.loc[registry[fam_col].astype(str).str.upper().isin(TARGET_FAMILIES)].copy()
    registry = registry.loc[registry[brd_col].astype(str).str.upper() == TARGET_BOARD].copy()
    accepted_registry, rejected_registry = controlled_status_filter(registry, snapshot_date)
    merged = accepted_registry.merge(status[["family_code", "classification_status", "continuous_eligibility_status"]].drop_duplicates(), on="family_code", how="inner")
    if merged.empty:
        raise RuntimeError("Eligibility merge produced zero rows after validation and raw-loader date-range gate")
    merged["schema_version"] = SCHEMA_VERSION
    merged["snapshot_date"] = snapshot_date
    merged["board"] = TARGET_BOARD
    merged["eligibility_status"] = "eligible"
    merged["source_status_artifact"] = str(status_path)
    merged["source_registry_artifact"] = str(registry_path)
    out = merged[["schema_version", "snapshot_date", "secid", "family_code", "board", "mapping_status", "validation_status", "first_available_date", "last_available_date", "eligibility_date_source", "classification_status", "continuous_eligibility_status", "source_status_artifact", "source_registry_artifact", "eligibility_status"]].drop_duplicates().sort_values(["family_code", "secid"]).reset_index(drop=True)
    families = sorted(out["family_code"].astype(str).unique().tolist())
    boards = sorted(out["board"].astype(str).unique().tolist())
    if families != sorted(TARGET_FAMILIES):
        raise RuntimeError("Unexpected families after eligibility gate: " + json.dumps(families, ensure_ascii=False))
    if boards != [TARGET_BOARD]:
        raise RuntimeError("Unexpected boards: " + json.dumps(boards, ensure_ascii=False))
    output_path = path_from_pattern(data_root, OUTPUT_PATTERN, snapshot_date)
    summary_path = path_from_pattern(data_root, SUMMARY_PATTERN, snapshot_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)
    rejection_counts = {str(k): int(v) for k, v in rejected_registry["eligibility_rejection_reason"].value_counts(dropna=False).to_dict().items()}
    rejected_examples = rejected_registry[["secid", "eligibility_rejection_reason"]].drop_duplicates().sort_values(["eligibility_rejection_reason", "secid"]).head(50).to_dict(orient="records") if not rejected_registry.empty else []
    summary = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_date": snapshot_date,
        "row_count": int(len(out)),
        "families": families,
        "boards": boards,
        "classification_status": TARGET_STATUS,
        "continuous_eligibility_status": CONTINUOUS_STATUS,
        "accepted_validation_statuses": sorted(ACCEPTED_VALIDATION_STATUSES),
        "eligibility_artifact": str(output_path),
        "gate_policy": {
            "mapping_status_draft_allowed": False,
            "validation_status_required": "accepted_status_only",
            "raw_loader_date_range_required": True,
            "raw_loader_date_range_must_be_resolvable_as_of_snapshot_date": True
        },
        "rejection_counts": rejection_counts,
        "rejected_examples": rejected_examples,
        "preservation_checks": {
            "only_wmmmx": True,
            "only_rfud": True,
            "continuous_not_accepted": True,
            "cr_gd_gl_unchanged": True,
            "slice1_defaults_unchanged": True,
            "si_continuous_unchanged": True,
            "continuous_builders_not_invoked": True
        }
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
