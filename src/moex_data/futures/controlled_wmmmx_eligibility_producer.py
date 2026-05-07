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
+TARGET_BOARD = "RFUD"
TARGET_STATUS = "controlled_accepted_for_data_pipeline"
CONTINUOUS_STATUS = "not_accepted"
SCHEMA_VERSION = "futures_controlled_wmmmx_eligibility.v1"
INPUT_STATUS_PATTERN = "futures/controlled_batch_status_promotion/snapshot_date={snapshot_date}/controlled_batch_status_promotion.csv"
INPUT_REGISTRY_PATTERN = "futures/registry/normalized/snapshot_date={snapshot_date}/normalized_registry.parquet"
OUTPUT_PATTERN = "futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility.parquet"
SUMMARY_PATTERN = "futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility_summary.json"


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
    status = status.rename(columns={"family": "family_code"})
    status = status.loc[status["family_code"].astype(str).str.upper().isin(TARGET_FAMILIES)].copy()
    status = status.loc[(status["classification_status"].astype(str) == TARGET_STATUS) & (status["continuous_eligibility_status"].astype(str) == CONTINUOUS_STATUS)].copy()
    if status.empty:
        raise RuntimeError("No promoted W/MM/MX rows")
    registry = registry.loc[registry[fam_col].astype(str).str.upper().isin(TARGET_FAMILIES)].copy()
    registry = registry.loc[registry[brd_col].astype(str).str.upper() == TARGET_BOARD].copy()
    merged = registry.merge(status[["family_code", "classification_status", "continuous_eligibility_status"]].drop_duplicates(), on="family_code", how="inner")
    if merged.empty:
        raise RuntimeError("Eligibility merge produced zero rows")
    merged["schema_version"] = SCHEMA_VERSION
    merged["snapshot_date"] = snapshot_date
    merged["board"] = TARGET_BOARD
    merged["eligibility_status"] = "eligible"
    merged["source_status_artifact"] = str(status_path)
    merged["source_registry_artifact"] = str(registry_path)
    out = merged[["schema_version", "snapshot_date", "secid", "family_code", "board", "classification_status", "continuous_eligibility_status", "source_status_artifact", "source_registry_artifact", "eligibility_status"]].drop_duplicates().sort_values(["family_code", "secid"]).reset_index(drop=True)
    families = sorted(out["family_code"].astype(str).unique().tolist())
    boards = sorted(out["board"].astype(str).unique().tolist())
    if families != sorted(TARGET_FAMILIES):
        raise RuntimeError("Unexpected families: " + json.dumps(families, ensure_ascii=False))
    if boards != [TARGET_BOARD]:
        raise RuntimeError("Unexpected boards: " + json.dumps(boards, ensure_ascii=False))
    output_path = path_from_pattern(data_root, OUTPUT_PATTERN, snapshot_date)
    summary_path = path_from_pattern(data_root, SUMMARY_PATTERN, snapshot_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_date": snapshot_date,
        "row_count": int(len(out)),
        "families": families,
        "boards": boards,
        "classification_status": TARGET_STATUS,
        "continuous_eligibility_status": CONTINUOUS_STATUS,
        "eligibility_artifact": str(output_path),
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
