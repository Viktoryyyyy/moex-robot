from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

ALLOWED_FAMILIES = {"W", "MM", "MX"}
BLOCKED_FAMILIES = {"SiH7", "SiM7"}
PILOT_FAMILIES = {"CR", "GD", "GL"}
SLICE1_WHITELIST = {"SiM6", "SiU6", "SiU7", "SiZ6", "USDRUBF"}
CONTROLLED_STATUS = "controlled_provisional"
CONTINUOUS_STATUS = "not_accepted"


def _load_config() -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[3]
    config_path = repo_root / "configs" / "datasets" / "futures_limited_controlled_batch_config.json"
    return json.loads(config_path.read_text(encoding="utf-8"))


def _resolve_path(data_root: str, pattern: str, snapshot_date: str) -> Path:
    return Path(data_root) / pattern.format(snapshot_date=snapshot_date)


def _read_required_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError("Missing required artifact for " + label + ": " + str(path))
    frame = pd.read_parquet(path)
    if frame.empty:
        raise RuntimeError("Artifact is empty for " + label + ": " + str(path))
    return frame


def classify(snapshot_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = _load_config()
    data_root = os.environ.get("MOEX_DATA_ROOT", "")
    if not data_root:
        raise RuntimeError("MOEX_DATA_ROOT is required")

    paths = config["evidence"]["input_paths"]

    normalized = _read_required_parquet(
        _resolve_path(data_root, paths["normalized_registry"], snapshot_date),
        "normalized_registry",
    )
    tradestats = _read_required_parquet(
        _resolve_path(data_root, paths["tradestats_availability"], snapshot_date),
        "tradestats_availability",
    )
    futoi = _read_required_parquet(
        _resolve_path(data_root, paths["futoi_availability"], snapshot_date),
        "futoi_availability",
    )
    liquidity = _read_required_parquet(
        _resolve_path(data_root, paths["liquidity_screen"], snapshot_date),
        "liquidity_screen",
    )
    history = _read_required_parquet(
        _resolve_path(data_root, paths["history_depth_screen"], snapshot_date),
        "history_depth_screen",
    )

    registry = normalized[["family_code", "secid", "board"]].drop_duplicates().copy()

    tradestats_ok = tradestats.loc[
        tradestats["availability_status"].astype(str) == "available",
        ["secid", "board"],
    ].drop_duplicates().copy()
    tradestats_ok["tradestats_ok"] = True

    futoi_ok = futoi.loc[
        futoi["availability_status"].astype(str) == "available",
        ["secid", "board"],
    ].drop_duplicates().copy()
    futoi_ok["futoi_ok"] = True

    liquidity_ok = liquidity.loc[
        liquidity["liquidity_status"].astype(str) == "review_required",
        ["secid", "board"],
    ].drop_duplicates().copy()
    liquidity_ok["liquidity_ok"] = True

    history_ok = history.loc[
        history["history_depth_status"].astype(str) == "review_required",
        ["secid", "board"],
    ].drop_duplicates().copy()
    history_ok["history_ok"] = True

    merged = registry.merge(tradestats_ok, on=["secid", "board"], how="left")
    merged = merged.merge(futoi_ok, on=["secid", "board"], how="left")
    merged = merged.merge(liquidity_ok, on=["secid", "board"], how="left")
    merged = merged.merge(history_ok, on=["secid", "board"], how="left")

    merged["tradestats_ok"] = merged["tradestats_ok"].fillna(False)
    merged["futoi_ok"] = merged["futoi_ok"].fillna(False)
    merged["liquidity_ok"] = merged["liquidity_ok"].fillna(False)
    merged["history_ok"] = merged["history_ok"].fillna(False)

    selected = merged.loc[
        merged["family_code"].astype(str).isin(ALLOWED_FAMILIES)
        & merged["tradestats_ok"]
        & merged["futoi_ok"]
        & merged["liquidity_ok"]
        & merged["history_ok"]
    ].copy()

    family_rows = []
    for family in sorted(selected["family_code"].astype(str).unique().tolist()):
        family_rows.append(
            {
                "snapshot_date": snapshot_date,
                "family": family,
                "classification_status": CONTROLLED_STATUS,
                "liquidity_history_status": "review_required",
                "raw_futoi_status": "complete",
                "continuous_eligibility_status": CONTINUOUS_STATUS,
                "evidence_source": "rfud_candidates_contract_artifacts",
                "controlled_batch_id": config["controlled_batch_id"],
            }
        )

    invalid = [row["family"] for row in family_rows if row["family"] not in ALLOWED_FAMILIES]
    if invalid:
        raise RuntimeError("Unexpected families selected: " + json.dumps(invalid))

    summary = {
        "snapshot_date": snapshot_date,
        "row_count": len(family_rows),
        "families": [row["family"] for row in family_rows],
        "classification_status": CONTROLLED_STATUS,
        "continuous_eligibility_status": CONTINUOUS_STATUS,
        "preserved_pilot": sorted(PILOT_FAMILIES),
        "preserved_slice_1": sorted(SLICE1_WHITELIST),
        "blocked_non_promoted": sorted(BLOCKED_FAMILIES),
    }

    output_pattern = config["output"]["path_pattern"].format(snapshot_date=snapshot_date)
    summary_pattern = config["output"]["summary_path_pattern"].format(snapshot_date=snapshot_date)

    output_path = Path(data_root) / output_pattern
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "snapshot_date",
            "family",
            "classification_status",
            "liquidity_history_status",
            "raw_futoi_status",
            "continuous_eligibility_status",
            "evidence_source",
            "controlled_batch_id",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(family_rows)

    summary_path = Path(data_root) / summary_pattern
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return family_rows, {
        "output_artifact": str(output_path),
        "summary_artifact": str(summary_path),
        "summary": summary,
    }


def main() -> None:
    if load_dotenv is not None:
        load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    args = parser.parse_args()

    rows, metadata = classify(snapshot_date=args.snapshot_date)
    print(json.dumps({
        "row_count": len(rows),
        "output_artifact": metadata["output_artifact"],
        "summary": metadata["summary"],
    }, indent=2))


if __name__ == "__main__":
    main()
