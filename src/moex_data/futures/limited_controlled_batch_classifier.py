from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from pathlib import Path
from typing import Any

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


def _candidate_paths(data_root: str, snapshot_date: str, patterns: list[str]) -> list[Path]:
    results: list[Path] = []
    for pattern in patterns:
        expanded = pattern.format(snapshot_date=snapshot_date)
        full_pattern = os.path.join(data_root, expanded)
        for match in glob.glob(full_pattern):
            path = Path(match)
            if path.is_file():
                results.append(path)
    deduped = []
    seen = set()
    for path in results:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            deduped.append(path)
    return deduped


def _normalize_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _extract_rows(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for value in payload.values():
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []
    if suffix == ".csv":
        with path.open("r", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    if suffix == ".parquet":
        try:
            import pandas as pd
        except Exception as exc:
            raise RuntimeError("pandas required for parquet support") from exc
        return pd.read_parquet(path).to_dict("records")
    return []


def _family_from_row(row: dict[str, Any]) -> str:
    for key in ["family", "family_code", "underlying_family", "symbol_family"]:
        value = row.get(key)
        if value:
            return str(value).strip()
    return ""


def _status_from_row(row: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def classify(snapshot_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = _load_config()
    data_root = os.environ.get("MOEX_DATA_ROOT", "")
    if not data_root:
        raise RuntimeError("MOEX_DATA_ROOT is required")

    patterns = config["evidence"]["input_patterns"]
    candidate_files = _candidate_paths(data_root, snapshot_date, patterns)
    if not candidate_files:
        raise RuntimeError("No rfud_candidates artifacts found")

    family_rows: dict[str, dict[str, Any]] = {}

    for path in candidate_files:
        for row in _extract_rows(path):
            family = _family_from_row(row)
            if family not in ALLOWED_FAMILIES:
                continue

            raw_status = _normalize_value(
                _status_from_row(
                    row,
                    ["raw_futoi_status", "raw_status", "futoi_status", "availability_status"],
                )
            )
            liquidity_status = _normalize_value(
                _status_from_row(
                    row,
                    ["liquidity_history_status", "history_status", "liquidity_status"],
                )
            )

            accepted_raw = raw_status in {
                "complete",
                "pass",
                "accepted",
                "available",
                "ok",
                "true",
            }
            review_required = liquidity_status == "review_required"

            if accepted_raw and review_required:
                family_rows[family] = {
                    "snapshot_date": snapshot_date,
                    "family": family,
                    "classification_status": CONTROLLED_STATUS,
                    "liquidity_history_status": "review_required",
                    "raw_futoi_status": "complete",
                    "continuous_eligibility_status": CONTINUOUS_STATUS,
                    "evidence_source": str(path),
                    "controlled_batch_id": config["controlled_batch_id"],
                }

    final_rows = [family_rows[key] for key in sorted(family_rows)]

    invalid = [row["family"] for row in final_rows if row["family"] not in ALLOWED_FAMILIES]
    if invalid:
        raise RuntimeError(f"Unexpected families selected: {invalid}")

    summary = {
        "snapshot_date": snapshot_date,
        "row_count": len(final_rows),
        "families": [row["family"] for row in final_rows],
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
        writer.writerows(final_rows)

    summary_path = Path(data_root) / summary_pattern
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return final_rows, {
        "output_artifact": str(output_path),
        "summary_artifact": str(summary_path),
        "summary": summary,
    }


def main() -> None:
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
