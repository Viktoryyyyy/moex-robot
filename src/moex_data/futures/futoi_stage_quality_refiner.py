#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

DATASET_STAGE = "futoi_raw"
SCHEMA_ELIGIBILITY = "futures_all_universe_eligibility_snapshot.v1"


def data_root(args):
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT or --data-root is required")
    return Path(raw).expanduser().resolve()


def input_path(root, snapshot_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return root / "futures" / "all_universe" / "eligibility_snapshot_futoi_raw" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def output_path(root, snapshot_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return root / "futures" / "all_universe" / "eligibility_snapshot_futoi_raw_refined" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def manifest_path(root, run_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return root / "futures" / "runs" / "futoi_stage_quality_refiner" / ("run_date=" + run_date) / "manifest.json"


def quality_report_paths(root, explicit_paths):
    if explicit_paths:
        return [Path(x).expanduser().resolve() for x in explicit_paths.split(",") if x.strip()]
    base = root / "futures" / "quality" / "futoi_raw_backfill"
    return sorted(base.glob("chunk_id=*/quality_report.parquet"))


def require_columns(frame, cols, name):
    missing = [x for x in cols if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing columns: " + ", ".join(missing))


def load_quality(paths, run_date):
    frames = []
    for path in paths:
        if path.exists():
            frame = pd.read_parquet(path)
            if len(frame):
                frames.append(frame)
    if not frames:
        raise RuntimeError("No non-empty FUTOI quality reports found")
    quality = pd.concat(frames, ignore_index=True)
    require_columns(quality, ["run_date", "secid", "quality_status", "rows", "partition_count"], "futoi_quality_reports")
    quality = quality.loc[quality["run_date"].astype(str) == str(run_date)].copy()
    if quality.empty:
        raise RuntimeError("No FUTOI quality rows for run_date=" + str(run_date))
    quality = quality.sort_values(["secid", "quality_status"]).drop_duplicates(subset=["secid"], keep="last")
    return quality


def refine(eligibility, quality):
    require_columns(eligibility, ["secid", "board", "classification_status", "futoi_eligible", "schema_version"], "eligibility_snapshot")
    pass_secids = set(quality.loc[
        (quality["quality_status"].astype(str) == "pass")
        & (pd.to_numeric(quality["rows"], errors="coerce") > 0)
        & (pd.to_numeric(quality["partition_count"], errors="coerce") > 0),
        "secid"
    ].astype(str).tolist())
    out = eligibility.copy()
    was_selected = out["futoi_eligible"] == True
    still_selected = was_selected & out["secid"].astype(str).isin(pass_secids)
    out["futoi_eligible"] = False
    out.loc[still_selected, "futoi_eligible"] = True
    if "futoi_check_status" not in out.columns:
        out["futoi_check_status"] = ""
    out.loc[was_selected & ~still_selected, "futoi_check_status"] = "futoi_zero_rows_deferred"
    out.loc[still_selected, "futoi_check_status"] = "pass"
    out["dataset_stage"] = DATASET_STAGE
    out["schema_version"] = SCHEMA_ELIGIBILITY
    notes = out["notes"].astype(str) if "notes" in out.columns else pd.Series([""] * len(out), index=out.index)
    out["notes"] = notes + " | PM L3-4 futoi_raw quality-refined eligibility"
    return out, was_selected, still_selected


def write_manifest(path, run_date, snapshot_date, input_value, output_value, quality_count, was_selected, still_selected):
    data = {
        "schema_version": "futoi_stage_quality_refiner_manifest.v1",
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "dataset_stage": DATASET_STAGE,
        "input_eligibility_snapshot": str(input_value),
        "output_eligibility_snapshot": str(output_value),
        "quality_rows_used": int(quality_count),
        "previous_futoi_eligible_count": int(was_selected.sum()),
        "refined_futoi_eligible_count": int(still_selected.sum()),
        "deferred_zero_row_count": int((was_selected & ~still_selected).sum()),
        "selection_rule": "classification_status=included and previous futoi_eligible=true and latest FUTOI quality_status=pass with rows>0 and partition_count>0",
        "created_at": utc_now_iso(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return data


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--input-eligibility", default="")
    parser.add_argument("--output-eligibility", default="")
    parser.add_argument("--quality-reports", default="")
    parser.add_argument("--output-manifest", default="")
    args = parser.parse_args()
    root = data_root(args)
    in_path = input_path(root, str(args.snapshot_date), str(args.input_eligibility or ""))
    out_path = output_path(root, str(args.snapshot_date), str(args.output_eligibility or ""))
    man_path = manifest_path(root, str(args.run_date), str(args.output_manifest or ""))
    if not in_path.exists():
        raise FileNotFoundError("Missing FUTOI-stage eligibility snapshot: " + str(in_path))
    eligibility = pd.read_parquet(in_path)
    quality = load_quality(quality_report_paths(root, str(args.quality_reports or "")), str(args.run_date))
    out, was_selected, still_selected = refine(eligibility, quality)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    data = write_manifest(man_path, str(args.run_date), str(args.snapshot_date), in_path, out_path, len(quality), was_selected, still_selected)
    print(json.dumps(data, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if data["refined_futoi_eligible_count"] > 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
