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

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

CONFIG_PATH = "configs/datasets/futures_all_universe_eligibility_config.json"
FUTOI_AVAILABILITY_CONTRACT = "contracts/datasets/futures_futoi_availability_report_contract.md"
SCHEMA_ELIGIBILITY = "futures_all_universe_eligibility_snapshot.v1"
DATASET_STAGE = "futoi_raw"


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def data_root(args):
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT or --data-root is required")
    return Path(raw).expanduser().resolve()


def input_eligibility_path(root, snapshot_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return root / "futures" / "all_universe" / "eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def futoi_availability_path(root, snapshot_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return root / "futures" / "availability" / ("snapshot_date=" + snapshot_date) / "futures_futoi_availability_report.parquet"


def output_eligibility_path(root, snapshot_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return root / "futures" / "all_universe" / "eligibility_snapshot_futoi_raw" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def output_manifest_path(root, run_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return root / "futures" / "runs" / "futoi_stage_eligibility" / ("run_date=" + run_date) / "manifest.json"


def require_columns(frame, required, name):
    missing = [x for x in required if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing columns: " + ", ".join(missing))


def build_stage_eligibility(eligibility, availability, board):
    require_columns(eligibility, ["board", "secid", "family_code", "classification_status", "futoi_eligible", "schema_version"], "eligibility_snapshot")
    require_columns(availability, ["board", "secid", "availability_status", "probe_status"], "futoi_availability_report")
    target_board = str(board).upper()
    available = availability.loc[
        (availability["board"].astype(str).str.upper() == target_board)
        & (availability["availability_status"].astype(str) == "available")
        & (availability["probe_status"].astype(str) == "completed")
    ].copy()
    ok = set(available["secid"].astype(str).tolist())
    out = eligibility.copy()
    board_mask = out["board"].astype(str).str.upper() == target_board
    included_mask = out["classification_status"].astype(str) == "included"
    selected_mask = board_mask & included_mask & out["secid"].astype(str).isin(ok)
    out["futoi_eligible"] = False
    out.loc[selected_mask, "futoi_eligible"] = True
    if "futoi_check_status" not in out.columns:
        out["futoi_check_status"] = ""
    out.loc[board_mask & included_mask & ~selected_mask, "futoi_check_status"] = "futoi_unavailable"
    out.loc[selected_mask, "futoi_check_status"] = "pass"
    out["dataset_stage"] = DATASET_STAGE
    out["schema_version"] = SCHEMA_ELIGIBILITY
    notes = out["notes"].astype(str) if "notes" in out.columns else pd.Series([""] * len(out), index=out.index)
    out["notes"] = notes + " | PM L3-4 futoi_raw stage eligibility"
    return out, selected_mask, available


def manifest(run_date, snapshot_date, input_path, availability_path_value, output_path, frame, selected_mask, available):
    return {
        "schema_version": "futoi_stage_eligibility_manifest.v1",
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "dataset_stage": DATASET_STAGE,
        "input_eligibility_snapshot": str(input_path),
        "input_futoi_availability_report": str(availability_path_value),
        "output_eligibility_snapshot": str(output_path),
        "rows": int(len(frame)),
        "rfud_included_futoi_eligible_count": int(selected_mask.sum()),
        "availability_available_completed_count": int(len(available)),
        "classification_counts": {str(k): int(v) for k, v in frame["classification_status"].astype(str).value_counts(dropna=False).to_dict().items()},
        "futoi_eligible_counts": {str(k): int(v) for k, v in frame["futoi_eligible"].astype(str).value_counts(dropna=False).to_dict().items()},
        "selection_rule": "board=RFUD classification_status=included futoi_eligible=true dataset_stage=futoi_raw",
        "created_at": utc_now_iso(),
    }


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--input-eligibility", default="")
    parser.add_argument("--futoi-availability", default="")
    parser.add_argument("--output-eligibility", default="")
    parser.add_argument("--output-manifest", default="")
    args = parser.parse_args()
    root = data_root(args)
    config = load_json(Path(args.config))
    board = ((config.get("l3_4_futoi_raw_included_universe") or {}).get("board") or "RFUD")
    in_path = input_eligibility_path(root, str(args.snapshot_date), str(args.input_eligibility or ""))
    av_path = futoi_availability_path(root, str(args.snapshot_date), str(args.futoi_availability or ""))
    out_path = output_eligibility_path(root, str(args.snapshot_date), str(args.output_eligibility or ""))
    man_path = output_manifest_path(root, str(args.run_date), str(args.output_manifest or ""))
    if not in_path.exists():
        raise FileNotFoundError("Missing input eligibility snapshot: " + str(in_path))
    if not av_path.exists():
        raise FileNotFoundError("Missing FUTOI availability report: " + str(av_path))
    eligibility = pd.read_parquet(in_path)
    availability = pd.read_parquet(av_path)
    out, selected_mask, available = build_stage_eligibility(eligibility, availability, board)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    data = manifest(str(args.run_date), str(args.snapshot_date), in_path, av_path, out_path, out, selected_mask, available)
    man_path.parent.mkdir(parents=True, exist_ok=True)
    man_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps(data, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if data["rfud_included_futoi_eligible_count"] > 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
