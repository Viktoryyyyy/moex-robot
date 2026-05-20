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

from moex_data.futures import all_universe_raw_5m_backfill_slice as au
from moex_data.futures import liquidity_history_metrics_probe as base


def dump_summary(data):
    print(json.dumps(data, ensure_ascii=False, sort_keys=True, default=str))


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=au.today_msk())
    parser.add_argument("--run-date", default=au.today_msk())
    parser.add_argument("--config", default="configs/datasets/futures_all_universe_eligibility_config.json")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--selection-mode", choices=[au.MODE_L3_2, au.MODE_L3_3], default=au.MODE_L3_3)
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    repo_root = Path.cwd().resolve()
    root = au.data_root(args)
    base.assert_files_exist(repo_root, au.REQUIRED_SOT_FILES)
    config = au.load_json(repo_root / args.config)
    mode = au.selection_mode(config, args.selection_mode)
    if config.get("continuous_build_enabled") is not False or config.get("w1_build_enabled") is not False:
        raise RuntimeError("continuous_build_enabled and w1_build_enabled must be false")

    source_path, normalized = au.load_registry(repo_root, root, args.snapshot_date)
    run_id = "all_universe_eligibility_snapshot_" + args.run_date + "_" + base.stable_id([args.snapshot_date, source_path, au.now_utc(), mode])
    registry = au.build_registry(normalized, args.snapshot_date, source_path, run_id, config)
    registry_snapshot_id = "registry_snapshot_" + base.stable_id([args.snapshot_date, source_path, len(registry)])

    if mode == au.MODE_L3_2:
        selected_ids, _ = au.choose(registry, config)
        recent_count = int((config.get("first_executable_slice") or {}).get("recent_trading_dates", 3))
    else:
        selected_ids = []
        recent_count = int((config.get("l3_3_raw_5m_included_universe") or {}).get("recent_trading_dates", 3))

    dates = au.recent_dates(args.snapshot_date, recent_count, float(args.timeout), str(args.iss_base_url))
    eligibility = au.build_eligibility(registry, selected_ids, dates, config, registry_snapshot_id, mode)
    selected = au.selected_universe(eligibility)
    chunk_id = "eligibility_snapshot_" + base.stable_id([registry_snapshot_id, mode, args.snapshot_date])
    out = au.paths(root, args.snapshot_date, chunk_id)
    au.write_parquet(out["registry_snapshot"], registry)
    au.write_parquet(out["eligibility_snapshot"], eligibility)

    summary = {
        "outputs": {"registry_snapshot": out["registry_snapshot"], "eligibility_snapshot": out["eligibility_snapshot"]},
        "selection_mode": mode,
        "snapshot_status": "succeeded",
        "selected_universe": {
            "dataset_stage": "eligibility_snapshot",
            "family_count": int(selected["family_code"].nunique()),
            "secid_count": int(len(selected)),
            "secids": selected["secid"].astype(str).tolist(),
            "trading_dates": dates,
        },
        "aggregate_report": {
            "candidate_universe_count": int(len(registry)),
            "included_count": int((eligibility["classification_status"] == "included").sum()),
            "deferred_count": int((eligibility["classification_status"] == "deferred").sum()),
            "excluded_count": int((eligibility["classification_status"] == "excluded").sum()),
            "raw_5m_eligible_count": int((eligibility["raw_5m_eligible"] == True).sum()),
            "raw_d1_eligible_count": int((eligibility["raw_d1_eligible"] == True).sum()),
            "continuous_v1_eligible_count": int((eligibility["continuous_v1_eligible"] == True).sum()),
            "w1_gap_count": int((eligibility["w1_status"].astype(str) == "known_gap").sum()),
        },
    }
    dump_summary(summary)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
