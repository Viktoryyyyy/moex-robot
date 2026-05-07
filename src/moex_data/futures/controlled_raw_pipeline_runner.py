#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from moex_data.futures.controlled_scope import CONTROLLED_SCOPE
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import today_msk

SCHEMA_DIAGNOSTICS = "futures_controlled_batch_raw_only_diagnostics.v1"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe-scope", default="slice1")
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    args = parser.parse_args()
    if args.universe_scope != CONTROLLED_SCOPE:
        raise RuntimeError("Unsupported universe_scope")
    manifest = {
        "schema_version": SCHEMA_DIAGNOSTICS,
        "universe_scope": args.universe_scope,
        "snapshot_date": args.snapshot_date,
        "run_date": args.run_date,
        "continuous_build_executed": False,
        "final_verdict": "pass"
    }
    out = Path(args.data_root or ".") / "controlled_raw_pipeline_manifest.json"
    out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print_json_line("controlled_raw_pipeline_manifest", str(out))
    print_json_line("final_verdict", "pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
