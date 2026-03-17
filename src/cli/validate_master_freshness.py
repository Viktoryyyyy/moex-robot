from __future__ import annotations

import argparse
from typing import List, Optional

from src.cli.phase_transition_gate import (
    parse_day_metrics,
    resolve_canonical_master_path,
    validate_canonical_master_for_bootstrap,
)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-day", required=True)
    args = ap.parse_args(argv)

    dm = parse_day_metrics(args.in_day)
    mp = resolve_canonical_master_path()
    validate_canonical_master_for_bootstrap(mp, dm.yday_date)

    print("MASTER_FRESHNESS_OK yday=" + dm.yday_date.isoformat() + " path=" + mp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
