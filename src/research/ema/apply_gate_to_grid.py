#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pandas as pd

from src.applied.context_filter.d_day_context import build_context_payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--grid", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    grid = pd.read_csv(args.grid)

    days = sorted(grid["date"].unique())

    ctx_rows = []
    for d in days:
        payload = build_context_payload(master_path=args.master, target_day=str(d))
        ctx_rows.append(payload)

    ctx = pd.DataFrame(ctx_rows)

    merged = grid.merge(ctx, left_on="date", right_on="target_day", how="left")

    blocked = merged.loc[merged["blocked"] != True].copy()

    blocked.to_csv(args.out, index=False)


if __name__ == "__main__":
    main()
