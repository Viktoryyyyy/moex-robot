#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pandas as pd
from itertools import product

from src.research.ema.ema_backtest_core import run_backtest


def parse_range(spec: str):
    a, b = spec.split(".."); a=int(a); b=int(b)
    return list(range(a, b+1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.master)

    grids = {
        "5m": (parse_range("3..8"), parse_range("10..20")),
        "15m": (parse_range("2..6"), parse_range("12..30")),
        "1h": (parse_range("2..6"), parse_range("20..50")),
    }

    rows = []

    for tf, (fasts, slows) in grids.items():
        for f, s in product(fasts, slows):
            if f >= s:
                continue
            res = run_backtest(df, timeframe=tf, fast=f, slow=s, fee=6)
            res["timeframe"] = tf
            res["ema_fast"] = f
            res["ema_slow"] = s
            rows.append(res)

    out = pd.concat(rows)
    out.to_csv(args.out, index=False)


if __name__ == "__main__":
    main()
