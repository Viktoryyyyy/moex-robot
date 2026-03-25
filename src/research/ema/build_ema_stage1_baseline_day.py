#!/usr/bin/env python3
import argparse
import glob
import os
import sys

import pandas as pd

from src.research.ema.lib_ema_search import (
    generate_ema_signals,
    run_point_backtest,
    summarize_backtest_by_day,
)


def pick_latest_master(path_glob: str) -> str:
    files = sorted(glob.glob(path_glob))
    if not files:
        raise FileNotFoundError("No files match: " + path_glob)
    return files[-1]


def resolve_ohlc_cols(df_cols):
    need = ["end", "open", "high", "low", "close"]
    if all(c in df_cols for c in need):
        return {k: k for k in need}

    candidates = [
        {"end": "end", "open": "open_fo", "high": "high_fo", "low": "low_fo", "close": "close_fo"},
        {"end": "end", "open": "OPEN", "high": "HIGH", "low": "LOW", "close": "CLOSE"},
    ]
    for m in candidates:
        if all(m[k] in df_cols for k in need):
            return m

    raise KeyError("Cannot resolve OHLC columns. Available columns sample: " + str(list(df_cols)[:30]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--master_csv",
        default="",
        help="Path to master CSV. If empty, auto-pick latest /home/trader/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv",
    )
    ap.add_argument(
        "--out_csv",
        default="data/research/ema_stage1_baseline_day.csv",
        help="Output CSV path",
    )
    args = ap.parse_args()

    master_csv = args.master_csv.strip()
    if not master_csv:
        master_csv = pick_latest_master("/home/trader/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv")

    if not os.path.exists(master_csv):
        raise FileNotFoundError(master_csv)

    df = pd.read_csv(master_csv)
    colmap = resolve_ohlc_cols(df.columns)

    bars = df[[colmap["end"], colmap["open"], colmap["high"], colmap["low"], colmap["close"]]].copy()
    bars.columns = ["ts", "open", "high", "low", "close"]
    bars["ts"] = pd.to_datetime(bars["ts"], errors="coerce")
    bars = bars.dropna(subset=["ts", "open", "high", "low", "close"]).sort_values("ts").reset_index(drop=True)

    bars = generate_ema_signals(
        bars,
        ema_fast_span=5,
        ema_slow_span=12,
        mode="trend_long_short",
    )
    bars = run_point_backtest(bars, commission_points=2.0)

    days = summarize_backtest_by_day(bars).copy()
    days["max_dd"] = days["dd_day"].abs()
    days["win_rate"] = (days["pnl_day"] > 0.0).astype(float)
    out = days[["date", "pnl_day", "win_rate", "max_dd", "num_trades_day"]].copy()

    out_dir = os.path.dirname(args.out_csv)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    out.to_csv(args.out_csv, index=False)

    print("MASTER: " + master_csv)
    print("OUT:    " + args.out_csv)
    print("DAYS:   " + str(len(out)))
    print("COLS:   " + ",".join(out.columns.tolist()))
    print(out.head(3).to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR: " + str(e), file=sys.stderr)
        sys.exit(1)
