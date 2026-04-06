#!/usr/bin/env python3
import argparse
import glob
import os
import sys

import pandas as pd

from src.research.ema.lib_ema_search import (
    generate_ema_signals,
    resample_ohlc,
    run_point_backtest,
    summarize_backtest_by_day,
)

EMA_BASELINE_MATRIX = {
    "5m": (5, 12),
    "15m": (3, 19),
    "1h": (2, 37),
}


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


def parse_timeframes(raw: str):
    default_items = list(EMA_BASELINE_MATRIX.keys())
    items = [x.strip() for x in str(raw).split(",") if x.strip()]
    if not items:
        return default_items

    for tf in items:
        if tf not in EMA_BASELINE_MATRIX:
            raise ValueError(
                "Unsupported timeframe: " + tf + ". Allowed: " + ", ".join(default_items)
            )

    out = []
    for tf in items:
        if tf not in out:
            out.append(tf)
    return out


def run_backtest_day_metrics(x: pd.DataFrame, timeframe: str, commission_points: float) -> pd.DataFrame:
    ema_fast_span, ema_slow_span = EMA_BASELINE_MATRIX[timeframe]
    bars = generate_ema_signals(
        x,
        ema_fast_span=ema_fast_span,
        ema_slow_span=ema_slow_span,
        mode="trend_long_short",
    )
    bars = run_point_backtest(bars, commission_points=commission_points)
    out = summarize_backtest_by_day(bars).copy()

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    out["max_dd_day"] = pd.to_numeric(out["dd_day"], errors="coerce").fillna(0.0).mul(-1.0)
    out["EMA_EDGE_DAY"] = (pd.to_numeric(out["pnl_day"], errors="coerce").fillna(0.0) > 0.0).astype(int)

    out = out[["date", "pnl_day", "max_dd_day", "num_trades_day", "EMA_EDGE_DAY"]].copy()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--master_csv",
        default="",
        help="Path to master CSV. If empty, auto-pick latest /home/trader/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv",
    )
    ap.add_argument(
        "--out_csv",
        default="data/research/usdrubf_ema_pnl_baseline_matrix.csv",
        help="Output CSV path",
    )
    ap.add_argument("--commission_points", type=float, default=2.0, help="Commission in points per trade action")
    ap.add_argument("--timeframes", default="5m,15m,1h", help="Comma-separated set from: 5m,15m,1h")
    args = ap.parse_args()

    master_csv = args.master_csv.strip()
    if not master_csv:
        master_csv = pick_latest_master("/home/trader/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv")

    if not os.path.exists(master_csv):
        raise FileNotFoundError(master_csv)

    tfs = parse_timeframes(args.timeframes)

    df = pd.read_csv(master_csv)
    colmap = resolve_ohlc_cols(df.columns)

    x = df[[colmap["end"], colmap["open"], colmap["high"], colmap["low"], colmap["close"]]].copy()
    x.columns = ["ts", "open", "high", "low", "close"]

    x["ts"] = pd.to_datetime(x["ts"], errors="coerce")
    x = x.dropna(subset=["ts", "open", "high", "low", "close"]).sort_values("ts").reset_index(drop=True)

    all_out = []
    for tf in tfs:
        bars = resample_ohlc(x, tf)
        out_tf = run_backtest_day_metrics(bars, timeframe=tf, commission_points=args.commission_points)
        out_tf.insert(0, "ema_slow", EMA_BASELINE_MATRIX[tf][1])
        out_tf.insert(0, "ema_fast", EMA_BASELINE_MATRIX[tf][0])
        out_tf.insert(0, "timeframe", tf)
        out_tf = out_tf[
            ["timeframe", "ema_fast", "ema_slow", "date", "pnl_day", "max_dd_day", "num_trades_day", "EMA_EDGE_DAY"]
        ]
        all_out.append(out_tf)

    out = pd.concat(all_out, ignore_index=True)

    out_dir = os.path.dirname(args.out_csv)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    out.to_csv(args.out_csv, index=False)

    print("MASTER: " + master_csv)
    print("OUT:    " + args.out_csv)
    print("TFS:    " + ",".join(tfs))
    print("ROWS:   " + str(len(out)))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR: " + str(e), file=sys.stderr)
        sys.exit(1)
