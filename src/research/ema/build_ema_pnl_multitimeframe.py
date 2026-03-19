#!/usr/bin/env python3
import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd


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
    items = [x.strip() for x in str(raw).split(",") if x.strip()]
    allowed = ["5m", "15m", "30m", "1h"]
    if not items:
        return allowed

    for tf in items:
        if tf not in allowed:
            raise ValueError("Unsupported timeframe: " + tf + ". Allowed: " + ", ".join(allowed))

    out = []
    for tf in items:
        if tf not in out:
            out.append(tf)
    return out


def resample_ohlc(x: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe == "5m":
        return x.copy()

    rule_map = {
        "15m": "15min",
        "30m": "30min",
        "1h": "1h",
    }
    rule = rule_map[timeframe]

    y = x.copy().set_index("end").sort_index()
    r = (
        y.resample(rule, label="right", closed="right")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna(subset=["close"])
        .reset_index()
    )
    return r


def run_backtest_day_metrics(x: pd.DataFrame, commission_points: float) -> pd.DataFrame:
    w = x.copy()

    w["ema_fast"] = w["close"].ewm(span=5, adjust=False).mean()
    w["ema_slow"] = w["close"].ewm(span=12, adjust=False).mean()

    diff_ema = w["ema_fast"] - w["ema_slow"]
    w["pos_raw"] = np.sign(diff_ema).astype(float)
    w["pos"] = w["pos_raw"].shift(1).fillna(0.0)

    w["dclose"] = w["close"].diff()
    w["trades"] = w["pos"].diff().abs().fillna(0.0)
    w["fee"] = w["trades"] * float(commission_points)
    w["bar_pnl"] = w["pos"] * w["dclose"].fillna(0.0) - w["fee"]

    w["date"] = w["end"].dt.date.astype(str)
    w["cum_pnl_day"] = w.groupby("date")["bar_pnl"].cumsum()
    w["run_max_day"] = w.groupby("date")["cum_pnl_day"].cummax()
    w["dd_day"] = w["cum_pnl_day"] - w["run_max_day"]
    dd = w.groupby("date")["dd_day"].min().mul(-1.0)

    out = pd.DataFrame(
        {
            "date": w.groupby("date")["date"].first(),
            "pnl_day": w.groupby("date")["bar_pnl"].sum(),
            "max_dd_day": dd,
            "num_trades_day": w.groupby("date")["trades"].sum(),
        }
    ).reset_index(drop=True)

    out["EMA_EDGE_DAY"] = (out["pnl_day"] > 0).astype(int)
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
        default="data/research/ema_pnl_multitimeframe.csv",
        help="Output CSV path",
    )
    ap.add_argument("--commission_points", type=float, default=2.0, help="Commission in points per trade action")
    ap.add_argument("--timeframes", default="5m,15m,30m,1h", help="Comma-separated set from: 5m,15m,30m,1h")
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
    x.columns = ["end", "open", "high", "low", "close"]

    x["end"] = pd.to_datetime(x["end"], errors="coerce")
    x = x.dropna(subset=["end", "close"]).sort_values("end").reset_index(drop=True)

    all_out = []
    for tf in tfs:
        bars = resample_ohlc(x, tf)
        out_tf = run_backtest_day_metrics(bars, commission_points=args.commission_points)
        out_tf.insert(0, "timeframe", tf)
        out_tf = out_tf[["timeframe", "date", "pnl_day", "max_dd_day", "num_trades_day", "EMA_EDGE_DAY"]]
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
