import argparse
import glob
import os
import sys
import pandas as pd
import numpy as np


def pick_latest_master(path_glob: str) -> str:
    files = sorted(glob.glob(path_glob))
    if not files:
        raise FileNotFoundError(f"No files match: {path_glob}")
    # Lexicographic sort works with your naming convention YYYY-MM-DD inside filename
    return files[-1]


def resolve_ohlc_cols(df_cols):
    # Spec says: end, open, high, low, close
    need = ["end", "open", "high", "low", "close"]
    if all(c in df_cols for c in need):
        return {k: k for k in need}

    # Fallbacks (if master was merged with suffixes)
    candidates = [
        {"end": "end", "open": "open_fo", "high": "high_fo", "low": "low_fo", "close": "close_fo"},
        {"end": "end", "open": "OPEN", "high": "HIGH", "low": "LOW", "close": "CLOSE"},
    ]
    for m in candidates:
        if all(m[k] in df_cols for k in need):
            return m

    raise KeyError(f"Cannot resolve OHLC columns. Available columns sample: {list(df_cols)[:30]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--master_csv",
        default="",
        help="Path to master CSV. If empty, auto-pick latest data/master/master_5m_si_cny_futoi_obstats_*.csv",
    )
    ap.add_argument(
        "--out_csv",
        default="data/research/ema_pnl_day.csv",
        help="Output CSV path",
    )
    ap.add_argument("--ema_fast", type=int, default=5)
    ap.add_argument("--ema_slow", type=int, default=12)
    ap.add_argument("--commission_points", type=float, default=2.0, help="Commission in points per trade action")
    args = ap.parse_args()

    if args.ema_fast != 5 or args.ema_slow != 12:
        raise ValueError("EMA params are fixed: fast=5 slow=12 (do not change).")

    master_csv = args.master_csv.strip()
    if not master_csv:
        master_csv = pick_latest_master("data/master/master_5m_si_cny_futoi_obstats_*.csv")

    if not os.path.exists(master_csv):
        raise FileNotFoundError(master_csv)

    df = pd.read_csv(master_csv)
    colmap = resolve_ohlc_cols(df.columns)

    x = df[[colmap["end"], colmap["open"], colmap["high"], colmap["low"], colmap["close"]]].copy()
    x.columns = ["end", "open", "high", "low", "close"]

    x["end"] = pd.to_datetime(x["end"], errors="coerce")
    x = x.dropna(subset=["end", "close"]).sort_values("end").reset_index(drop=True)

    # EMA on close
    x["ema_fast"] = x["close"].ewm(span=5, adjust=False).mean()
    x["ema_slow"] = x["close"].ewm(span=12, adjust=False).mean()

    # pos_t = sign(EMA5 - EMA12)
    diff_ema = x["ema_fast"] - x["ema_slow"]
    x["pos_raw"] = np.sign(diff_ema).astype(float)

    # anti-cheat shift: position is applied with 1 bar delay
    x["pos"] = x["pos_raw"].shift(1).fillna(0.0)

    # PnL points on close-to-close move with applied position
    x["dclose"] = x["close"].diff()

    # trades: number of trade actions implied by position change
    # flip +1 -> -1 => abs(delta)=2 => 2 trades (close + open)
    x["trades"] = x["pos"].diff().abs().fillna(0.0)

    x["fee"] = x["trades"] * float(args.commission_points)
    x["bar_pnl"] = x["pos"] * x["dclose"].fillna(0.0) - x["fee"]

    x["date"] = x["end"].dt.date.astype(str)

    # Intraday cumulative PnL and max drawdown per day
    x["cum_pnl_day"] = x.groupby("date")["bar_pnl"].cumsum()
    x["run_max_day"] = x.groupby("date")["cum_pnl_day"].cummax()
    x["dd_day"] = x["cum_pnl_day"] - x["run_max_day"]  # <= 0
    # Max DD as positive magnitude
    dd = x.groupby("date")["dd_day"].min().mul(-1.0)

    out = pd.DataFrame({
        "date": x.groupby("date")["date"].first(),
        "pnl_day": x.groupby("date")["bar_pnl"].sum(),
        "max_dd_day": dd,
        "num_trades_day": x.groupby("date")["trades"].sum(),
    }).reset_index(drop=True)

    out["EMA_EDGE_DAY"] = (out["pnl_day"] > 0).astype(int)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    out.to_csv(args.out_csv, index=False)

    # Minimal reproducibility print
    print(f"MASTER: {master_csv}")
    print(f"OUT:    {args.out_csv}")
    print(f"DAYS:   {len(out)}")
    print(out.head(3).to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
