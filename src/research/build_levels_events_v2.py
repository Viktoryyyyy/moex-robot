import argparse
from pathlib import Path
import numpy as np
import pandas as pd

Z_ATR = 0.25

def atr14(d1):
    hl = d1["high"] - d1["low"]
    hc = (d1["high"] - d1["close"].shift()).abs()
    lc = (d1["low"] - d1["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(14, min_periods=5).mean()

def classify_approach(d1, i, L, direction, N, K, X):
    t = i
    Z = Z_ATR * d1.loc[t, "ATR14"]

    if pd.isna(Z):
        return None

    lo = max(0, t - K)
    loN = max(0, t - N)

    # rejection
    for d1_idx in range(lo, t):
        low = d1.loc[d1_idx, "low"]
        high = d1.loc[d1_idx, "high"]
        close = d1.loc[d1_idx, "close"]
        atr = d1.loc[d1_idx, "ATR14"]

        if pd.isna(atr):
            continue

        touched = (low <= L <= high) or abs(close - L) <= Z
        if not touched:
            continue

        for d2_idx in range(d1_idx + 1, t):
            atr2 = d1.loc[d2_idx, "ATR14"]
            if pd.isna(atr2):
                continue

            if direction == "up":
                excursion = L - d1.loc[d2_idx, "low"]
            else:
                excursion = d1.loc[d2_idx, "high"] - L

            if excursion >= X * atr2:
                return "rejection"

    # retest
    had_touch = False
    for d in range(lo, t):
        low = d1.loc[d, "low"]
        high = d1.loc[d, "high"]
        close = d1.loc[d, "close"]
        atr = d1.loc[d, "ATR14"]

        if pd.isna(atr):
            continue

        if (low <= L <= high) or abs(close - L) <= Z:
            had_touch = True

        if direction == "up":
            excursion = L - low
        else:
            excursion = high - L

        if excursion >= X * atr:
            return None

    if had_touch:
        return "retest"

    # direct
    for d in range(loN, t):
        low = d1.loc[d, "low"]
        high = d1.loc[d, "high"]
        close = d1.loc[d, "close"]

        if (low <= L <= high) or abs(close - L) <= Z:
            return None

    return "direct"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--levels", default="data/research/levels_v1.csv")
    ap.add_argument("--events", default="data/research/level_events_v1.csv")
    ap.add_argument("--regime", default="data/research/regime_day_r1r4.csv")
    ap.add_argument("--out", default="data/research/level_events_v2.csv")
    ap.add_argument("--N", type=int, default=10)
    ap.add_argument("--K", type=int, default=5)
    ap.add_argument("--X", type=float, default=1.0)
    args = ap.parse_args()

    df5 = pd.read_csv(args.master)
    df5["end_dt"] = pd.to_datetime(df5["end"])
    df5 = df5.sort_values("end_dt")
    df5["TRADEDATE"] = df5["end_dt"].dt.date

    d1 = (
        df5.groupby("TRADEDATE")
           .agg(open=("open_fo", "first"),
                high=("high_fo", "max"),
                low=("low_fo", "min"),
                close=("close_fo", "last"))
           .reset_index()
    )
    d1["TRADEDATE"] = pd.to_datetime(d1["TRADEDATE"])
    d1["ATR14"] = atr14(d1)

    reg = pd.read_csv(args.regime, parse_dates=["TRADEDATE"])
    d1 = d1.merge(reg, on="TRADEDATE", how="left")

    levels = pd.read_csv(args.levels)
    events = pd.read_csv(args.events, parse_dates=["TRADEDATE"])

    out = []
    for _, ev in events.iterrows():
        t = d1.index[d1["TRADEDATE"] == ev["TRADEDATE"]]
        if len(t) == 0:
            continue
        i = int(t[0])

        state = classify_approach(
            d1,
            i,
            float(ev["level_price"]),
            ev["direction"],
            args.N,
            args.K,
            args.X,
        )

        if state is None:
            continue

        row = ev.to_dict()
        row["approach_state"] = state
        out.append(row)

    out = pd.DataFrame(out)
    out.to_csv(args.out, index=False)

    print("OK")
    print("params:", args.N, args.K, args.X)
    print("rows:", len(out))
    print("out:", args.out)

if __name__ == "__main__":
    main()
