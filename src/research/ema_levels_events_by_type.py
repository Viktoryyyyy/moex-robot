import argparse
import os
import sys
import pandas as pd
import numpy as np


RESISTANCES = [87500, 86000, 85100, 84100, 83000]
SUPPORTS    = [82225, 80650, 80100, 79600]
POC_VA      = [81300, 80760, 83660]

ALL_LEVELS = {
    "resistance": RESISTANCES,
    "support": SUPPORTS,
    "poc_va": POC_VA,
}


def load_master(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    for c in ["end", "open_fo", "high_fo", "low_fo", "close_fo"]:
        if c not in df.columns:
            raise ValueError(f"Master missing column: {c}")

    df["dt"] = pd.to_datetime(df["end"])
    df = df.rename(columns={
        "open_fo": "open",
        "high_fo": "high",
        "low_fo": "low",
        "close_fo": "close",
    })
    df = df.sort_values("dt").reset_index(drop=True)
    df["date"] = df["dt"].dt.date.astype(str)

    return df[["dt", "date", "open", "high", "low", "close"]]


def load_day_labels(path: str) -> pd.DataFrame:
    d = pd.read_csv(path)
    if "date" not in d.columns or "EMA_EDGE_DAY" not in d.columns:
        raise ValueError("ema_pnl_day.csv must contain date, EMA_EDGE_DAY")
    d["date"] = d["date"].astype(str)
    d["EMA_EDGE_DAY"] = d["EMA_EDGE_DAY"].astype(int)
    return d[["date", "EMA_EDGE_DAY"]]


def compute_events(df: pd.DataFrame, x: float) -> pd.DataFrame:
    df["prev_close"] = df.groupby("date")["close"].shift(1)

    rows = []

    for level_type, levels in ALL_LEVELS.items():
        for L in levels:
            L = float(L)

            # breakout up
            cond = (
                (df["close"] > L + x) &
                (df["prev_close"] <= L + x)
            )
            rows.append(pd.DataFrame({
                "date": df.loc[cond, "date"],
                "event": "breakout_up",
                "level_type": level_type,
            }))

            # breakout down
            cond = (
                (df["close"] < L - x) &
                (df["prev_close"] >= L - x)
            )
            rows.append(pd.DataFrame({
                "date": df.loc[cond, "date"],
                "event": "breakout_down",
                "level_type": level_type,
            }))

            # rebound from support
            if level_type in ["support", "poc_va"]:
                cond = (
                    (df["low"] <= L + x) &
                    (df["close"] >= L + x)
                )
                rows.append(pd.DataFrame({
                    "date": df.loc[cond, "date"],
                    "event": "rebound_support",
                    "level_type": level_type,
                }))

            # rebound from resistance
            if level_type in ["resistance", "poc_va"]:
                cond = (
                    (df["high"] >= L - x) &
                    (df["close"] <= L - x)
                )
                rows.append(pd.DataFrame({
                    "date": df.loc[cond, "date"],
                    "event": "rebound_resistance",
                    "level_type": level_type,
                }))

    events = pd.concat(rows, ignore_index=True).drop_duplicates()
    events["flag"] = 1

    day_events = (
        events
        .groupby(["date", "level_type", "event"], as_index=False)
        .agg(flag=("flag", "max"))
    )

    return day_events


def summarize(day_events, labels, period_name):
    df = day_events.merge(labels, on="date", how="left")
    base = df["EMA_EDGE_DAY"].mean()

    out = []

    for (lvl, ev), g in df.groupby(["level_type", "event"]):
        freq = len(g["date"].unique()) / labels["date"].nunique()
        p_good = g["EMA_EDGE_DAY"].mean()
        lift = p_good / base if base > 0 else np.nan

        out.append({
            "period": period_name,
            "level_type": lvl,
            "event_type": ev,
            "event_freq": freq,
            "p_good_given_event": p_good,
            "lift_vs_base": lift,
        })

    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-12-08.csv")
    ap.add_argument("--ema_day", default="data/research/ema_pnl_day.csv")
    ap.add_argument("--out", default="data/research/ema_levels_events_by_type.csv")
    ap.add_argument("--x", type=float, default=100.0)
    args = ap.parse_args()

    m = load_master(args.master)
    labels = load_day_labels(args.ema_day)
    events = compute_events(m, args.x)

    labels["date_dt"] = pd.to_datetime(labels["date"])

    train_labels = labels[labels["date_dt"] <= "2023-12-31"]
    test_labels  = labels[labels["date_dt"] >= "2024-01-01"]

    train_events = events[events["date"].isin(train_labels["date"])]
    test_events  = events[events["date"].isin(test_labels["date"])]

    s_train = summarize(train_events, train_labels, "train_2020_2023")
    s_test  = summarize(test_events, test_labels, "test_2024_2025")

    out = pd.concat([s_train, s_test], ignore_index=True)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False)

    print("OUTPUT:", args.out)
    print("X:", args.x)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(2)
