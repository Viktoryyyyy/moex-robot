import argparse
import os
import sys
import pandas as pd
import numpy as np


LEVELS = [
    # resistances
    87500, 86000, 85100, 84100, 83000,
    # supports
    82225, 80650, 80100, 79600,
    # POC + VA bounds
    81300, 80760, 83660,
]


def _pick_col(cols, candidates):
    s = {c.lower() for c in cols}
    for cand in candidates:
        if cand in s:
            for c in cols:
                if c.lower() == cand:
                    return c
    return None


def load_master(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    dt_col = _pick_col(df.columns, ["end"])
    if dt_col is None:
        raise ValueError("Master: missing datetime column 'end'.")

    c_close = _pick_col(df.columns, ["close_fo"])
    c_high  = _pick_col(df.columns, ["high_fo"])
    c_low   = _pick_col(df.columns, ["low_fo"])

    missing = [name for name, col in [("close_fo", c_close), ("high_fo", c_high), ("low_fo", c_low)] if col is None]
    if missing:
        raise ValueError(f"Master: missing required FO price columns: {missing}")

    df["dt"] = pd.to_datetime(df[dt_col])
    df = df.rename(columns={c_close: "close", c_high: "high", c_low: "low"})
    df = df.sort_values("dt").reset_index(drop=True)
    df["date"] = df["dt"].dt.date.astype(str)
    return df[["dt", "date", "close", "high", "low"]].copy()


def load_day_labels(path: str) -> pd.DataFrame:
    d = pd.read_csv(path)
    c_date = _pick_col(d.columns, ["date", "tradedate"])
    c_lab  = _pick_col(d.columns, ["ema_edge_day"])
    if c_date is None or c_lab is None:
        raise ValueError("ema_pnl_day.csv: expected columns: date + EMA_EDGE_DAY")
    d = d.rename(columns={c_date: "date", c_lab: "EMA_EDGE_DAY"})
    d["date"] = d["date"].astype(str)
    d["EMA_EDGE_DAY"] = d["EMA_EDGE_DAY"].astype(int)
    return d[["date", "EMA_EDGE_DAY"]].copy()


def compute_events(df: pd.DataFrame, x: float) -> pd.DataFrame:
    levels = np.array(LEVELS, dtype=float)

    close = df["close"].to_numpy(dtype=float)
    dist = np.min(np.abs(close.reshape(-1, 1) - levels.reshape(1, -1)), axis=1)
    df["near_level"] = dist <= float(x)

    # anti-cheat: within-day prev close only
    df["prev_close"] = df.groupby("date")["close"].shift(1)

    prev = df["prev_close"].to_numpy(dtype=float)
    cur = df["close"].to_numpy(dtype=float)

    up = np.zeros(len(df), dtype=bool)
    dn = np.zeros(len(df), dtype=bool)

    for L in LEVELS:
        L = float(L)
        up |= (cur > (L + x)) & (prev <= (L + x))
        dn |= (cur < (L - x)) & (prev >= (L - x))

    df["breakout_bar"] = (up | dn) & np.isfinite(prev)

    lo = df["low"].to_numpy(dtype=float)
    hi = df["high"].to_numpy(dtype=float)

    rb = np.zeros(len(df), dtype=bool)
    for L in LEVELS:
        L = float(L)
        rb |= (lo <= (L + x)) & (cur >= (L + x))      # support-like rebound
        rb |= (hi >= (L - x)) & (cur <= (L - x))      # resistance-like rebound

    df["rebound_bar"] = rb

    day = df.groupby("date", as_index=False).agg(
        near_level_bar_share=("near_level", "mean"),
        breakout=("breakout_bar", "max"),
        rebound=("rebound_bar", "max"),
    )
    day["breakout"] = day["breakout"].astype(int)
    day["rebound"] = day["rebound"].astype(int)
    return day


def summarize(day: pd.DataFrame, period_name: str) -> pd.DataFrame:
    base = float(day["EMA_EDGE_DAY"].mean()) if len(day) else np.nan

    def row_near(tag, mask):
        v = float(day.loc[mask, "near_level_bar_share"].mean()) if mask.any() else np.nan
        return {
            "period": period_name,
            "event_type": tag,
            "event_freq": v,
            "p_good_given_event": np.nan,
            "lift_vs_base": np.nan,
        }

    def row_event(event_col: str):
        has = day[event_col] == 1
        freq = float(has.mean()) if len(day) else np.nan
        p_good = float(day.loc[has, "EMA_EDGE_DAY"].mean()) if has.any() else np.nan
        lift = (p_good / base) if (np.isfinite(p_good) and np.isfinite(base) and base > 0) else np.nan
        return {
            "period": period_name,
            "event_type": event_col,
            "event_freq": freq,
            "p_good_given_event": p_good,
            "lift_vs_base": lift,
        }

    out = []
    out.append(row_near("near_levels_bar_share_good", day["EMA_EDGE_DAY"] == 1))
    out.append(row_near("near_levels_bar_share_bad",  day["EMA_EDGE_DAY"] == 0))
    out.append(row_event("breakout"))
    out.append(row_event("rebound"))
    return pd.DataFrame(out)


def pass_fail(summary_df: pd.DataFrame) -> dict:
    # PASS if lift>1 and event occurs on >=10% of days in BOTH train and test
    res = {}
    for ev in ["breakout", "rebound"]:
        ok = True
        for p in ["train_2020_2023", "test_2024_2025"]:
            r = summary_df[(summary_df["period"] == p) & (summary_df["event_type"] == ev)]
            if len(r) != 1:
                ok = False
                continue
            freq = r["event_freq"].iloc[0]
            lift = r["lift_vs_base"].iloc[0]
            if not (pd.notna(freq) and pd.notna(lift)):
                ok = False
                continue
            if not (freq >= 0.10 and lift > 1.00):
                ok = False
        res[ev] = "PASS" if ok else "FAIL"
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-12-08.csv")
    ap.add_argument("--ema_day", default="data/research/ema_pnl_day.csv")
    ap.add_argument("--out", default="data/research/ema_levels_events_summary.csv")
    ap.add_argument("--x", type=float, default=100.0)  # fixed, no tuning
    args = ap.parse_args()

    m = load_master(args.master)
    lab = load_day_labels(args.ema_day)
    day = compute_events(m, args.x).merge(lab, on="date", how="inner")

    day["date_dt"] = pd.to_datetime(day["date"])
    train = day[day["date_dt"] <= pd.Timestamp("2023-12-31")].copy()
    test  = day[day["date_dt"] >= pd.Timestamp("2024-01-01")].copy()

    s_train = summarize(train, "train_2020_2023")
    s_test  = summarize(test,  "test_2024_2025")
    s_all = pd.concat([s_train, s_test], ignore_index=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    s_all.to_csv(args.out, index=False)

    pf = pass_fail(s_all)
    print("OUTPUT:", args.out)
    print("X:", args.x)
    print("H1 (Breakout):", pf["breakout"])
    print("H2 (Rebound) :", pf["rebound"])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        sys.exit(2)
