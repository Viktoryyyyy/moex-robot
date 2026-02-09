import argparse
import os
import sys
import numpy as np
import pandas as pd


def load_master_daily(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    need = ["end", "open_fo", "high_fo", "low_fo", "close_fo"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"Master missing column: {c}")

    df["dt"] = pd.to_datetime(df["end"])
    df = df.sort_values("dt").reset_index(drop=True)
    df["date"] = df["dt"].dt.date.astype(str)

    g = df.groupby("date", as_index=False)

    # open_first / close_last within day, high_max / low_min
    daily = g.agg(
        open_first=("open_fo", "first"),
        close_last=("close_fo", "last"),
        high_day=("high_fo", "max"),
        low_day=("low_fo", "min"),
    )

    daily["range_day"] = daily["high_day"] - daily["low_day"]
    daily["ret_day"] = daily["close_last"] - daily["open_first"]

    # avoid div by zero
    daily["trend_strength"] = np.where(
        daily["range_day"] > 0,
        np.abs(daily["ret_day"]) / daily["range_day"],
        np.nan,
    )

    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date_dt").reset_index(drop=True)

    return daily


def load_ema_day(path: str) -> pd.DataFrame:
    d = pd.read_csv(path)
    if "date" not in d.columns or "EMA_EDGE_DAY" not in d.columns:
        raise ValueError("ema_pnl_day.csv must contain columns: date, EMA_EDGE_DAY")
    d["date"] = d["date"].astype(str)
    d["EMA_EDGE_DAY"] = d["EMA_EDGE_DAY"].astype(int)
    return d[["date", "EMA_EDGE_DAY"]].copy()


def zscore_past_only(s: pd.Series, window: int) -> pd.Series:
    # z_t uses only t-1, t-2, ... history
    s1 = s.shift(1)
    mu = s1.rolling(window=window, min_periods=window).mean()
    sd = s1.rolling(window=window, min_periods=window).std(ddof=0)
    return (s - mu) / sd


def add_regimes(daily: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    daily = daily.copy()

    daily["range_z"] = zscore_past_only(daily["range_day"], window)
    daily["trend_z"] = zscore_past_only(daily["trend_strength"], window)

    # fixed thresholds (no tuning)
    daily["vol_regime"] = np.where(
        daily["range_z"] < -0.3, "LOW_VOL",
        np.where(daily["range_z"] > 0.7, "HIGH_VOL", "MID_VOL")
    )

    daily["trend_regime"] = np.where(daily["trend_z"] < 0, "FLAT", "TREND")

    # drop days we cannot classify (first window, or bad data)
    daily = daily.dropna(subset=["range_z", "trend_z"]).reset_index(drop=True)
    return daily


def summarize(df: pd.DataFrame, period_name: str) -> pd.DataFrame:
    base = float(df["EMA_EDGE_DAY"].mean()) if len(df) else np.nan

    out = []
    total_days = len(df)

    for (v, t), g in df.groupby(["vol_regime", "trend_regime"]):
        freq = len(g) / total_days if total_days else np.nan
        p_good = float(g["EMA_EDGE_DAY"].mean()) if len(g) else np.nan
        lift = (p_good / base) if (np.isfinite(p_good) and np.isfinite(base) and base > 0) else np.nan

        out.append({
            "period": period_name,
            "vol_regime": v,
            "trend_regime": t,
            "event_freq": freq,
            "p_good_given_regime": p_good,
            "lift_vs_base": lift,
        })

    return pd.DataFrame(out)


def pass_fail(summary: pd.DataFrame) -> pd.DataFrame:
    # PASS per (vol,trend) if lift>1 and freq>=10% in BOTH train and test
    need_periods = ["train_2020_2023", "test_2024_2025"]
    rows = []
    keys = summary[["vol_regime", "trend_regime"]].drop_duplicates().to_records(index=False)

    for vol, trend in keys:
        ok = True
        for p in need_periods:
            r = summary[(summary["period"] == p) & (summary["vol_regime"] == vol) & (summary["trend_regime"] == trend)]
            if len(r) != 1:
                ok = False
                continue
            freq = r["event_freq"].iloc[0]
            lift = r["lift_vs_base"].iloc[0]
            if not (pd.notna(freq) and pd.notna(lift) and freq >= 0.10 and lift > 1.00):
                ok = False
        rows.append({"vol_regime": vol, "trend_regime": trend, "PASS": "PASS" if ok else "FAIL"})
    return pd.DataFrame(rows).sort_values(["vol_regime", "trend_regime"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-12-08.csv")
    ap.add_argument("--ema_day", default="data/research/ema_pnl_day.csv")
    ap.add_argument("--out", default="data/research/ema_regime_vol_trend_summary.csv")
    ap.add_argument("--window", type=int, default=60)
    args = ap.parse_args()

    daily = load_master_daily(args.master)
    daily = add_regimes(daily, window=args.window)

    lab = load_ema_day(args.ema_day)
    d = daily.merge(lab, on="date", how="inner")
    d["date_dt"] = pd.to_datetime(d["date"])

    train = d[d["date_dt"] <= pd.Timestamp("2023-12-31")].copy()
    test  = d[d["date_dt"] >= pd.Timestamp("2024-01-01")].copy()

    s_train = summarize(train, "train_2020_2023")
    s_test  = summarize(test,  "test_2024_2025")
    s_all = pd.concat([s_train, s_test], ignore_index=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    s_all.to_csv(args.out, index=False)

    pf = pass_fail(s_all)

    print("OUTPUT:", args.out)
    print("WINDOW:", args.window)
    print("--- PASS/FAIL (needs lift>1 and freq>=10% in both train and test) ---")
    print(pf.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        sys.exit(2)
