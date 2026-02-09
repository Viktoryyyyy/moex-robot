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
    daily = g.agg(
        open_first=("open_fo", "first"),
        close_last=("close_fo", "last"),
        high_day=("high_fo", "max"),
        low_day=("low_fo", "min"),
    )

    daily["range_day"] = daily["high_day"] - daily["low_day"]
    daily["ret_day"] = daily["close_last"] - daily["open_first"]
    daily["trend_strength"] = np.where(
        daily["range_day"] > 0,
        np.abs(daily["ret_day"]) / daily["range_day"],
        np.nan,
    )

    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date_dt").reset_index(drop=True)
    return daily


def load_pnl(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    need = ["date", "pnl_day", "EMA_EDGE_DAY"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"ema_pnl_day.csv missing column: {c}")

    df["date"] = df["date"].astype(str)
    df["pnl_day"] = df["pnl_day"].astype(float)
    df["EMA_EDGE_DAY"] = df["EMA_EDGE_DAY"].astype(int)
    df["date_dt"] = pd.to_datetime(df["date"])
    return df[["date", "date_dt", "pnl_day", "EMA_EDGE_DAY"]].copy()


def zscore_past_only(s: pd.Series, window: int) -> pd.Series:
    s1 = s.shift(1)
    mu = s1.rolling(window=window, min_periods=window).mean()
    sd = s1.rolling(window=window, min_periods=window).std(ddof=0)
    return (s - mu) / sd


def add_regimes_and_transitions(daily: pd.DataFrame, window: int) -> pd.DataFrame:
    d = daily.copy()

    d["range_z"] = zscore_past_only(d["range_day"], window)
    d["trend_z"] = zscore_past_only(d["trend_strength"], window)

    d["vol_regime"] = np.where(
        d["range_z"] < -0.3, "LOW_VOL",
        np.where(d["range_z"] > 0.7, "HIGH_VOL", "MID_VOL")
    )
    d["trend_regime"] = np.where(d["trend_z"] < 0, "FLAT", "TREND")

    # keep only days with valid classification (after warmup window)
    d = d.dropna(subset=["range_z", "trend_z"]).reset_index(drop=True)

    d["prev_trend_regime"] = d["trend_regime"].shift(1)
    d["prev_vol_regime"] = d["vol_regime"].shift(1)

    d["enter_trend"] = (d["trend_regime"] == "TREND") & (d["prev_trend_regime"] == "FLAT")
    d["exit_trend"]  = (d["trend_regime"] == "FLAT") & (d["prev_trend_regime"] == "TREND")

    rank = {"LOW_VOL": 0, "MID_VOL": 1, "HIGH_VOL": 2}
    d["vol_rank"] = d["vol_regime"].map(rank)
    d["prev_vol_rank"] = d["prev_vol_regime"].map(rank)
    d["vol_upshift"] = d["vol_rank"] > d["prev_vol_rank"]

    return d


def assign_bucket(row) -> str:
    if bool(row["enter_trend"]):
        return "enter_trend"
    if bool(row["vol_upshift"]):
        return "vol_upshift"
    if bool(row["exit_trend"]):
        return "exit_trend"
    return "none"


def summarize(df: pd.DataFrame, period_name: str) -> pd.DataFrame:
    total_days = len(df)
    total_pnl = df["pnl_day"].sum()
    total_pos_days = (df["pnl_day"] > 0).sum()

    out = []
    for bucket, g in df.groupby("bucket"):
        num_days = len(g)
        sum_pnl = g["pnl_day"].sum()
        pos_days = (g["pnl_day"] > 0).sum()

        out.append({
            "period": period_name,
            "bucket": bucket,
            "num_days": num_days,
            "share_of_days": num_days / total_days if total_days else np.nan,
            "mean_pnl": float(g["pnl_day"].mean()) if num_days else np.nan,
            "median_pnl": float(g["pnl_day"].median()) if num_days else np.nan,
            "sum_pnl": float(sum_pnl) if num_days else np.nan,
            "share_of_total_pnl": (sum_pnl / total_pnl) if total_pnl != 0 else np.nan,
            "share_of_positive_pnl_days": (pos_days / total_pos_days) if total_pos_days else np.nan,
        })

    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-12-08.csv")
    ap.add_argument("--ema_day", default="data/research/ema_pnl_day.csv")
    ap.add_argument("--out", default="data/research/ema_pnl_decomposition.csv")
    ap.add_argument("--days_out", default="data/research/ema_pnl_transition_days.csv")
    ap.add_argument("--window", type=int, default=60)
    args = ap.parse_args()

    pnl = load_pnl(args.ema_day)
    daily = load_master_daily(args.master)
    daily = add_regimes_and_transitions(daily, window=args.window)

    # IMPORTANT: keep universe as "daily" (regime-days). Join pnl with LEFT join.
    d = daily.merge(pnl, on="date", how="left", suffixes=("", "_pnl"))

    # sanity: ensure we have pnl for most days; otherwise show counts
    missing_pnl = int(d["pnl_day"].isna().sum())
    total_days = len(d)

    # drop days without pnl (cannot decompose PnL), but after counting
    d = d.dropna(subset=["pnl_day"]).reset_index(drop=True)

    d["bucket"] = d.apply(assign_bucket, axis=1)

    os.makedirs(os.path.dirname(args.days_out), exist_ok=True)
    d_out = d[[
        "date", "vol_regime", "trend_regime",
        "enter_trend", "vol_upshift", "exit_trend",
        "bucket", "EMA_EDGE_DAY", "pnl_day"
    ]].copy()
    d_out.to_csv(args.days_out, index=False)

    train = d[d["date_dt"] <= pd.Timestamp("2023-12-31")].copy()
    test  = d[d["date_dt"] >= pd.Timestamp("2024-01-01")].copy()

    s_train = summarize(train, "train_2020_2023")
    s_test  = summarize(test,  "test_2024_2025")
    out = pd.concat([s_train, s_test], ignore_index=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False)

    print("OUT_DECOMP:", args.out)
    print("OUT_DAYS  :", args.days_out)
    print("WINDOW    :", args.window)
    print("UNIVERSE_DAYS (regime-days):", total_days)
    print("MISSING_PNL_IN_UNIVERSE   :", missing_pnl)
    print("USED_DAYS (after dropna)  :", len(d))

    for name, part in [("train_2020_2023", train), ("test_2024_2025", test)]:
        vc = part["bucket"].value_counts(normalize=True).sort_index()
        print("BUCKET_SHARE", name, vc.to_dict())

    # also show raw counts of events
    print("EVENT_COUNTS:", {
        "enter_trend": int(d["enter_trend"].sum()),
        "vol_upshift": int(d["vol_upshift"].sum()),
        "exit_trend": int(d["exit_trend"].sum()),
    })


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        sys.exit(2)
