import argparse
import os
import sys
import numpy as np
import pandas as pd


def load_master_5m(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    need = ["end", "open_fo", "high_fo", "low_fo", "close_fo"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"Master missing column: {c}")

    df["dt"] = pd.to_datetime(df["end"])
    df = df.sort_values("dt").reset_index(drop=True)
    df["date"] = df["dt"].dt.date.astype(str)

    df = df.rename(columns={
        "open_fo": "open",
        "high_fo": "high",
        "low_fo": "low",
        "close_fo": "close",
    })

    return df[["dt", "date", "open", "high", "low", "close"]].copy()


def load_ema_day(path: str) -> pd.DataFrame:
    d = pd.read_csv(path)
    if "date" not in d.columns or "EMA_EDGE_DAY" not in d.columns:
        raise ValueError("ema_pnl_day.csv must contain columns: date, EMA_EDGE_DAY")
    d["date"] = d["date"].astype(str)
    d["EMA_EDGE_DAY"] = d["EMA_EDGE_DAY"].astype(int)
    return d[["date", "EMA_EDGE_DAY"]].copy()


def rolling_z_past_only(s: pd.Series, window: int) -> pd.Series:
    # z_t uses history up to t-1 only (anti-cheat)
    s1 = s.shift(1)
    mu = s1.rolling(window=window, min_periods=window).mean()
    sd = s1.rolling(window=window, min_periods=window).std(ddof=0)
    return (s - mu) / sd


def add_regime_5m(df: pd.DataFrame, win_bars: int = 24) -> pd.DataFrame:
    df = df.copy()

    # bar range as volatility proxy
    df["bar_range"] = df["high"] - df["low"]
    # directional proxy: close-open normalized by range (abs)
    df["bar_trend_strength"] = np.where(
        df["bar_range"] > 0,
        np.abs(df["close"] - df["open"]) / df["bar_range"],
        np.nan
    )

    # compute z within each day using only past bars of same day (no cross-day leakage)
    df["vol_z"] = df.groupby("date")["bar_range"].apply(lambda s: rolling_z_past_only(s, win_bars)).reset_index(level=0, drop=True)
    df["trend_z"] = df.groupby("date")["bar_trend_strength"].apply(lambda s: rolling_z_past_only(s, win_bars)).reset_index(level=0, drop=True)

    # fixed thresholds (project canon)
    df["vol_regime"] = np.where(
        df["vol_z"] < -0.3, "LOW_VOL",
        np.where(df["vol_z"] > 0.7, "HIGH_VOL", "MID_VOL")
    )
    df["trend_regime"] = np.where(df["trend_z"] < 0, "FLAT", "TREND")

    # drop bars without regime (first win_bars bars per day, or NaNs)
    df = df.dropna(subset=["vol_z", "trend_z"]).reset_index(drop=True)
    return df


def summarize_bars(df_bars: pd.DataFrame, period_name: str) -> pd.DataFrame:
    # base probability is per-day label mean, but we evaluate conditional on bar regime (bar-weighted)
    base = float(df_bars["EMA_EDGE_DAY"].mean()) if len(df_bars) else np.nan
    total = len(df_bars)

    out = []
    for (v, t), g in df_bars.groupby(["vol_regime", "trend_regime"]):
        freq = len(g) / total if total else np.nan
        p_good = float(g["EMA_EDGE_DAY"].mean()) if len(g) else np.nan
        lift = (p_good / base) if (np.isfinite(p_good) and np.isfinite(base) and base > 0) else np.nan
        out.append({
            "period": period_name,
            "vol_regime": v,
            "trend_regime": t,
            "bar_share": freq,
            "p_good_given_regime": p_good,
            "lift_vs_base": lift,
        })
    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-12-08.csv")
    ap.add_argument("--ema_day", default="data/research/ema_pnl_day.csv")
    ap.add_argument("--out", default="data/research/ema_regime_5m_summary.csv")
    ap.add_argument("--win_bars", type=int, default=24)
    args = ap.parse_args()

    bars = load_master_5m(args.master)
    bars = add_regime_5m(bars, win_bars=args.win_bars)

    lab = load_ema_day(args.ema_day)
    bars = bars.merge(lab, on="date", how="inner")
    bars["date_dt"] = pd.to_datetime(bars["date"])

    train = bars[bars["date_dt"] <= pd.Timestamp("2023-12-31")].copy()
    test  = bars[bars["date_dt"] >= pd.Timestamp("2024-01-01")].copy()

    s_train = summarize_bars(train, "train_2020_2023")
    s_test  = summarize_bars(test,  "test_2024_2025")
    out = pd.concat([s_train, s_test], ignore_index=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False)

    print("OUTPUT:", args.out)
    print("WIN_BARS:", args.win_bars)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        sys.exit(2)
