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


def load_ema_day(path: str) -> pd.DataFrame:
    d = pd.read_csv(path)
    if "date" not in d.columns or "EMA_EDGE_DAY" not in d.columns:
        raise ValueError("ema_pnl_day.csv must contain columns: date, EMA_EDGE_DAY")
    d["date"] = d["date"].astype(str)
    d["EMA_EDGE_DAY"] = d["EMA_EDGE_DAY"].astype(int)
    return d[["date", "EMA_EDGE_DAY"]].copy()


def zscore_past_only(s: pd.Series, window: int) -> pd.Series:
    s1 = s.shift(1)
    mu = s1.rolling(window=window, min_periods=window).mean()
    sd = s1.rolling(window=window, min_periods=window).std(ddof=0)
    return (s - mu) / sd


def add_regimes(daily: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    daily = daily.copy()
    daily["range_z"] = zscore_past_only(daily["range_day"], window)
    daily["trend_z"] = zscore_past_only(daily["trend_strength"], window)

    daily["vol_regime"] = np.where(
        daily["range_z"] < -0.3, "LOW_VOL",
        np.where(daily["range_z"] > 0.7, "HIGH_VOL", "MID_VOL")
    )
    daily["trend_regime"] = np.where(daily["trend_z"] < 0, "FLAT", "TREND")

    daily = daily.dropna(subset=["range_z", "trend_z"]).reset_index(drop=True)
    return daily


def add_transitions(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    d["prev_trend_regime"] = d["trend_regime"].shift(1)
    d["prev_vol_regime"] = d["vol_regime"].shift(1)

    # transitions (minimal, interpretable)
    d["enter_trend"] = (d["trend_regime"] == "TREND") & (d["prev_trend_regime"] == "FLAT")
    d["exit_trend"]  = (d["trend_regime"] == "FLAT") & (d["prev_trend_regime"] == "TREND")

    # escalation into higher volatility bucket
    rank = {"LOW_VOL": 0, "MID_VOL": 1, "HIGH_VOL": 2}
    d["vol_rank"] = d["vol_regime"].map(rank)
    d["prev_vol_rank"] = d["prev_vol_regime"].map(rank)
    d["vol_upshift"] = (d["vol_rank"] > d["prev_vol_rank"])

    # combined (often the intuitive "trend ignition" day)
    d["enter_trend_with_vol_upshift"] = d["enter_trend"] & d["vol_upshift"]
    return d


def summarize_events(d: pd.DataFrame, period_name: str) -> pd.DataFrame:
    base = float(d["EMA_EDGE_DAY"].mean()) if len(d) else np.nan

    def row(event_col: str):
        has = d[event_col] == True
        freq = float(has.mean()) if len(d) else np.nan
        p_good = float(d.loc[has, "EMA_EDGE_DAY"].mean()) if has.any() else np.nan
        lift = (p_good / base) if (np.isfinite(p_good) and np.isfinite(base) and base > 0) else np.nan
        return {
            "period": period_name,
            "event_type": event_col,
            "event_freq": freq,
            "p_good_given_event": p_good,
            "lift_vs_base": lift,
        }

    out = [
        row("enter_trend"),
        row("enter_trend_with_vol_upshift"),
        row("exit_trend"),
        row("vol_upshift"),
    ]
    return pd.DataFrame(out)


def pass_fail(summary: pd.DataFrame, event_type: str) -> str:
    # PASS if lift>1 and freq>=10% in BOTH train and test
    ok = True
    for p in ["train_2020_2023", "test_2024_2025"]:
        r = summary[(summary["period"] == p) & (summary["event_type"] == event_type)]
        if len(r) != 1:
            ok = False
            continue
        freq = r["event_freq"].iloc[0]
        lift = r["lift_vs_base"].iloc[0]
        if not (pd.notna(freq) and pd.notna(lift) and freq >= 0.10 and lift > 1.00):
            ok = False
    return "PASS" if ok else "FAIL"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-12-08.csv")
    ap.add_argument("--ema_day", default="data/research/ema_pnl_day.csv")
    ap.add_argument("--out", default="data/research/ema_regime_transition_summary.csv")
    ap.add_argument("--window", type=int, default=60)
    args = ap.parse_args()

    daily = load_master_daily(args.master)
    daily = add_regimes(daily, window=args.window)

    lab = load_ema_day(args.ema_day)
    d = daily.merge(lab, on="date", how="inner")
    d = d.sort_values("date_dt").reset_index(drop=True)
    d = add_transitions(d)

    train = d[d["date_dt"] <= pd.Timestamp("2023-12-31")].copy()
    test  = d[d["date_dt"] >= pd.Timestamp("2024-01-01")].copy()

    s_train = summarize_events(train, "train_2020_2023")
    s_test  = summarize_events(test,  "test_2024_2025")
    s_all = pd.concat([s_train, s_test], ignore_index=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    s_all.to_csv(args.out, index=False)

    print("OUTPUT:", args.out)
    print("WINDOW:", args.window)
    for ev in ["enter_trend", "enter_trend_with_vol_upshift", "exit_trend", "vol_upshift"]:
        print(ev + ":", pass_fail(s_all, ev))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        sys.exit(2)
