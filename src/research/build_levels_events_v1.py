import argparse
from pathlib import Path
import numpy as np
import pandas as pd

def atr14(d1: pd.DataFrame) -> pd.Series:
    hl = d1["high"] - d1["low"]
    hc = (d1["high"] - d1["close"].shift()).abs()
    lc = (d1["low"] - d1["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(14, min_periods=5).mean()

def levels_from_recency_hist(close: np.ndarray, bins: int, smooth: int) -> np.ndarray:
    n = int(len(close))
    w = np.exp(np.linspace(-3.0, 0.0, n))
    hist, edges = np.histogram(close, bins=bins, weights=w)
    hist = pd.Series(hist).rolling(int(smooth), center=True, min_periods=1).mean().to_numpy()
    centers = (edges[:-1] + edges[1:]) / 2.0

    lvls = []
    for i in range(1, len(hist) - 1):
        if hist[i] > hist[i - 1] and hist[i] > hist[i + 1]:
            lvls.append(float(centers[i]))
    return np.array(lvls, dtype=float)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path to master_5m_si_cny_futoi_obstats_*.csv")
    ap.add_argument("--regime", default="data/research/regime_day_r1r4.csv", help="Path to regime_day_r1r4.csv")
    ap.add_argument("--outdir", default="data/research", help="Output dir")
    ap.add_argument("--bins", type=int, default=220, help="Histogram bins")
    ap.add_argument("--smooth", type=int, default=7, help="Histogram smoothing window")
    args = ap.parse_args()

    master_path = Path(args.master)
    if not master_path.exists():
        raise RuntimeError("Master not found: " + str(master_path))

    df5 = pd.read_csv(master_path)

    required = ["end", "open_fo", "high_fo", "low_fo", "close_fo"]
    for col in required:
        if col not in df5.columns:
            raise RuntimeError("Master missing column: " + col)

    df5["end_dt"] = pd.to_datetime(df5["end"])
    df5 = df5.sort_values("end_dt")
    df5["TRADEDATE"] = df5["end_dt"].dt.date

    d1 = (
        df5.groupby("TRADEDATE")
           .agg(
               open=("open_fo", "first"),
               high=("high_fo", "max"),
               low=("low_fo", "min"),
               close=("close_fo", "last"),
           )
           .reset_index()
    )
    d1["TRADEDATE"] = pd.to_datetime(d1["TRADEDATE"])
    d1["ATR14"] = atr14(d1)

    regime_path = Path(args.regime)
    d1["regime_day"] = np.nan
    if regime_path.exists():
        reg = pd.read_csv(regime_path, parse_dates=["TRADEDATE"])
        if "regime_day" not in reg.columns:
            raise RuntimeError("Regime missing column: regime_day")
        d1 = d1.drop(columns=["regime_day"]).merge(reg[["TRADEDATE", "regime_day"]], on="TRADEDATE", how="left")

    cl = d1["close"].to_numpy(dtype=float)
    if len(cl) < 200:
        raise RuntimeError("Too few D1 bars: " + str(len(cl)))

    level_prices = levels_from_recency_hist(cl, bins=int(args.bins), smooth=int(args.smooth))
    levels = pd.DataFrame({
        "level_id": np.arange(len(level_prices), dtype=int),
        "level_price": level_prices,
    })

    events = []
    for _, lvl in levels.iterrows():
        L = float(lvl["level_price"])
        level_id = int(lvl["level_id"])

        for i in range(len(d1) - 2):
            c0 = float(d1.loc[i, "close"])
            c1 = float(d1.loc[i + 1, "close"])
            c2 = float(d1.loc[i + 2, "close"])

            crossed_up = (c0 <= L) and (c1 > L)
            crossed_dn = (c0 >= L) and (c1 < L)

            if crossed_up:
                outcome = "break_confirmed" if (c1 > L and c2 > L) else "false_break"
                direction = "up"
            elif crossed_dn:
                outcome = "break_confirmed" if (c1 < L and c2 < L) else "false_break"
                direction = "down"
            else:
                continue

            events.append({
                "TRADEDATE": d1.loc[i + 1, "TRADEDATE"],
                "level_id": level_id,
                "level_price": L,
                "direction": direction,
                "outcome": outcome,
                "regime_day": d1.loc[i + 1, "regime_day"],
                "ATR14": d1.loc[i + 1, "ATR14"],
            })

    events = pd.DataFrame(events)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_levels = outdir / "levels_v1.csv"
    out_events = outdir / "level_events_v1.csv"

    levels.to_csv(out_levels, index=False)
    events.to_csv(out_events, index=False)

    print("OK")
    print("master:", str(master_path))
    print("d1_rows:", len(d1))
    print("levels_rows:", len(levels))
    print("events_rows:", len(events))
    print("out_levels:", str(out_levels))
    print("out_events:", str(out_events))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
