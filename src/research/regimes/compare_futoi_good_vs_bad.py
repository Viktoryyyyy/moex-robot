import numpy as np
import pandas as pd

try:
    from scipy.stats import mannwhitneyu
except Exception:
    mannwhitneyu = None

FUTOI = "data/master/futoi_si_5m_2020-01-03_2025-11-13.csv"
LABELS = "data/research/ema_pnl_day.csv"
OUT = "data/research/futoi_good_vs_bad.csv"

REQ_COLS = [
    "pos_long_fiz","pos_short_fiz",
    "pos_long_yur","pos_short_yur"
]

def cliffs_delta(a, b):
    a = a[~np.isnan(a)]
    b = b[~np.isnan(b)]
    if len(a) == 0 or len(b) == 0:
        return np.nan
    gt = 0
    lt = 0
    for x in a:
        gt += int(np.sum(x > b))
        lt += int(np.sum(x < b))
    return (gt - lt) / (len(a) * len(b))

def mw_pvalue(a, b):
    if mannwhitneyu is None:
        return np.nan
    a = a[~np.isnan(a)]
    b = b[~np.isnan(b)]
    if len(a) < 10 or len(b) < 10:
        return np.nan
    return float(mannwhitneyu(a, b, alternative="two-sided").pvalue)

def main():
    f = pd.read_csv(FUTOI)
    lab = pd.read_csv(LABELS)

    for c in REQ_COLS:
        if c not in f.columns:
            raise SystemExit(f"Missing column in FUTOI: {c}")

    f["end"] = pd.to_datetime(f["end"])
    f["date"] = f["end"].dt.date
    lab["date"] = pd.to_datetime(lab["date"]).dt.date

    rows = []
    for d, g in f.groupby("date"):
        g = g.sort_values("end")
        row = {"date": d}

        row["fiz_net_open"]  = g["pos_long_fiz"].iloc[0]  - g["pos_short_fiz"].iloc[0]
        row["fiz_net_close"] = g["pos_long_fiz"].iloc[-1] - g["pos_short_fiz"].iloc[-1]
        row["yur_net_open"]  = g["pos_long_yur"].iloc[0]  - g["pos_short_yur"].iloc[0]
        row["yur_net_close"] = g["pos_long_yur"].iloc[-1] - g["pos_short_yur"].iloc[-1]

        row["d_fiz_net"] = row["fiz_net_close"] - row["fiz_net_open"]
        row["d_yur_net"] = row["yur_net_close"] - row["yur_net_open"]
        row["imbalance_close"] = row["fiz_net_close"] - row["yur_net_close"]

        rows.append(row)

    day = pd.DataFrame(rows)
    m = lab[["date","EMA_EDGE_DAY"]].merge(day, on="date", how="inner")

    metrics = [c for c in day.columns if c != "date"]
    out_rows = []

    for col in metrics:
        a = m.loc[m["EMA_EDGE_DAY"] == 1, col].to_numpy(float)
        b = m.loc[m["EMA_EDGE_DAY"] == 0, col].to_numpy(float)
        out_rows.append({
            "metric": col,
            "median_good": float(np.nanmedian(a)),
            "median_bad": float(np.nanmedian(b)),
            "cliffs_delta": cliffs_delta(a, b),
            "mw_pvalue": mw_pvalue(a, b),
        })

    out = pd.DataFrame(out_rows)
    out["abs_delta"] = out["cliffs_delta"].abs()
    out = out.sort_values("abs_delta", ascending=False)
    out.to_csv(OUT, index=False)

    print("=== STEP 4.2A: FUTOI GOOD vs BAD ===")
    print(f"Merged days: {len(m)}")
    print(f"GOOD days: {(m['EMA_EDGE_DAY']==1).sum()}")
    print(f"BAD days : {(m['EMA_EDGE_DAY']==0).sum()}")
    print("Top metrics by |Cliff's delta|:")
    print(out.head(10)[["metric","median_good","median_bad","cliffs_delta","mw_pvalue"]])
    print(f"Output: {OUT}")
    print("STATUS: STEP 4.2A COMPLETE")

if __name__ == "__main__":
    main()
