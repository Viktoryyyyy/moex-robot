import numpy as np
import pandas as pd

try:
    from scipy.stats import mannwhitneyu
except Exception:
    mannwhitneyu = None

MASTER = "data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-12-08.csv"
LABELS = "data/research/ema_pnl_day.csv"
OUT = "data/research/obstats_si_good_vs_bad.csv"

SPREAD_COLS = ["spread_l1", "spread_l5", "spread_l20"]

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
    df = pd.read_csv(MASTER)
    lab = pd.read_csv(LABELS)

    df["end"] = pd.to_datetime(df["end"])
    df["date"] = df["end"].dt.date
    lab["date"] = pd.to_datetime(lab["date"]).dt.date

    rows = []
    for d, g in df.groupby("date"):
        row = {"date": d}
        for c in SPREAD_COLS:
            if c in g.columns:
                x = g[c].to_numpy(float)
                row[f"{c}_mean"] = float(np.nanmean(x))
                row[f"{c}_p90"]  = float(np.nanpercentile(x, 90))
            else:
                row[f"{c}_mean"] = np.nan
                row[f"{c}_p90"]  = np.nan
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

    print("=== STEP 4.2B: OBSTATS Si GOOD vs BAD ===")
    print(f"Master: {MASTER}")
    print(f"Merged days: {len(m)}")
    print(f"GOOD days: {(m['EMA_EDGE_DAY']==1).sum()}")
    print(f"BAD days : {(m['EMA_EDGE_DAY']==0).sum()}")
    print("Top metrics by |Cliff's delta|:")
    print(out.head(10)[['metric','median_good','median_bad','cliffs_delta','mw_pvalue']])
    print(f"Output: {OUT}")
    print("STATUS: STEP 4.2B COMPLETE")

if __name__ == "__main__":
    main()
