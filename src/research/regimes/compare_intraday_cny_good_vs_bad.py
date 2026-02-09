import numpy as np
import pandas as pd

try:
    from scipy.stats import mannwhitneyu
except Exception:
    mannwhitneyu = None

LABELS = "data/research/ema_pnl_day.csv"
FEATS = "data/research/intraday_5m_day_features.csv"
OUT = "data/research/intraday_cny_good_vs_bad.csv"

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
    lab = pd.read_csv(LABELS)
    f = pd.read_csv(FEATS)

    if "EMA_EDGE_DAY" not in lab.columns:
        raise SystemExit("Missing EMA_EDGE_DAY in ema_pnl_day.csv")

    lab["date"] = pd.to_datetime(lab["date"]).dt.date
    f["date"] = pd.to_datetime(f["date"]).dt.date

    m = lab[["date", "EMA_EDGE_DAY"]].merge(f, on="date", how="inner")

    metric_cols = [c for c in f.columns if c != "date"]
    rows = []

    for col in metric_cols:
        a = m.loc[m["EMA_EDGE_DAY"] == 1, col].to_numpy(dtype=float)
        b = m.loc[m["EMA_EDGE_DAY"] == 0, col].to_numpy(dtype=float)
        rows.append({
            "metric": col,
            "n_good": int(np.sum(~np.isnan(a))),
            "n_bad": int(np.sum(~np.isnan(b))),
            "median_good": float(np.nanmedian(a)) if np.sum(~np.isnan(a)) else np.nan,
            "median_bad": float(np.nanmedian(b)) if np.sum(~np.isnan(b)) else np.nan,
            "cliffs_delta": cliffs_delta(a, b),
            "mw_pvalue": mw_pvalue(a, b),
        })

    out = pd.DataFrame(rows)
    out["abs_delta"] = out["cliffs_delta"].abs()
    out = out.sort_values("abs_delta", ascending=False)
    out.to_csv(OUT, index=False)

    n_good = int((m["EMA_EDGE_DAY"] == 1).sum())
    n_bad = int((m["EMA_EDGE_DAY"] == 0).sum())

    print("=== STEP 3.2: INTRADAY CNY FEATURES GOOD vs BAD ===")
    print(f"Merged days: {len(m)}")
    print(f"GOOD days in merge: {n_good}")
    print(f"BAD days in merge : {n_bad}")
    print("Top metrics by |Cliff's delta|:")
    print(out.head(10)[["metric","median_good","median_bad","cliffs_delta","mw_pvalue"]])
    print(f"Output: {OUT}")
    print("STATUS: STEP 3.2 COMPLETE")

if __name__ == "__main__":
    main()
