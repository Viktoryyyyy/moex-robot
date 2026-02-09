import numpy as np
import pandas as pd

try:
    from scipy.stats import mannwhitneyu
except Exception:
    mannwhitneyu = None

LABELS = "data/research/ema_gb_series_labels.csv"
METRICS = "data/research/day_metrics_from_master.csv"
OUT = "data/research/in_series_anomalies_gs_mid_vs_g1.csv"

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
    met = pd.read_csv(METRICS)

    # normalize dates to pure day
    lab["date"] = pd.to_datetime(lab["date"]).dt.date
    met["date"] = pd.to_datetime(met["date"]).dt.date

    # groups
    gs_mid_days = lab.loc[lab["series_pos"] == "mid", "date"]
    g1_days = lab.loc[lab["series_pos"] == "single", "date"]

    g_mid = pd.DataFrame({"date": gs_mid_days, "group": "GS_mid"})
    g1 = pd.DataFrame({"date": g1_days, "group": "G1"})

    sel = pd.concat([g_mid, g1], ignore_index=True)
    m = sel.merge(met, on="date", how="inner")

    metric_cols = [c for c in met.columns if c != "date"]
    rows = []

    for col in metric_cols:
        a = m.loc[m["group"] == "GS_mid", col].to_numpy(dtype=float)
        b = m.loc[m["group"] == "G1", col].to_numpy(dtype=float)
        rows.append({
            "metric": col,
            "n_gs_mid": int(np.sum(~np.isnan(a))),
            "n_g1": int(np.sum(~np.isnan(b))),
            "median_gs_mid": float(np.nanmedian(a)) if np.sum(~np.isnan(a)) else np.nan,
            "median_g1": float(np.nanmedian(b)) if np.sum(~np.isnan(b)) else np.nan,
            "cliffs_delta": cliffs_delta(a, b),
            "mw_pvalue": mw_pvalue(a, b),
        })

    out = pd.DataFrame(rows)
    out["abs_delta"] = out["cliffs_delta"].abs()
    out = out.sort_values("abs_delta", ascending=False)
    out.to_csv(OUT, index=False)

    print("=== IN-SERIES ANOMALIES: GS_mid vs G1 ===")
    print(f"GS_mid samples: {int((m['group']=='GS_mid').sum())}")
    print(f"G1 samples:     {int((m['group']=='G1').sum())}")
    print("Top metrics by |Cliff's delta|:")
    print(out.head(10)[['metric','median_gs_mid','median_g1','cliffs_delta','mw_pvalue']])
    print(f"Output: {OUT}")
    print("STATUS: STEP 2.2 COMPLETE")

if __name__ == "__main__":
    main()
