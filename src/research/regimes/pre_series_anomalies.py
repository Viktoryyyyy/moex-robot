import numpy as np
import pandas as pd

try:
    from scipy.stats import mannwhitneyu
except Exception:
    mannwhitneyu = None

LABELS = "data/research/ema_gb_series_labels.csv"
METRICS = "data/research/day_metrics_from_master.csv"
OUT = "data/research/pre_series_anomalies.csv"

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

    # === CRITICAL FIX: normalize dates to pure day ===
    lab["date"] = pd.to_datetime(lab["date"]).dt.date
    met["date"] = pd.to_datetime(met["date"]).dt.date

    # события
    gs_start = lab.loc[lab["series_pos"] == "start", "date"]
    g1 = lab.loc[lab["series_pos"] == "single", "date"]

    # PRE = t-1
    pre_gs = pd.DataFrame({
        "date": gs_start - pd.to_timedelta(1, unit="D"),
        "group": "PRE_GS_START"
    })
    pre_g1 = pd.DataFrame({
        "date": g1 - pd.to_timedelta(1, unit="D"),
        "group": "PRE_G1"
    })

    pre = pd.concat([pre_gs, pre_g1], ignore_index=True)

    # merge
    m = pre.merge(met, on="date", how="inner")

    metric_cols = [c for c in met.columns if c != "date"]
    rows = []

    for col in metric_cols:
        a = m.loc[m["group"] == "PRE_GS_START", col].to_numpy(dtype=float)
        b = m.loc[m["group"] == "PRE_G1", col].to_numpy(dtype=float)
        rows.append({
            "metric": col,
            "n_pre_gs": int(np.sum(~np.isnan(a))),
            "n_pre_g1": int(np.sum(~np.isnan(b))),
            "median_pre_gs": float(np.nanmedian(a)) if np.sum(~np.isnan(a)) else np.nan,
            "median_pre_g1": float(np.nanmedian(b)) if np.sum(~np.isnan(b)) else np.nan,
            "cliffs_delta": cliffs_delta(a, b),
            "mw_pvalue": mw_pvalue(a, b),
        })

    out = pd.DataFrame(rows)
    out["abs_delta"] = out["cliffs_delta"].abs()
    out = out.sort_values("abs_delta", ascending=False)
    out.to_csv(OUT, index=False)

    print("=== PRE-SERIES ANOMALIES (t-1) ===")
    print(f"PRE_GS_START samples: {int((m['group']=='PRE_GS_START').sum())}")
    print(f"PRE_G1 samples:       {int((m['group']=='PRE_G1').sum())}")
    print("Top metrics by |Cliff's delta|:")
    print(out.head(10)[['metric','median_pre_gs','median_pre_g1','cliffs_delta','mw_pvalue']])
    print(f"Output: {OUT}")
    print("STATUS: STEP 2.1 COMPLETE")

if __name__ == "__main__":
    main()
