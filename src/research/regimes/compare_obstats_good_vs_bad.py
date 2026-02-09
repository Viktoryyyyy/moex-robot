import glob
import numpy as np
import pandas as pd

try:
    from scipy.stats import mannwhitneyu
except Exception:
    mannwhitneyu = None

LABELS = "data/research/ema_pnl_day.csv"
OUT = "data/research/obstats_good_vs_bad.csv"
MASTER_GLOB = "data/master/*.csv"

def detect_col(cols, names):
    for n in names:
        if n in cols:
            return n
    return None

def pick_obstats_master():
    files = sorted(glob.glob(MASTER_GLOB))
    for p in files:
        try:
            df = pd.read_csv(p, nrows=5)
        except Exception:
            continue
        cols = set(df.columns)
        # heuristics for obstats presence
        if any(c in cols for c in ["spread","spread_mean","liq","liq_mean","liquidity"]):
            dt = detect_col(cols, ["end","datetime","timestamp"])
            if dt:
                return p, dt
    raise SystemExit("No master with OBSTATS fields found")

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
    path, dt_col = pick_obstats_master()
    df = pd.read_csv(path)
    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    df = df.dropna(subset=[dt_col]).sort_values(dt_col).copy()
    df["date"] = df[dt_col].dt.date

    spread_col = detect_col(df.columns, ["spread","spread_mean"])
    liq_col = detect_col(df.columns, ["liq","liq_mean","liquidity"])

    if not spread_col and not liq_col:
        raise SystemExit("OBSTATS columns not detected after load")

    rows = []
    for d, g in df.groupby("date"):
        row = {"date": d}
        if spread_col:
            s = g[spread_col].to_numpy(float)
            row["spread_mean"] = float(np.nanmean(s))
            row["spread_p90"] = float(np.nanpercentile(s, 90))
        else:
            row["spread_mean"] = np.nan
            row["spread_p90"] = np.nan

        if liq_col:
            l = g[liq_col].to_numpy(float)
            row["liq_mean"] = float(np.nanmean(l))
            row["liq_p10"] = float(np.nanpercentile(l, 10))
        else:
            row["liq_mean"] = np.nan
            row["liq_p10"] = np.nan

        rows.append(row)

    day = pd.DataFrame(rows)

    lab = pd.read_csv(LABELS)
    lab["date"] = pd.to_datetime(lab["date"]).dt.date

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

    print("=== STEP 4.2B: OBSTATS GOOD vs BAD ===")
    print(f"Master: {path}")
    print(f"Datetime col: {dt_col}")
    print(f"Detected spread col: {spread_col}")
    print(f"Detected liq col: {liq_col}")
    print(f"Merged days: {len(m)}")
    print(f"GOOD days: {(m['EMA_EDGE_DAY']==1).sum()}")
    print(f"BAD days : {(m['EMA_EDGE_DAY']==0).sum()}")
    print("Top metrics by |Cliff's delta|:")
    print(out.head(10)[["metric","median_good","median_bad","cliffs_delta","mw_pvalue"]])
    print(f"Output: {OUT}")
    print("STATUS: STEP 4.2B COMPLETE")

if __name__ == "__main__":
    main()
