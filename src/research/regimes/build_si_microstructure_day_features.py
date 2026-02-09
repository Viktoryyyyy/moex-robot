import glob
import numpy as np
import pandas as pd

MASTER_GLOB = "data/master/*.csv"
OUT = "data/research/si_microstructure_day_features.csv"

def detect_col(cols, names):
    for n in names:
        if n in cols:
            return n
    return None

def pick_si_master():
    files = sorted(glob.glob(MASTER_GLOB))
    if not files:
        raise SystemExit("No master files found")
    for p in files:
        try:
            df = pd.read_csv(p, nrows=5)
        except Exception:
            continue
        cols = set(df.columns)
        # heuristic: Si masters usually have openinterest / oi / futoi fields
        if any(c in cols for c in ["openinterest","oi","OPENPOSITION","pos_fiz","pos_yur"]):
            dt = detect_col(cols, ["end","datetime","timestamp"])
            if dt:
                return p, dt
    raise SystemExit("Cannot detect Si master with FUTOI")

def main():
    path, dt_col = pick_si_master()
    df = pd.read_csv(path)
    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    df = df.dropna(subset=[dt_col]).sort_values(dt_col).copy()
    df["date"] = df[dt_col].dt.floor("D")

    # ---- FUTOI detection ----
    oi_col = detect_col(df.columns, [
        "openinterest","oi","OPENPOSITION","open_position"
    ])
    prev_oi_col = detect_col(df.columns, [
        "prevopeninterest","PREVOPENPOSITION","prev_open_position"
    ])

    # ---- OBSTATS-like detection ----
    spread_col = detect_col(df.columns, ["spread","spread_mean"])
    liq_col = detect_col(df.columns, ["liq","liq_mean","liquidity"])

    rows = []
    for d, g in df.groupby("date"):
        row = {"date": pd.to_datetime(d).date()}

        if oi_col:
            oi_open = g[oi_col].dropna().iloc[0]
            oi_close = g[oi_col].dropna().iloc[-1]
            row["oi_open"] = oi_open
            row["oi_close"] = oi_close
            row["oi_change"] = oi_close - oi_open
            row["oi_change_abs"] = abs(oi_close - oi_open)
            row["oi_change_rel"] = (oi_close - oi_open) / oi_open if oi_open != 0 else np.nan
        elif prev_oi_col:
            row["oi_open"] = np.nan
            row["oi_close"] = np.nan
            row["oi_change"] = g[prev_oi_col].dropna().sum()
            row["oi_change_abs"] = abs(row["oi_change"])
            row["oi_change_rel"] = np.nan
        else:
            row["oi_open"] = np.nan
            row["oi_close"] = np.nan
            row["oi_change"] = np.nan
            row["oi_change_abs"] = np.nan
            row["oi_change_rel"] = np.nan

        if spread_col:
            s = g[spread_col].to_numpy(float)
            row["spread_mean"] = float(np.nanmean(s))
            row["spread_p90"] = float(np.nanpercentile(s, 90))
        else:
            row["spread_mean"] = np.nan
            row["spread_p90"] = np.nan

        if liq_col:
            row["liq_mean"] = float(np.nanmean(g[liq_col].to_numpy(float)))
        else:
            row["liq_mean"] = np.nan

        rows.append(row)

    out = pd.DataFrame(rows).sort_values("date")
    out.to_csv(OUT, index=False)

    print("=== STEP 4.1: SI MICROSTRUCTURE DAY FEATURES ===")
    print(f"Master: {path}")
    print(f"Datetime col: {dt_col}")
    print(f"Detected oi col: {oi_col}")
    print(f"Detected spread col: {spread_col}")
    print(f"Detected liq col: {liq_col}")
    print(f"Days: {len(out)}")
    print(f"Output: {OUT}")
    print("STATUS: STEP 4.1 COMPLETE")

if __name__ == "__main__":
    main()
