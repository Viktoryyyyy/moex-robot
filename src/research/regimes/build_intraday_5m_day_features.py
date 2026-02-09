import glob
import numpy as np
import pandas as pd

MASTER_GLOB = "data/master/*.csv"
OUT = "data/research/intraday_5m_day_features.csv"

def detect_col(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

def detect_ohlc(cols):
    patterns = [
        ("open","high","low","close"),
        ("OPEN","HIGH","LOW","CLOSE"),
        ("open_fo","high_fo","low_fo","close_fo"),
        ("open_si","high_si","low_si","close_si"),
        ("pr_open","pr_high","pr_low","pr_close"),
    ]
    for o,h,l,c in patterns:
        if o in cols and h in cols and l in cols and c in cols:
            return o,h,l,c
    return None

def pick_master_with_ohlc():
    files = sorted(glob.glob(MASTER_GLOB))
    if not files:
        raise SystemExit(f"No master CSV found: {MASTER_GLOB}")
    skipped = []
    for path in files:
        try:
            head = pd.read_csv(path, nrows=5)
        except Exception as e:
            skipped.append((path, f"read_error:{e}"))
            continue
        cols = set(head.columns)
        dt = detect_col(cols, ["end","datetime","timestamp","ts"])
        ohlc = detect_ohlc(cols)
        if dt and ohlc:
            return path, dt, ohlc
        reason=[]
        if not dt: reason.append("no_datetime")
        if not ohlc: reason.append("no_ohlc")
        skipped.append((path, ",".join(reason)))
    print("ERROR: cannot find master with OHLC")
    for p,r in skipped[:20]:
        print(" -", p, r)
    raise SystemExit("No suitable master")

def safe_corr(a, b):
    a = np.asarray(a, float)
    b = np.asarray(b, float)
    if len(a) < 4:
        return np.nan
    sa, sb = np.std(a, ddof=1), np.std(b, ddof=1)
    if sa == 0 or sb == 0:
        return np.nan
    return float(np.corrcoef(a, b)[0,1])

def count_sign_flips(x):
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    if len(x) < 3:
        return 0
    s = np.sign(x)
    for i in range(1, len(s)):
        if s[i] == 0:
            s[i] = s[i-1]
    return int(np.sum(s[1:] != s[:-1]))

def main():
    master, dt_col, ohlc = pick_master_with_ohlc()
    o_col, h_col, l_col, c_col = ohlc

    df = pd.read_csv(master)
    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    df = df.dropna(subset=[dt_col]).sort_values(dt_col).copy()
    df["date"] = df[dt_col].dt.floor("D")

    for c in [o_col,h_col,l_col,c_col]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=[c_col]).copy()

    # 5m log returns (reset at day boundary)
    df["lr"] = np.log(df[c_col]).diff()
    df.loc[df["date"].ne(df["date"].shift(1)), "lr"] = np.nan

    rows = []
    for d, g in df.groupby("date"):
        # OHLC day
        o = float(g[o_col].dropna().iloc[0]) if g[o_col].notna().any() else np.nan
        h = float(g[h_col].max()) if g[h_col].notna().any() else np.nan
        l = float(g[l_col].min()) if g[l_col].notna().any() else np.nan
        c = float(g[c_col].dropna().iloc[-1]) if g[c_col].notna().any() else np.nan

        range_5m = (h - l) if np.isfinite(h) and np.isfinite(l) else np.nan
        trend_ratio_5m = (abs(c - o) / range_5m) if np.isfinite(range_5m) and range_5m != 0 else np.nan

        lr = g["lr"].to_numpy(float)
        vol_5m = float(np.nanstd(lr, ddof=1)) if np.sum(~np.isnan(lr)) >= 3 else np.nan
        acf1_5m = safe_corr(lr[1:], lr[:-1]) if np.sum(~np.isnan(lr)) >= 4 else np.nan
        flips = count_sign_flips(lr)

        # first vs second half (by bar count)
        closes = g[c_col].to_numpy(float)
        closes = closes[~np.isnan(closes)]
        if len(closes) >= 4:
            mid = len(closes)//2
            move_first = abs(closes[mid-1] - closes[0])
            move_total = abs(closes[-1] - closes[0])
            move_first_half_share = (move_first / move_total) if move_total != 0 else np.nan
        else:
            move_first_half_share = np.nan

        rows.append({
            "date": pd.to_datetime(d).date(),
            "range_5m": range_5m,
            "vol_5m": vol_5m,
            "trend_ratio_5m": trend_ratio_5m,
            "sign_flips_5m": flips,
            "acf1_5m": acf1_5m,
            "move_first_half_share": move_first_half_share,
        })

    out = pd.DataFrame(rows).sort_values("date")
    out.to_csv(OUT, index=False)

    print("=== INTRADAY 5M DAY FEATURES ===")
    print(f"Master: {master}")
    print(f"Datetime col: {dt_col}")
    print(f"OHLC cols: {o_col}, {h_col}, {l_col}, {c_col}")
    print(f"Days: {len(out)}")
    print(f"Output: {OUT}")
    print("STATUS: STEP 3.1 COMPLETE")

if __name__ == "__main__":
    main()
