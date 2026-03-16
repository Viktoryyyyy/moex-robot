import glob
import os
import numpy as np
import pandas as pd

def resolve_canonical_master_glob() -> str:
    return os.path.expanduser("~/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv")


OUT_PATH = "data/research/day_metrics_from_master.csv"

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
        ("open_si_fo","high_si_fo","low_si_fo","close_si_fo"),
        ("pr_open","pr_high","pr_low","pr_close"),
    ]
    for o,h,l,c in patterns:
        if o in cols and h in cols and l in cols and c in cols:
            return o,h,l,c
    return None

def pick_master_with_ohlc():
    master_glob = resolve_canonical_master_glob()
    files = sorted(glob.glob(master_glob))
    if not files:
        raise SystemExit(f"No canonical external master CSV found: {master_glob}")

    skipped = []
    for path in files:
        try:
            # читаем только шапку/пару строк, чтобы узнать колонки
            head = pd.read_csv(path, nrows=5)
        except Exception as e:
            skipped.append((path, f"read_error: {e}"))
            continue

        cols = set(head.columns)
        dt_col = detect_col(cols, ["end", "datetime", "timestamp", "ts"])
        ohlc = detect_ohlc(cols)

        if dt_col and ohlc:
            return path, dt_col, ohlc, head.columns.tolist()

        # запомним кратко, почему пропустили
        reason = []
        if not dt_col:
            reason.append("no_datetime")
        if not ohlc:
            reason.append("no_ohlc")
        skipped.append((path, ",".join(reason)))

    # если не нашли — печатаем диагностику и падаем
    print("ERROR: cannot find any master CSV with OHLC")
    print("Checked files:")
    for p, r in skipped[:30]:
        print(f" - {p} [{r}]")
    raise SystemExit("No suitable master found")

def safe_corr(a, b):
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if len(a) < 4:
        return np.nan
    sa = np.std(a, ddof=1)
    sb = np.std(b, ddof=1)
    if sa == 0 or sb == 0:
        return np.nan
    return float(np.corrcoef(a, b)[0, 1])

def count_sign_flips(x):
    x = np.asarray(x, dtype=float)
    x = x[~np.isnan(x)]
    if len(x) < 3:
        return 0
    s = np.sign(x)
    for i in range(1, len(s)):
        if s[i] == 0:
            s[i] = s[i-1]
    return int(np.sum(s[1:] != s[:-1]))

def local_extrema_count(x):
    x = np.asarray(x, dtype=float)
    x = x[~np.isnan(x)]
    if len(x) < 5:
        return 0
    cnt = 0
    for i in range(1, len(x)-1):
        if x[i] > x[i-1] and x[i] > x[i+1]:
            cnt += 1
        elif x[i] < x[i-1] and x[i] < x[i+1]:
            cnt += 1
    return int(cnt)

def main():
    master_path, dt_col, ohlc, cols_list = pick_master_with_ohlc()
    o_col, h_col, l_col, c_col = ohlc

    df = pd.read_csv(master_path)
    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    df = df.dropna(subset=[dt_col]).copy()
    df = df.sort_values(dt_col).copy()
    df["date"] = df[dt_col].dt.floor("D")

    for c in [o_col, h_col, l_col, c_col]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=[c_col]).copy()

    df["lr"] = np.log(df[c_col]).diff()
    df.loc[df["date"].ne(df["date"].shift(1)), "lr"] = np.nan

    cols = set(df.columns)
    liq_col = detect_col(cols, ["liq_smooth", "liq", "LIQ_SMOOTH"])
    spr_col = detect_col(cols, ["spread", "sprd", "SPREAD", "spread_fo"])

    rows = []
    for d, g in df.groupby("date"):
        o = float(g[o_col].dropna().iloc[0]) if g[o_col].notna().any() else np.nan
        h = float(g[h_col].max()) if g[h_col].notna().any() else np.nan
        l = float(g[l_col].min()) if g[l_col].notna().any() else np.nan
        c = float(g[c_col].dropna().iloc[-1]) if g[c_col].notna().any() else np.nan

        day_range = (h - l) if np.isfinite(h) and np.isfinite(l) else np.nan
        rel_range = (day_range / c) if np.isfinite(day_range) and np.isfinite(c) and c != 0 else np.nan

        trend_move = abs(c - o) if np.isfinite(o) and np.isfinite(c) else np.nan
        trend_ratio = (trend_move / day_range) if np.isfinite(trend_move) and np.isfinite(day_range) and day_range != 0 else np.nan

        lr = g["lr"].to_numpy(dtype=float)
        flips = count_sign_flips(lr)
        acf1 = safe_corr(lr[1:], lr[:-1]) if len(lr) >= 4 else np.nan
        extrema = local_extrema_count(g[c_col].to_numpy(dtype=float))

        liq_mean = float(np.nanmean(g[liq_col])) if liq_col else np.nan
        spr_mean = float(np.nanmean(g[spr_col])) if spr_col else np.nan

        rows.append({
            "date": pd.to_datetime(d),
            "range": day_range,
            "rel_range": rel_range,
            "trend_ratio": trend_ratio,
            "sign_flips_5m": flips,
            "acf1_5m": acf1,
            "local_extrema_5m": extrema,
            "liq_mean": liq_mean,
            "spread_mean": spr_mean,
        })

    out = pd.DataFrame(rows).sort_values("date")
    out.to_csv(OUT_PATH, index=False)

    print("=== DAY METRICS BUILT FROM MASTER ===")
    print(f"Master selected: {master_path}")
    print(f"Datetime col: {dt_col}")
    print(f"OHLC cols: {o_col}, {h_col}, {l_col}, {c_col}")
    if liq_col: print(f"Liq col: {liq_col}")
    if spr_col: print(f"Spread col: {spr_col}")
    print(f"Days: {len(out)}")
    print(f"Output: {OUT_PATH}")
    print("STATUS: OK")

if __name__ == "__main__":
    main()
