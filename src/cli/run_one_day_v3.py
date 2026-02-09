#!/usr/bin/env python3
import os, argparse, requests, pandas as pd, numpy as np
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com")
UA  = os.getenv("MOEX_UA", "moex_bot_day_v3/1.3").strip()

def H():
    tk = (os.getenv("MOEX_API_KEY","") or "").strip().strip('"').strip("'")
    h = {"User-Agent": UA}
    if tk: h["Authorization"] = "Bearer " + tk
    return h

def get_json(path, params):
    r = requests.get(API + path, headers=H(), params=params, timeout=45)
    r.raise_for_status()
    return r.json()

def to_df(block):
    if not isinstance(block, dict): return pd.DataFrame()
    cols = block.get("columns"); data = block.get("data"); meta = block.get("metadata", {})
    if isinstance(data, list) and data and isinstance(data[0], dict): return pd.DataFrame(data)
    if not cols and isinstance(meta, dict): cols = list(meta.keys())
    return pd.DataFrame(data or [], columns=cols or [])

def pick(df, names):
    s = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in s: return s[n.lower()]
    return None

def end_from(df, cdate, ctime, tz):
    if cdate and ctime and cdate in df.columns and ctime in df.columns:
        dt = pd.to_datetime(df[cdate].astype(str) + " " + df[ctime].astype(str), errors="coerce")
        z = ZoneInfo(tz)
        try: dt = dt.dt.tz_localize(z, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(z)
        except Exception: pass
        return dt.dt.tz_localize(None)
    return pd.NaT

def norm_ts(raw, tz, day):
    t = to_df(raw.get("tradestats", {}))
    if t.empty: return pd.DataFrame()
    co, ch, cl, cc = pick(t,["pr_open","open"]), pick(t,["pr_high","high"]), pick(t,["pr_low","low"]), pick(t,["pr_close","close"])
    cv, cd, ct = pick(t,["vol","volume"]), pick(t,["tradedate","date"]), pick(t,["tradetime","time"])
    end = end_from(t, cd, ct, tz)
    out = pd.DataFrame({
        "end": end,
        "OPEN": pd.to_numeric(t.get(co), errors="coerce") if co else np.nan,
        "HIGH": pd.to_numeric(t.get(ch), errors="coerce") if ch else np.nan,
        "LOW":  pd.to_numeric(t.get(cl), errors="coerce") if cl else np.nan,
        "CLOSE":pd.to_numeric(t.get(cc), errors="coerce") if cc else np.nan,
        "volume": pd.to_numeric(t.get(cv), errors="coerce") if cv else np.nan
    }).dropna(subset=["end"])
    out = out[out["end"].dt.date.astype(str) == day].sort_values("end")
    out["end5"] = out["end"].dt.floor("5min")
    out = (out.groupby("end5", as_index=False)
           .agg({"OPEN":"first","HIGH":"max","LOW":"min","CLOSE":"last","volume":"sum"}))
    out = out.rename(columns={"end5":"end"})
    return out

def norm_ob(raw, tz, day):
    o = to_df(raw.get("obstats", {}))
    if o.empty: return pd.DataFrame()
    cs, cm = pick(o,["spread_mean","sprd_mean","spread"]), pick(o,["mid_mean","midprice_mean","mid"])
    cd, ct = pick(o,["tradedate","date"]), pick(o,["tradetime","time"])
    end = end_from(o, cd, ct, tz)
    out = pd.DataFrame({"end": end}).dropna(subset=["end"])
    if cs: out["spread_mean"] = pd.to_numeric(o[cs], errors="coerce")
    if cm: out["mid_mean"]    = pd.to_numeric(o[cm], errors="coerce")
    out = out[out["end"].dt.date.astype(str) == day]
    out["end5"] = out["end"].dt.floor("5min")
    out = (out.groupby("end5", as_index=False)
           .agg({"spread_mean":"mean","mid_mean":"mean"}))
    out = out.rename(columns={"end5":"end"})
    if {"spread_mean","mid_mean"}.issubset(out.columns):
        with np.errstate(divide='ignore', invalid='ignore'):
            out["liq_raw"] = out["spread_mean"] / out["mid_mean"]
        out["liq_smooth"] = out["liq_raw"].ewm(span=6, adjust=False, min_periods=1).mean()
    return out

def try_norm_futoi(day, tz):
    try:
        raw = get_json("/iss/analyticalproducts/futoi/securities/si.json",
                       {"from": day, "till": day, "iss.meta":"off"})
    except Exception:
        return pd.DataFrame()
    f = to_df(raw.get("futoi", {}))
    if f.empty: return pd.DataFrame()
    cg, pos = pick(f,["clgroup"]), pick(f,["pos"])
    cd, ct   = pick(f,["tradedate"]), pick(f,["tradetime"])
    end = end_from(f, cd, ct, tz)
    if not isinstance(end, pd.Series): return pd.DataFrame()
    f2 = pd.DataFrame({"end": end, "clgroup": f[cg], "pos": pd.to_numeric(f[pos], errors="coerce")})
    f2 = f2.dropna(subset=["end","clgroup"]).sort_values("end").drop_duplicates(["end","clgroup"], keep="last")
    f2["end5"] = f2["end"].dt.floor("5min")
    piv = f2.pivot_table(index="end5", columns="clgroup", values="pos", aggfunc="last").rename_axis(None,axis=1)
    for col in ("FIZ","YUR"):
        if col not in piv.columns: piv[col] = np.nan
    piv = piv[["FIZ","YUR"]].rename(columns={"FIZ":"oi_fiz","YUR":"oi_yur"})
    piv["oi_total"] = piv[["oi_fiz","oi_yur"]].sum(axis=1, min_count=1)
    piv = piv.reset_index().rename(columns={"end5":"end"})
    return piv[piv["end"].dt.date.astype(str) == day]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--futures", default="SiZ5")
    ap.add_argument("--tz", default=os.getenv("MOEX_TZ","Europe/Moscow"))
    ap.add_argument("--outfile_prefix", default="si_5m")
    args = ap.parse_args()

    day, tz = args.date, args.tz

    ts_raw = get_json(f"/iss/datashop/algopack/fo/tradestats/{args.futures}.json",
                      {"from": day, "till": day, "iss.meta":"on"})
    ob_raw = get_json(f"/iss/datashop/algopack/fo/obstats/{args.futures}.json",
                      {"from": day, "till": day, "iss.meta":"on"})

    df_ts = norm_ts(ts_raw, tz, day)   # база
    df_ob = norm_ob(ob_raw, tz, day)   # ликвидность
    df_oi = try_norm_futoi(day, tz)    # опционально

    if df_ts.empty:
        print("ERROR: tradestats is empty for the day — нет базы для OHLCV.")
        base = pd.DataFrame(columns=["end","OPEN","HIGH","LOW","CLOSE","volume"])
    else:
        base = df_ts

    def merge_on_end(left, right):
        if right is None or right.empty: return left
        overlap = [c for c in right.columns if c in left.columns and c!="end"]
        if overlap: right = right.drop(columns=overlap)
        return pd.merge(left, right, on="end", how="left")

    out = merge_on_end(base, df_ob)
    out = merge_on_end(out, df_oi)

    if "liq_smooth" in out.columns:
        out["mr1_liq_ok"] = out["liq_smooth"] < 0.5

    out = out.sort_values("end").drop_duplicates("end", keep="last")

    cols_order = [c for c in ["end","OPEN","HIGH","LOW","CLOSE","volume",
                              "spread_mean","mid_mean","liq_raw","liq_smooth",
                              "oi_fiz","oi_yur","oi_total","mr1_liq_ok"] if c in out.columns]
    out = out[cols_order]

    path = f"{args.outfile_prefix}_{day}.csv"
    tmp = path + ".tmp"
    out.to_csv(tmp, index=False); os.replace(tmp, path)

    print(out.tail(10).to_string(index=False))
    print(f"\nSaved: {path}")

if __name__ == "__main__":
    main()
