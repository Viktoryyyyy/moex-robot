#!/usr/bin/env python3
import os, sys, requests, pandas as pd, numpy as np

API = "https://apim.moex.com"
H = {"Authorization":"Bearer " + os.getenv("MOEX_API_KEY",""),
     "User-Agent":"moex_bot_merge_on_the_fly/1.2"}

def _to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns"); data = block.get("data"); meta = block.get("metadata")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    else:
        if not cols and isinstance(meta, dict): cols = list(meta.keys())
        df = pd.DataFrame(data=data, columns=cols)
    df.columns = [str(c).lower() for c in df.columns]
    return df

# -------- TRADESTATS --------
def fetch_tradestats(ticker: str, d: str) -> pd.DataFrame:
    urls = [
        f"{API}/iss/datashop/algopack/fo/tradestats/{ticker}.json?from={d}&till={d}",
        f"{API}/iss/datashop/algopack/fo/tradestats.json?ticker={ticker}&from={d}&till={d}",
    ]
    for u in urls:
        r = requests.get(u, headers=H, timeout=30)
        if r.ok and "application/json" in r.headers.get("content-type",""):
            j = r.json()
            blk = j.get("tradestats") or j.get("candles") or j.get("data") or {}
            if not isinstance(blk, dict) and "tradestats" in j and isinstance(j["tradestats"], list):
                blk = j["tradestats"][0]
            df = _to_df(blk)
            if "tradedate" in df.columns:
                df = df[df["tradedate"].astype(str).str[:10] == d].copy()
            return df
    raise RuntimeError("tradestats: no data")

def enrich_tradestats(df: pd.DataFrame, ma: int = 20) -> pd.DataFrame:
    need = ["tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol","pr_vwap","disb","systime","secid","asset_code"]
    for c in need:
        if c not in df.columns: df[c] = None
    out = pd.DataFrame({
        "tradedate": df["tradedate"].astype(str).str[:10],
        "tradetime": df["tradetime"].astype(str),
        "open":  pd.to_numeric(df["pr_open"],  errors="coerce"),
        "high":  pd.to_numeric(df["pr_high"],  errors="coerce"),
        "low":   pd.to_numeric(df["pr_low"],   errors="coerce"),
        "close": pd.to_numeric(df["pr_close"], errors="coerce"),
        "volume":pd.to_numeric(df["vol"],      errors="coerce"),
        "secid": df["secid"],
        "asset_code": df["asset_code"],
        "systime": df["systime"],
        "vwap": pd.to_numeric(df["pr_vwap"], errors="coerce"),
        "imbalance": pd.to_numeric(df["disb"], errors="coerce"),
    })
    # MR-1
    w = max(2, int(ma))
    out["ret1"] = out["close"].pct_change()
    out["ma_close"] = out["close"].rolling(w, min_periods=1).mean()
    out["std_close"] = out["close"].rolling(w, min_periods=2).std()
    out["z_close"] = (out["close"] - out["ma_close"]) / out["std_close"]
    out["dev_ma"] = out["close"] / out["ma_close"]
    out["hl_range"] = (out["high"] - out["low"]).abs()
    return out

# -------- OBSTATS --------
def fetch_obstats(ticker: str, d: str) -> pd.DataFrame:
    urls = [
        f"{API}/iss/datashop/algopack/fo/obstats/{ticker}.json?from={d}&till={d}",
        f"{API}/iss/datashop/algopack/fo/obstats.json?ticker={ticker}&from={d}&till={d}",
    ]
    for u in urls:
        r = requests.get(u, headers=H, timeout=30)
        if r.ok and "application/json" in r.headers.get("content-type",""):
            j = r.json()
            blk = j.get("obstats") or j.get("data") or {}
            if not isinstance(blk, dict) and "obstats" in j and isinstance(j["obstats"], list):
                blk = j["obstats"][0]
            df = _to_df(blk)
            if "tradedate" in df.columns:
                df = df[df["tradedate"].astype(str).str[:10] == d].copy()
            return df
    raise RuntimeError("obstats: no data")

def enrich_obstats(df: pd.DataFrame, smooth: int = 12) -> pd.DataFrame:
    need = [
        "tradedate","tradetime","secid","asset_code","systime",
        "mid_price","micro_price",
        "spread_l1","spread_l2","spread_l3","spread_l5","spread_l10","spread_l20",
        "levels_b","levels_s",
        "vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5","vol_b_l10","vol_b_l20",
        "vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5","vol_s_l10","vol_s_l20",
        "vwap_b_l3","vwap_b_l5","vwap_b_l10","vwap_b_l20",
        "vwap_s_l3","vwap_s_l5","vwap_s_l10","vwap_s_l20",
    ]
    for c in need:
        if c not in df.columns: df[c] = None
    num_cols = [c for c in need if c not in ("tradedate","tradetime","secid","asset_code","systime")]
    for c in num_cols: df[c] = pd.to_numeric(df[c], errors="coerce")
    # относительные спрэды
    for k in ["l1","l2","l3","l5","l10","l20"]:
        s = f"spread_{k}"
        if s in df.columns:
            df[f"rel_{s}"] = df[s] / df["mid_price"]
    # глубины
    df["depth_b_l3"]  = df[["vol_b_l1","vol_b_l2","vol_b_l3"]].sum(axis=1, skipna=True)
    df["depth_s_l3"]  = df[["vol_s_l1","vol_s_l2","vol_s_l3"]].sum(axis=1, skipna=True)
    df["depth_tot_l3"]= df["depth_b_l3"] + df["depth_s_l3"]
    df["depth_b_l5"]  = df[["vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5"]].sum(axis=1, skipna=True)
    df["depth_s_l5"]  = df[["vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5"]].sum(axis=1, skipna=True)
    df["depth_tot_l5"]= df["depth_b_l5"] + df["depth_s_l5"]
    df["depth_b_l10"] = df[["vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5","vol_b_l10"]].sum(axis=1, skipna=True)
    df["depth_s_l10"] = df[["vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5","vol_s_l10"]].sum(axis=1, skipna=True)
    df["depth_tot_l10"]= df["depth_b_l10"] + df["depth_s_l10"]
    df["depth_b_l20"] = df[["vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5","vol_b_l10","vol_b_l20"]].sum(axis=1, skipna=True)
    df["depth_s_l20"] = df[["vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5","vol_s_l10","vol_s_l20"]].sum(axis=1, skipna=True)
    df["depth_tot_l20"]= df["depth_b_l20"] + df["depth_s_l20"]
    # ликвидность
    w = max(1, int(smooth))
    df["rel_spread_l1"] = df.get("rel_spread_l1")
    df["liq_smooth"] = df["rel_spread_l1"].rolling(w, min_periods=1).mean()
    return df

# -------- FUTOI --------
def fetch_futoi_si(d: str) -> pd.DataFrame:
    u = f"{API}/iss/analyticalproducts/futoi/securities/si.json?from={d}&till={d}"
    r = requests.get(u, headers=H, timeout=30)
    r.raise_for_status()
    j = r.json()
    blk = j.get("futoi") or j.get("securities") or {}
    df = _to_df(blk)
    if "tradedate" in df.columns:
        df = df[df["tradedate"].astype(str).str[:10] == d].copy()
    return df

def futoi_join_fiz_yur(df: pd.DataFrame) -> pd.DataFrame:
    need = ["tradedate","tradetime","clgroup","pos","pos_long","pos_short","pos_long_num","pos_short_num"]
    for c in need:
        if c not in df.columns: df[c] = None
    df = df[df["clgroup"].isin(["FIZ","YUR"])].copy()
    df["_ts"] = df["tradedate"].astype(str).str[:10] + " " + df["tradetime"].astype(str)
    cols_val = ["pos","pos_long","pos_short","pos_long_num","pos_short_num"]
    wide = None
    for grp in ["FIZ","YUR"]:
        part = df[df["clgroup"]==grp][["_ts"]+cols_val].rename(columns={c:f"{grp.lower()}_{c}" for c in cols_val})
        wide = part if wide is None else wide.merge(part, on="_ts", how="outer")
    meta = (df.sort_values(["tradedate","tradetime"])
              .groupby("_ts", as_index=False)[["tradedate","tradetime"]]
              .agg("last"))
    res = meta.merge(wide, on="_ts", how="left")
    for c in [c for c in res.columns if any(k in c for k in ["fiz_","yur_"])]:
        res[c] = pd.to_numeric(res[c], errors="coerce")
    res["oi_total"] = res["fiz_pos"].fillna(0) + res["yur_pos"].fillna(0)
    res = res.sort_values(["tradedate","tradetime"])
    res["oi_delta"] = res["oi_total"].diff()
    with np.errstate(divide='ignore', invalid='ignore'):
        res["fiz_share"] = res["fiz_pos"] / res["oi_total"]
        res["yur_share"] = res["yur_pos"] / res["oi_total"]
    return res

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--futs", default="SiZ5")
    ap.add_argument("--ma", type=int, default=20)
    ap.add_argument("--smooth", type=int, default=12)
    args = ap.parse_args()
    d = args.date

    # 1) TRADES
    ts = enrich_tradestats(fetch_tradestats(args.futs, d), ma=args.ma)
    ts["datetime"] = pd.to_datetime(ts["tradedate"] + " " + ts["tradetime"], errors="coerce")

    # 2) OBSTATS
    ob = enrich_obstats(fetch_obstats(args.futs, d), smooth=args.smooth)
    ob["datetime"] = pd.to_datetime(ob["tradedate"] + " " + ob["tradetime"], errors="coerce")
    ob_small = ob.drop(columns=[c for c in ["tradedate","tradetime","secid","asset_code","systime"] if c in ob.columns])

    # 3) FUTOI
    fo = futoi_join_fiz_yur(fetch_futoi_si(d))

    # ---- MERGE (без HI2) ----
    m = pd.merge(ts, ob_small, on="datetime", how="left")
    m = pd.merge(m, fo, on=["tradedate","tradetime"], how="left")

    # Финальный datetime как строка
    m["datetime"] = pd.to_datetime(m["tradedate"] + " " + m["tradetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    base_order = [
        "datetime","tradedate","tradetime","secid","asset_code",
        "open","high","low","close","volume","vwap","imbalance",
        "spread_l1","rel_spread_l1","levels_b","levels_s",
        "depth_tot_l3","depth_tot_l5","depth_tot_l10","depth_tot_l20",
        "liq_smooth",
        "fiz_pos","fiz_pos_long","fiz_pos_short","fiz_pos_long_num","fiz_pos_short_num",
        "yur_pos","yur_pos_long","yur_pos_short","yur_pos_long_num","yur_pos_short_num",
        "oi_total","oi_delta","fiz_share","yur_share",
        "ret1","ma_close","std_close","z_close","dev_ma","hl_range"
    ]
    for c in base_order:
        if c not in m.columns: m[c] = np.nan
    m = m[base_order].sort_values("datetime").reset_index(drop=True)

    out = f"si_5m_{d}.csv"
    m.to_csv(out, index=False)
    print(f"OK: {out} rows={len(m)} cols={len(m.columns)}  (no HI2)")
if __name__ == "__main__":
    main()
