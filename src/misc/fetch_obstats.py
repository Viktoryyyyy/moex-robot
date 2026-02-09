#!/usr/bin/env python3
import os, sys, argparse, requests, pandas as pd, datetime as dt

API = "https://apim.moex.com"
H = {"Authorization":"Bearer "+os.getenv("MOEX_API_KEY",""),
     "User-Agent":"moex_bot_obstats_export/1.0"}

def to_df(block):
    cols = block.get("columns"); data = block.get("data"); meta = block.get("metadata")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    else:
        if not cols and isinstance(meta, dict): cols = list(meta.keys())
        df = pd.DataFrame(data=data, columns=cols)
    df.columns = [str(c).lower() for c in df.columns]
    return df

def fetch(ticker, d1, d2):
    urls = [
        f"{API}/iss/datashop/algopack/fo/obstats/{ticker}.json?from={d1}&till={d2}",
        f"{API}/iss/datashop/algopack/fo/obstats.json?ticker={ticker}&from={d1}&till={d2}",
    ]
    last = None
    for u in urls:
        r = requests.get(u, headers=H, timeout=30)
        last = (r.status_code, u, r.headers.get("Content-Type"))
        if r.ok and "application/json" in r.headers.get("Content-Type",""):
            j = r.json()
            blk = j.get("obstats") or j.get("data") or {}
            if not isinstance(blk, dict) and "obstats" in j and isinstance(j["obstats"], list):
                blk = j["obstats"][0]
            try:
                return to_df(blk), u
            except Exception:
                continue
    raise SystemExit(f"ERR: no data, last={last}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True)      # SiZ5
    ap.add_argument("--date", required=True)        # YYYY-MM-DD
    ap.add_argument("--smooth", type=int, default=12, help="окно сглаживания для liq_smooth (в барах)")
    args = ap.parse_args()

    d = dt.date.fromisoformat(args.date).isoformat()
    df, used = fetch(args.ticker, d, d)

    # фильтруем по дате и готовим время
    if "tradedate" in df.columns:
        df = df[df["tradedate"].astype(str).str[:10] == d].copy()
    df["datetime"] = pd.to_datetime(df.get("tradedate", d).astype(str).str[:10] + " " + df.get("tradetime","00:00:00").astype(str), errors="coerce")
    df = df.sort_values("datetime")

    # гарантируем поля из пробы
    need = [
        "mid_price","micro_price",
        "spread_l1","spread_l2","spread_l3","spread_l5","spread_l10","spread_l20",
        "levels_b","levels_s",
        "vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5","vol_b_l10","vol_b_l20",
        "vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5","vol_s_l10","vol_s_l20",
        "vwap_b_l3","vwap_b_l5","vwap_b_l10","vwap_b_l20",
        "vwap_s_l3","vwap_s_l5","vwap_s_l10","vwap_s_l20",
        "secid","asset_code","systime","tradedate","tradetime"
    ]
    for c in need:
        if c not in df.columns: df[c] = None

    # числовые
    num_cols = [c for c in need if c not in ("secid","asset_code","systime","tradedate","tradetime")]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # производные метрики (без догадок о шагах цены — только относительные и суммарные)
    # относительный спред (unitless)
    for k in ["l1","l2","l3","l5","l10","l20"]:
        s = f"spread_{k}"
        if s in df.columns:
            df[f"rel_{s}"] = df[s] / df["mid_price"]

    # глубины по сторонам и суммарно
    def sum_exist(row, cols): 
        return sum([row[c] if pd.notna(row.get(c)) else 0 for c in cols])
    for k, cols_b, cols_s in [
        ("l3",  ["vol_b_l1","vol_b_l2","vol_b_l3"], ["vol_s_l1","vol_s_l2","vol_s_l3"]),
        ("l5",  ["vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5"], ["vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5"]),
        ("l10", ["vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5","vol_b_l10"], ["vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5","vol_s_l10"]),
        ("l20", ["vol_b_l1","vol_b_l2","vol_b_l3","vol_b_l5","vol_b_l10","vol_b_l20"], ["vol_s_l1","vol_s_l2","vol_s_l3","vol_s_l5","vol_s_l10","vol_s_l20"]),
    ]:
        df[f"depth_b_{k}"] = df[cols_b].sum(axis=1, skipna=True)
        df[f"depth_s_{k}"] = df[cols_s].sum(axis=1, skipna=True)
        df[f"depth_tot_{k}"] = df[f"depth_b_{k}"] + df[f"depth_s_{k}"]

    # базовый индикатор "тонкости" книги: rel_spread_l1 (чем выше — тоньше)
    df["liq_raw"] = df["rel_spread_l1"]

    # сглаживание (по умолчанию ~1 час при 5м барах)
    w = max(1, int(args.smooth))
    df["liq_smooth"] = df["liq_raw"].rolling(w, min_periods=1).mean()

    # итоговые колонки (минимум для конвейера)
    out_cols = [
        "datetime","tradedate","tradetime","secid","asset_code","systime",
        "mid_price","micro_price","spread_l1","rel_spread_l1","levels_b","levels_s",
        "depth_b_l3","depth_s_l3","depth_tot_l3",
        "depth_b_l5","depth_s_l5","depth_tot_l5",
        "depth_b_l10","depth_s_l10","depth_tot_l10",
        "depth_b_l20","depth_s_l20","depth_tot_l20",
        "vwap_b_l3","vwap_b_l5","vwap_b_l10","vwap_b_l20",
        "vwap_s_l3","vwap_s_l5","vwap_s_l10","vwap_s_l20",
        "liq_raw","liq_smooth"
    ]
    for c in out_cols:
        if c not in df.columns: df[c] = None
    out = df[out_cols].sort_values("datetime")

    fname = f"obstats_{args.ticker}_{d}.csv"
    out.to_csv(fname, index=False)
    print(f"OK: {fname} rows={len(out)} URL={used}  (smooth={w})")

if __name__ == "__main__":
    main()
