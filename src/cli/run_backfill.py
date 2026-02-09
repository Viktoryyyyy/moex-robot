#!/usr/bin/env python3
# coding: utf-8
import os, sys, argparse, subprocess, datetime as dt, pandas as pd

H = {
    "Authorization": "Bearer " + os.getenv("MOEX_API_KEY",""),
    "User-Agent": "moex_bot backfill",
    "Accept": "application/json"
}

def run(cmd):
    print("+"," ".join(cmd)); subprocess.run(cmd, check=True)

def get_trade_dates(start, end, futures_ticker):
    """
    Проверяем каждый день [start, end] запросом /fo/tradestats/{ticker}.json?date=...
    Берём дни, где секция не пустая.
    """
    import requests
    tpl = "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/{tik}.json"
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)
    dates, cur = [], d0
    while cur <= d1:
        d = cur.isoformat()
        try:
            js = requests.get(tpl.format(tik=futures_ticker), headers=H, params={"date": d}, timeout=15).json()
            sec = next((v for v in js.values() if isinstance(v,dict) and 'columns' in v and 'data' in v), None)
            if sec and sec.get('data'):
                dates.append(d)
        except Exception:
            pass
        cur += dt.timedelta(days=1)
    return dates

def ensure_key(df, dcols=("tradedate","ts_tradedate","date"), tcols=("tradetime","ts_tradetime","time")):
    if "key" in df.columns: 
        return df
    lc = {c.lower(): c for c in df.columns}
    dcol = next((lc[c] for c in dcols if c in lc), None)
    tcol = next((lc[c] for c in tcols if c in lc), None)
    if not (dcol and tcol):
        raise SystemExit("Не удалось построить key")
    df["key"] = df[dcol].astype(str) + " " + df[tcol].astype(str)
    return df

def force_key_date(df, requested_date, time_candidates=("tradetime","ts_tradetime","time","ob_tradetime")):
    """Заменить дату в key на requested_date, сохранив время; либо построить key из столбца времени."""
    import pandas as pd
    if "key" in df.columns:
        tm = pd.to_datetime(df["key"], errors="coerce").dt.strftime("%H:%M:%S")
        df["key"] = requested_date + " " + tm.fillna("00:00:00")
        return df
    lc = {c.lower(): c for c in df.columns}
    tcol = next((lc[c] for c in time_candidates if c in lc), None)
    if tcol is None:
        raise SystemExit("нет поля времени для формирования key")
    tm = pd.to_datetime(df[tcol].astype(str), errors="coerce").dt.strftime("%H:%M:%S")
    df["key"] = requested_date + " " + tm.fillna("00:00:00")
    return df

def add_norm_spreads(df):
    mid = None
    for cand in ("ob_mid_price","ob_ob_mid_price"):
        if cand in df.columns: 
            mid=cand; break
    if not mid: 
        return df
    for lvl in ["l1","l2","l3","l5","l10","l20"]:
        for base in ["ob_spread_","ob_ob_spread_"]:
            col = f"{base}{lvl}"
            if col in df.columns:
                df[f"{col}_rel"] = df[col] / df[mid]
    df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)
    return df

def add_liq_flag(df, thresh=2e-5):
    col = next((c for c in ["ob_spread_l1_rel","ob_ob_spread_l1_rel"] if c in df.columns), None)
    df["liq_flag_low"] = (df[col] > thresh).astype("int8") if col else pd.NA
    return df

def merge_hi2(base_path, hi2_path, out_path):
    df = pd.read_csv(base_path)
    df = ensure_key(df)
    if not (os.path.exists(hi2_path) and os.path.getsize(hi2_path)>0):
        df.to_csv(out_path, index=False); 
        return
    h = pd.read_csv(hi2_path)
    if h.empty: 
        df.to_csv(out_path, index=False); 
        return
    lc = {c.lower(): c for c in h.columns}
    datec = lc.get("tradedate") or lc.get("date")
    timec = lc.get("tradetime") or lc.get("time")
    metric = lc.get("metric")
    value  = lc.get("value") or lc.get("val")
    if not (datec and timec and metric and value):
        df.to_csv(out_path, index=False); 
        return
    h["key"] = h[datec].astype(str) + " " + h[timec].astype(str)
    wide = h.pivot_table(index="key", columns=metric, values=value, aggfunc="first").reset_index()
    wide.columns = ["key"] + [f"hi2_{c}" for c in wide.columns[1:]]
    df.merge(wide, on="key", how="left").to_csv(out_path, index=False)

def main():
    ap = argparse.ArgumentParser(description="Backfill MOEX Si by available trade dates")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD")
    ap.add_argument("--futures", default="SiZ5")
    ap.add_argument("--futoi",   default="si")
    ap.add_argument("--liq_thresh", type=float, default=2e-5)
    args = ap.parse_args()

    d0 = dt.date.fromisoformat(args.start).isoformat()
    d1 = dt.date.fromisoformat(args.end).isoformat()
    dates = get_trade_dates(d0, d1, args.futures)
    if not dates:
        print("В диапазоне нет торговых дат"); sys.exit(0)
    print("Торговые даты в диапазоне:", dates)

    for d in dates:
        print(f"\n=== {d} ===")
        # fetch
        try:
            run(["./fetch_tradestats.py","--ticker",args.futures,"--date",d])
            run(["./fetch_obstats.py","--ticker",args.futures,"--date",d])
            run(["./fetch_futoi.py","--ticker",args.futoi,"--date",d])
            run(["./pivot_futoi_apply.py","--ticker",args.futoi,"--date",d])
        except subprocess.CalledProcessError as e:
            print("fetch error; skip", d, e); 
            continue

        ts = pd.read_csv(f"tradestats_{args.futures}_{d}.csv")
        ob = pd.read_csv(f"obstats_{args.futures}_{d}.csv")
        fo = pd.read_csv(f"futoi_{args.futoi}_{d}.csv")

        ts = ensure_key(ts); ob = ensure_key(ob)
        if "key" not in fo.columns: 
            fo = ensure_key(fo)

        # префиксы
        ts = ts.rename(columns={c:(c if c=="key" else f"ts_{c}") for c in ts.columns})
        ob = ob.rename(columns={c:(c if c=="key" else f"ob_{c}") for c in ob.columns})
        # fo уже с колоночными префиксами fo_* (после pivot)

        # привести дату в key к запрошенной (МОEX использует дату сессии)
        ts = force_key_date(ts.copy(), d)
        ob = force_key_date(ob.copy(), d)
        fo = force_key_date(fo.copy(), d)

        # merge
        merged = ts.merge(ob, on="key", how="left").merge(fo, on="key", how="left")

        # фильтр по дате в key
        merged = merged[ merged["key"].astype(str).str.startswith(d + " ") ]

        # обогащение
        merged = add_norm_spreads(merged)
        merged = add_liq_flag(merged, args.liq_thresh)

        core = f"si_5m_{d}.csv"
        merged.to_csv(core, index=False)
        print("->", core, len(merged), "rows")

        # HI2
        try:
            run(["./fetch_hi2.py","--ticker",args.futures,"--date",d])
        except subprocess.CalledProcessError:
            pass
        hi2p = f"hi2_{args.futures}_{d}.csv"
        final = f"si_5m_{d}_hi2.csv"
        merge_hi2(core, hi2p, final)
        print("->", final)

        # cleanup intermed
        for f in [f"tradestats_{args.futures}_{d}.csv",
                  f"obstats_{args.futures}_{d}.csv",
                  f"futoi_{args.futoi}_{d}.csv",
                  f"futoi_{args.futoi}_{d}.csv.bak",
                  f"hi2_{args.futures}_{d}.csv"]:
            if os.path.exists(f): 
                try: os.remove(f)
                except Exception: pass

if __name__ == "__main__":
    main()
