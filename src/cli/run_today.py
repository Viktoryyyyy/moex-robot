#!/usr/bin/env python3
# coding: utf-8
import argparse, subprocess, sys, os, pandas as pd

def run(cmd):
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)

def add_hi2(date, fut_ticker):
    base = f"si_5m_{date}.csv"
    hi   = f"hi2_{fut_ticker}_{date}.csv"
    if not (os.path.exists(base) and os.path.exists(hi)):
        print("skip HI2 merge: missing files"); return
    df = pd.read_csv(base)
    h  = pd.read_csv(hi)
    lc = {c.lower(): c for c in h.columns}
    datec = lc.get("tradedate") or lc.get("date")
    timec = lc.get("tradetime") or lc.get("time")
    metric = lc.get("metric")
    value  = lc.get("value") or lc.get("val")
    if not (datec and timec and metric and value):
        print("skip HI2 merge: unexpected columns", list(h.columns)); return
    h["key"] = h[datec].astype(str) + " " + h[timec].astype(str)
    wide = h.pivot_table(index="key", columns=metric, values=value, aggfunc="first").reset_index()
    wide.columns = ["key"] + [f"hi2_{c}" for c in wide.columns[1:]]
    # ensure key in base
    if "key" not in df.columns:
        lc2 = {c.lower(): c for c in df.columns}
        dk = lc2.get("ts_tradedate") or lc2.get("tradedate")
        tk = lc2.get("ts_tradetime") or lc2.get("tradetime")
        df["key"] = df[dk].astype(str) + " " + df[tk].astype(str)
    out = f"si_5m_{date}_hi2.csv"
    df.merge(wide, on="key", how="left").to_csv(out, index=False)
    print("->", out)


def resolve_trade_date():
    """
    Возвращает последнюю доступную торговую дату из /fo/tradestats.json,
    не позже сегодняшней (по календарной дате сервера).
    """
    import os, requests, datetime as dt
    H = {
        "Authorization": "Bearer " + os.getenv("MOEX_API_KEY",""),
        "User-Agent": "moex_bot date_resolver",
        "Accept": "application/json"
    }
    url = "https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json"
    r = requests.get(url, headers=H, timeout=15); r.raise_for_status()
    j = r.json()
    sec = j.get("data.dates")
    if not (isinstance(sec, dict) and "columns" in sec and "data" in sec):
        # fallback: вернём вчера
        return (dt.date.today() - dt.timedelta(days=1)).isoformat()
    cols, data = sec["columns"], sec["data"] or []
    if "date" not in cols or not data:
        return (dt.date.today() - dt.timedelta(days=1)).isoformat()
    idx = cols.index("date")
    all_dates = sorted({row[idx] for row in data if row and row[idx]}, reverse=True)
    today = dt.date.today().isoformat()
    # берём максимальную дату, не превышающую сегодня
    for d in all_dates:
        if d <= today:
            return d
    # если все даты > сегодня, вернём сегодня
    return today


def main():
    ap = argparse.ArgumentParser(description="Run full MOEX Si pipeline for a day")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (по умолчанию сегодня)")
    ap.add_argument("--futures", default="SiZ5", help="тикер фьючерса (для tradestats/obstats/hi2)")
    ap.add_argument("--futoi", default="si", help="тикер для FUTOI (обычно базовый si)")
    args = ap.parse_args()

    import datetime as dt
    d = args.date or resolve_trade_date()

    # 1) fetch
    run(["./fetch_tradestats.py", "--ticker", args.futures, "--date", d])
    run(["./fetch_obstats.py",    "--ticker", args.futures, "--date", d])
    run(["./fetch_futoi.py",      "--ticker", args.futoi,   "--date", d])
    run(["./pivot_futoi_apply.py","--ticker", args.futoi,   "--date", d])

    # 2) merge core
    run(["./merge_5m.py", "--ticker", args.futures, "--date", d, "--futoi_ticker", args.futoi])

    # 3) hi2 fetch + merge
    run(["./fetch_hi2.py", "--ticker", args.futures, "--date", d])
    add_hi2(d, args.futures)
    cleanup_intermediate(d)

def cleanup_intermediate(date):
    import os, glob
    keep = {f"si_5m_{date}.csv", f"si_5m_{date}_hi2.csv"}
    for f in glob.glob("*.csv"):
        if f not in keep:
            os.remove(f)
    print("🧹 Удалены промежуточные файлы, оставлены только финальные CSV.")

if __name__ == "__main__":
    main()
