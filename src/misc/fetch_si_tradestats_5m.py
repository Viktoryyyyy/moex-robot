#!/usr/bin/env python3
import os, sys, time, argparse, requests
import pandas as pd
from datetime import datetime, date
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_si_tradestats_5m/1.0").strip()
MSK = ZoneInfo("Europe/Moscow")

def H(use_token=True):
    h = {"User-Agent": UA, "Accept": "application/json"}
    if use_token:
        tk = os.getenv("MOEX_API_KEY","").strip()
        if tk:
            h["Authorization"] = "Bearer " + tk
    return h

def fetch_tradestats(ticker: str, d0: str, d1: str) -> dict:
    path = f"/iss/datashop/algopack/fo/tradestats/{ticker}.json"
    url = f"{API}{path}"
    params = {"from": d0, "till": d1}
    # try with token
    r = requests.get(url, headers=H(True), params=params, timeout=(5,30))
    if r.status_code in (401,403):
        # try without token (иногда прокатывает, но чаще нужен токен)
        r = requests.get(url, headers=H(False), params=params, timeout=(5,30))
    r.raise_for_status()
    return r.json()

def normalize_tradestats(j: dict) -> pd.DataFrame:
    blk = j.get("tradestats", {})
    cols = blk.get("columns", [])
    data = blk.get("data", [])
    print("tradestats.columns:", cols)
    print("tradestats.rows:", len(data))
    if not data:
        return pd.DataFrame(columns=["end","OPEN","HIGH","LOW","CLOSE","volume"])
    # показать первые 3 строки «как есть»
    for row in data[:3]:
        print("raw row:", row)

    df = pd.DataFrame(data, columns=cols)

    # Варианты полей по опытным дням:
    # tradedate, tradetime, pr_open, pr_high, pr_low, pr_close, vol
    # Иногда может быть уже 'end'
    if "end" in df.columns:
        # приведём к tz-aware MSK
        if pd.api.types.is_datetime64_any_dtype(df["end"]):
            df["end"] = pd.to_datetime(df["end"], errors="coerce")
        else:
            df["end"] = pd.to_datetime(df["end"], errors="coerce")
        # если наивное, локализуем в MSK
        if df["end"].dt.tz is None:
            df["end"] = df["end"].dt.tz_localize(MSK)
    else:
        date_col = "tradedate" if "tradedate" in df.columns else None
        time_col = "tradetime" if "tradetime" in df.columns else None
        if not date_col or not time_col:
            raise RuntimeError("tradestats: no (end) and no (tradedate, tradetime)")
        ts = pd.to_datetime(df[date_col].astype(str) + " " + df[time_col].astype(str), errors="coerce")
        df["end"] = ts.dt.tz_localize(MSK)

    # Маппинг на проектные поля
    mapping = {
        "pr_open":"OPEN", "pr_high":"HIGH", "pr_low":"LOW", "pr_close":"CLOSE",
        "open":"OPEN", "high":"HIGH", "low":"LOW", "close":"CLOSE",
        "vol":"volume", "volume":"volume", "VAL":"volume"
    }
    for src, dst in mapping.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]

    need = ["end","OPEN","HIGH","LOW","CLOSE","volume"]
    for c in need:
        if c not in df.columns:
            df[c] = pd.NA

    out = df[need].dropna(subset=["end"]).drop_duplicates(subset=["end"]).sort_values("end").reset_index(drop=True)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d0", required=True, help="YYYY-MM-DD")
    ap.add_argument("--till", dest="d1", required=True, help="YYYY-MM-DD")
    ap.add_argument("--si", dest="ticker", default=os.getenv("SI_TICKER","SiZ5"))
    args = ap.parse_args()

    j = fetch_tradestats(args.ticker, args.d0, args.d1)
    df = normalize_tradestats(j)
    fn = f"si_5m_{args.d0}_{args.d1}.csv"
    df.to_csv(fn, index=False)
    print(f"Saved: {fn}")
    if not df.empty:
        print(df.head(3).to_string(index=False))
        print("... tail:")
        print(df.tail(3).to_string(index=False))

if __name__ == "__main__":
    pd.set_option("display.width", 200)
    main()
