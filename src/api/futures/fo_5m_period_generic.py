#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_fo_5m_period/1.0").strip()
TZ_MSK = ZoneInfo("Europe/Moscow")

def H():
    tk = os.getenv("MOEX_API_KEY", "").strip()
    h = {"User-Agent": UA}
    if tk:
        h["Authorization"] = "Bearer " + tk
    return h

def _to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns") or []
    data = block.get("data") or []
    meta = block.get("metadata")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    else:
        if not cols and isinstance(meta, dict):
            cols = list(meta.keys())
        df = pd.DataFrame(data=data, columns=cols)
    df.columns = [str(c).lower() for c in df.columns]
    return df

def fetch_tradestats_day(ticker: str, d: date) -> pd.DataFrame:
    day = d.isoformat()
    paths = [
        f"/iss/datashop/algopack/fo/tradestats/{ticker}.json",
        "/iss/datashop/algopack/fo/tradestats.json",
    ]
    last_err = None
    for p in paths:
        params = {"from": day, "till": day}
        if "tradestats.json" in p and "{ticker}" not in p:
            params["ticker"] = ticker
        try:
            r = requests.get(API + p, headers=H(), params=params, timeout=25)
        except Exception as e:
            last_err = f"req_error={e}"
            continue
        if not r.ok or "application/json" not in r.headers.get("content-type", ""):
            last_err = f"http={r.status_code}"
            continue
        try:
            j = r.json()
        except Exception as e:
            last_err = f"json_error={e}"
            continue
        blk = j.get("tradestats") or j.get("data") or {}
        if not isinstance(blk, dict) and "tradestats" in j and isinstance(j["tradestats"], list):
            blk = j["tradestats"][0]
        df = _to_df(blk)
        if df.empty:
            return df
        if "tradedate" in df.columns:
            df = df[df["tradedate"].astype(str).str[:10] == day].copy()
        return df
    sys.stderr.write(f"WARN {day} {ticker}: tradestats empty or error: {last_err}\n")
    return pd.DataFrame()

def normalize_tradestats(df_raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame(columns=["end","open","high","low","close","volume","trades","ticker"])

    need = ["tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol"]
    for c in need:
        if c not in df_raw.columns:
            df_raw[c] = None

    tradedate = df_raw["tradedate"].astype(str).str[:10]
    tradetime = df_raw["tradetime"].astype(str)
    dt = pd.to_datetime(tradedate + " " + tradetime, errors="coerce")
    dt = dt.dt.tz_localize(TZ_MSK, nonexistent="shift_forward", ambiguous="NaT")

    out = pd.DataFrame()
    out["end"]   = dt.astype(str)
    out["open"]  = pd.to_numeric(df_raw["pr_open"],  errors="coerce")
    out["high"]  = pd.to_numeric(df_raw["pr_high"],  errors="coerce")
    out["low"]   = pd.to_numeric(df_raw["pr_low"],   errors="coerce")
    out["close"] = pd.to_numeric(df_raw["pr_close"], errors="coerce")
    out["volume"]= pd.to_numeric(df_raw["vol"],      errors="coerce").fillna(0).astype(int)

    trades_col = None
    for c in ["numtrades","trades","deals","ntrades"]:
        if c in df_raw.columns:
            trades_col = c
            break
    if trades_col:
        out["trades"] = pd.to_numeric(df_raw[trades_col], errors="coerce").fillna(0).astype(int)
    else:
        out["trades"] = 0

    if "secid" in df_raw.columns:
        out["ticker"] = df_raw["secid"].astype(str)
    else:
        out["ticker"] = ticker

    out = out.sort_values("end").reset_index(drop=True)
    return out[["end","open","high","low","close","volume","trades","ticker"]]

def drange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True, help="FO тикер, например RIZ5")
    ap.add_argument("--from", dest="d_from", required=True, help="начало периода YYYY-MM-DD")
    ap.add_argument("--till", dest="d_till", required=True, help="конец периода YYYY-MM-DD")
    ap.add_argument("--out",  dest="out", default=None, help="имя выходного CSV (опционально)")
    args = ap.parse_args()

    try:
        d_from = datetime.strptime(args.d_from, "%Y-%m-%d").date()
        d_till = datetime.strptime(args.d_till, "%Y-%m-%d").date()
    except ValueError:
        sys.stderr.write("ERROR: --from/--till must be YYYY-MM-DD\n")
        sys.exit(1)

    if d_till < d_from:
        sys.stderr.write("ERROR: till < from\n")
        sys.exit(1)

    frames = []
    for d in drange(d_from, d_till):
        if d.weekday() >= 5:
            continue
        df_raw = fetch_tradestats_day(args.ticker, d)
        if df_raw.empty:
            continue
        day_norm = normalize_tradestats(df_raw, args.ticker)
        if day_norm.empty:
            continue
        frames.append(day_norm)
        print(f"{d} {args.ticker}: rows={len(day_norm)}", file=sys.stderr, flush=True)

    if not frames:
        sys.stderr.write("No data collected\n")
        sys.exit(2)

    big = pd.concat(frames, ignore_index=True)
    big = big.sort_values("end").reset_index(drop=True)

    out = args.out
    if not out:
        out = f"{args.ticker.lower()}_5m_{d_from.isoformat()}_{d_till.isoformat()}.csv"

    big.to_csv(out, index=False)
    print(f"OK -> {out} rows={len(big)} days={len(frames)}")

if __name__ == "__main__":
    import argparse
    main()
