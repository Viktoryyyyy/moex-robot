#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
HEADERS = {
    "Authorization": "Bearer " + os.getenv("MOEX_API_KEY", ""),
    "User-Agent": os.getenv("MOEX_UA", "moex_bot_tradestats_chain/1.0").strip(),
}
TZ_MSK = ZoneInfo("Europe/Moscow")

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
    urls = [
        f"{API}/iss/datashop/algopack/fo/tradestats/{ticker}.json?from={day}&till={day}",
        f"{API}/iss/datashop/algopack/fo/tradestats.json?ticker={ticker}&from={day}&till={day}",
    ]
    last_err = None
    for u in urls:
        try:
            r = requests.get(u, headers=HEADERS, timeout=25)
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

def third_thursday(year: int, month: int) -> date:
    d = date(year, month, 1)
    first_thu_delta = (3 - d.weekday()) % 7  # Thursday=3
    first_thu = d + timedelta(days=first_thu_delta)
    third_thu = first_thu + timedelta(days=14)
    return third_thu

def build_si_expiries(year_from: int, year_to: int, base: str = "Si"):
    tbl = []
    for y in range(year_from, year_to + 1):
        for month, code in [(3,"H"),(6,"M"),(9,"U"),(12,"Z")]:
            exp = third_thursday(y, month)
            year_digit = y % 10
            ticker = f"{base}{code}{year_digit}"
            tbl.append((exp, ticker))
    tbl.sort(key=lambda x: x[0])
    return tbl

def get_si_contract(d: date, expiries) -> str:
    for exp, ticker in expiries:
        if d <= exp:
            return ticker
    return expiries[-1][1]

def drange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="Si")
    ap.add_argument("--from", dest="d_from", default="2020-01-01")
    ap.add_argument("--till", dest="d_till", default=None)
    args = ap.parse_args()

    try:
        d_from = datetime.strptime(args.d_from, "%Y-%m-%d").date()
    except ValueError:
        sys.stderr.write("ERROR: --from must be YYYY-MM-DD\n")
        sys.exit(1)

    if args.d_till:
        try:
            d_till = datetime.strptime(args.d_till, "%Y-%m-%d").date()
        except ValueError:
            sys.stderr.write("ERROR: --till must be YYYY-MM-DD\n")
            sys.exit(1)
    else:
        d_till = date.today()

    if d_till < d_from:
        sys.stderr.write("ERROR: till < from\n")
        sys.exit(1)

    exp_tbl = build_si_expiries(d_from.year - 1, d_till.year + 1, base=args.base)

    frames = []
    for d in drange(d_from, d_till):
        if d.weekday() >= 5:
            continue
        fut = get_si_contract(d, exp_tbl)
        df_raw = fetch_tradestats_day(fut, d)
        if df_raw.empty:
            continue
        day_norm = normalize_tradestats(df_raw, fut)
        frames.append(day_norm)
        print(f"{d} {fut}: rows={len(day_norm)}", file=sys.stderr, flush=True)

    if not frames:
        sys.stderr.write("No data collected\n")
        sys.exit(2)

    big = pd.concat(frames, ignore_index=True)
    big = big.sort_values("end").reset_index(drop=True)

    base_lower = args.base.lower()
    out = f"{base_lower}_5m_{d_from.isoformat()}_{d_till.isoformat()}.csv"
    big.to_csv(out, index=False)
    print(f"OK -> {out} rows={len(big)} days={len(frames)}")

if __name__ == "__main__":
    main()
