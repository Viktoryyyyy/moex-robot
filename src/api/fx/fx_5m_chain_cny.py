#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
HEADERS = {
    "Authorization": "Bearer " + os.getenv("MOEX_API_KEY", ""),
    "User-Agent": os.getenv("MOEX_UA", "moex_bot_fx_chain/1.0").strip(),
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

def fetch_candles_1m_day(ticker: str, d: date) -> pd.DataFrame:
    day = d.isoformat()
    url = (
        f"{API}/iss/engines/currency/markets/selt/boards/CETS/"
        f"securities/{ticker}/candles.json?from={day}&till={day}&interval=1"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if not r.ok or "application/json" not in r.headers.get("content-type",""):
            return pd.DataFrame()
        j = r.json()
        blk = j.get("candles") or j.get("data") or {}
        if not isinstance(blk, dict) and "candles" in j and isinstance(j["candles"], list):
            blk = j["candles"][0]
        return _to_df(blk)
    except Exception:
        return pd.DataFrame()

def normalize_1m_to_5m(df_raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame(columns=["end","open","high","low","close","volume","trades","ticker"])

    need = ["begin","open","high","low","close","volume"]
    for c in need:
        if c not in df_raw.columns:
            df_raw[c] = None

    dt = pd.to_datetime(df_raw["begin"].astype(str), errors="coerce")
    dt = dt.dt.tz_localize(TZ_MSK, nonexistent="shift_forward", ambiguous="NaT")
    df = pd.DataFrame({
        "dt": dt,
        "open":  pd.to_numeric(df_raw["open"], errors="coerce"),
        "high":  pd.to_numeric(df_raw["high"], errors="coerce"),
        "low":   pd.to_numeric(df_raw["low"], errors="coerce"),
        "close": pd.to_numeric(df_raw["close"], errors="coerce"),
        "volume":pd.to_numeric(df_raw["volume"], errors="coerce").fillna(0),
    }).dropna(subset=["dt"])

    if df.empty:
        return pd.DataFrame(columns=["end","open","high","low","close","volume","trades","ticker"])

    df = df.set_index("dt").sort_index()

    agg = df.resample("5T", label="right", closed="right").agg({
        "open":"first",
        "high":"max",
        "low":"min",
        "close":"last",
        "volume":"sum"
    }).dropna(subset=["open","high","low","close"])

    if agg.empty:
        return pd.DataFrame(columns=["end","open","high","low","close","volume","trades","ticker"])

    out = agg.reset_index()
    out["end"] = out["dt"].astype(str)
    out["volume"] = out["volume"].fillna(0).round().astype(int)
    out["trades"] = 0
    out["ticker"] = ticker

    return out[["end","open","high","low","close","volume","trades","ticker"]]

def drange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default="CNYRUB_TOM")
    ap.add_argument("--from", dest="d_from", default="2020-01-01")
    ap.add_argument("--till", dest="d_till", default=None)
    args = ap.parse_args()

    try:
        d_from = datetime.strptime(args.d_from, "%Y-%m-%d").date()
        d_till = datetime.strptime(args.d_till, "%Y-%m-%d").date() if args.d_till else date.today()
    except ValueError:
        sys.stderr.write("ERROR: invalid --from/--till\n")
        sys.exit(1)

    frames = []
    for d in drange(d_from, d_till):
        if d.weekday() >= 5:
            continue
        df_1m = fetch_candles_1m_day(args.ticker, d)
        if df_1m.empty:
            continue
        df_5m = normalize_1m_to_5m(df_1m, args.ticker)
        if df_5m.empty:
            continue
        frames.append(df_5m)
        print(f"{d} {args.ticker}: rows={len(df_5m)}", file=sys.stderr)

    if not frames:
        sys.stderr.write("No data collected\n")
        sys.exit(2)

    big = pd.concat(frames, ignore_index=True)
    big = big.sort_values("end").reset_index(drop=True)

    out = f"{args.ticker.lower()}_5m_{d_from}_{d_till}.csv"
    big.to_csv(out, index=False)
    print(f"OK -> {out} rows={len(big)} days={len(frames)}")

if __name__ == "__main__":
    main()
