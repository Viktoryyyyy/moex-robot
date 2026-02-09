#!/usr/bin/env python3
import os, sys, argparse, pandas as pd
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from fx_lib_api import get_json, blocks, to_df, resolve_fx_by_key

MSK = ZoneInfo("Europe/Moscow")

def resolve_secid(arg_ticker: str | None, arg_key: str | None) -> str:
    env_ticker = os.getenv("FX_TICKER", "").strip()
    if env_ticker:
        return env_ticker
    if arg_ticker:
        return arg_ticker.strip()
    if arg_key:
        return resolve_fx_by_key(arg_key)
    return resolve_fx_by_key("CNYRUB")

def fetch_1m(secid: str, day: str) -> pd.DataFrame:
    js = get_json(
        f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}/candles.json",
        params={"from": day, "till": day, "interval": 1}
    )
    b = blocks(js)
    c = to_df(b.get("candles", {}))
    return c

def to_5m(c: pd.DataFrame, secid: str) -> pd.DataFrame:
    if c.empty:
        return c
    for k in ("open", "high", "low", "close", "volume"):
        if k in c.columns:
            c[k] = pd.to_numeric(c[k], errors="coerce")
    c["end"] = pd.to_datetime(c["end"]).dt.tz_localize(
        MSK, nonexistent="shift_forward", ambiguous="NaT"
    )
    c = c.dropna(subset=["end", "close"]).set_index("end").sort_index()
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    r = (
        c.resample("5min", label="right", closed="right")
         .agg(agg)
         .dropna(subset=["close"])
         .reset_index()
    )
    r["ticker"] = secid
    r = r[["end", "open", "high", "low", "close", "volume", "ticker"]]
    return r

def iter_days(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    if d1 < d0:
        raise ValueError("end < start")
    cur = d0
    while cur <= d1:
        yield cur.isoformat()
        cur += timedelta(days=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", help="ключ для поиска (CNYRUB, USDRUB и т.п.)")
    ap.add_argument("--ticker", help="точный тикер (например, CNYRUB_TOM)")
    ap.add_argument("--start", required=True, help="начало периода YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="конец периода YYYY-MM-DD")
    ap.add_argument("--out", help="имя файла результата")
    args = ap.parse_args()

    try:
        secid = resolve_secid(args.ticker, args.key)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    frames = []
    stats = []

    for day in iter_days(args.start, args.end):
        try:
            c1 = fetch_1m(secid, day)
        except Exception as e:
            print(f"WARN: fetch failed {secid} {day}: {e}", file=sys.stderr)
            continue
        if c1.empty:
            stats.append((day, 0))
            continue
        r5 = to_5m(c1, secid)
        if not r5.empty:
            r5["date"] = day
            frames.append(r5)
            stats.append((day, len(r5)))
        else:
            stats.append((day, 0))

    if not frames:
        print(f"ERROR: no data for {secid} in period {args.start}..{args.end}")
        sys.exit(2)

    all_df = pd.concat(frames, ignore_index=True).sort_values("end")
    all_df = all_df[["end", "open", "high", "low", "close", "volume", "ticker"]]

    out_name = args.out or f"fx_5m_{args.start}_{args.end}_{secid.lower()}.csv"
    all_df.to_csv(out_name, index=False)

    print(f"Ticker: {secid}")
    print(f"Period: {args.start}..{args.end}")
    print(f"Saved: {out_name} rows={len(all_df)}")

    stat_df = pd.DataFrame(stats, columns=["date","rows"])
    print("\nPer-day rows:")
    print(stat_df.to_string(index=False))

if __name__ == "__main__":
    main()
