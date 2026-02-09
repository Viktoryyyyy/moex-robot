#!/usr/bin/env python3
import os, sys, argparse, pandas as pd
from zoneinfo import ZoneInfo
from fx_lib_api import get_json, blocks, to_df, resolve_fx_by_key, resolve_trade_date

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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", help="ключ для поиска (CNYRUB, USDRUB и т.п.)")
    ap.add_argument("--ticker", help="точный тикер (например, CNYRUB_TOM)")
    ap.add_argument("--date", default="auto", help="YYYY-MM-DD или auto")
    ap.add_argument("--out", help="имя файла, по умолчанию fx_5m_<date>_<ticker>.csv")
    args = ap.parse_args()

    try:
        secid = resolve_secid(args.ticker, args.key)
        day = resolve_trade_date(secid) if args.date == "auto" else args.date
        c1 = fetch_1m(secid, day)
        if c1.empty:
            cols = ["end", "open", "high", "low", "close", "volume", "ticker"]
            print(f"ERROR: empty 1m candles for {secid} {day}")
            print(pd.DataFrame(columns=cols).to_string(index=False))
            sys.exit(2)
        r5 = to_5m(c1, secid)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    out_name = args.out or f"fx_5m_{day}_{secid.lower()}.csv"
    r5.to_csv(out_name, index=False)

    print(f"Ticker: {secid}")
    print(f"Date: {day}")
    print(f"Saved: {out_name} rows={len(r5)}")
    try:
        print(r5.head(3).to_string(index=False))
        print(r5.tail(3).to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()
