#!/usr/bin/env python3
import os, sys, json
import pandas as pd
from fx_lib_api import get_json, blocks, to_df, resolve_fx_by_key

def resolve_ticker(arg_key: str | None) -> str:
    env_ticker = os.getenv("FX_TICKER", "").strip()
    if env_ticker:
        return env_ticker
    if arg_key:
        return resolve_fx_by_key(arg_key)
    return resolve_fx_by_key("CNYRUB")

def fx_snapshot(secid: str) -> dict:
    js = get_json(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}.json")
    b = blocks(js)
    md = to_df(b.get("marketdata", {}))
    if md.empty:
        raise RuntimeError("marketdata is empty")
    row = md.iloc[0]

    def num(x):
        try:
            return float(x)
        except Exception:
            return None

    bid = row.get("BID")
    ask = row.get("OFFER")
    last = row.get("LAST")
    spread = row.get("SPREAD")

    bid_v = num(bid)
    ask_v = num(ask)
    last_v = num(last)
    if spread is not None:
        spread_v = num(spread)
    else:
        spread_v = num(ask_v - bid_v) if bid_v is not None and ask_v is not None else None

    t = row.get("TIME") or row.get("UPDATETIME") or ""
    systime = row.get("SYSTIME") or ""

    return {
        "ticker": secid,
        "bid": bid_v,
        "ask": ask_v,
        "last": last_v,
        "spread": spread_v,
        "time": str(t),
        "systime": str(systime),
    }

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", help="ключ для поиска (CNYRUB, USDRUB и т.п.)")
    args = ap.parse_args()

    try:
        secid = resolve_ticker(args.key)
        snap = fx_snapshot(secid)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(json.dumps(snap, ensure_ascii=False))

if __name__ == "__main__":
    main()
