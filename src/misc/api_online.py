#!/usr/bin/env python3
# coding: utf-8
import sys, pandas as pd
from lib_moex import fetch_tradestats_5m, fetch_candles_iss_5m

def fetch_moex_5m(symbol: str = "SiZ5", limit: int = 120) -> pd.DataFrame:
    # 1) APIM Datashop
    try:
        df = fetch_tradestats_5m(symbol, limit)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    # 2) Публичный ISS
    try:
        return fetch_candles_iss_5m(symbol, limit)
    except Exception:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])

if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else "SiZ5"
    lim = int(sys.argv[2]) if len(sys.argv) > 2 else 120
    df = fetch_moex_5m(sym, lim)
    print("rows:", len(df))
    print(df.tail(5).to_string(index=False))
