#!/usr/bin/env python3
import os, sys, pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from ..utils.lib_moex_api import get_json, resolve_fut_by_key

TZ_MSK = ZoneInfo("Europe/Moscow")

def load_tradestats(ticker: str, day: str) -> pd.DataFrame:
    j = get_json(f"/iss/datashop/algopack/fo/tradestats/{ticker}.json",
                 {"from":day,"till":day}, timeout=25.0)
    b = j.get("data") or {}
    cols, data = b.get("columns",[]), b.get("data",[])
    if not cols or not data:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    raw = pd.DataFrame(data, columns=cols)
    need = {"tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol"}
    if not need.issubset(raw.columns):
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    raw["end"] = raw["tradedate"] + " " + raw["tradetime"] + "+03:00"
    df = pd.DataFrame({
        "end": raw["end"],
        "open": pd.to_numeric(raw["pr_open"],  errors="coerce"),
        "high": pd.to_numeric(raw["pr_high"],  errors="coerce"),
        "low":  pd.to_numeric(raw["pr_low"],   errors="coerce"),
        "close":pd.to_numeric(raw["pr_close"], errors="coerce"),
        "volume":pd.to_numeric(raw["vol"],     errors="coerce"),
    }).sort_values("end").reset_index(drop=True)
    return df

def main():
    key = os.getenv("FO_KEY") or (sys.argv[1] if len(sys.argv)>1 else "")
    day = os.getenv("FO_DAY") or (sys.argv[2] if len(sys.argv)>2 else "") \
          or datetime.now(TZ_MSK).date().isoformat()
    if not key:
        print("Usage: FO_KEY=<substr> [FO_DAY=YYYY-MM-DD] python fo_5m_day.py", file=sys.stderr); sys.exit(2)
    secid = resolve_fut_by_key(key, board="rfud", limit_probe_day=day)
    if not secid:
        print(f"ERROR: no futures match key='{key}'", file=sys.stderr); sys.exit(3)
    df = load_tradestats(secid, day)
    if df.empty:
        print(f"WARN: no data for {secid} {day}"); sys.exit(0)
    out = f"{secid.lower()}_5m_{day}.csv"
    df.to_csv(out, index=False)
    print(f"Ticker: {secid}")
    print(f"Saved: {out} rows={len(df)}")
    print("# head(3)"); print(df.head(3).to_csv(index=False))
    print("# tail(3)"); print(df.tail(3).to_csv(index=False))

if __name__=="__main__":
    main()
