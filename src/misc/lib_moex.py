#!/usr/bin/env python3
# coding: utf-8
import os, requests, pandas as pd
from typing import Optional, Dict
from dotenv import load_dotenv

APIM_BASE = "https://apim.moex.com"
ISS_BASE  = "https://iss.moex.com"

def mk_session() -> requests.Session:
    load_dotenv()
    s = requests.Session()
    tok = os.getenv("MOEX_API_KEY", "")
    if tok:
        s.headers["Authorization"] = "Bearer " + tok
    s.headers["User-Agent"] = "moex_bot_lib/1.0"
    return s

def norm_tradestats(df: pd.DataFrame) -> pd.DataFrame:
    low = {c.lower(): c for c in df.columns}
    c = low.get("ts_pr_close") or low.get("ts_sec_pr_close") or low.get("close")
    o = low.get("ts_pr_open")  or low.get("open")
    h = low.get("ts_pr_high")  or low.get("high")
    l = low.get("ts_pr_low")   or low.get("low")
    v = low.get("ts_vol") or low.get("volume") or low.get("vol")
    d = low.get("ts_tradedate") or low.get("tradedate") or low.get("date")
    t = low.get("ts_tradetime") or low.get("tradetime") or low.get("time")
    ts = low.get("timestamp") or low.get("ts_systime")

    if ts and ts in df.columns:
        ts_ser = pd.to_datetime(df[ts], errors="coerce")
    elif d and t and d in df.columns and t in df.columns:
        ts_ser = pd.to_datetime(df[d].astype(str) + " " + df[t].astype(str), errors="coerce")
    else:
        ts_ser = pd.Series(pd.NaT, index=df.index)

    out = pd.DataFrame({
        "timestamp": ts_ser,
        "open":  pd.to_numeric(df[o], errors="coerce") if o in df.columns else pd.NA,
        "high":  pd.to_numeric(df[h], errors="coerce") if h in df.columns else pd.NA,
        "low":   pd.to_numeric(df[l], errors="coerce") if l in df.columns else pd.NA,
        "close": pd.to_numeric(df[c], errors="coerce") if c in df.columns else pd.NA,
        "volume":pd.to_numeric(df[v], errors="coerce") if v in df.columns else pd.NA,
    }).dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return out

def norm_candles_iss(js: Dict) -> pd.DataFrame:
    cols = (js.get("candles") or {}).get("columns", [])
    data = (js.get("candles") or {}).get("data", [])
    if not cols or not data:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])
    df = pd.DataFrame(data, columns=cols)
    out = pd.DataFrame({
        "timestamp": pd.to_datetime(df.get("begin"), errors="coerce"),
        "open":  pd.to_numeric(df.get("open"), errors="coerce"),
        "high":  pd.to_numeric(df.get("high"), errors="coerce"),
        "low":   pd.to_numeric(df.get("low"), errors="coerce"),
        "close": pd.to_numeric(df.get("close"), errors="coerce"),
        "volume":pd.to_numeric(df.get("volume"), errors="coerce"),
    }).dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return out

def fetch_tradestats_5m(symbol: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """APIM Datashop: /iss/datashop/algopack/fo/tradestats.json → нормализованный DF."""
    s = mk_session()
    url = f"{APIM_BASE}/iss/datashop/algopack/fo/tradestats.json"
    params = {"ticker": symbol, "tickers": symbol, "interval": 5, "limit": int(limit)}
    r = s.get(url, params=params, timeout=15)
    r.raise_for_status()
    js = r.json()
    tbl = None
    for v in js.values():
        if isinstance(v, dict) and "columns" in v and "data" in v:
            import pandas as pd
            tbl = pd.DataFrame(v["data"], columns=v["columns"])
            break
    if tbl is None or tbl.empty:
        return None
    return norm_tradestats(tbl)

def fetch_candles_iss_5m(symbol: str, limit: int = 120) -> pd.DataFrame:
    """Публичный ISS: /iss/engines/futures/markets/forts/securities/{symbol}/candles.json"""
    s = mk_session()
    url = f"{ISS_BASE}/iss/engines/futures/markets/forts/securities/{symbol}/candles.json"
    params = {"interval": 5, "limit": int(limit)}
    r = s.get(url, params=params, timeout=15)
    r.raise_for_status()
    return norm_candles_iss(r.json())
