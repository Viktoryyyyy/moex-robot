#!/usr/bin/env python3
import pandas as pd, numpy as np
from zoneinfo import ZoneInfo
import sys
from scripts.lib_moex_v3 import get_json

def to_df(block):
    cols = (block or {}).get("columns")
    data = (block or {}).get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return pd.DataFrame(data)
    return pd.DataFrame(data or [], columns=cols or [])

def find_col(cols, names):
    s = {c.lower(): c for c in cols}
    for n in names:
        if n.lower() in s: return s[n.lower()]
    return None

def pick_price_qty(df):
    cols = list(df.columns)
    p = find_col(cols, ["price","PRICE","last_price"])
    q = find_col(cols, ["quantity","qty","NUMBER","volume"])
    return p, q

ticker = sys.argv[1] if len(sys.argv)>1 else "SiZ5"
depth  = int(sys.argv[2]) if len(sys.argv)>2 else 20
tz     = ZoneInfo("Europe/Moscow")

raw = get_json(f"/iss/engines/futures/markets/forts/securities/{ticker}/orderbook.json", {"depth": depth})
bids = to_df(raw.get("bids", {}))
offers_df = to_df(raw.get("offers", {}))
asks_df = to_df(raw.get("asks", {}))
asks = offers_df if not offers_df.empty else asks_df

# время
dt = None
for key in ["orderbook","marketdata","securities"]:
    blk = to_df(raw.get(key, {}))
    if not blk.empty:
        tcol = find_col(list(blk.columns), ["systime","time","datetime","timestamp"])
        if tcol:
            try:
                dt = pd.to_datetime(blk[tcol].iloc[0], utc=True).tz_convert(tz)
            except Exception:
                dt = pd.to_datetime(blk[tcol].iloc[0]).tz_localize(tz)
            break
if dt is None:
    dt = pd.Timestamp.now(tz)

def topn(df, side, n=5):
    if df.empty: return np.nan, 0.0
    p, q = pick_price_qty(df)
    if not p or not q: return np.nan, 0.0
    df2 = df[[p,q]].dropna().copy()
    df2[p] = pd.to_numeric(df2[p], errors="coerce")
    df2[q] = pd.to_numeric(df2[q], errors="coerce")
    df2 = df2.dropna()
    if side=="bid":
        df2 = df2.sort_values(p, ascending=False).head(n)
        best = df2[p].max() if not df2.empty else np.nan
    else:
        df2 = df2.sort_values(p, ascending=True).head(n)
        best = df2[p].min() if not df2.empty else np.nan
    depth = float(df2[q].sum()) if not df2.empty else 0.0
    return best, depth

bb, db = topn(bids, "bid", 5)
ba, da = topn(asks, "ask", 5)
spread = (ba - bb) if pd.notna(ba) and pd.notna(bb) else np.nan

print(f"dt={dt.tz_convert(tz).tz_localize(None)}  best_bid={bb}  best_ask={ba}  spread={spread}")
print("bids_cols:", list(bids.columns)[:8], "… rows:", len(bids))
print("asks_cols:", list(asks.columns)[:8], "… rows:", len(asks))
