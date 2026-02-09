#!/usr/bin/env python3
import os, requests, pandas as pd
from zoneinfo import ZoneInfo

API=os.getenv("MOEX_API_URL","https://apim.moex.com").rstrip("/")
UA=os.getenv("MOEX_UA","moex_bot_fx_tail/1.0").strip()
TK=os.getenv("MOEX_API_KEY","").strip()
SEC=os.getenv("SEC","CNYRUB_TOM")

HDR={"Authorization":("Bearer "+TK) if TK else "","User-Agent":UA}
MSK=ZoneInfo("Europe/Moscow")

def get(p,params=None):
    r=requests.get(f"{API}{p}",headers=HDR,params=params,timeout=20)
    r.raise_for_status()
    return r.json()

def to_df(b):
    c=b.get("columns") or []
    d=b.get("data") or []
    if isinstance(d,list) and d and isinstance(d[0],dict):
        return pd.DataFrame(d)
    return pd.DataFrame(d,columns=c)

m=get(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{SEC}.json")
dv=to_df(m.get("dataversion",{}))
D=str(dv.iloc[0]["trade_session_date"])

j=get(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{SEC}/candles.json",
      params={"from":D,"till":D,"interval":1})
c=to_df(j.get("candles",{}))

for k in ("open","high","low","close","volume"):
    if k in c.columns:
        c[k]=pd.to_numeric(c[k],errors="coerce")

c["end"]=pd.to_datetime(c["end"]).dt.tz_localize(MSK,nonexistent="shift_forward",ambiguous="NaT")
c=c.dropna(subset=["end","close"]).set_index("end").sort_index()

print("Last 1m bar:")
print(c.tail(1).to_string())

agg={"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
r=c.resample("5min",label="right",closed="right").agg(agg).dropna(subset=["close"]).reset_index()

print("\nLast 5m bar:")
print(r.tail(1).to_string(index=False))
