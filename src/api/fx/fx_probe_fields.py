#!/usr/bin/env python3
import os, requests, pandas as pd
API=os.getenv("MOEX_API_URL","https://apim.moex.com").rstrip("/")
UA=os.getenv("MOEX_UA","moex_bot_fx_probe/1.0").strip()
TK=os.getenv("MOEX_API_KEY","").strip()
H={"Authorization":("Bearer "+TK) if TK else "","User-Agent":UA}
def get(p,params=None):
    r=requests.get(f"{API}{p}",headers=H(),params=params,timeout=20); r.raise_for_status(); return r.json()
def to_df(b):
    c=b.get("columns") or []; d=b.get("data") or []
    if isinstance(d,list) and d and isinstance(d[0],dict): return pd.DataFrame(d)
    return pd.DataFrame(d,columns=c)
def show(ep):
    j=get(ep); print("ENDPOINT:",ep)
    for k,v in j.items():
        if isinstance(v,dict) and "columns" in v:
            cols=v.get("columns") or []; print(k,"COLUMNS:",cols)
            df=to_df(v)
            if not df.empty: print(df.head(3).to_string(index=False))
    print("-"*60)
for sec in ("USDRUB_TOM","CNYRUB_TOM"):
    show(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{sec}.json")
    show(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{sec}/candles.json")
