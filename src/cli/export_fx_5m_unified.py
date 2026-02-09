#!/usr/bin/env python3
import os, sys, argparse, requests, pandas as pd
from zoneinfo import ZoneInfo
API=os.getenv("MOEX_API_URL","https://apim.moex.com").rstrip("/")
UA=os.getenv("MOEX_UA","moex_bot_fx_unified/1.0").strip()
MSK=ZoneInfo("Europe/Moscow")
def H():
    t=os.getenv("MOEX_API_KEY","").strip()
    return {"Authorization":("Bearer "+t) if t else "","User-Agent":UA}
def getj(p,params=None):
    r=requests.get(f"{API}{p}",headers=H(),params=params,timeout=25)
    r.raise_for_status()
    return r.json()
def to_df(b):
    c=b.get("columns") or []; d=b.get("data") or []
    if isinstance(d,list) and d and isinstance(d[0],dict): return pd.DataFrame(d)
    return pd.DataFrame(d,columns=c)
def resolve_fx_by_key(key:str)->str:
    j=getj("/iss/engines/currency/markets/selt/boards/CETS/securities.json",
           params={"securities.columns":"SECID,SHORTNAME,BOARDID"})
    df=to_df(j.get("securities",{}))
    if df.empty: raise SystemExit("ERROR: empty securities list")
    df=df[df["BOARDID"].astype(str).str.upper().eq("CETS")].copy()
    ku=key.upper()
    df["score"]=df["SECID"].astype(str).str.upper().str.contains(ku).astype(int)*2+df["SHORTNAME"].astype(str).str.upper().str.contains(ku).astype(int)
    df=df[df["score"]>0].copy()
    if df.empty: raise SystemExit(f"ERROR: no match for key={key}")
    df["is_TOM"]=df["SECID"].astype(str).str.upper().str.endswith("_TOM")
    df=df.sort_values(["is_TOM","score","SECID"],ascending=[False,False,True])
    return str(df.iloc[0]["SECID"])
def resolve_trade_date(secid:str)->str:
    j=getj(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}.json")
    dv=to_df(j.get("dataversion",{}))
    if dv.empty or "trade_session_date" not in dv.columns: raise SystemExit("ERROR: no dataversion")
    return str(dv.iloc[0]["trade_session_date"])
def fetch_1m(secid:str,d:str)->pd.DataFrame:
    j=getj(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}/candles.json",
           params={"from":d,"till":d,"interval":1})
    return to_df(j.get("candles",{}))
def to_5m_unified(c:pd.DataFrame, secid:str)->pd.DataFrame:
    if c.empty: return c
    for k in ("open","high","low","close","volume"):
        if k in c.columns: c[k]=pd.to_numeric(c[k],errors="coerce")
    c["end"]=pd.to_datetime(c["end"]).dt.tz_localize(MSK,nonexistent="shift_forward",ambiguous="NaT")
    c=c.dropna(subset=["end","close"]).set_index("end").sort_index()
    r=c.resample("5min",label="right",closed="right").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna(subset=["close"]).reset_index()
    r["ticker"]=secid
    r=r[["end","open","high","low","close","volume","ticker"]]
    return r
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--key",required=True)
    ap.add_argument("--date",default="auto")
    args=ap.parse_args()
    secid=resolve_fx_by_key(args.key)
    d=resolve_trade_date(secid) if args.date=="auto" else args.date
    c1=fetch_1m(secid,d)
    if c1.empty:
        print(f"ERROR: empty 1m for {secid} {d}")
        print(pd.DataFrame(columns=["end","open","high","low","close","volume","ticker"])); sys.exit(2)
    r5=to_5m_unified(c1,secid)
    out=f"fx_5m_{d}_{secid.lower()}.csv"
    r5.to_csv(out,index=False)
    print(f"Ticker: {secid}")
    print(f"Saved: {out} rows={len(r5)}")
    try:
        print(r5.head(3).to_string(index=False)); print(r5.tail(3).to_string(index=False))
    except Exception: pass
if __name__=="__main__": main()
