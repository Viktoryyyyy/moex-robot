#!/usr/bin/env python3
import os, sys, argparse, requests, pandas as pd
from zoneinfo import ZoneInfo

API=os.getenv("MOEX_API_URL","https://apim.moex.com").rstrip("/")
UA=os.getenv("MOEX_UA","moex_bot_fx_today/1.1").strip()
MSK=ZoneInfo("Europe/Moscow")

def H():
    t=os.getenv("MOEX_API_KEY","").strip()
    return {"Authorization":("Bearer "+t) if t else "","User-Agent":UA}

def getj(p,params=None):
    r=requests.get(f"{API}{p}",headers=H(),params=params,timeout=25)
    r.raise_for_status()
    return r.json()

def to_df(b):
    c=b.get("columns") or []
    d=b.get("data") or []
    if isinstance(d,list) and d and isinstance(d[0],dict):
        return pd.DataFrame(d)
    return pd.DataFrame(d,columns=c)

def resolve_fx_by_key(key:str)->str:
    j=getj("/iss/engines/currency/markets/selt/boards/CETS/securities.json",
           params={"securities.columns":"SECID,SHORTNAME,BOARDID"})
    df=to_df(j.get("securities",{}))
    df=df[df["BOARDID"].astype(str).str.upper().eq("CETS")].copy()
    ku=key.upper()
    df["score"]=(
        df["SECID"].astype(str).str.upper().str.contains(ku).astype(int)*2 +
        df["SHORTNAME"].astype(str).str.upper().str.contains(ku).astype(int)
    )
    df=df[df["score"]>0].copy()
    df["is_TOM"]=df["SECID"].astype(str).str.upper().str.endswith("_TOM")
    df=df.sort_values(["is_TOM","score","SECID"],ascending=[False,False,True])
    return str(df.iloc[0]["SECID"])

def resolve_trade_date(secid:str)->str:
    j=getj(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}.json")
    dv=to_df(j.get("dataversion",{}))
    return str(dv.iloc[0]["trade_session_date"])

def fetch_1m(secid:str,d:str)->pd.DataFrame:
    j=getj(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}/candles.json",
           params={"from":d,"till":d,"interval":1})
    return to_df(j.get("candles",{}))

def to_5m(c:pd.DataFrame,secid:str)->pd.DataFrame:
    if c.empty: return c
    for k in ("open","high","low","close","volume"):
        if k in c.columns: c[k]=pd.to_numeric(c[k],errors="coerce")
    c["end"]=pd.to_datetime(c["end"]).dt.tz_localize(MSK,nonexistent="shift_forward",ambiguous="NaT")
    c=c.dropna(subset=["end","close"]).set_index("end").sort_index()
    r=c.resample("5min",label="right",closed="right").agg({
        "open":"first","high":"max","low":"min","close":"last","volume":"sum"
    }).dropna(subset=["close"]).reset_index()
    r["ticker"]=secid
    return r[["end","open","high","low","close","volume","ticker"]]

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--keys",default="CNYRUB,USDRUB")
    ap.add_argument("--date",default="auto")
    ap.add_argument("--outdir",default="data/fx")
    args=ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    keys=[k.strip() for k in args.keys.split(",") if k.strip()]
    frames=[]
    day=None
    picked=[]

    for k in keys:
        secid=resolve_fx_by_key(k)
        d=resolve_trade_date(secid) if args.date=="auto" else args.date
        if day is None: day=d
        c1=fetch_1m(secid,d)
        r5=to_5m(c1,secid)
        if not r5.empty:
            frames.append(r5)
            picked.append(secid)

    if not frames:
        print("ERROR: no data frames"); sys.exit(2)

    all_df=pd.concat(frames,ignore_index=True).sort_values(["ticker","end"])
    out=os.path.join(args.outdir, f"fx_5m_{day}.csv")
    all_df.to_csv(out,index=False)

    print(f"Saved: {out} rows={len(all_df)} tickers={picked}")
    print(all_df.groupby("ticker")["end"].agg(['min','max','count']).to_string())

if __name__=="__main__":
    main()
