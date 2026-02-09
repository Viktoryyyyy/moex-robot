#!/usr/bin/env python3
import os, sys, argparse, requests, pandas as pd, datetime as dt

API = "https://apim.moex.com"
H = {"Authorization":"Bearer "+os.getenv("MOEX_API_KEY",""),
     "User-Agent":"moex_bot_probe_hi2/1.0"}

def to_df(block):
    cols = block.get("columns"); data = block.get("data"); meta = block.get("metadata")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    else:
        if not cols and isinstance(meta, dict): cols = list(meta.keys())
        df = pd.DataFrame(data=data, columns=cols)
    df.columns = [str(c).lower() for c in df.columns]
    return df

def fetch(ticker, d1, d2):
    urls = [
        f"{API}/iss/datashop/algopack/fo/hi2/{ticker}.json?from={d1}&till={d2}",
        f"{API}/iss/datashop/algopack/fo/hi2.json?ticker={ticker}&from={d1}&till={d2}",
    ]
    last=None
    for u in urls:
        r=requests.get(u, headers=H, timeout=30)
        last=(r.status_code,u,r.headers.get("Content-Type"))
        if r.ok and "application/json" in r.headers.get("Content-Type",""):
            j=r.json()
            blk=j.get("hi2") or j.get("data") or {}
            if not isinstance(blk, dict) and "hi2" in j and isinstance(j["hi2"], list):
                blk=j["hi2"][0]
            try:
                df=to_df(blk)
                return df,u
            except Exception:
                continue
    raise SystemExit(f"ERR: no data, last={last}")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--date", required=True)  # YYYY-MM-DD
    args=ap.parse_args()
    d=dt.date.fromisoformat(args.date).isoformat()
    df,u=fetch(args.ticker,d,d)
    if "tradedate" in df.columns:
        df=df[df["tradedate"].astype(str).str[:10]==d]
    print("URL:", u)
    print("COLUMNS:", ", ".join(df.columns))
    print("NROWS:", len(df))
    print("HEAD(5):")
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(df.head(5).to_string(index=False))
if __name__=="__main__":
    main()
