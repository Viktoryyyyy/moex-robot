#!/usr/bin/env python3
# coding: utf-8
import os, sys, argparse, requests, pandas as pd
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

API=os.getenv("MOEX_API_KEY")
if not API:
    print("MOEX_API_KEY not found in .env"); sys.exit(1)

H={"Authorization":f"Bearer {API}","User-Agent":"moex_bot fetch_hi2","Accept":"application/json"}

p=argparse.ArgumentParser()
p.add_argument("--ticker", default="SiZ5")
p.add_argument("--date", default=None, help="Дата YYYY-MM-DD (если не указана, возьмёт последнюю доступную)")
a=p.parse_args()

# если дата не задана — берём последнюю доступную
date=a.date
if not date:
    r=requests.get("https://apim.moex.com/iss/datashop/algopack/fo/hi2.json",headers=H,timeout=15)
    j=r.json()
    sec=j.get("data.dates")
    if sec and "data" in sec and sec["data"]:
        dates=[d[0] for d in sec["data"] if d]
        date=max(dates)
    else:
        print("Не удалось получить список дат HI2"); sys.exit(1)

url=f"https://apim.moex.com/iss/datashop/algopack/fo/hi2/{a.ticker}.json"
r=requests.get(url,headers=H,params={"date":date},timeout=25)
r.raise_for_status()
j=r.json()

sec=next((v for v in j.values() if isinstance(v,dict) and "columns" in v and "data" in v),None)
if not sec:
    print("No HI2 section. Keys:", list(j.keys())); sys.exit(2)

cols, data = sec["columns"], sec["data"]
df = pd.DataFrame(data, columns=cols)
out = f"hi2_{a.ticker}_{date}.csv"
df.to_csv(out, index=False)
print(f"Saved {len(df)} rows to {out}")
