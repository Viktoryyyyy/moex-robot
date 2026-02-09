#!/usr/bin/env python3
# coding: utf-8
import os, sys, argparse, requests, pandas as pd
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

API = os.getenv("MOEX_API_KEY")
if not API:
    print("MOEX_API_KEY not found in env (.env)"); sys.exit(1)

H = {
    "Authorization": f"Bearer {API}",
    "User-Agent": "moex_bot fetch_tradestats",
    "Accept": "application/json"
}

p = argparse.ArgumentParser()
p.add_argument("--ticker", default="SiZ5")
p.add_argument("--date", required=True, help="YYYY-MM-DD")
a = p.parse_args()

url = f"https://apim.moex.com/iss/datashop/algopack/fo/tradestats/{a.ticker}.json"
r = requests.get(url, headers=H, params={"date": a.date}, timeout=25)
r.raise_for_status()
j = r.json()

sec = next((v for v in j.values() if isinstance(v, dict) and "columns" in v and "data" in v), None)
if not sec:
    print("No table section. Keys:", list(j.keys())); sys.exit(2)

cols, data = sec["columns"], sec["data"]
df = pd.DataFrame(data, columns=cols)
out = f"tradestats_{a.ticker}_{a.date}.csv"
df.to_csv(out, index=False)
print(f"Saved {len(df)} rows to {out}")
