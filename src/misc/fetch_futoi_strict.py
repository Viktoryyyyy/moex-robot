#!/usr/bin/env python3
import os, argparse, requests, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)
ap.add_argument('--date',   required=True)
a = ap.parse_args()

H = {"Authorization": "Bearer " + os.getenv("MOEX_API_KEY",""),
     "User-Agent": "moex_bot_futoi_strict"}

u = f"https://apim.moex.com/iss/analyticalproducts/futoi/securities.json?securities={a.ticker}&date={a.date}&limit=5000"
r = requests.get(u, headers=H, timeout=30)
r.raise_for_status()
j = r.json()

def to_df(j):
    for k,v in j.items():
        if isinstance(v, dict) and 'columns' in v and 'data' in v:
            return pd.DataFrame(v['data'], columns=v['columns'])
    for k,v in j.items():
        if isinstance(v, list) and v and all(isinstance(x, list) for x in v):
            cols, rows = v[0], v[1:]
            if all(isinstance(c, str) for c in cols):
                return pd.DataFrame(rows, columns=cols)
    stack=[j]
    while stack:
        x=stack.pop()
        if isinstance(x, dict):
            if 'columns' in x and 'data' in x:
                return pd.DataFrame(x['data'], columns=x['columns'])
            stack.extend(x.values())
        elif isinstance(x, list):
            stack.extend(x)
    return pd.DataFrame()

df = to_df(j)

lc = {c.lower(): c for c in df.columns}
dcol = lc.get('trade_session_date') or lc.get('tradedate')
if dcol is None or df.empty or df[dcol].astype(str).str.strip().eq('').all():
    df['trade_session_date'] = a.date
    dcol = 'trade_session_date'

dd = pd.to_datetime(df[dcol], errors='coerce').dt.date
df = df[dd == pd.to_datetime(a.date).date()].copy()

out = f"futoi_{a.ticker}_{a.date}.csv"
df.to_csv(out, index=False)
print(out, len(df))
