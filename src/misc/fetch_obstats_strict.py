#!/usr/bin/env python3
import os, argparse, requests, pandas as pd
ap=argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)
ap.add_argument('--date', required=True)
a=ap.parse_args()
H={"Authorization":"Bearer "+os.getenv("MOEX_API_KEY",""),"User-Agent":"moex_bot_strict"}
U=f"https://apim.moex.com/iss/datashop/algopack/fo/obstats.json?securities={a.ticker}&date={a.date}&limit=5000"
r=requests.get(U,headers=H,timeout=30); r.raise_for_status()
j=r.json()

def to_df(j):
    for k in j:
        v=j[k]
        if isinstance(v,dict) and 'columns' in v and 'data' in v:
            return pd.DataFrame(v['data'], columns=v['columns'])
    for k in j:
        v=j[k]
        if isinstance(v,list) and v and all(isinstance(x,list) for x in v):
            cols=v[0]; rows=v[1:]; 
            if all(isinstance(c,str) for c in cols):
                return pd.DataFrame(rows, columns=cols)
    stack=[j]
    while stack:
        x=stack.pop()
        if isinstance(x,dict):
            if 'columns' in x and 'data' in x:
                return pd.DataFrame(x['data'], columns=x['columns'])
            stack.extend(x.values())
        elif isinstance(x,list):
            stack.extend(x)
    raise RuntimeError("no_table")
df=to_df(j)
out=f"obstats_{a.ticker}_{a.date}.csv"
df.to_csv(out,index=False)
print(out, len(df))
