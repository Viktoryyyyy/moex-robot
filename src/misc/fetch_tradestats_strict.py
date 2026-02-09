#!/usr/bin/env python3
import os, argparse, requests, pandas as pd, json
ap=argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)
ap.add_argument('--date', required=True)
a=ap.parse_args()
H={"Authorization":"Bearer "+os.getenv("MOEX_API_KEY",""),"User-Agent":"moex_bot_strict"}
U=f"https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json?securities={a.ticker}&date={a.date}&limit=5000"
r=requests.get(U,headers=H,timeout=30); r.raise_for_status()
j=r.json()

def to_df(j, prefer=('tradestats','data','columns')):
    # 1) типичный формат: j[key] = {'columns':[], 'data':[[]]}
    for k in j:
        v=j[k]
        if isinstance(v,dict) and 'columns' in v and 'data' in v and isinstance(v['data'],list):
            return pd.DataFrame(v['data'], columns=v['columns'])
    # 2) матричный формат: j[key] = [columns_row, row1, row2, ...]
    for k in j:
        v=j[k]
        if isinstance(v,list) and v and all(isinstance(x,list) for x in v):
            cols=v[0]; rows=v[1:]
            if all(isinstance(c,str) for c in cols):
                return pd.DataFrame(rows, columns=cols)
    # 3) глубокий поиск
    stack=[j]
    while stack:
        x=stack.pop()
        if isinstance(x,dict):
            if 'columns' in x and 'data' in x and isinstance(x['data'],list):
                return pd.DataFrame(x['data'], columns=x['columns'])
            stack.extend(x.values())
        elif isinstance(x,list):
            stack.extend(x)
    raise RuntimeError("no_table")
df=to_df(j)
out=f"tradestats_{a.ticker}_{a.date}.csv"
df.to_csv(out,index=False)
print(out, len(df))
