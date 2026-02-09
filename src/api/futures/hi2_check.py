#!/usr/bin/env python3
import argparse, pandas as pd, sys, os
ap=argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)   # SiZ5
ap.add_argument('--date',   required=True)   # YYYY-MM-DD
a=ap.parse_args()

f = f"hi2_{a.ticker}_{a.date}.csv"
if not os.path.exists(f):
    print(f"not_found {f}")
    sys.exit(2)

df = pd.read_csv(f, engine='python', sep=None, on_bad_lines='skip')
lc = {c.lower(): c for c in df.columns}

def pick(*names):
    for n in names:
        if n in lc: return lc[n]
    return None

dcol = pick('tradedate','date')
tcol = pick('tradetime','time')
dt = None
if dcol and tcol:
    s = df[dcol].astype(str).str.strip() + ' ' + df[tcol].astype(str).str.strip()
    dt = pd.to_datetime(s.str.replace('.',':',regex=False).str.replace(',',':',regex=False), errors='coerce')
else:
    # пробуем единое поле
    u = pick('datetime','end','timestamp')
    if u:
        dt = pd.to_datetime(df[u], errors='coerce')

if dt is None:
    print("no_time_columns")
    sys.exit(0)

ok = dt.dropna()
print("rows", len(df), "notna", ok.size, "uniq_times", ok.nunique())
if ok.size:
    vc = ok.dt.strftime("%Y-%m-%d %H:%M:%S").value_counts()
    print("top_times", vc.head(10).to_dict())
