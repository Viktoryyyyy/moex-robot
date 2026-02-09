#!/usr/bin/env python3
import argparse, os, pandas as pd

def r(p): return pd.read_csv(p, engine='python', sep=None, on_bad_lines='skip')

def pick(lc, *names):
    for n in names:
        if n in lc: return lc[n]
    return None

def build_dt(df):
    lc={c.lower():c for c in df.columns}
    dcol = pick(lc, 'tradedate_ts','tradedate','date')
    tcol = pick(lc, 'tradetime_ts','tradetime','time')
    if dcol and tcol:
        s = df[dcol].astype(str).str.strip() + ' ' + df[tcol].astype(str).str.strip()
        s = s.str.replace('.',':',regex=False).str.replace(',',':',regex=False)
        return pd.to_datetime(s, errors='coerce')
    u = pick(lc, 'datetime','end','timestamp','_dt','_dt5')
    if u: return pd.to_datetime(df[u], errors='coerce')
    return pd.Series(pd.NaT, index=df.index)

ap=argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)          # SiZ5
ap.add_argument('--date', required=True)            # YYYY-MM-DD (торговая)
a=ap.parse_args()

bars = f"si_5m_{a.date}.csv"
hi2  = f"hi2_{a.ticker}_{a.date}.csv"
if not (os.path.exists(bars) and os.path.exists(hi2)):
    raise SystemExit("input_missing")

m = r(bars).copy()
h = r(hi2).copy()

m_dt = build_dt(m)
h_dt = build_dt(h)

m['_dt'] = m_dt
h['_dt'] = h_dt

m = m[m['_dt'].notna()].sort_values('_dt').reset_index(drop=True)
h = h[h['_dt'].notna()].sort_values('_dt').drop_duplicates('_dt', keep='last').reset_index(drop=True)

keep = [c for c in h.columns if c not in ('_dt','tradedate','tradetime','tradedate_ts','tradetime_ts','SYSTIME')]
x = pd.merge_asof(m, h[['_dt']+keep], on='_dt', direction='backward', tolerance=pd.Timedelta('12H'))

out = f"si_5m_{a.date}_hi2.csv"
x.to_csv(out, index=False)
print("rows", len(x), "file", out, "min", str(x['_dt'].min()), "max", str(x['_dt'].max()))
