#!/usr/bin/env python3
import argparse, os, pandas as pd

def r(p):
    return pd.read_csv(p, engine='python', sep=None, on_bad_lines='skip')

def pick(cols, *cands):
    m={c.lower():c for c in cols}
    for names in cands:
        if isinstance(names,(list,tuple)):
            for n in names:
                if n in m: return m[n]
        else:
            if names in m: return m[names]
    return None

def prep(df, ticker):
    df=df.copy()
    dcol=pick(df.columns,['tradedate_ts','tradedate_ob','tradedate'])
    tcol=pick(df.columns,['tradetime_ts','tradetime_ob','tradetime','time'])
    scol=pick(df.columns,['secid_ts','secid_ob','secid'])
    if not (dcol and tcol and scol): raise SystemExit("missing tradedate/tradetime/secid")
    df['_secid']=df[scol].astype(str).str.strip()
    df=df[df['_secid']==ticker]
    s=(df[dcol].astype(str).str.strip()+' '+df[tcol].astype(str).str.strip())
    s=s.str.replace('.',':',regex=False).str.replace(',',':',regex=False)
    df['_dt']=pd.to_datetime(s, errors='coerce')
    df=df[df['_dt'].notna()]
    return df

def attach_futoi(m, fof, trade_date):
    if not os.path.exists(fof): return m
    fo=r(fof)
    if 'key' not in fo.columns: return m
    fo1=fo.tail(1).copy()
    fo1['_date']=pd.to_datetime(trade_date)
    num=[c for c in fo1.columns if c not in ('key','_date') and pd.api.types.is_numeric_dtype(fo1[c])]
    for c in num: fo1[c]=fo1[c].astype('float64')
    m['_date']=m['_dt'].dt.normalize()
    m=pd.merge(m, fo1.drop(columns=['key']), on='_date', how='left')
    m=m.drop(columns=['_date'])
    return m

ap=argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)
ap.add_argument('--date', required=True)
ap.add_argument('--futoi_ticker', default='si')
a=ap.parse_args()

tsf=f"tradestats_{a.ticker}_{a.date}.csv"
obf=f"obstats_{a.ticker}_{a.date}.csv"
fof=f"futoi_{a.futoi_ticker}_{a.date}.csv"
if not (os.path.exists(tsf) and os.path.exists(obf)): raise SystemExit("input_missing")

ts=prep(r(tsf), a.ticker)
ob=prep(r(obf), a.ticker)

drop_ob={'SYSTIME','tradedate','tradetime','tradedate_ts','tradetime_ts','tradedate_ob','tradetime_ob','_secid','_dt'}
ob_cols=['_secid','_dt']+[c for c in ob.columns if c not in drop_ob]
m=pd.merge(ts, ob[ob_cols], on=['_secid','_dt'], how='left', suffixes=('_ts','_ob'))

m=attach_futoi(m, fof, a.date)
m=m.sort_values('_dt').reset_index(drop=True)
out=f"si_5m_{a.date}.csv"
m.to_csv(out, index=False)
print("rows",len(m),"min",str(m['_dt'].min()) if len(m) else "NA","max",str(m['_dt'].max()) if len(m) else "NA")
