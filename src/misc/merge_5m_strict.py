#!/usr/bin/env python3
import argparse, pandas as pd

ap=argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)
ap.add_argument('--date', required=True)
a=ap.parse_args()

def r(p): return pd.read_csv(p, engine='python', sep=None, on_bad_lines='skip')

def prep(df, secid_target):
    df=df.copy()
    cols={c.lower():c for c in df.columns}
    dcol=cols.get('tradedate') or cols.get('tradedate_ts') or cols.get('tradedate_ob')
    tcol=cols.get('tradetime') or cols.get('tradetime_ts') or cols.get('tradetime_ob') or cols.get('time')
    scol=cols.get('secid') or cols.get('secid_ts') or cols.get('secid_ob')
    if not (dcol and tcol and scol): raise SystemExit("missing tradedate/tradetime/secid")
    df['_secid']=df[scol].astype(str).str.strip()
    df=df[df['_secid']==secid_target]
    s=(df[dcol].astype(str).str.strip()+' '+df[tcol].astype(str).str.strip())
    s=s.str.replace('.',':',regex=False).str.replace(',',':',regex=False)
    # парсим и нормализуем к концу пяти минут
    dt=pd.to_datetime(s, errors='coerce')
    dt5=dt.dt.floor('5min')+pd.Timedelta(minutes=5)
    df['_dt5']=dt5
    return df

ts=r(f"tradestats_{a.ticker}_{a.date}.csv")
ob=r(f"obstats_{a.ticker}_{a.date}.csv")

ts=prep(ts, a.ticker)
ob=prep(ob, a.ticker)

# левый джойн по пяти минутам и secid
drop_ob={'SYSTIME','tradedate','tradetime','tradedate_ts','tradetime_ts','tradedate_ob','tradetime_ob','_secid','_dt5'}
ob_cols=['_secid','_dt5']+[c for c in ob.columns if c not in drop_ob]
m=pd.merge(ts, ob[ob_cols], on=['_secid','_dt5'], how='left', suffixes=('_ts','_ob'))

m=m.sort_values('_dt5').reset_index(drop=True)
m.to_csv(f"si_5m_{a.date}.csv", index=False)

print("rows",len(m))
if len(m):
    s=m['_dt5'].dropna().sort_values()
    print("time_min",str(s.iloc[0]),"time_max",str(s.iloc[-1]),"unique",s.nunique())
