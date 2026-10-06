"""Independent edge-case arithmetic and temporal checks for this research only."""
from types import SimpleNamespace
import json
import numpy as np,pandas as pd
from replay import Engine,rounded_stop,P,O
from analyze import Funding,FOLDS,greedy
checks=[]
def check(name,condition):
 assert condition,name
 checks.append(name)
check('protective_long_tick_rounding',abs(rounded_stop(85.54,1,.051)-85.48)<1e-8)
check('protective_short_tick_rounding',abs(rounded_stop(85.54,-1,.051)-85.60)<1e-8)
check('five_kopecks_example',abs(rounded_stop(85.54,1,.05)-85.49)<1e-8)
def mock(bars,side=1,target=102,level=99,ti=1,minutes=None):
 z=Engine.__new__(Engine);z.b=pd.DataFrame(bars,columns=['open','high','low','close']);z.b['ts']=pd.date_range('2025-01-10 12:05',periods=len(bars),freq='5min')
 for c in ['open','high','low','close']:setattr(z,c,z.b[c].to_numpy(float))
 z.end=z.b.ts.to_numpy(dtype='datetime64[ns]').astype(np.int64);z.begin=z.end-300*10**9;z.minutes=minutes or {};z.minute_conflicts=set();z.ambiguous=set();z.event_targets={0:ti};z.buffers=lambda r:[('fixed_0.05',-1,.05)]
 r=SimpleNamespace(event_id=0,ei=0,xi=len(bars)-1,side=side,target=target,level=level)
 return z,r
z,r=mock([[100,100.5,98.9,99],[99,103,99,102],[102,102,102,102]])
a=z.event(r)[0];check('stop_before_later_target',a[8]=='stop' and a[12] and abs(a[6]-98.95)<1e-8)
z,r=mock([[100,101,99.9,100],[98,98.5,97,98],[102,102,102,102]],ti=2)
a=z.event(r)[0];check('stop_gap_fills_worse_than_trigger',a[8]=='stop' and a[6]==98 and a[13]>.9)
z,r=mock([[100,102.5,98.5,101],[101,101,101,101]],ti=0)
a=z.event(r)[0];check('ambiguous_5m_conservative',a[8]=='stop' and a[9] and a[10])
z,r=mock([[100,103,98.5,99],[99,99,99,99]],side=-1,target=98,level=102,ti=0)
a=z.event(r)[0];check('short_stop_symmetric',a[8]=='stop' and abs(a[6]-102.05)<1e-8)
z,r=mock([[103,103,103,103],[103,103,103,103]],ti=0)
a=z.event(r)[0];check('target_gap_at_open',a[8]=='target' and a[6]==103)
fund=Funding();h=pd.read_csv(O/'official_history.csv');day=h[h.SWAPRATE.abs().gt(.02)&h.TRADEDATE.gt('2025-01-01')].iloc[0]
en=np.array([pd.Timestamp(day.TRADEDATE+' 12:00').value]);ex=np.array([pd.Timestamp(day.TRADEDATE+' 20:00').value])
p=np.array([85.]);long,_=fund.value(en,ex,p,np.array([1.]));short,_=fund.value(en,ex,p,np.array([-1.]))
check('funding_signed_exact_amount',abs(long[0]-day.SWAPRATE*1000)<1e-7 and abs(long[0]+short[0])<1e-7)
check('funding_clock_transition',pd.Timestamp(fund.times[np.searchsorted(fund.times,pd.Timestamp('2026-03-23').value)]).hour==23)
e=pd.read_csv(O/'events.csv',parse_dates=['entry_time','expiry_time','confirmed_at']);q=pd.read_csv(O/'quantile_models.csv')
check('all_confirmations_causal',bool((e.confirmed_at<e.entry_time).all()))
check('all_quantile_labels_purged',all(pd.Timestamp(r.train_last_expiry)<pd.Timestamp(FOLDS[r.model_fold][0]) for r in q.itertuples()))
for key,g in e.groupby(['tf','horizon']):
 take=g[g.event_id.isin(greedy(g))].sort_values(['entry_time','event_id'])
 check('no_overlap_'+str(key),all(take.entry_time.iloc[1:].to_numpy()>take.expiry_time.iloc[:-1].to_numpy()))
paths=pd.read_pickle(O/'paths.pkl');paired=paths[paths.model_fold.eq(-1)].merge(e[['event_id','side','level']],on='event_id')
check('all_stops_protective',bool((paired.side*(paired.level-paired.stop)>=-1e-7).all()))
check('all_stops_on_tick',bool((np.abs(paired.stop*100-np.round(paired.stop*100))<1e-6).all()))
joined=paths.merge(e[['event_id','expiry_time','entry_time']],on='event_id')
expiry_ns=joined.expiry_time.to_numpy(dtype='datetime64[ns]').astype(np.int64)
entry_ns=joined.entry_time.to_numpy(dtype='datetime64[ns]').astype(np.int64)
check('no_exit_after_fixed_expiry',bool((joined.exit_ns<=expiry_ns).all()))
check('no_exit_before_entry',bool((joined.exit_ns>=entry_ns).all()))
check('no_ns_us_unit_mixing',bool(((joined.exit_ns-entry_ns)/86400e9<33).all()))
out={'project':'MOEX_Bot','passed':len(checks),'checks':checks}
(O/'verification_checks.json').write_text(json.dumps(out,indent=2),encoding='utf-8');print(json.dumps(out,indent=2))
