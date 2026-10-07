from pathlib import Path
import json,hashlib
import pandas as pd,numpy as np
P=Path(__file__).resolve().parent;I=P/'inputs';O=P/'results';O.mkdir(exist_ok=True)
def dump(name,obj): (O/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
def atr(f):
 prev=f.close.shift();return pd.concat([f.high-f.low,(f.high-prev).abs(),(f.low-prev).abs()],axis=1).max(axis=1).rolling(14,min_periods=14).mean()
def main():
 raw=pd.read_csv(I/'accepted_bars.csv',parse_dates=['ts']);q={}
 q['raw_rows']=len(raw);q['duplicate_keys']=int(raw.duplicated(['trade_date','ts']).sum());assert not q['duplicate_keys']
 good=raw[['open','high','low','close']].notna().all(axis=1)&raw.low.gt(0)&raw.high.ge(raw[['open','close']].max(axis=1))&raw.low.le(raw[['open','close']].min(axis=1))&raw.volume.gt(0)
 q['invalid_or_zero_volume_rows']=int((~good).sum());f=raw.loc[good].sort_values('ts').reset_index(drop=True)
 q['off_tick_prices']=int((np.abs(f[['open','high','low','close']]*100-np.round(f[['open','high','low','close']]*100))>1e-6).sum().sum())
 q['min_timestamp']=str(f.ts.min());q['max_timestamp']=str(f.ts.max());q['dates']=f.trade_date.nunique()
 f['i']=np.arange(len(f));f['begin']=f.ts-pd.Timedelta(minutes=5);f['hour']=f.ts.dt.ceil('h')
 h=f.groupby('hour').agg(open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),volume=('volume','sum'),n=('i','size'),first=('ts','first'),last=('ts','last'),last_i=('i','last')).reset_index()
 valid=(h.n==12)&(h['first']==h.hour-pd.Timedelta(minutes=55))&(h['last']==h.hour)
 q['observed_hours']=len(h);q['complete_hours']=int(valid.sum());q['incomplete_hours']=int((~valid).sum())
 h=h.loc[valid].reset_index(drop=True);h['confirmed_at']=h.hour;h['atr']=atr(h)
 d=f.groupby('trade_date').agg(open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),volume=('volume','sum'),n=('i','size'),first=('ts','first'),last=('ts','last'),last_i=('i','last')).reset_index()
 # D1 civil-date close is available conservatively at next midnight; only prior completed dates.
 d['confirmed_at']=pd.to_datetime(d.trade_date)+pd.Timedelta(days=1);d['atr']=atr(d);d['sma50']=d.close.rolling(50).mean()
 d['vol_ref']=(d.atr/d.close).rolling(252,min_periods=60).median().shift()
 hist=pd.DataFrame(json.loads((I/'history_all.json').read_bytes())['rows']);hist.TRADEDATE=hist.TRADEDATE.astype(str)
 comp=d.merge(hist,left_on='trade_date',right_on='TRADEDATE',how='outer',indicator=True)
 q['daily_comparison_counts']=comp['_merge'].value_counts().to_dict();q['official_funding_missing']=int(hist.SWAPRATE.isna().sum())
 q['funding_missing_minmax']=[hist.loc[hist.SWAPRATE.isna(),'TRADEDATE'].min(),hist.loc[hist.SWAPRATE.isna(),'TRADEDATE'].max()]
 for col in ['open','high','low','close','volume']:
  q['daily_'+col+'_mismatches']=int(((comp[col]-comp[col.upper()]).abs()>1e-6).fillna(False).sum())
 comp.to_csv(O/'daily_source_comparison.csv',index=False)
 cached=pd.read_csv(I/'tradestats.csv',low_memory=False);cached['ts']=pd.to_datetime(cached.tradedate+' '+cached.tradetime)
 tie=f[f.trade_date.le('2026-10-02')].merge(cached,on='ts',how='outer',indicator=True)
 q['cache_vs_accepted_keys']=tie['_merge'].value_counts().to_dict()
 q['cache_vs_accepted_ohlc_mismatch']=int(sum(((tie[c]-tie['pr_'+c]).abs()>1e-7).sum() for c in ['open','high','low','close']))
 ob=pd.read_csv(I/'obstats.csv',low_memory=False);ob['ob_ts']=pd.to_datetime(ob.tradedate+' '+ob.tradetime);ob['spread']=ob.mid_price*ob.spread_l1/10000
 ob=ob.loc[ob.spread.gt(0)&ob.spread.lt(ob.mid_price),['ob_ts','spread','vol_b_l1','vol_s_l1']].sort_values('ob_ts')
 # Previous completed interval, strictly before modeled execution.
 f=pd.merge_asof(f.sort_values('begin'),ob,left_on='begin',right_on='ob_ts',direction='backward',allow_exact_matches=False,tolerance=pd.Timedelta(minutes=60))
 same_day=f.ob_ts.dt.date==f.begin.dt.date;f.loc[~same_day,['spread','vol_b_l1','vol_s_l1']]=np.nan
 q['spread_missing_bars']=int(f.spread.isna().sum());q['spread_quantiles_rub']=f.spread.quantile([.5,.9,.99]).to_dict()
 q['yearly']=f.groupby(f.trade_date.str[:4]).agg(bars=('i','size'),dates=('trade_date','nunique'),missing_spread=('spread',lambda x:int(x.isna().sum()))).to_dict('index')
 # Funding on actual official clearing dates, including dates with null rate.
 hist.to_csv(O/'official_history.csv',index=False)
 f.to_csv(O/'bars.csv.gz',index=False);h.to_csv(O/'h1.csv',index=False);d.to_csv(O/'d1.csv',index=False)
 begins=f.begin.to_numpy(dtype='datetime64[ns]');ends=f.ts.to_numpy(dtype='datetime64[ns]')
 hs=h.confirmed_at.to_numpy(dtype='datetime64[ns]');ds=d.confirmed_at.to_numpy(dtype='datetime64[ns]')
 events=[];skips={}
 for tf,tfdata in [('H1',h),('D1',d)]:
  for j in range(2,len(tfdata)-2):
   w=tfdata.iloc[j-2:j+3];pivot=tfdata.iloc[j];cf=tfdata.iloc[j+2];known=np.datetime64(cf.confirmed_at,'ns')
   sides=[]
   if pivot.low==w.low.min() and (w.low==pivot.low).sum()==1:sides.append(1)
   if pivot.high==w.high.max() and (w.high==pivot.high).sum()==1:sides.append(-1)
   for side in sides:
    ei=int(np.searchsorted(begins,known,side='right'))
    if ei>=len(f):continue
    entry=f.iloc[ei];level=float(pivot.low if side==1 else pivot.high);dist=side*(entry.open-level)
    hi=int(np.searchsorted(hs,known,side='right'))-1;di=int(np.searchsorted(ds,known,side='right'))-1
    if dist<=0 or hi<0 or di<0:skips['wrong_side_or_no_context']=skips.get('wrong_side_or_no_context',0)+1;continue
    ah=float(h.iloc[hi].atr);ad=float(d.iloc[di].atr)
    if not np.isfinite(ah+ad) or min(ah,ad)<=0:skips['atr_warmup']=skips.get('atr_warmup',0)+1;continue
    # A complete next-day D1 is not known until its midnight, enforced by ds lookup.
    lag=d.iloc[di];vol='high' if lag.atr/lag.close>lag.vol_ref else 'low';trend='up' if lag.close>lag.sma50 else 'down'
    if not np.isfinite(lag.vol_ref):vol='unavailable'
    if not np.isfinite(lag.sma50):trend='unavailable'
    target=entry.open+side*2*dist;target=(np.ceil(target*100-1e-8) if side==1 else np.floor(target*100+1e-8))/100
    for horizon,delta in [('intraday',pd.Timedelta(hours=6)),('week',pd.Timedelta(days=7)),('two_weeks',pd.Timedelta(days=14)),('four_weeks',pd.Timedelta(days=28))]:
     if horizon=='intraday' and entry.begin.hour>=12:continue
     deadline=entry.begin+delta;xi=int(np.searchsorted(begins,np.datetime64(deadline,'ns'),side='left'))
     if xi>=len(f) or f.iloc[xi].begin-deadline>pd.Timedelta(days=4):continue
     if horizon=='intraday' and f.iloc[xi].begin.date()!=entry.begin.date():continue
     events.append(dict(event_id=len(events),tf=tf,horizon=horizon,side=side,pivot_time=str(pivot.confirmed_at),confirmed_at=str(cf.confirmed_at),entry_time=str(entry.begin),expiry_time=str(f.iloc[xi].begin),deadline=str(deadline),ei=ei,xi=xi,entry=float(entry.open),level=level,target=float(target),atr_h1=ah,atr_d1=ad,spread=float(entry.spread),volatility=vol,trend=trend,session='before10' if entry.begin.hour<10 else '10to19' if entry.begin.hour<19 else 'after19',structure_regime='pre_20240613' if entry.begin<pd.Timestamp('2024-06-13') else 'post_20240613'))
 e=pd.DataFrame(events);e.to_csv(O/'events.csv',index=False);q['events']=e.groupby(['tf','horizon']).size().to_dict();q['events']={str(k):v for k,v in q['events'].items()};q['excluded_events']=skips
 dump('quality.json',q)
 dump('input_hashes.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [I/'accepted_bars.csv',I/'tradestats.csv',I/'obstats.csv',I/'history_all.json',P/'protocol.json']})
 print(json.dumps(q,ensure_ascii=False,default=str,indent=2))
if __name__=='__main__':main()
