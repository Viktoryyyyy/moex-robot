"""Deterministic event-study replay, isolated from production. See frozen protocol.
No account data, network writes, strategy/config changes or order actions.
"""
from pathlib import Path
import json,sys,hashlib
import pandas as pd,numpy as np
P=Path(__file__).resolve().parent;O=P/'results';I=P/'inputs'
C=json.loads((P/'protocol.json').read_bytes())
FOLDS=C['split']['walk_forward_tests']+[C['split']['final_test']]
def rounded_stop(level,side,buffer):
 return (np.floor((level-buffer)*100+1e-8) if side==1 else np.ceil((level+buffer)*100-1e-8))/100
def first(mask):
 idx=np.flatnonzero(mask);return int(idx[0]) if len(idx) else -1
def candidate_specs():
 specs=[]
 for b in C['fixed_buffers_rub']:specs.append((f'fixed_{b:g}','fixed',b))
 for b in C['price_fractions']:specs.append((f'price_{b:g}','price',b))
 for b in C['atr_h1_multipliers']:specs.append((f'atrH_{b:g}','atrH',b))
 for b in C['atr_d1_multipliers']:specs.append((f'atrD_{b:g}','atrD',b))
 for b in C['combined']:specs.append((f"combo_{b['atr_h1']:g}_{b['spread']:g}",'combo',b))
 return specs
class Engine:
 def __init__(self):
  self.b=pd.read_csv(O/'bars.csv.gz',parse_dates=['ts','begin']);self.e=pd.read_csv(O/'events.csv',parse_dates=['entry_time','expiry_time','confirmed_at'])
  for c in ['open','high','low','close','spread']:setattr(self,c,self.b[c].to_numpy())
  self.begin=self.b.begin.to_numpy(dtype='datetime64[ns]').astype('int64');self.end=self.b.ts.to_numpy(dtype='datetime64[ns]').astype('int64')
  self.minutes={};mp=I/'minute_bars.json'
  if mp.exists():self.minutes=json.loads(mp.read_bytes())
  self.ambiguous=set();self.minute_conflicts=set()
  self.penetrations={};self.event_targets={}
  for r in self.e.itertuples(index=False):
   ei,xi,s=r.ei,r.xi,r.side
   touch=first(self.high[ei:xi]>=r.target-1e-8) if s==1 else first(self.low[ei:xi]<=r.target+1e-8)
   ti=ei+touch if touch>=0 else -1
   # Include expiry-open touch, never expiry bar's later extremes.
   if ti<0 and s*(self.open[xi]-r.target)>=-1e-8:ti=xi
   until=ti+1 if ti>=0 and ti<xi else xi
   adverse=float(np.min(self.low[ei:until]) if s==1 else np.max(self.high[ei:until])) if until>ei else float(self.open[ei])
   if ti<0:adverse=min(adverse,self.open[xi]) if s==1 else max(adverse,self.open[xi])
   scale=r.atr_h1 if r.tf=='H1' else r.atr_d1
   self.penetrations[r.event_id]=max(0,s*(r.level-adverse))/scale
   self.event_targets[r.event_id]=ti
  self.e['penetration_atr']=self.e.event_id.map(self.penetrations)
  self.qmodels=[]
  for fold,(start,end) in enumerate(FOLDS):
   train=self.e[self.e.expiry_time<pd.Timestamp(start)]
   for (tf,horizon),g in train.groupby(['tf','horizon']):
    for q in C['penetration_quantiles']:
     self.qmodels.append(dict(model_fold=fold,tf=tf,horizon=horizon,method=f'q_{q:g}',value=float(g.penetration_atr.quantile(q)),train_n=len(g),train_last_expiry=str(g.expiry_time.max())))
  self.qmap={(r['model_fold'],r['tf'],r['horizon'],r['method']):r for r in self.qmodels}
 def buffers(self,r,quantiles=True):
  arr=[]
  for name,typ,b in candidate_specs():
   if typ=='fixed':val=b
   elif typ=='price':val=b*r.entry
   elif typ=='atrH':val=b*r.atr_h1
   elif typ=='atrD':val=b*r.atr_d1
   else:
    if not np.isfinite(r.spread):continue
    val=max(.01,b['atr_h1']*r.atr_h1,b['spread']*r.spread)
   arr.append((name,-1,val))
  if quantiles:
   for fold,(start,end) in enumerate(FOLDS):
    if r.expiry_time>=pd.Timestamp(end)+pd.Timedelta(days=1):continue
    for q in C['penetration_quantiles']:
     name=f'q_{q:g}';m=self.qmap.get((fold,r.tf,r.horizon,name))
     if m and m['train_n']>=30:arr.append((name,fold,m['value']*(r.atr_h1 if r.tf=='H1' else r.atr_d1)))
  return arr
 def minute_resolve(self,bi,side,stop,target):
  key=str(self.b.iloc[bi].ts);rows=self.minutes.get(key)
  if not rows:return None
  # Never mix conflicting revisions/sessions from two source products.
  highs=[x['high'] for x in rows];lows=[x['low'] for x in rows]
  if abs(max(highs)-self.high[bi])>.011 or abs(min(lows)-self.low[bi])>.011 or abs(rows[0]['open']-self.open[bi])>.011 or abs(rows[-1]['close']-self.close[bi])>.011:
   self.minute_conflicts.add(key);return None
  for x in rows:
   op=x['open'];tm=pd.Timestamp(x['begin']).value
   if side*(op-stop)<=1e-8:return ('stop',op,tm,False)
   if side*(op-target)>=-1e-8:return ('target',op,tm,False)
   sl=(x['low']<=stop+1e-8) if side==1 else (x['high']>=stop-1e-8)
   tp=(x['high']>=target-1e-8) if side==1 else (x['low']<=target+1e-8)
   tm=pd.Timestamp(x['end']).value
   if sl:return ('stop',stop,tm,bool(tp))
   if tp:return ('target',target,tm,False)
  self.minute_conflicts.add(key);return None
 def event(self,r,collect_only=False):
  ei,xi,s=r.ei,r.xi,r.side;ti=self.event_targets[r.event_id];records=[]
  for method,fold,buff in self.buffers(r):
   stop=rounded_stop(r.level,s,buff);buffer=s*(r.level-stop)
   hit=first(self.low[ei:xi]<=stop+1e-8) if s==1 else first(self.high[ei:xi]>=stop-1e-8)
   si=ei+hit if hit>=0 else -1
   if si<0 and s*(self.open[xi]-stop)<=1e-8:si=xi
   bi=xi;price=self.open[xi];tm=self.begin[xi];reason='expiry';amb5=False;amb1=False;resolved=False;gap=0.
   if si>=0 and (ti<0 or si<=ti):
    bi=si;reason='stop';price=min(stop,self.open[bi]) if s==1 else max(stop,self.open[bi]);tm=self.begin[bi] if price!=stop else self.end[bi]
    if bi==xi:tm=self.begin[bi]
    if ti==si and bi<xi:
     if s*(self.open[bi]-stop)<=1e-8:price=self.open[bi];tm=self.begin[bi]
     elif s*(self.open[bi]-r.target)>=-1e-8:reason='target';price=self.open[bi];tm=self.begin[bi]
     else:
      amb5=True;self.ambiguous.add(str(self.b.iloc[bi].ts))
      if not collect_only:
       refined=self.minute_resolve(bi,s,stop,r.target)
       if refined:reason,price,tm,amb1=refined;resolved=not amb1
       else:amb1=True
    gap=max(0,s*(stop-price)) if reason=='stop' else 0.
   elif ti>=0:
    bi=ti;reason='target';price=max(r.target,self.open[bi]) if s==1 else min(r.target,self.open[bi]);tm=self.begin[bi] if price!=r.target else self.end[bi]
    if bi==xi:tm=self.begin[bi]
   if collect_only:continue
   false_stop=reason=='stop' and ti>=0 and ti>bi
   # Confirm same-5m later target only if finer minute path proves subsequent separate minute hit.
   if reason=='stop' and ti==bi and resolved:
    rows=self.minutes.get(str(self.b.iloc[bi].ts),[])
    false_stop=any(pd.Timestamp(x['begin']).value>tm and ((x['high']>=r.target-1e-8) if s==1 else (x['low']<=r.target+1e-8)) for x in rows)
   records.append((r.event_id,method,fold,buffer,stop,bi,price,tm,reason,amb5,amb1,resolved,false_stop,gap))
  return records
 def run(self,collect_only=False):
  rows=[]
  for k,r in enumerate(self.e.itertuples(index=False)):
   rows.extend(self.event(r,collect_only))
   if k%2000==0:print('REPLAY',k,'of',len(self.e),flush=True)
  (O/'ambiguous_5m_timestamps.json').write_text(json.dumps(sorted(self.ambiguous)),encoding='utf-8')
  pd.DataFrame(self.qmodels).to_csv(O/'quantile_models.csv',index=False)
  if collect_only:return
  cols=['event_id','method','model_fold','buffer','stop','exit_i','raw_exit','exit_ns','reason','ambiguous5','unresolved1','resolved1','false_stop','gap_rub_price']
  result=pd.DataFrame(rows,columns=cols);result.to_pickle(O/'paths.pkl')
  (O/'minute_conflicts.json').write_text(json.dumps(sorted(self.minute_conflicts)),encoding='utf-8')
  print('PATHS',len(result),'ambiguous_5m',len(self.ambiguous),'minute_conflicts',len(self.minute_conflicts),flush=True)
if __name__=='__main__':Engine().run('--collect' in sys.argv)
