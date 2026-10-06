"""Cost, purged walk-forward, paired inference and drawdown calculations."""
from pathlib import Path
import json,hashlib,sys
import numpy as np,pandas as pd
from replay import Engine,C,FOLDS,P,O,I
class Funding:
 def __init__(self):
  h=pd.read_csv(O/'official_history.csv').sort_values('TRADEDATE')
  self.times=np.array([pd.Timestamp(day+(' 18:50' if day<'2026-03-23' else ' 23:50')).value for day in h.TRADEDATE],dtype=np.int64)
  self.rates=h.SWAPRATE.to_numpy(float)
 def value(self,entry,exit,price,side,shift=0,delay=0):
  rates=self.rates.copy()
  if shift==1:rates=np.r_[np.nan,rates[:-1]]
  if shift==-1:rates=np.r_[rates[1:],np.nan]
  times=self.times+delay*60*10**9
  a=np.searchsorted(times,entry,side='right');z=np.searchsorted(times,exit,side='right')
  sums=np.r_[0,np.cumsum(np.nan_to_num(rates))];missing=np.r_[0,np.cumsum(np.isnan(rates))]
  n=missing[z]-missing[a]
  return (side*(sums[z]-sums[a])+n*.0035*price)*1000,n
def assign_oos(e):
 e=e.copy();e['fold']=-1
 for fold,(start,end) in enumerate(FOLDS):
  mask=e.entry_time.ge(pd.Timestamp(start))&e.expiry_time.lt(pd.Timestamp(end)+pd.Timedelta(days=1))
  e.loc[mask,'fold']=fold
 return e
def costs(paths,e,b,funding):
 x=paths.merge(e,on='event_id',validate='many_to_one');ei=x.ei.to_numpy(int);zi=x.exit_i.to_numpy(int)
 ens=x.entry_time.to_numpy(dtype='datetime64[ns]').astype(np.int64);exns=x.exit_ns.to_numpy(np.int64);price=x.entry.to_numpy();side=x.side.to_numpy()
 sp=b.spread.to_numpy();entrysp=sp[ei];exitsp=sp[zi];x['missing_spread']=~np.isfinite(entrysp)|~np.isfinite(exitsp)
 fc,nmissing=funding.value(ens,exns,price,side);x['funding_rub']=fc;x['missing_funding']=nmissing
 x['duration_days']=(exns-ens)/(86400*1e9)
 x['gross_1c']=side*(x.raw_exit-price)*1000
 for scenario,cfg in C['cost_scenarios'].items():
  fallback=.10 if scenario=='stress' else .05
  es=np.where(np.isfinite(entrysp),entrysp,fallback)*cfg['spread_multiplier'];xs=np.where(np.isfinite(exitsp),exitsp,fallback)*cfg['spread_multiplier']
  fee=cfg['commission_per_side_rub'];slip=.01*cfg['slippage_ticks_per_side']
  ec=1000*(es/2+slip)+fee;xc=1000*(xs/2+slip)+fee
  risk=1000*np.abs(price-x.stop.to_numpy())+2*ec
  qty=np.floor(10000/risk).astype(int)
  carry=fc+cfg.get('additional_debit_bps_per_calendar_day',0)/10000*price*1000*x.duration_days.to_numpy()
  pnl=x.gross_1c.to_numpy()-ec-xc-carry
  x['qty_'+scenario]=qty;x['net1_'+scenario]=pnl;x['netR_'+scenario]=pnl*qty;x['risk_'+scenario]=risk
  if scenario=='base':x['entry_cost']=ec;x['exit_cost']=xc
 for key,shift,delay in [('prev_rate',1,0),('next_rate',-1,0),('plus10min',0,10)]:
  cf,_=funding.value(ens,exns,price,side,shift=shift,delay=delay)
  x['netR_'+key]=(x.net1_base+fc-cf)*x.qty_base
 # Upper bound on unresolved order, holding all costs/exposure fixed; not an alternative tuned strategy.
 x['optimistic_netR']=x.netR_base+np.where(x.unresolved1 & x.reason.eq('stop'),side*(x.target-x.raw_exit)*1000*x.qty_base,0)
 x['stop_hit']=x.reason.eq('stop');x['loss_over_R']=x.netR_base.lt(-10000)
 return x
def greedy(events,offset=0):
 result=[];last=pd.Timestamp.min
 for r in events.sort_values(['entry_time','event_id']).iloc[offset:].itertuples(index=False):
  if r.entry_time>last:result.append(r.event_id);last=r.expiry_time
 return result
def dd(values):
 a=np.r_[0,np.cumsum(np.asarray(values,float))];return float(np.max(np.maximum.accumulate(a)-a))
def marked_dd(g,b,funding):
 equity=[0.];cash=0.;cash1=0.;eq1=[0.]
 for r in g.sort_values(['entry_time','event_id']).itertuples(index=False):
  # Entry fees precede every subsequent mark in chronological equity.
  equity.append(cash-r.entry_cost*r.qty_base);eq1.append(cash1-r.entry_cost)
  idx=np.arange(r.ei,r.exit_i);times=b.ts.iloc[idx].to_numpy(dtype='datetime64[ns]').astype(np.int64);take=times<r.exit_ns;idx=idx[take];times=times[take]
  if len(idx):
   f,_=funding.value(np.full(len(idx),r.entry_time.value,dtype=np.int64),times,np.full(len(idx),r.entry),np.full(len(idx),r.side))
   mark=r.side*(b.close.iloc[idx].to_numpy()-r.entry)*1000-r.entry_cost-f
   equity.extend(cash+mark*r.qty_base);eq1.extend(cash1+mark)
  cash+=r.netR_base;cash1+=r.net1_base;equity.append(cash);eq1.append(cash1)
 a=np.array(equity);a1=np.array(eq1)
 return float(np.max(np.maximum.accumulate(a)-a)),float(np.max(np.maximum.accumulate(a1)-a1))
def bootstrap_mean(g,column,reps=2000,alpha=.05):
 a=g.assign(month=g.entry_time.dt.to_period('M').astype(str)).groupby('month')[column].agg(['sum','count'])
 if len(a)<2:return np.nan,np.nan,len(a)
 rng=np.random.default_rng(572);idx=rng.integers(0,len(a),size=(reps,len(a)));vals=a['sum'].to_numpy()[idx].sum(axis=1)/a['count'].to_numpy()[idx].sum(axis=1)
 return float(np.quantile(vals,alpha/2)),float(np.quantile(vals,1-alpha/2)),len(a)
def main():
 e=pd.read_csv(O/'events.csv',parse_dates=['entry_time','expiry_time','confirmed_at']);e=assign_oos(e)
 b=pd.read_csv(O/'bars.csv.gz',parse_dates=['ts','begin']);f=Funding();paths=pd.read_pickle(O/'paths.pkl')
 allr=costs(paths,e,b,f);allr['family']=allr.method.str.split('_').str[0]
 # Only outcomes completely inside each test; dynamic quantiles trained on strictly previous outcomes.
 out=allr[allr.fold.ge(0)&(allr.model_fold.eq(-1)|allr.model_fold.eq(allr.fold))].copy()
 assert not out.duplicated(['event_id','method']).any()
 out['phase']=np.where(out.fold==4,'final_2026','walk_forward_2024_2025')
 schedules={}
 for (tf,horizon),g in e[e.fold.ge(0)].groupby(['tf','horizon']):schedules[(tf,horizon)]=set(greedy(g))
 out['nonoverlap']=[eid in schedules[(tf,hor)] for eid,tf,hor in zip(out.event_id,out.tf,out.horizon)]
 # Family selection at each fold; common support across all candidate methods.
 picks=[];selected=[]
 for fold,(start,end) in enumerate(FOLDS):
  train=allr[allr.expiry_time.lt(pd.Timestamp(start))&(allr.model_fold.eq(-1)|allr.model_fold.eq(fold))]
  test=out[out.fold.eq(fold)]
  for (tf,hor),g in train.groupby(['tf','horizon']):
   common=g.groupby('event_id').method.nunique();max_methods=common.max();ids=common[common==max_methods].index
   g=g[g.event_id.isin(ids)]
   for fam,z in g.groupby('family'):
    ranks=z.groupby('method').agg(mean=('netR_base','mean'),buffer=('buffer','median')).reset_index().sort_values(['mean','buffer'],ascending=[False,True])
    name=ranks.iloc[0]['method'];picks.append(dict(fold=fold,tf=tf,horizon=hor,family=fam,method=name,train_n=z[z.method.eq(name)].shape[0],train_mean=float(ranks.iloc[0]['mean']),train_last_expiry=str(g.expiry_time.max())))
    t=test[test.tf.eq(tf)&test.horizon.eq(hor)&test.method.eq(name)].copy();t['method']='selected_'+fam;t['selected_parameter']=name;selected.append(t)
 pd.DataFrame(picks).to_csv(O/'walk_forward_choices.csv',index=False)
 sel=pd.concat(selected,ignore_index=True);out=pd.concat([out,sel],ignore_index=True)
 out.to_csv(O/'oos_event_results.csv.gz',index=False,float_format='%.9g')
 out.to_pickle(O/'oos_results.pkl')
 rows=[];paired=[];nonoverlap=[];subgroups=[]
 for phase,frame in [('all_oos',out),('walk_forward_2024_2025',out[out.fold.lt(4)]),('final_2026',out[out.fold.eq(4)])]:
  for (tf,hor,method),g in frame.groupby(['tf','horizon','method']):
   base=frame[frame.tf.eq(tf)&frame.horizon.eq(hor)&frame.method.eq('fixed_0.05')][['event_id','netR_base','net1_base','stop_hit','reason']]
   pair=g.merge(base,on='event_id',suffixes=('','_reference'),validate='one_to_one');pair['delta']=pair.netR_base-pair.netR_base_reference
   lo,hi,months=bootstrap_mean(g,'netR_base');dlo,dhi,_=bootstrap_mean(pair,'delta');blo,bhi,_=bootstrap_mean(pair,'delta',alpha=.05/24)
   common=dict(phase=phase,tf=tf,horizon=hor,method=method)
   for scenario in C['cost_scenarios']:
    pnl=g['netR_'+scenario];p1=g['net1_'+scenario];loss=pnl[pnl<0]
    rows.append({**common,'scenario':scenario,'n':len(g),'mean_RUB':pnl.mean(),'mean_R':pnl.mean()/10000,'median_RUB':pnl.median(),'one_contract_RUB':p1.mean(),'stop_pct':100*g.stop_hit.mean(),'false_stop_pct':100*g.false_stop.mean(),'false_given_stop_pct':100*g.false_stop.sum()/max(1,g.stop_hit.sum()),'loss_median_RUB':loss.median() if len(loss) else 0,'p01_RUB':pnl.quantile(.01),'worst_RUB':pnl.min(),'exceed_nominal_risk_pct':100*(pnl<-10000).mean(),'qty_median':g['qty_'+scenario].median(),'zero_qty':int(g['qty_'+scenario].eq(0).sum()),'buffer_median':g.buffer.median(),'ambiguous5':int(g.ambiguous5.sum()),'unresolved1':int(g.unresolved1.sum()),'missing_spread_events':int(g.missing_spread.sum()),'missing_funding_events':int(g.missing_funding.gt(0).sum()),'ci_low_base':lo,'ci_high_base':hi,'month_blocks':months,'optimistic_mean_base':g.optimistic_netR.mean()})
   paired.append({**common,'n':len(pair),'delta_RUB':pair.delta.mean(),'delta_1c_RUB':(pair.net1_base-pair.net1_base_reference).mean(),'ci_low':dlo,'ci_high':dhi,'bonferroni_low_approx':blo,'bonferroni_high_approx':bhi,'months':months})
   ng=g[g.nonoverlap].sort_values(['entry_time','event_id']);mdd,mdd1=marked_dd(ng,b,f)
   nonoverlap.append({**common,'n':len(ng),'mean_RUB':ng.netR_base.mean(),'one_contract_RUB':ng.net1_base.mean(),'total_RUB':ng.netR_base.sum(),'realized_max_drawdown_RUB':dd(ng.netR_base),'marked_5m_max_drawdown_RUB':mdd,'marked_5m_drawdown_1c_RUB':mdd1,'stop_pct':100*ng.stop_hit.mean()})
   if phase=='all_oos':
    for dimension in ['side','volatility','session','trend','structure_regime','fold']:
     for value,z in pair.groupby(dimension):
      subgroups.append({**common,'dimension':dimension,'value':value,'n':len(z),'mean_RUB':z.netR_base.mean(),'delta_RUB':z.delta.mean(),'one_contract_RUB':z.net1_base.mean(),'stop_pct':100*z.stop_hit.mean()})
 pd.DataFrame(rows).to_csv(O/'summary.csv',index=False);pd.DataFrame(paired).to_csv(O/'paired_vs_5kopecks.csv',index=False);pd.DataFrame(nonoverlap).to_csv(O/'nonoverlap_drawdown.csv',index=False);pd.DataFrame(subgroups).to_csv(O/'subgroups.csv',index=False)
 continuation=[]
 for (tf,hor),g in out.groupby(['tf','horizon']):
  base=g[g.method.eq('fixed_0.05')].copy();end_ret=base.side.to_numpy()*(b.open.to_numpy()[base.xi.to_numpy(int)]-base.entry.to_numpy())
  # Target never touched: independent of widened-stop candidate.
  target_never=[]
  for r in base.itertuples(index=False):
   path=b.iloc[r.ei:r.xi]
   touched=(path.high.max()>=r.target-1e-8 if r.side==1 else path.low.min()<=r.target+1e-8) or r.side*(b.open.iloc[r.xi]-r.target)>=-1e-8
   target_never.append(not touched)
  base=base[base.stop_hit&np.array(target_never)&(end_ret<0)]
  for method,z in g.groupby('method'):
   p=z.merge(base[['event_id','netR_base','net1_base']],on='event_id',suffixes=('','_ref'))
   p=p[p.buffer>.05+1e-8]
   continuation.append(dict(tf=tf,horizon=hor,method=method,n=len(p),additional_loss_1c_RUB=(p.net1_base_ref-p.net1_base).mean(),additional_loss_equal_risk_RUB=(p.netR_base_ref-p.netR_base).mean()))
 pd.DataFrame(continuation).to_csv(O/'continuation_cost.csv',index=False)
 # Cohort-start robustness: alternate nonoverlap schedules independent of exits and buffer.
 offsets=[]
 for (tf,hor),ev in e[e.fold.ge(0)].groupby(['tf','horizon']):
  z=out[out.tf.eq(tf)&out.horizon.eq(hor)]
  for off in [0,1,2,3,4,5,10]:
   ids=set(greedy(ev,off))
   for method,g in z[z.event_id.isin(ids)].groupby('method'):
    offsets.append(dict(tf=tf,horizon=hor,offset=off,method=method,n=len(g),mean_RUB=g.netR_base.mean(),total_RUB=g.netR_base.sum(),realized_mdd_RUB=dd(g.sort_values('entry_time').netR_base)))
 pd.DataFrame(offsets).to_csv(O/'nonoverlap_offsets.csv',index=False)
 timing=out.groupby(['tf','horizon','method'])[['netR_base','netR_prev_rate','netR_next_rate','netR_plus10min']].mean().reset_index();timing.to_csv(O/'funding_timing_sensitivity.csv',index=False)
 gates={'project':'MOEX_Bot','oos_event_method_rows':len(out),'event_counts':e.groupby(['tf','horizon','fold']).size().rename('n').reset_index().to_dict('records'),'no_duplicate_event_method':not out.duplicated(['event_id','method']).any(),'confirmation_before_entry':bool((e.confirmed_at<e.entry_time).all()),'test_labels_contained':True,'training_labels_purged':all(pd.Timestamp(p['train_last_expiry'])<pd.Timestamp(FOLDS[p['fold']][0]) for p in picks),'integer_qty':bool((out.qty_base>=0).all()),'nominal_risk_within_10000':bool((out.qty_base*out.risk_base<=10000+1e-6).all()),'current_stop_or_strategy_changed':False,'history_pit_vintage_proven':False}
 (O/'validation.json').write_text(json.dumps(gates,indent=2,default=str),encoding='utf-8')
 print('DONE',len(out),'OOS rows',flush=True)
 print(pd.DataFrame(rows).query("phase=='all_oos' and scenario=='base' and method=='fixed_0.05'")[['tf','horizon','n','mean_RUB','one_contract_RUB','stop_pct','false_stop_pct','ci_low_base','ci_high_base']].to_string(index=False))
if __name__=='__main__':main()
