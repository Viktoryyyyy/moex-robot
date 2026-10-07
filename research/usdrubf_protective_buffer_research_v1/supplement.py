"""Fair common-support comparisons and dependence/cost diagnostics. No refitting."""
import pandas as pd,numpy as np,json
from pathlib import Path
from analyze import bootstrap_mean
P=Path(__file__).resolve().parent;O=P/'results';x=pd.read_pickle(O/'oos_results.pkl')
rows=[];dependence=[];sub=[];costs=[]
for (tf,hor),f in x.groupby(['tf','horizon']):
 original=f[~f.method.str.startswith('selected_')]
 counts=original.groupby('event_id').method.nunique();ids=counts[counts.eq(counts.max())].index
 ref=f[f.method.eq('fixed_0.05')][['event_id','netR_base','entry_time']]
 for method,g in f.groupby('method'):
  z=g[g.event_id.isin(ids)]
  pair=z.merge(ref[['event_id','netR_base']],on='event_id',suffixes=('','_ref'));pair['delta']=pair.netR_base-pair.netR_base_ref
  lo,hi,nblock=bootstrap_mean(pair,'delta')
  rows.append(dict(tf=tf,horizon=hor,method=method,n=len(z),mean_RUB=z.netR_base.mean(),reference_mean=pair.netR_base_ref.mean(),delta_RUB=pair.delta.mean(),ci_low=lo,ci_high=hi))
  pair=g.merge(ref[['event_id','netR_base']],on='event_id',suffixes=('','_ref'));pair['delta']=pair.netR_base-pair.netR_base_ref
  # Two-month cluster sensitivity. It tests inference only, not candidate selection.
  month=pair.entry_time.dt.year*12+pair.entry_time.dt.month-1
  a=pair.assign(block=month//2).groupby('block').delta.agg(['sum','count'])
  rng=np.random.default_rng(572);sample=rng.integers(0,len(a),size=(2000,len(a)));means=a['sum'].to_numpy()[sample].sum(1)/a['count'].to_numpy()[sample].sum(1)
  dependence.append(dict(tf=tf,horizon=hor,method=method,n=len(pair),blocks=len(a),delta_RUB=pair.delta.mean(),two_month_ci_low=np.quantile(means,.025),two_month_ci_high=np.quantile(means,.975)))
  for dim in ['side','volatility','session','trend','structure_regime']:
   for val,z in pair.groupby(dim):
    lo,hi,nb=bootstrap_mean(z,'delta');sub.append(dict(tf=tf,horizon=hor,method=method,dimension=dim,value=val,n=len(z),delta_RUB=z.delta.mean(),ci_low=lo,ci_high=hi,month_blocks=nb))
  for scenario in ['low','base','stress','adverse_funding']:
   costs.append(dict(tf=tf,horizon=hor,method=method,scenario=scenario,n=len(g),mean_RUB=g['netR_'+scenario].mean(),one_contract_RUB=g['net1_'+scenario].mean()))
pd.DataFrame(rows).to_csv(O/'common_support.csv',index=False)
pd.DataFrame(dependence).to_csv(O/'two_month_uncertainty.csv',index=False)
pd.DataFrame(sub).to_csv(O/'subgroup_uncertainty.csv',index=False)
pd.DataFrame(costs).to_csv(O/'cost_sensitivity.csv',index=False)
# Per-event baseline ratios, to distinguish buffer from complete risk distance.
base=x[x.method.eq('fixed_0.05')].copy();base['buffer_ATRH']=.05/base.atr_h1;base['buffer_ATRD']=.05/base.atr_d1;base['buffer_spread']=.05/base.spread
ratios=base.groupby(['tf','horizon'])[['buffer_ATRH','buffer_ATRD','buffer_spread','duration_days']].quantile([.1,.5,.9]);ratios.to_csv(O/'baseline_scale.csv')
print('COMMON SUPPORT');print(pd.DataFrame(rows).query("method=='combo_0.5_2'").round(2).to_string(index=False))
print('FINAL COSTS');print(x[x.fold.eq(4)&x.method.eq('fixed_0.05')].groupby(['tf','horizon'])[['netR_base','net1_base']].mean().round(2).to_string())
