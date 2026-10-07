from pathlib import Path
import pandas as pd,numpy as np,json,math
from statistics import NormalDist
P=Path(__file__).resolve().parent;O=P/'results';x=pd.read_pickle(O/'oos_results.pkl');b=pd.read_csv(O/'bars.csv.gz')
base=x[x.method.eq('fixed_0.05')].copy();caps=[];power=[]
for (tf,hor),g in base.groupby(['tf','horizon']):
 dep=np.where(g.side.to_numpy()==1,b.vol_s_l1.to_numpy()[g.ei],b.vol_b_l1.to_numpy()[g.ei])
 known=np.isfinite(dep);caps.append(dict(tf=tf,horizon=hor,n=len(g),depth_known=int(known.sum()),qty_exceeds_lagged_L1_pct=100*np.mean(g.qty_base.to_numpy()[known]>dep[known]),qty_p95=g.qty_base.quantile(.95),max_qty=g.qty_base.max(),median_entry_risk_distance=float((g.entry-g.stop).abs().median())))
 for name in ['fixed_0.1','fixed_0.2','combo_0.5_2']:
  a=x[x.tf.eq(tf)&x.horizon.eq(hor)&x.method.eq(name)&x.nonoverlap]
  paired=a.merge(g[['event_id','netR_base']],on='event_id',suffixes=('','_ref'));delta=paired.netR_base-paired.netR_base_ref;sd=delta.std()
  n=math.ceil(((NormalDist().inv_cdf(1-.05/2)+NormalDist().inv_cdf(.8))*sd/500)**2) if sd>0 else 0
  nadj=math.ceil(((NormalDist().inv_cdf(1-.05/(2*24))+NormalDist().inv_cdf(.8))*sd/500)**2) if sd>0 else 0
  power.append(dict(tf=tf,horizon=hor,method=name,current_nonoverlap_n=len(paired),paired_sd_RUB=sd,effect_to_detect_RUB=500,approx_n_80pct_power=n,approx_n_24_tests=nadj,assumption='normal approximation, independent paired events, 5% two-sided alpha; planning only'))
pd.DataFrame(caps).to_csv(O/'capacity_diagnostic.csv',index=False);pd.DataFrame(power).to_csv(O/'additional_sample_planning.csv',index=False)
for filename in ['two_month_uncertainty.csv','cost_sensitivity.csv','nonoverlap_offsets.csv','subgroups.csv']:
 f=pd.read_csv(O/filename);f=f[f.horizon.isin(['two_weeks','four_weeks'])&f.method.isin(['fixed_0.05','fixed_0.1','combo_0.5_2'])]
 if filename=='subgroups.csv':f=f[f.dimension.isin(['side','volatility','session'])]
 print(filename);print(f.round(1).to_string(index=False))
print('POWER');print(pd.DataFrame(power).query("horizon in ['two_weeks','four_weeks'] and method=='fixed_0.1'").round(0).to_string(index=False))
