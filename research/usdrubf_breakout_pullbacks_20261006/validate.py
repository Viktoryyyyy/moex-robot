"""Independent invariants and bounded sensitivity checks, no strategy retuning."""
from pathlib import Path
from datetime import date
import contextlib,io,runpy,json,math
import numpy as np
def json_native(value):
    if isinstance(value,np.generic):return value.item()
    raise TypeError(f'Unsupported JSON value: {type(value).__name__}')
ROOT=Path(__file__).resolve().parent
with contextlib.redirect_stdout(io.StringIO()):s=runpy.run_path(str(ROOT/'study.py'))
events=s['mature'];sim=s['simulate'];G=sim.__globals__;bars=s['bars'];dates=s['dates']
for e in events:
    t=e['t'];sign=e['sign'];p=s['ix'][e['pivot_date']]
    prev=s['confirmed_levels']((s['H'] if sign==1 else s['L'])[:t],'H' if sign==1 else 'L')
    assert p in prev[e['level']]['points']
    assert np.all(sign*(s['C'][p+1:t]-e['level_price'])<=1e-9)
    assert sign*(s['C'][t]-e['level_price'])>0
    for name,r in e['trades'].items():
        if not r['filled']:
            assert r['R']==r['price_return']==0
            continue
        ei=s['ix'][r['entry_date']];xi=s['ix'][r['exit_date']]
        assert t<ei<=t+5 and ei<=xi<=t+20
        assert math.isclose(r['price_return'],sign*(r['exit']-r['entry'])-.05,abs_tol=1e-8)
        if r['funding'] is not None:assert math.isclose(r['net_return'],r['price_return']-r['funding'],abs_tol=1e-8)
ind=[e for e in events if e['independent']]
assert all(b['t']-a['t']>=20 for a,b in zip(ind,ind[1:]))

names=['next_open','retest_level','atr_0.25','atr_0.50','atr_1.00','swing_0.50','trail_0.50_0.25']
def summarize(tr):
    fill=[r for r in tr if r['filled']]
    return {'n':len(tr),'fill_pct':100*len(fill)/len(tr),'mean_R':float(np.mean([r['R'] for r in tr]))}
sensitivity={}
for tag,wait,horizon,reward in [('wait3',3,20,2),('wait10',10,20,2),('horizon10',5,10,2),('target1R',5,20,1),('target3R',5,20,3)]:
    G['WAIT']=wait;G['HORIZON']=horizon
    sensitivity[tag]={name:summarize([sim(e,name,reward=reward) for e in events]) for name in names}
G['WAIT']=5;G['HORIZON']=20
weekdays=[]
weekday_atr={}
for t,day in enumerate(dates):
    if date.fromisoformat(day).weekday()<5:weekdays.append(t)
    if len(weekdays)>=20:weekday_atr[t]=float(np.mean(s['TR'][weekdays[-20:]]))
modified=[{**e,'atr':weekday_atr[e['t']]} for e in events]
sensitivity['weekday_ATR']={name:summarize([sim(e,name) for e in modified]) for name in names}
for cost in [.02,.10]:
    sensitivity['cost_'+str(cost)]={}
    for name in names:
        vals=[r['R']-(cost-.05)/r['risk'] if r['filled'] else 0 for r in [e['trades'][name] for e in events]]
        sensitivity['cost_'+str(cost)][name]={'n':len(vals),'mean_R':float(np.mean(vals))}

rng=np.random.default_rng(20261006);boot={}
for name in ['atr_0.25','atr_0.50','atr_1.00','swing_0.50','trail_0.50_0.25']:
    delta=np.array([e['trades'][name]['R']-e['trades']['next_open']['R'] for e in ind])
    means=delta[rng.integers(0,len(delta),size=(10000,len(delta)))].mean(axis=1)
    boot[name]={'n':len(delta),'mean_difference_R':float(delta.mean()),'bootstrap95':list(map(float,np.quantile(means,[.025,.975])))}
missed={name:{'unfilled':sum(not e['trades'][name]['filled'] for e in events),
             'missed_profitable_next_open':sum(not e['trades'][name]['filled'] and e['trades']['next_open']['R']>0 for e in events),
             'avoided_losing_next_open':sum(not e['trades'][name]['filled'] and e['trades']['next_open']['R']<=0 for e in events)} for name in names}

# Report a fixed, relevant subgroup without claiming a separate fitted model.
subgroups={
 'long_H2_exact':[e for e in events if e['sign']==1 and e['level']==2],
 'long_H2plus_strong':[e for e in events if e['sign']==1 and e['level']>=2 and e['strong_candle']],
 'long_later_2025_2026':[e for e in events if e['sign']==1 and e['date']>='2025-01-01']}
substats={key:{name:summarize([e['trades'][name] for e in group]) for name in names} for key,group in subgroups.items()}
out={'verified_events':len(events),'verified_simulations':sum(len(e['trades']) for e in events),'overlap_free_count':len(ind),
     'causal_pivot_and_execution_checks_passed':True,'paired_bootstrap_vs_next_open':boot,'sensitivity':sensitivity,'missed_opportunities':missed,'subgroups':substats,
     'bootstrap_limit':'Nonoverlapping 20-session outcome windows, but long market regimes may correlate. Not a calibrated future probability.'}
(s['ROOT']/'pullback_robustness.json').write_text(json.dumps(out,ensure_ascii=False,indent=2,default=json_native),encoding='utf-8')
print(json.dumps({'checks':{k:out[k] for k in ['verified_events','verified_simulations','overlap_free_count','causal_pivot_and_execution_checks_passed']},
  'paired_bootstrap':boot,'sensitivity_winners':{key:max(values,key=lambda name:values[name]['mean_R']) for key,values in sensitivity.items()},'subgroups':substats},indent=2,default=json_native))
