"""Causal breakout/pullback research, all source data through Oct05 only."""
from pathlib import Path
from datetime import date
import json,math,hashlib,collections
import numpy as np
from confirmed_levels import confirmed_levels
import argparse
PACKAGE=Path(__file__).resolve().parent
parser=argparse.ArgumentParser(description='Reproduce the frozen USDRUBF breakout study from externally held, hash-verified inputs.')
parser.add_argument('--input-dir',required=True,type=Path)
parser.add_argument('--output-dir',required=True,type=Path)
args=parser.parse_args()
INPUT=args.input_dir.resolve();ROOT=args.output_dir.resolve()
if INPUT==ROOT:raise ValueError('Output directory must differ from the source input directory')
ROOT.mkdir(parents=True,exist_ok=True)
protocol=json.loads((PACKAGE/'protocol.json').read_text(encoding='utf-8'))
manifest=json.loads((PACKAGE/'input_manifest.json').read_text(encoding='utf-8'))
for filename,info in manifest['inputs'].items():
    if hashlib.sha256((INPUT/filename).read_bytes()).hexdigest()!=info['sha256']:
        raise ValueError('Input hash mismatch: '+filename)
verification={'sha256':manifest['source_workbook_sha256']}
d=json.loads((INPUT/'update_oct06_rows.json').read_text(encoding='utf-8'))
rows=[dict(zip(d['headers'],r)) for r in d['rows'] if r[d['headers'].index('USDRUBF_close')] is not None]
dates=[r['trade_date'] for r in rows];n=len(rows);ix={day:i for i,day in enumerate(dates)}
assert dates[-1]==protocol['asof']
def array(field):return np.array([r['USDRUBF_'+field] for r in rows],dtype=float)
O,H,L,C,V=map(array,['open','high','low','close','volume'])
TR=np.maximum(H-L,np.maximum(abs(H-np.r_[C[0],C[:-1]]),abs(L-np.r_[C[0],C[:-1]])))
ATR=np.full(n,np.nan)
for t in range(19,n):ATR[t]=np.mean(TR[t-19:t+1])
raw=json.loads((INPUT/'pullback_bars.json').read_text(encoding='utf-8'))
bars=np.array([[ix[r[0]],int(r[1][:2])*60+int(r[1][3:5]),*r[2:]] for r in raw['data']],dtype=float)
starts=np.searchsorted(bars[:,0],np.arange(n),'left');ends=np.searchsorted(bars[:,0],np.arange(n),'right')
for i in range(n):
    z=bars[starts[i]:ends[i]]
    assert len(z) and np.allclose([z[0,2],z[:,3].max(),z[:,4].min(),z[-1,5],z[:,6].sum()],[O[i],H[i],L[i],C[i],V[i]],rtol=1e-12,atol=1e-7),dates[i]
funding=json.loads((INPUT/'pullback_funding.json').read_text(encoding='utf-8-sig'))['records']
funding_by_day={r['tradedate']:r['swaprate'] for r in funding}
fund_arr=np.array([np.nan if funding_by_day.get(day,0.) is None else funding_by_day.get(day,0.) for day in dates])
# Published dated funding amounts are booked at 18:50 in the base estimate.
# Entry/exit-day uncertainty is carried separately as bounds.
def funding_cost(start_bar,end_bar,sign):
    a,b=bars[start_bar],bars[end_bar];first,last=int(a[0]),int(b[0]);core=0.;edges=[]
    if not np.isfinite(fund_arr[first:last+1]).all():return None,None
    if first==last:
        val=fund_arr[first];estimate=val if a[1]<1130<=b[1] else 0.
        edges=[val] if a[1]<1170 and b[1]>1080 else []
    else:
        core=float(fund_arr[first+1:last].sum());estimate=core
        if a[1]<1130:estimate+=fund_arr[first]
        if b[1]>=1130:estimate+=fund_arr[last]
        edges=[fund_arr[first],fund_arr[last]]
    lo=core+sum(min(0,x) for x in edges);hi=core+sum(max(0,x) for x in edges)
    oriented=sorted([sign*lo,sign*hi])
    return sign*estimate,oriented

events=[]
for t in range(21,n):
    state={k:confirmed_levels(x[:t],k) for k,x in [('H',H),('L',L)]}
    for sign,kind,opp,x in [(1,'H','L',H),(-1,'L','H',L)]:
        candidates=[]
        for level in [1,2,3]:
            points=state[kind][level]['points']
            if not len(points):continue
            j=int(points[-1]);price=float(x[j])
            if sign*(C[t]-price)>1e-9 and np.all(sign*(C[j+1:t]-price)<=1e-9):candidates.append((level,j,price))
        if not candidates or not len(state[opp][1]['points']):continue
        level,j,price=max(candidates)
        base_i=int(state[opp][1]['points'][-1]);base=float(L[base_i] if sign==1 else H[base_i])
        peak=float(H[t] if sign==1 else L[t]);stop=sign*base-.01
        if sign*C[t]-stop<.25*ATR[t]:continue
        confirmed4=[(int(i),k) for k in ['H','L'] for i in state[k][4]['points']]
        causal4=('up' if max(confirmed4)[1]=='L' else 'down') if confirmed4 else None
        location=(C[t]-L[t])/(H[t]-L[t]) if sign==1 else (H[t]-C[t])/(H[t]-L[t])
        events.append({'id':len(events),'t':t,'date':dates[t],'sign':sign,'level':level,'crossed_levels':[p[0] for p in candidates],
           'pivot_date':dates[j],'level_price':price,'base_date':dates[base_i],'base':base,'peak':peak,'close':float(C[t]),'atr':float(ATR[t]),
           'close_extension_atr':float(sign*(C[t]-price)/ATR[t]),'strong_candle':bool(location>=.75 and H[t]-L[t]>=ATR[t]),
           'retro3':rows[t]['USDRUBF_trend_3'],'retro4':rows[t]['USDRUBF_trend_4'],'causal4_proxy':causal4})
assert any(e['date']=='2026-10-05' and e['level_price']==85.61 and e['sign']==1 for e in events)
fixed_names=list(protocol['entry_strategies'])
trailing_names=[f'trail_{advance:.2f}_{depth:.2f}' for advance in [0.,.5,1.,1.5] for depth in [.25,.5,.75,1.]]
names=fixed_names+trailing_names;TICK=.01;WAIT=5;HORIZON=20
def entry_level(e,name):
    if name=='next_open' or name.startswith('trail_'):return None
    sign=e['sign'];peak=sign*e['peak'];base=sign*e['base']
    if name=='retest_level':val=sign*e['level_price']
    elif name.startswith('atr_'):val=peak-float(name[4:])*e['atr']
    else:val=peak-float(name[6:])*(peak-base)
    return math.floor((val+1e-9)/TICK)*TICK

def simulate(e,name,cost=.05,reward=2.):
    t=e['t'];sign=e['sign'];stop=sign*e['base']-TICK;limit=entry_level(e,name)
    trailing=name.startswith('trail_')
    if trailing:
        _,advance,depth=name.split('_');activation=sign*e['level_price']+float(advance)*e['atr'];depth=float(depth)*e['atr'];observed_peak=sign*e['peak']
    first=int(starts[t+1]);last=int(ends[t+HORIZON])-1;expires=int(ends[t+WAIT])-1
    if limit is not None and limit-stop<.25*e['atr']:return {'filled':False,'reason':'entry_too_near_invalidation','R':0.,'net_R':0.,'price_return':0.,'net_return':0.}
    filled=False;entry=None;entry_bar=None;ambiguous=0;exit_price=None;reason=None
    max_favorable=0.;max_adverse=0.
    for b in range(first,last+1):
        r=bars[b];bo=sign*r[2];bh=sign*r[3 if sign==1 else 4];bl=sign*r[4 if sign==1 else 3];bc=sign*r[5]
        new=False;known_at_open=False
        if not filled:
            if b>expires:break
            if trailing:
                if observed_peak<activation:
                    if bl<=stop:break
                    observed_peak=max(observed_peak,bh);continue
                limit=math.floor((observed_peak-depth+1e-9)/TICK)*TICK
                if limit-stop<.25*e['atr']:
                    if bl<=stop:break
                    observed_peak=max(observed_peak,bh);continue
            if name=='next_open' or bo<=limit or bl<=limit:
                # A standing limit can fill through a gap. A gap beyond the stop
                # causes immediate exit, rather than a free cancellation.
                entry=bo if name=='next_open' or bo<=limit else limit
                planned_risk=(limit if limit is not None else sign*e['close'])-stop
                risk=entry-stop
                if risk<=0:
                    return {'filled':True,'reason':'gap_through_structure','R':-cost/planned_risk,'net_R':-cost/planned_risk,
                       'price_return':-cost,'net_return':-cost,'entry':sign*entry,'exit':sign*bo,'entry_date':dates[int(r[0])],
                       'exit_date':dates[int(r[0])],'risk':planned_risk,'funding':0.,'fill_wait_sessions':int(r[0])-t,'held_sessions':0,'ambiguous_bars':0}
                if risk<.25*e['atr']:
                    # Immediate risk is small after a favorable gap: retain actual
                    # price outcome but normalize by the pre-order planned risk.
                    risk_norm=max(risk,.25*e['atr'])
                else:risk_norm=risk
                target=entry+reward*risk;filled=True;entry_bar=b;new=True;known_at_open=bo<=entry+1e-9
            else:
                if trailing:observed_peak=max(observed_peak,bh)
                continue
        max_favorable=max(max_favorable,bh-entry);max_adverse=max(max_adverse,entry-bl)
        if bo<=stop:
            exit_price=bo;reason='stop_gap';break
        if bo>=target and (not new or known_at_open):exit_price=bo;reason='target_gap';break
        hit_stop=bl<=stop;hit_target=bh>=target
        if hit_stop:
            ambiguous+=int(hit_target);exit_price=stop;reason='stop';break
        if hit_target:
            if new and not known_at_open:ambiguous+=1
            else:exit_price=target;reason='target';break
    if not filled:return {'filled':False,'reason':'no_fill','R':0.,'net_R':0.,'price_return':0.,'net_return':0.}
    if exit_price is None:b=last;exit_price=sign*bars[last,5];reason='time_exit'
    carry,carry_bounds=funding_cost(entry_bar,b,sign)
    profit=exit_price-entry-cost
    assert risk_norm>0
    return {'filled':True,'reason':reason,'R':profit/risk_norm,'net_R':(profit-carry)/risk_norm if carry is not None else None,
       'price_return':profit,'net_return':profit-carry if carry is not None else None,'return_pct':100*profit/abs(entry),
       'entry':sign*entry,'exit':sign*exit_price,'entry_date':dates[int(bars[entry_bar,0])],'exit_date':dates[int(bars[b,0])],
       'risk':risk_norm,'funding':carry,'funding_bounds':carry_bounds,'net_R_bounds':[(profit-carry_bounds[1])/risk_norm,(profit-carry_bounds[0])/risk_norm] if carry_bounds is not None else None,
       'fill_wait_sessions':int(bars[entry_bar,0])-t,'held_sessions':int(bars[b,0])-int(bars[entry_bar,0]),
       'mfe_R':max_favorable/risk_norm,'mae_R':max_adverse/risk_norm,'ambiguous_bars':ambiguous}

def correction(e):
    t=e['t'];sign=e['sign'];peak=sign*e['peak'];peak_b=int(ends[t])-1;trigger_b=None;peak_fixed=None;trough=None;trough_b=None
    stop=sign*e['base']-TICK;threshold=.5*e['atr'];start=int(starts[t+1]);end=int(ends[t+HORIZON])
    level_retest=False
    for b in range(start,end):
        r=bars[b];bh=sign*r[3 if sign==1 else 4];bl=sign*r[4 if sign==1 else 3]
        if bl<=sign*e['level_price']:level_retest=True
        if bl<=stop:status='structure_broken';break
        if trigger_b is None:
            if bl<=peak-threshold:
                trigger_b=b;peak_fixed=peak;trough=bl;trough_b=b
            elif bh>peak:peak=bh;peak_b=b
        else:
            if bh>=peak_fixed+TICK:
                # Do not count an unknown intrabar low after the recovery.
                status='resumed';break
            if bl<trough:trough=bl;trough_b=b
    else:status='unresolved' if trigger_b is not None else 'no_half_atr_pullback'
    out={'status':status,'level_retest20':level_retest}
    if trigger_b is not None:
        impulse=peak_fixed-sign*e['base'];depth=peak_fixed-trough
        out.update({'peak_price':sign*peak_fixed,'pullback_low_price':sign*trough,'extension_from_level_atr':(peak_fixed-sign*e['level_price'])/e['atr'],
          'extension_from_level_pct':100*(peak_fixed-sign*e['level_price'])/abs(e['level_price']),
          'sessions_until_first_pullback':int(bars[trigger_b,0])-t,'correction_depth_atr':depth/e['atr'],
          'correction_depth_pct':100*depth/abs(peak_fixed),'retracement_fraction':depth/impulse if impulse>0 else None,
          'correction_to_low_sessions':int(bars[trough_b,0])-int(bars[peak_b,0]),
          'correction_to_recovery_sessions':int(bars[b,0])-int(bars[peak_b,0]) if status=='resumed' else None})
    return out

mature=[e for e in events if e['t']+HORIZON<n]
last=-100
for e in mature:
    e['funding_complete']=bool(np.isfinite(fund_arr[e['t']+1:e['t']+HORIZON+1]).all())
    e['independent']=e['t']-last>=HORIZON
    if e['independent']:last=e['t']
    e['trades']={name:simulate(e,name) for name in names};e['correction']=correction(e)
def summaries(group):
    out={}
    for name in names:
        tr=[e['trades'][name] for e in group];filled=[t for t in tr if t['filled']]
        net_tr=[e['trades'][name] for e in group if e['funding_complete']];net_filled=[t for t in net_tr if t['filled']]
        out[name]={'signals':len(tr),'filled':len(filled),'fill_pct':100*len(filled)/len(tr) if tr else None,
          'mean_R_per_signal':float(np.mean([t['R'] for t in tr])) if tr else None,
          'funding_covered_signals':len(net_tr),'mean_net_R_per_signal':float(np.mean([t['net_R'] for t in net_tr])) if net_tr else None,
          'mean_R_per_fill':float(np.mean([t['R'] for t in filled])) if filled else None,
          'mean_net_R_per_fill':float(np.mean([t['net_R'] for t in net_filled])) if net_filled else None,
          'mean_net_price_per_signal':float(np.mean([t['net_return'] for t in net_tr])) if net_tr else None,
          'profitable_net_pct':100*sum(t['net_R']>0 for t in net_filled)/len(net_filled) if net_filled else None,
          'stops':sum(t['reason'].startswith('stop') or t['reason']=='gap_through_structure' for t in filled),
          'targets':sum(t['reason'].startswith('target') for t in filled),
          'mean_fill_wait':float(np.mean([t['fill_wait_sessions'] for t in filled])) if filled else None,
          'ambiguous_bars':sum(t.get('ambiguous_bars',0) for t in filled)}
    return out
groups={'all':mature,'long':[e for e in mature if e['sign']==1],'short':[e for e in mature if e['sign']==-1],
        'independent':[e for e in mature if e['independent']],
        'train_2022_2024':[e for e in mature if e['date']<'2025-01-01'],
        'validation_2025':[e for e in mature if '2025-01-01'<=e['date']<'2026-01-01'],
        'test_2026':[e for e in mature if e['date']>='2026-01-01'],
        'long_strong':[e for e in mature if e['sign']==1 and e['strong_candle']],
        'long_level2plus':[e for e in mature if e['sign']==1 and e['level']>=2]}
for level in [1,2,3]:groups['level'+str(level)]=[e for e in mature if e['level']==level]
for tag,direction in [('up','Вверх'),('down','Вниз')]:groups['long_retro4_'+tag]=[e for e in mature if e['sign']==1 and e['retro4']==direction]
summary={key:summaries(group) for key,group in groups.items()}
train=summary['train_2022_2024']
selected=max(names,key=lambda name:train[name]['mean_net_R_per_signal'])
selected_price=max(names,key=lambda name:train[name]['mean_R_per_signal'])
selection={'funding_adjusted_training_winner':selected,'price_only_training_winner':selected_price,
           'funding_adjusted_winner_by_period':{key:max(names,key=lambda name:summary[key][name]['mean_net_R_per_signal']) for key in ['validation_2025','test_2026']}}
corrections={}
for key,group in groups.items():
    counts=dict(collections.Counter(e['correction']['status'] for e in group));successful=[e['correction'] for e in group if e['correction']['status']=='resumed']
    st={'n':len(group),'statuses':counts,'successful_corrections':len(successful),'retested_level20':sum(e['correction']['level_retest20'] for e in group)}
    for field in ['extension_from_level_atr','extension_from_level_pct','sessions_until_first_pullback','correction_depth_atr','correction_depth_pct','retracement_fraction','correction_to_low_sessions','correction_to_recovery_sessions']:
        a=[r[field] for r in successful if r.get(field) is not None]
        st[field]={'q25':float(np.quantile(a,.25)),'median':float(np.median(a)),'q75':float(np.quantile(a,.75)),'n':len(a)} if a else None
    corrections[key]=st
current=next(e for e in events if e['date']=='2026-10-05' and e['sign']==1)
current['entry_prices']={name:(None if name=='next_open' else current['sign']*entry_level(current,name)) for name in fixed_names}
current['trailing_activation_prices']={str(a):current['level_price']+a*current['atr'] for a in [0,.5,1,1.5]}
current['stop']=current['base']-.01
weekday_indices=[i for i in range(current['t']+1) if date.fromisoformat(dates[i]).weekday()<5][-20:]
current['atr20_weekdays_only']=float(np.mean(TR[weekday_indices]))
out={'protocol':protocol,'source_sha256':verification['sha256'],'bar_rows':len(bars),'daily_rows':n,'all_daily_ohlcv_reconciled':True,
     'funding_rows':len(funding),'funding_assumption':'Published dated SwapRate applied at 18:50 Moscow; missing non-settlement dates not charged separately; actual settlement timing and entry/exit-day uncertainty are sensitivity limits, not broker accounting.',
     'events_total':len(events),'events_mature':len(mature),'strategy_names':names,'summary':summary,'selection':selection,'corrections':corrections,'current':current,'events':mature}
(ROOT/'breakout_pullback_results.json').write_text(json.dumps(out,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
print(json.dumps({'counts':{k:len(v) for k,v in groups.items()},'selection':selection,'current':current,
 'top_train':[{'name':name,**train[name]} for name in sorted(names,key=lambda x:train[x]['mean_net_R_per_signal'],reverse=True)[:5]],
 'selected_validation':summary['validation_2025'][selected],'selected_test':summary['test_2026'][selected]},ensure_ascii=True,indent=2))
