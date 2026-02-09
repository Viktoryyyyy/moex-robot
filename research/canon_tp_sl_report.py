import pandas as pd
import numpy as np

PATH = 'data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2026-02-03.csv'
L, U = 79600, 84100
COMMISSION = 2
STOP_K = 1.2
TP_K = 2.0
TRAIN_END = pd.Timestamp('2023-12-31 23:59:59')

df = pd.read_csv(PATH, parse_dates=['end']).sort_values('end').reset_index(drop=True)
df['date'] = df['end'].dt.date
daily = df.groupby('date').agg(high=('high_fo','max'), low=('low_fo','min'), close=('close_fo','last'))
prev_close = daily['close'].shift(1)
tr = pd.concat([
    (daily['high']-daily['low']).rename('hl'),
    (daily['high']-prev_close).abs().rename('hc'),
    (daily['low']-prev_close).abs().rename('lc')
], axis=1).max(axis=1)
daily['atr14'] = tr.rolling(14).mean()
df = df.merge(daily['atr14'], left_on='date', right_index=True, how='left')
df = df.dropna(subset=['atr14']).reset_index(drop=True)

trades=[]
pos=None
n=len(df)

for i in range(n):
    r=df.iloc[i]
    if pos is None:
        if r.high_fo > U and r.close_fo < U:
            entry=float(r.close_fo); atr=float(r.atr14)
            pos={'side':'SHORT','entry_time':r.end,'entry':entry,'sl':entry+STOP_K*atr,'tp':entry-TP_K*atr}
            continue
        if r.low_fo < L and r.close_fo > L:
            entry=float(r.close_fo); atr=float(r.atr14)
            pos={'side':'LONG','entry_time':r.end,'entry':entry,'sl':entry-STOP_K*atr,'tp':entry+TP_K*atr}
            continue
    else:
        side=pos['side']; exit_price=None; reason=None
        if side=='SHORT':
            if r.high_fo >= pos['sl']:
                exit_price=float(pos['sl']); reason='SL'
            elif r.low_fo <= pos['tp']:
                exit_price=float(pos['tp']); reason='TP'
        else:
            if r.low_fo <= pos['sl']:
                exit_price=float(pos['sl']); reason='SL'
            elif r.high_fo >= pos['tp']:
                exit_price=float(pos['tp']); reason='TP'
        if exit_price is not None:
            entry=pos['entry']
            pnl=(entry-exit_price) if side=='SHORT' else (exit_price-entry)
            pnl-=COMMISSION
            trades.append((pos['entry_time'],side,entry,pos['sl'],pos['tp'],r.end,exit_price,reason,pnl))
            pos=None

if pos is not None:
    last=df.iloc[-1]
    side=pos['side']; exit_price=float(last.close_fo)
    entry=pos['entry']
    pnl=(entry-exit_price) if side=='SHORT' else (exit_price-entry)
    pnl-=COMMISSION
    trades.append((pos['entry_time'],side,entry,pos['sl'],pos['tp'],last.end,exit_price,'FORCE_END',pnl))

tdf=pd.DataFrame(trades, columns=['entry_time','side','entry','sl','tp','exit_time','exit','reason','pnl']).sort_values('entry_time').reset_index(drop=True)
tdf['entry_time']=pd.to_datetime(tdf['entry_time'])
TRAIN_END_TZ = TRAIN_END.tz_localize(tdf['entry_time'].dt.tz)
tdf['is_train']=tdf['entry_time']<=TRAIN_END_TZ

tdf['equity']=tdf['pnl'].cumsum()
tdf['peak']=tdf['equity'].cummax()
tdf['dd']=tdf['equity']-tdf['peak']

tdf['month']=tdf['entry_time'].dt.to_period('M')
monthly=tdf.groupby('month').agg(trades=('pnl','count'),net_pnl=('pnl','sum'),avg_pnl=('pnl','mean'),win_rate=('pnl',lambda s:(s>0).mean()),pf=('pnl',lambda s: (s[s>0].sum()/(-s[s<0].sum())) if (-s[s<0].sum())>0 else np.inf)).reset_index()

def blk(sub):
    if sub.empty: return {'trades':0,'net_pnl':0.0,'max_dd':np.nan,'pf':np.nan,'win':np.nan}
    eq=sub['pnl'].cumsum(); peak=eq.cummax(); dd=eq-peak
    wins=sub.loc[sub['pnl']>0,'pnl'].sum(); loss=-sub.loc[sub['pnl']<0,'pnl'].sum()
    pf=float(wins/loss) if loss>0 else np.inf
    return {'trades':int(len(sub)),'net_pnl':float(sub['pnl'].sum()),'max_dd':float(dd.min()),'pf':pf,'win':float((sub['pnl']>0).mean())}

tr=blk(tdf[tdf['is_train']]); te=blk(tdf[~tdf['is_train']])

print('PARAMS:', 'stop_k', STOP_K, 'tp_k', TP_K)
print('TRAIN:', tr)
print('TEST :', te)
print('REASON:', tdf['reason'].value_counts().to_dict())

tdf.to_csv('research/canon_tp_sl_trades.csv', index=False)
monthly.to_csv('research/canon_tp_sl_monthly.csv', index=False)
print('Saved: research/canon_tp_sl_trades.csv')
print('Saved: research/canon_tp_sl_monthly.csv')
