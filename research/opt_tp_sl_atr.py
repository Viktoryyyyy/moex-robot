import pandas as pd
import numpy as np

PATH = 'data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2026-02-03.csv'

L, U = 79600, 84100
COMMISSION = 2

STOP_K = [0.4, 0.6, 0.8, 1.0, 1.2, 1.5]
TP_K   = [0.6, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5]

TRAIN_END = pd.Timestamp('2023-12-31 23:59:59')

df = pd.read_csv(PATH, parse_dates=['end']).sort_values('end').reset_index(drop=True)

for col in ['open_fo','high_fo','low_fo','close_fo']:
    if col not in df.columns:
        raise SystemExit(f'missing column: {col}')

df['date'] = df['end'].dt.date

# daily ATR(14) from daily OHLC
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

def run_one(stop_k, tp_k):
    n = len(df)
    pos = None
    trades = []

    for i in range(n):
        r = df.iloc[i]

        if pos is None:
            # entries (fake breakout of balance)
            if r.high_fo > U and r.close_fo < U:
                entry = float(r.close_fo)
                atr = float(r.atr14)
                pos = {
                    'side': 'SHORT',
                    'entry_time': r.end,
                    'entry': entry,
                    'sl': entry + stop_k*atr,
                    'tp': entry - tp_k*atr
                }
                continue
            if r.low_fo < L and r.close_fo > L:
                entry = float(r.close_fo)
                atr = float(r.atr14)
                pos = {
                    'side': 'LONG',
                    'entry_time': r.end,
                    'entry': entry,
                    'sl': entry - stop_k*atr,
                    'tp': entry + tp_k*atr
                }
                continue

        # manage open position
        if pos is not None:
            side = pos['side']
            exit_price = None
            reason = None

            if side == 'SHORT':
                # SL first
                if r.high_fo >= pos['sl']:
                    exit_price = float(pos['sl'])
                    reason = 'SL'
                elif r.low_fo <= pos['tp']:
                    exit_price = float(pos['tp'])
                    reason = 'TP'
            else:
                if r.low_fo <= pos['sl']:
                    exit_price = float(pos['sl'])
                    reason = 'SL'
                elif r.high_fo >= pos['tp']:
                    exit_price = float(pos['tp'])
                    reason = 'TP'

            if exit_price is not None:
                entry = pos['entry']
                pnl = (entry - exit_price) if side == 'SHORT' else (exit_price - entry)
                pnl -= COMMISSION
                trades.append((pos['entry_time'], side, entry, pos['sl'], pos['tp'], r.end, exit_price, reason, pnl))
                pos = None

    # if still open, force close at last close (anti-vanish)
    if pos is not None:
        last = df.iloc[-1]
        side = pos['side']
        exit_price = float(last.close_fo)
        entry = pos['entry']
        pnl = (entry - exit_price) if side == 'SHORT' else (exit_price - entry)
        pnl -= COMMISSION
        trades.append((pos['entry_time'], side, entry, pos['sl'], pos['tp'], last.end, exit_price, 'FORCE_END', pnl))

    tdf = pd.DataFrame(trades, columns=['entry_time','side','entry','sl','tp','exit_time','exit','reason','pnl'])
    tdf['entry_time'] = pd.to_datetime(tdf['entry_time'])
    TRAIN_END_TZ = TRAIN_END.tz_localize(tdf['entry_time'].dt.tz)

    if tdf.empty:
        return None, None

    tdf = tdf.sort_values('entry_time').reset_index(drop=True)
    tdf['is_train'] = tdf['entry_time'] <= TRAIN_END_TZ

    def stats(sub):
        if sub.empty:
            return {'trades':0,'net_pnl':0.0,'win_rate':np.nan,'avg_pnl':np.nan,'max_dd':np.nan,'pf':np.nan}
        eq = sub['pnl'].cumsum()
        peak = eq.cummax()
        dd = eq - peak
        max_dd = float(dd.min())
        wins = sub.loc[sub['pnl']>0,'pnl'].sum()
        loss = -sub.loc[sub['pnl']<0,'pnl'].sum()
        pf = float(wins/loss) if loss>0 else np.inf
        return {
            'trades': int(len(sub)),
            'net_pnl': float(sub['pnl'].sum()),
            'win_rate': float((sub['pnl']>0).mean()),
            'avg_pnl': float(sub['pnl'].mean()),
            'max_dd': max_dd,
            'pf': pf
        }

    tr = stats(tdf[tdf['is_train']])
    te = stats(tdf[~tdf['is_train']])
    return tr, te

rows = []
for sk in STOP_K:
    for tk in TP_K:
        out = run_one(sk, tk)
        if out == (None, None):
            continue
        tr, te = out
        rows.append({
            'stop_k': sk,
            'tp_k': tk,
            'train_trades': tr['trades'],
            'train_net_pnl': tr['net_pnl'],
            'train_win_rate': tr['win_rate'],
            'train_avg_pnl': tr['avg_pnl'],
            'train_max_dd': tr['max_dd'],
            'train_pf': tr['pf'],
            'test_trades': te['trades'],
            'test_net_pnl': te['net_pnl'],
            'test_win_rate': te['win_rate'],
            'test_avg_pnl': te['avg_pnl'],
            'test_max_dd': te['max_dd'],
            'test_pf': te['pf']
        })

res = pd.DataFrame(rows)
if res.empty:
    raise SystemExit('no results')

# rank by train_net_pnl, but show test beside it
res = res.sort_values(['train_net_pnl','train_pf'], ascending=[False, False]).reset_index(drop=True)

top = res.head(20).copy()

print('TOP 20 by TRAIN net_pnl (with TEST shown):')
print(top.to_string(index=False))

res.to_csv('research/opt_tp_sl_results.csv', index=False)
top.to_csv('research/opt_tp_sl_top20.csv', index=False)

print()
print('Saved: research/opt_tp_sl_results.csv')
print('Saved: research/opt_tp_sl_top20.csv')
