import pandas as pd

PATH = 'data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2026-02-03.csv'

L, U = 79600, 84100
POC = 81300
COMMISSION = 2

df = pd.read_csv(PATH, parse_dates=['end']).sort_values('end').reset_index(drop=True)
n = len(df)

trades = []
pos = None

def close_pos(exit_time, exit_price, reason):
    global pos, trades
    side = pos['side']
    entry = pos['entry']
    pnl = (entry - exit_price) if side == 'SHORT' else (exit_price - entry)
    pnl -= COMMISSION
    trades.append({
        'entry_time': pos['entry_time'],
        'side': side,
        'entry': entry,
        'stop': pos['stop'],
        'exit_time': exit_time,
        'exit': exit_price,
        'reason': reason,
        'pnl': pnl
    })
    pos = None

for i in range(n):
    r = df.iloc[i]

    if pos is None:
        # Signal bar defines entry and stop
        # Fake UP -> SHORT
        if r.high_fo > U and r.close_fo < U:
            pos = {
                'side': 'SHORT',
                'entry_time': r.end,
                'entry': float(r.close_fo),
                'stop': float(r.high_fo)
            }
            continue

        # Fake DOWN -> LONG
        if r.low_fo < L and r.close_fo > L:
            pos = {
                'side': 'LONG',
                'entry_time': r.end,
                'entry': float(r.close_fo),
                'stop': float(r.low_fo)
            }
            continue

    else:
        # Manage open position on current bar
        if pos['side'] == 'SHORT':
            # STOP intrabar first
            if r.high_fo >= pos['stop']:
                close_pos(r.end, float(pos['stop']), 'STOP')
                continue
            # Take-profit by close to POC
            if r.close_fo <= POC:
                close_pos(r.end, float(r.close_fo), 'POC')
                continue
        else:
            # STOP intrabar first
            if r.low_fo <= pos['stop']:
                close_pos(r.end, float(pos['stop']), 'STOP')
                continue
            # Take-profit by close to POC
            if r.close_fo >= POC:
                close_pos(r.end, float(r.close_fo), 'POC')
                continue

# Force-close if still open at end of dataset
if pos is not None:
    last = df.iloc[-1]
    close_pos(last.end, float(last.close_fo), 'FORCE_END')

res = pd.DataFrame(trades)
if res.empty:
    print('NO TRADES')
    raise SystemExit(0)

res['month'] = pd.to_datetime(res['entry_time']).dt.to_period('M')
monthly = res.groupby('month').agg(
    trades=('pnl','count'),
    net_pnl=('pnl','sum'),
    avg_pnl=('pnl','mean'),
    win_rate=('pnl', lambda s: (s > 0).mean()),
    stop_rate=('reason', lambda s: (s == 'STOP').mean())
).reset_index()

print(monthly)
print()
print('TOTAL:')
print(res[['pnl']].sum().rename({'pnl':'net_pnl'}))
print()
print('REASON:')
print(res['reason'].value_counts())

res.to_csv('research/pnl_fake_poc_stop_trades.csv', index=False)
monthly.to_csv('research/pnl_fake_poc_stop_monthly.csv', index=False)
