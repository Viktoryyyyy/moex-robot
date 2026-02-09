import pandas as pd

PATH = 'data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2026-02-03.csv'

L, U = 79600, 84100
POC = 81300
COMMISSION = 2

df = pd.read_csv(PATH, parse_dates=['end']).sort_values('end').reset_index(drop=True)

trades = []
n = len(df)
i = 0

while i < n - 1:
    r = df.iloc[i]

    # fake breakout UP -> SHORT
    if r.high_fo > U and r.close_fo < U:
        entry = r.close_fo
        t = r.end
        j = i + 1
        while j < n:
            if df.iloc[j].close_fo <= POC:
                pnl = (entry - df.iloc[j].close_fo) - COMMISSION
                trades.append((t, 'SHORT', pnl))
                break
            j += 1

    # fake breakout DOWN -> LONG
    if r.low_fo < L and r.close_fo > L:
        entry = r.close_fo
        t = r.end
        j = i + 1
        while j < n:
            if df.iloc[j].close_fo >= POC:
                pnl = (df.iloc[j].close_fo - entry) - COMMISSION
                trades.append((t, 'LONG', pnl))
                break
            j += 1

    i += 1

res = pd.DataFrame(trades, columns=['entry_time', 'side', 'pnl'])
res['month'] = res.entry_time.dt.to_period('M')

monthly = (
    res.groupby('month')
       .agg(
           trades=('pnl', 'count'),
           net_pnl=('pnl', 'sum'),
           avg_pnl=('pnl', 'mean')
       )
       .reset_index()
)

print(monthly)
print()
print('TOTAL:')
print(monthly[['trades', 'net_pnl']].sum())
