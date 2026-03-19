import pandas as pd, glob
p=sorted(glob.glob('~/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv'))[-1]
df=pd.read_csv(p,parse_dates=['end'])
df['date']=df['end'].dt.date
d=df.groupby('date').agg(high=('high_fo','max'),low=('low_fo','min'),close=('close_fo','last'),open=('open_fo','first'))
d['range']=d['high']-d['low']
d['trend_ratio']=(d['close']-d['open']).abs()/d['range']
pnl=pd.read_csv('data/research/ema_pnl_multitimeframe_test.csv')
m=pnl.merge(d.reset_index(),left_on='date',right_on='date')
m['bucket']=pd.qcut(m['trend_ratio'],5,labels=['Q1_low','Q2','Q3','Q4','Q5_high'])
print(m.groupby('bucket')['pnl_day'].agg(['count','mean','median','sum']))
