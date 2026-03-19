"""
EMA YEARLY SUMMARY (Research Utility)

Purpose:
Compute yearly EMA(5,12) performance on canonical 5m master dataset.

Why needed:
- Fast sanity check of structural edge stability across years
- Detect regime sensitivity (trend vs noise periods)
- Provide reproducible baseline for future research branches
- Canonical reproducibility artifact (no gate, no filters)

Usage:
python -m src.research.ema.ema_yearly_summary

Input:
~/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv

Output:
stdout yearly table:
year | pnl | trades | days | profitable

Notes:
- Commission fixed = 2 RUB per position change
- Execution = bar close → next bar
- No gate / no regime filters
"""

import pandas as pd, numpy as np, glob, os

p=sorted(glob.glob(os.path.expanduser("~/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv")))[-1]
df=pd.read_csv(p,low_memory=False)

x=df[["end","open_fo","high_fo","low_fo","close_fo"]].copy()
x.columns=["end","open","high","low","close"]
x=x.dropna(subset=["end","close"]).sort_values("end").reset_index(drop=True)
x["end"]=pd.to_datetime(x["end"])
x["date"]=x["end"].dt.date
x["year"]=x["end"].dt.year

def run(x):
    w=x.copy()
    w["ema_fast"]=w["close"].ewm(span=5,adjust=False).mean()
    w["ema_slow"]=w["close"].ewm(span=12,adjust=False).mean()
    w["pos_raw"]=np.sign(w["ema_fast"]-w["ema_slow"])
    w["pos"]=w["pos_raw"].shift(1).fillna(0)
    w["dclose"]=w["close"].diff().fillna(0)
    w["trades"]=w["pos"].diff().abs().fillna(0)
    w["bar_pnl"]=w["pos"]*w["dclose"]-2*w["trades"]

    y=w.groupby("year").agg(
        pnl=("bar_pnl","sum"),
        trades=("trades","sum"),
        days=("date","nunique")
    )
    y["profitable"]=(y["pnl"]>0).astype(int)
    print(y)

run(x)

