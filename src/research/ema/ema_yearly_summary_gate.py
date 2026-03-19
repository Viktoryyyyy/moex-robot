"""
EMA YEARLY SUMMARY WITH PHASE GATE

Purpose:
Evaluate EMA(5,12) yearly performance conditioned on phase_transition_gate.

Why needed:
- Validate whether gate improves structural edge
- Detect gate overfiltering / underfiltering regimes
- Provide reproducible research artifact

Usage:
python -m src.research.ema.ema_yearly_summary_gate

Inputs:
canonical master CSV
data/state/phase_transition_risk history (rel_range_history used indirectly)

Logic:
Trade only when phase_transition_risk == 0

Output:
year | pnl | trades | days | win_rate | profitable
"""

import pandas as pd, numpy as np, glob, os

master=sorted(glob.glob(os.path.expanduser("~/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv")))[-1]
gate=pd.read_csv("data/state/rel_range_history.csv")

df=pd.read_csv(master,low_memory=False)
x=df[["end","open_fo","high_fo","low_fo","close_fo"]].copy()
x.columns=["end","open","high","low","close"]
x=x.dropna(subset=["end","close"]).sort_values("end").reset_index(drop=True)
x["end"]=pd.to_datetime(x["end"])
x["date"]=x["end"].dt.date
x["year"]=x["end"].dt.year

w=x.copy()
w["ema_fast"]=w["close"].ewm(span=5,adjust=False).mean()
w["ema_slow"]=w["close"].ewm(span=12,adjust=False).mean()
w["pos_raw"]=np.sign(w["ema_fast"]-w["ema_slow"])
w["pos"]=w["pos_raw"].shift(1).fillna(0)
w["dclose"]=w["close"].diff().fillna(0)
w["trades"]=w["pos"].diff().abs().fillna(0)
w["bar_pnl"]=w["pos"]*w["dclose"]-2*w["trades"]

gate["date"]=pd.to_datetime(gate["date"]).dt.date
gset=set(gate["date"])

w=w[w["date"].isin(gset)]

y=w.groupby("year").agg(
    pnl=("bar_pnl","sum"),
    trades=("trades","sum"),
    days=("date","nunique")
)
y["win_rate"]=(w.groupby(["year","date"])["bar_pnl"].sum()>0).groupby("year").mean()
y["profitable"]=(y["pnl"]>0).astype(int)

print(y)

