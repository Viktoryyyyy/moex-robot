#!/usr/bin/env python3
import argparse, pandas as pd, numpy as np

DEF_CNY = "cnyrub_5m_2025-10-30_2025-10-30_from_trades.csv"
DEF_SI  = "si_5m_2025-10-30_2025-10-30_from_candles_public.csv"

def load_cny(fn):
    df = pd.read_csv(fn)
    df['end'] = pd.to_datetime(df['end'])
    close = df['close'] if 'close' in df.columns else df['CLOSE']
    return df[['end']].assign(CNY_close=close.values)

def load_si(fn):
    df = pd.read_csv(fn)
    df['end'] = pd.to_datetime(df['end'])
    close = df['CLOSE'] if 'CLOSE' in df.columns else df['close']
    return df[['end']].assign(Si_close=close.values)

def build_signals(cny, si, span=3, thr_hi=6e-4, thr_lo=3e-4):
    m = pd.merge(cny, si, on='end', how='inner').sort_values('end').reset_index(drop=True)
    m['r_cny'] = np.log(m['CNY_close']).diff()
    m['r_si']  = np.log(m['Si_close']).diff()
    # EMA по р_cny
    m['ema'] = m['r_cny'].ewm(span=span, adjust=False).mean()
    # Гистерезисное состояние
    pos = np.zeros(len(m))
    state = 0
    for i, x in enumerate(m['ema'].values):
        ax = abs(x) if np.isfinite(x) else 0.0
        sgn = 0 if not np.isfinite(x) else (1 if x>0 else (-1 if x<0 else 0))
        if state == 0:
            if ax >= thr_hi:
                state = sgn
        else:
            if ax < thr_lo:
                state = 0
        pos[i] = state
    # Торгуем на следующем баре
    m['position'] = pd.Series(pos).shift(1).fillna(0)
    return m

def backtest(m, cost_per_side=2.0, slip_pts=0.0):
    # Пункт Si ~ ₽/контракт
    dP = m['Si_close'].diff()
    # Слиппедж одним «прыжком» при изменении позиции
    turns = (m['position'] != m['position'].shift(1).fillna(0)).astype(int)
    slip = turns * slip_pts * np.sign(m['position'] - m['position'].shift(1).fillna(0)).abs()
    m['pnl_gross'] = m['position'] * dP - slip
    m['cost'] = turns * cost_per_side
    m['pnl_net'] = m['pnl_gross'] - m['cost']
    stats = {
        'bars': int(len(m)),
        'trades': int(turns.sum()),
        'pnl_net': float(m['pnl_net'].sum(skipna=True)),
        'avg_per_trade': float(m['pnl_net'].sum(skipna=True) / turns.sum()) if turns.sum()>0 else 0.0,
        'winrate': float((m.loc[m['pnl_gross']!=0,'pnl_gross']>0).mean()) if (m['pnl_gross']!=0).any() else None
    }
    return m, stats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cny", default=DEF_CNY)
    ap.add_argument("--si",  default=DEF_SI)
    ap.add_argument("--span", type=int, default=3, help="EMA span по r_cny")
    ap.add_argument("--thr-hi", type=float, default=6e-4, help="верхний порог входа")
    ap.add_argument("--thr-lo", type=float, default=3e-4, help="нижний порог выхода")
    ap.add_argument("--cost", type=float, default=2.0, help="комиссия ₽/сторону")
    ap.add_argument("--slip", type=float, default=0.0, help="слиппедж, пунктов на смену позы")
    args = ap.parse_args()

    cny = load_cny(args.cny)
    si  = load_si(args.si)
    m   = build_signals(cny, si, span=args.span, thr_hi=args.thr_hi, thr_lo=args.thr_lo)
    mbt, stats = backtest(m, cost_per_side=args.cost, slip_pts=args.slip)

    print("# SUMMARY (EMA+HYS)")
    wr = "" if stats['winrate'] is None else f"{round(stats['winrate']*100,1)}%"
    print(f"bars={stats['bars']} trades={stats['trades']} pnl_net={stats['pnl_net']:.2f} avg/trade={stats['avg_per_trade']:.2f} winrate={wr}")
    print(f"span={args.span} thr_hi={args.thr_hi} thr_lo={args.thr_lo} cost={args.cost} slip={args.slip}")

    cols = ['end','CNY_close','Si_close','r_cny','ema','position','pnl_gross','cost','pnl_net']
    print("\n# TAIL(10)")
    print(mbt[cols].tail(10).to_string(index=False))

    mbt[['end','CNY_close','Si_close','r_cny','ema','position','pnl_gross','cost','pnl_net']].to_csv("cny_track_signals_5m_ema.csv", index=False)
    print("Saved signals: cny_track_signals_5m_ema.csv")

if __name__ == "__main__":
    pd.set_option("display.width", 160)
    main()
