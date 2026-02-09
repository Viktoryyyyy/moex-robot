#!/usr/bin/env python3
import argparse, pandas as pd, numpy as np

# По умолчанию — файлы из шага 1
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

def build_signals(cny, si, thr=5e-4):
    # Выравниваем по правому краю 5м бара
    m = pd.merge(cny, si, on='end', how='inner').sort_values('end').reset_index(drop=True)
    # Лог-изменения CNY и Si
    m['r_cny'] = np.log(m['CNY_close']).diff()
    m['r_si']  = np.log(m['Si_close']).diff()
    # Сигнал «слежения»: знак р_cny, но только если |р_cny| >= thr
    sig = np.sign(m['r_cny'])
    sig[(m['r_cny'].abs() < thr) | (~np.isfinite(sig))] = 0
    # Торгуем на следующем баре: позиция(t) = сигнал(t-1)
    m['signal']   = sig
    m['position'] = m['signal'].shift(1).fillna(0)
    return m

def backtest(m, cost_per_side=2.0):
    # PnL на баре = pos_{t-1} * (Si_close_t - Si_close_{t-1})  (1 пункт = 1 ₽ на контракт)
    m['dP'] = m['Si_close'].diff()
    m['pnl_gross'] = m['position'] * m['dP']
    # Комиссия: когда |pos_t - pos_{t-1}| > 0 — платим cost_per_side за сделку (смена/вход/выход)
    pos = m['position'].fillna(0)
    turns = (pos != pos.shift(1).fillna(0)).astype(int)
    m['cost'] = turns * cost_per_side
    m['pnl_net'] = m['pnl_gross'] - m['cost']
    # Метрики
    trades = turns.sum()
    pnl = float(m['pnl_net'].sum(skipna=True))
    avg = pnl / trades if trades > 0 else 0.0
    winrate = (m.loc[m['pnl_gross'] != 0, 'pnl_gross'] > 0).mean() if (m['pnl_gross'] != 0).any() else np.nan
    stats = {
        'bars': int(len(m)),
        'trades': int(trades),
        'winrate': float(winrate) if pd.notna(winrate) else None,
        'pnl_net': pnl,
        'avg_per_trade': avg
    }
    return m, stats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cny", default=DEF_CNY)
    ap.add_argument("--si",  default=DEF_SI)
    ap.add_argument("--thr", type=float, default=5e-4, help="порог по |log-return CNY|, напр. 0.0005 = 0.05%")
    ap.add_argument("--cost", type=float, default=2.0, help="комиссия за одну сделку (вход/выход), ₽/контракт")
    args = ap.parse_args()

    cny = load_cny(args.cny)
    si  = load_si(args.si)
    m   = build_signals(cny, si, thr=args.thr)
    mbt, stats = backtest(m, cost_per_side=args.cost)

    print("# SUMMARY")
    print(f"bars={stats['bars']} trades={stats['trades']} pnl_net={stats['pnl_net']:.2f} avg/trade={stats['avg_per_trade']:.2f} winrate={'' if stats['winrate'] is None else round(stats['winrate']*100,1)}%")
    print(f"thr={args.thr} cost_per_side={args.cost}")
    # Показать последние 10 баров (end, r_cny, signal, position, dP, pnl_net)
    tail_cols = ['end','CNY_close','Si_close','r_cny','signal','position','dP','pnl_gross','cost','pnl_net']
    print("\n# TAIL(10)")
    print(mbt[tail_cols].tail(10).to_string(index=False))

    # Сохраним сигналы
    out = mbt[['end','CNY_close','Si_close','r_cny','signal','position','pnl_gross','cost','pnl_net']].copy()
    out.to_csv("cny_track_signals_5m.csv", index=False)
    print("Saved signals: cny_track_signals_5m.csv")

if __name__ == "__main__":
    pd.set_option("display.width", 160)
    main()
