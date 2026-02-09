#!/usr/bin/env python3
import pandas as pd
import numpy as np
from math import sqrt
from datetime import datetime

# --- ВХОДНЫЕ ФАЙЛЫ (из шага 1) ---
CNY_FILE = "cnyrub_5m_2025-10-30_2025-10-30_from_trades.csv"
SI_FILE  = "si_5m_2025-10-30_2025-10-30_from_candles_public.csv"

# --- ПАРАМЕТРЫ ---
LAGS = list(range(-12, 13))  # в 5-мин барах (±60 минут)

def read_cny(fn: str) -> pd.DataFrame:
    df = pd.read_csv(fn)
    # ожидаемые колонки: end, open, high, low, close, volume, turnover
    # нормализуем
    if 'end' not in df.columns:
        raise RuntimeError("CNY csv must contain 'end'")
    df['end'] = pd.to_datetime(df['end'], errors='coerce')
    # унифицируем к одному неймингу
    if 'close' not in df.columns:
        # на всякий
        for alt in ['CLOSE','Close','ClosePrice']:
            if alt in df.columns:
                df['close'] = df[alt]; break
    need = ['end','close']
    for c in need:
        if c not in df.columns: df[c] = np.nan
    df = df.dropna(subset=['end','close']).sort_values('end').reset_index(drop=True)
    return df[['end','close']].rename(columns={'close':'CNY_close'})

def read_si(fn: str) -> pd.DataFrame:
    df = pd.read_csv(fn)
    # ожидаемые: end, OPEN,HIGH,LOW,CLOSE,volume
    if 'end' not in df.columns:
        raise RuntimeError("Si csv must contain 'end'")
    df['end'] = pd.to_datetime(df['end'], errors='coerce')
    if 'CLOSE' not in df.columns:
        # fallback для нижнего регистра
        if 'close' in df.columns: df['CLOSE'] = df['close']
    df = df.dropna(subset=['end','CLOSE']).sort_values('end').reset_index(drop=True)
    return df[['end','CLOSE']].rename(columns={'CLOSE':'Si_close'})

def align_5m(cny: pd.DataFrame, si: pd.DataFrame) -> pd.DataFrame:
    # На всякий приводим к минутной сетке без секунд/милисекунд
    cny['end'] = cny['end'].dt.floor('min')
    si['end']  = si['end'].dt.floor('min')
    # Жёсткое inner-join по end
    m = pd.merge(cny, si, on='end', how='inner')
    m = m.sort_values('end').drop_duplicates(subset=['end']).reset_index(drop=True)
    # Лог-доходности (стабильнее на разных масштабах)
    m['r_cny'] = np.log(m['CNY_close']).diff()
    m['r_si']  = np.log(m['Si_close']).diff()
    m = m.dropna(subset=['r_cny','r_si']).reset_index(drop=True)
    return m

def corr_with_lag(df: pd.DataFrame, lags) -> pd.DataFrame:
    out = []
    x = df['r_cny'].values
    y = df['r_si'].values
    n0 = len(df)
    for k in lags:
        if k > 0:
            # CNY ведет на k баров: corr( r_cny(t), r_si(t+k) )
            xk = x[:-k]; yk = y[k:]
        elif k < 0:
            # Si ведет на |k| баров: corr( r_cny(t), r_si(t+k) ) = corr( r_cny(t), r_si(t-|k|) )
            xk = x[-k:]; yk = y[:k]  # k отрицательный -> срез ок
        else:
            xk = x; yk = y
        n = min(len(xk), len(yk))
        if n < 10:
            out.append((k, np.nan, n, np.nan)); continue
        c = np.corrcoef(xk[:n], yk[:n])[0,1]
        # грубая t-статистика для корреляции
        t = c * sqrt((n-2) / max(1e-9, 1 - c*c))
        out.append((k, c, n, t))
    res = pd.DataFrame(out, columns=['lag_5m','corr','N','t_stat']).sort_values('lag_5m')
    # Сортировки для вывода
    res_abs = res.dropna().copy()
    res_abs['abs_corr'] = res_abs['corr'].abs()
    top3 = res_abs.sort_values('abs_corr', ascending=False).head(3).copy()
    return res, top3

def main():
    cny = read_cny(CNY_FILE)
    si  = read_si(SI_FILE)
    merged = align_5m(cny, si)
    if len(merged) < 30:
        print(f"WARN: мало совместных баров: {len(merged)}. Проверьте пересечение дат/время.")
    corr_tbl, top3 = corr_with_lag(merged, LAGS)

    # Короткий отчёт
    print("\n# ALIGNMENT")
    print(f"bars CNY: {len(cny)}, bars Si: {len(si)}, merged: {len(merged)}")
    print(f"first end: {merged['end'].iloc[0] if len(merged)>0 else 'NA'}")
    print(f"last  end: {merged['end'].iloc[-1] if len(merged)>0 else 'NA'}")

    print("\n# TOP-3 by |corr| (lag in 5m bars)")
    if not top3.empty:
        # who leads
        def who(k):
            if k>0: return "CNY → Si (CNY ведёт)"
            if k<0: return "Si → CNY (Si ведёт)"
            return "одновременная"
        top3['direction'] = top3['lag_5m'].map(who)
        print(top3[['lag_5m','corr','N','t_stat','direction']].to_string(index=False))
    else:
        print("нет значимых лагов (недостаточно данных)")

    print("\n# FULL CORR TABLE (lag, corr, N, t)")
    print(corr_tbl.to_string(index=False))

if __name__ == "__main__":
    pd.set_option("display.width", 160)
    main()
