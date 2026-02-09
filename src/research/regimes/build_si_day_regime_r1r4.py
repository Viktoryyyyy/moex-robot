import glob
import os
import pandas as pd
import numpy as np


def latest_master(pattern: str) -> str:
    paths = glob.glob(pattern)
    if not paths:
        raise FileNotFoundError(f'no files matched: {pattern}')
    return max(paths, key=os.path.getmtime)


def build_d1_ohlc(master_path: str) -> pd.DataFrame:
    usecols = ['end', 'open_fo', 'high_fo', 'low_fo', 'close_fo', 'asset_code']
    df = pd.read_csv(master_path, usecols=usecols)

    ac = df['asset_code'].astype(str).str.upper()
    df = df[ac.eq('SI')].copy()
    df = df.dropna(subset=['end', 'open_fo', 'high_fo', 'low_fo', 'close_fo'])

    dt = pd.to_datetime(df['end'], errors='coerce', utc=True)
    df = df[dt.notna()].copy()
    df['dt'] = dt[dt.notna()].dt.tz_convert('Europe/Moscow')
    df['TRADEDATE'] = df['dt'].dt.date.astype(str)

    df = df.sort_values('dt')
    g = df.groupby('TRADEDATE', sort=True)

    d1 = pd.DataFrame({
        'TRADEDATE': g.size().index,
        'open': g['open_fo'].first().values,
        'high': g['high_fo'].max().values,
        'low': g['low_fo'].min().values,
        'close': g['close_fo'].last().values,
    })

    return d1.reset_index(drop=True)


def classify_r1r4(d1: pd.DataFrame) -> pd.DataFrame:
    eps = 1e-12

    rng = d1['high'] - d1['low']
    body = (d1['close'] - d1['open']).abs()
    pos = (d1['close'] - d1['low']) / np.maximum(rng, eps)

    atr20 = rng.shift(1).rolling(window=20, min_periods=1).mean()
    range_ratio = rng / np.maximum(atr20, eps)
    body_ratio = body / np.maximum(rng, eps)

    r1 = (range_ratio <= 0.85) & (pos >= 0.40) & (pos <= 0.60)
    r2 = (range_ratio >= 1.25) & ((pos >= 0.70) | (pos <= 0.30)) & (body_ratio >= 0.60)
    r3 = (range_ratio >= 1.25) & (pos >= 0.40) & (pos <= 0.60)

    regime = np.where(r1, 'R1', np.where(r2, 'R2', np.where(r3, 'R3', 'R4')))
    out = pd.DataFrame({'TRADEDATE': d1['TRADEDATE'].astype(str), 'regime_day': regime})
    return out


def main() -> None:
    master_pattern = 'data/master/master_5m_si_cny_futoi_obstats_*.csv'
    master_path = latest_master(master_pattern)

    d1 = build_d1_ohlc(master_path)
    out = classify_r1r4(d1)

    out_path = 'data/research/regime_day_r1r4.csv'
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out.to_csv(out_path, index=False)

    print('MASTER=', master_path)
    print('OUT=', out_path, 'rows=', len(out))
    print('DATE_MIN=', out['TRADEDATE'].min(), 'DATE_MAX=', out['TRADEDATE'].max())
    print('COUNTS=')
    print(out['regime_day'].value_counts().sort_index().to_string())
    print('TAIL10=')
    print(out.tail(10).to_string(index=False))


if __name__ == '__main__':
    main()
