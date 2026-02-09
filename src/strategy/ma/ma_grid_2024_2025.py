#!/usr/bin/env python3
import numpy as np
import pandas as pd
from pathlib import Path


# Полный мастер-файл без regime
MASTER_PATH = Path(
    "data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-11-13.csv"
)

# Диапазон EMA (в барах, 5-минутные бары)
EMA_MIN = 2
EMA_MAX = 15


def load_master(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["end"])
    df = df.sort_values("end").reset_index(drop=True)

    # Фильтр по датам: 2024-01-01..2025-11-13 (можно снять, если нужна вся история)
    df = df[(df["end"] >= "2024-01-01") & (df["end"] <= "2025-11-13 23:59:59")]
    df = df.reset_index(drop=True)

    print(f"Loaded master slice 2024-2025: rows={len(df)}")
    if not df.empty:
        print(f"Start={df['end'].iloc[0]}, End={df['end'].iloc[-1]}")
    return df


def compute_ma_pnl(
    df: pd.DataFrame,
    ema_short: int,
    ema_long: int,
) -> dict:
    """
    Always-in-market EMA cross.

    Позиция:
      pos_t = sign(EMA_short_t - EMA_long_t), где 0 -> держим pos_{t-1}.
      pos_t ∈ {-1, +1, 0}, фактически после прогрева EMA почти всегда ±1.

    PnL:
      ret_t  = close_t - close_{t-1}
      pnl_t  = pos_{t-1} * ret_t
      equity = pnl_t.cumsum()
    """
    df = df.copy()

    close = df["close_fo"].astype(float)

    # EMA
    df["ema_short"] = close.ewm(span=ema_short, adjust=False).mean()
    df["ema_long"] = close.ewm(span=ema_long, adjust=False).mean()

    diff = df["ema_short"] - df["ema_long"]
    dir_raw = np.sign(diff).astype(int)  # -1, 0, +1

    # 0 заменяем на NaN и тянем предыдущее значение, потом заполняем нулём в самом начале
    pos = pd.Series(dir_raw, index=df.index)
    pos = pos.replace(0, np.nan).ffill().fillna(0).astype(int)

    df["pos"] = pos

    # Доходность close-to-close
    df["ret"] = close.diff()

    # PnL стратегии: позиция предыдущего бара * доходность текущего
    df["pos_prev"] = df["pos"].shift(1).fillna(0).astype(int)
    df["pnl"] = df["pos_prev"] * df["ret"]

    # Отбрасываем первый бар (ret NaN)
    df = df.iloc[1:].copy()

    if df.empty:
        return {
            "ema_short": ema_short,
            "ema_long": ema_long,
            "pnl_sum": 0.0,
            "bars": 0,
            "trades": 0,
            "winrate": 0.0,
            "avg_pnl": 0.0,
            "sharpe": 0.0,
            "max_dd": 0.0,
        }

    # Кол-во "сделок" как число смен знака позиции
    trades = (df["pos"] != df["pos"].shift(1)).sum()

    pnl_series = df["pnl"].fillna(0.0)

    pnl_sum = float(pnl_series.sum())
    bars = int(len(pnl_series))
    avg_pnl = float(pnl_series.mean())

    wins = (pnl_series > 0).sum()
    non_zero = (pnl_series != 0).sum()
    winrate = float(wins / non_zero) if non_zero > 0 else 0.0

    std = float(pnl_series.std(ddof=0))
    sharpe = float(avg_pnl / std) if std > 0 else 0.0

    equity = pnl_series.cumsum()
    peak = equity.cummax()
    drawdown = equity - peak
    max_dd = float(drawdown.min()) if not drawdown.empty else 0.0

    return {
        "ema_short": ema_short,
        "ema_long": ema_long,
        "pnl_sum": pnl_sum,
        "bars": bars,
        "trades": int(trades),
        "winrate": winrate,
        "avg_pnl": avg_pnl,
        "sharpe": sharpe,
        "max_dd": max_dd,
    }


def main() -> None:
    df_master = load_master(MASTER_PATH)

    required = ["end", "close_fo"]
    for c in required:
        if c not in df_master.columns:
            raise ValueError(f"Column {c} not found in master file")

    if df_master.empty:
        print("Master slice is empty after date filter, abort.")
        return

    results = []

    for ema_s in range(EMA_MIN, EMA_MAX + 1):
        for ema_l in range(EMA_MIN, EMA_MAX + 1):
            if ema_l <= ema_s:
                continue

            res = compute_ma_pnl(df_master, ema_s, ema_l)
            results.append(res)

            print(
                f"EMA {ema_s:2d}/{ema_l:2d} -> "
                f"pnl={res['pnl_sum']:9.0f}, "
                f"bars={res['bars']:6d}, "
                f"trades={res['trades']:5d}, "
                f"win={res['winrate']:5.2f}, "
                f"sharpe={res['sharpe']:6.3f}, "
                f"max_dd={res['max_dd']:9.0f}"
            )

    res_df = pd.DataFrame(results)
    out_path = Path("ma_grid_ema2_15_2024_2025.csv")
    res_df.to_csv(out_path, index=False)

    print("\nSaved:", out_path)
    print("\n=== ТОП-20 по pnl_sum ===")
    print(
        res_df.sort_values("pnl_sum", ascending=False)
              .head(20)
              .round(4)
    )

    print("\n=== ТОП-20 по Sharpe (фильтр bars > 1000) ===")
    filt = res_df[res_df["bars"] > 1000]
    if not filt.empty:
        print(
            filt.sort_values("sharpe", ascending=False)
                .head(20)
                .round(4)
        )
    else:
        print("No rows with bars > 1000")
    

if __name__ == "__main__":
    main()
