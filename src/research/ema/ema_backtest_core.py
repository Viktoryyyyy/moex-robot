"""Thin canonical EMA backtest adapter for grid research."""

from __future__ import annotations

import pandas as pd

from src.research.ema.lib_ema_search import (
    generate_ema_signals,
    resample_ohlc,
    run_point_backtest,
    summarize_backtest_by_day,
)


def resolve_ohlc_cols(df_cols):
    need = ["end", "open", "high", "low", "close"]
    if all(c in df_cols for c in need):
        return {k: k for k in need}

    candidates = [
        {"end": "end", "open": "open_fo", "high": "high_fo", "low": "low_fo", "close": "close_fo"},
        {"end": "end", "open": "OPEN", "high": "HIGH", "low": "LOW", "close": "CLOSE"},
    ]
    for mapping in candidates:
        if all(mapping[k] in df_cols for k in need):
            return mapping

    raise KeyError("Cannot resolve OHLC columns. Available columns sample: " + str(list(df_cols)[:30]))


def normalize_master_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    colmap = resolve_ohlc_cols(df.columns)
    out = df[[colmap["end"], colmap["open"], colmap["high"], colmap["low"], colmap["close"]]].copy()
    out.columns = ["ts", "open", "high", "low", "close"]
    out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["ts", "open", "high", "low", "close"]).sort_values("ts").reset_index(drop=True)
    if out.empty:
        raise ValueError("No valid OHLC rows after normalization.")
    return out


def run_backtest(df: pd.DataFrame, timeframe: str, fast: int, slow: int, fee: float) -> pd.DataFrame:
    bars = normalize_master_ohlc(df)
    if timeframe != "5m":
        bars = resample_ohlc(bars, timeframe)

    bars = generate_ema_signals(
        bars,
        ema_fast_span=int(fast),
        ema_slow_span=int(slow),
        mode="trend_long_short",
    )
    bars = run_point_backtest(bars, commission_points=float(fee))
    out = summarize_backtest_by_day(bars).copy()

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    out["max_dd_day"] = pd.to_numeric(out["dd_day"], errors="coerce").fillna(0.0).mul(-1.0)
    out["EMA_EDGE_DAY"] = (pd.to_numeric(out["pnl_day"], errors="coerce").fillna(0.0) > 0.0).astype(int)

    return out[["date", "pnl_day", "max_dd_day", "num_trades_day", "EMA_EDGE_DAY"]].copy()
