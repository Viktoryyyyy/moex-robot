"""Foundational OHLC loading, normalization, and resampling utilities for EMA research."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REQUIRED_SCHEMA_KEYS = ("timestamp", "open", "high", "low", "close")
OPTIONAL_SCHEMA_KEYS = ("volume", "delimiter", "datetime_format", "timezone")

_ALLOWED_TIMEFRAMES = {
    "5m": "5min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


class OhlcSchemaError(ValueError):
    """Raised when schema content is invalid."""


class OhlcDataError(ValueError):
    """Raised when OHLC data is missing or invalid after normalization."""


def load_ohlc_schema(schema_path: str | Path) -> dict[str, Any]:
    """Load and validate the research OHLC mapping schema from JSON."""
    path = Path(schema_path)
    with path.open("r", encoding="utf-8") as f:
        schema = json.load(f)

    if not isinstance(schema, dict):
        raise OhlcSchemaError("Schema must be a JSON object.")

    missing = [k for k in REQUIRED_SCHEMA_KEYS if not schema.get(k)]
    if missing:
        raise OhlcSchemaError("Schema is missing required key(s): " + str(missing))

    allowed = set(REQUIRED_SCHEMA_KEYS) | set(OPTIONAL_SCHEMA_KEYS)
    extra = sorted(set(schema.keys()) - allowed)
    if extra:
        raise OhlcSchemaError("Schema contains unsupported key(s): " + str(extra))

    return schema


def load_source_ohlc_csv(csv_path: str | Path, schema: dict[str, Any]) -> pd.DataFrame:
    """Load source OHLC CSV and normalize into canonical column names."""
    path = Path(csv_path)
    delimiter = schema.get("delimiter", ",")
    if not isinstance(delimiter, str) or not delimiter:
        raise OhlcSchemaError("Schema key delimiter must be a non-empty string when provided.")

    df = pd.read_csv(path, sep=delimiter)
    return normalize_ohlc_dataframe(df=df, schema=schema)


def normalize_ohlc_dataframe(df: pd.DataFrame, schema: dict[str, Any]) -> pd.DataFrame:
    """Normalize source OHLC columns to canonical schema and validate integrity."""
    colmap = {
        "ts": schema["timestamp"],
        "open": schema["open"],
        "high": schema["high"],
        "low": schema["low"],
        "close": schema["close"],
    }
    if schema.get("volume"):
        colmap["volume"] = schema["volume"]

    _validate_source_columns(df=df, colmap=colmap)

    out = df[[colmap[k] for k in colmap]].copy()
    out.columns = list(colmap.keys())

    dt_format = schema.get("datetime_format")
    out["ts"] = pd.to_datetime(out["ts"], format=dt_format, errors="coerce")

    timezone = schema.get("timezone")
    if timezone:
        out["ts"] = _apply_timezone(out["ts"], timezone)

    numeric_columns = ["open", "high", "low", "close"]
    if "volume" in out.columns:
        numeric_columns.append("volume")

    for col in numeric_columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    required_after_coercion = ["ts", "open", "high", "low", "close"]
    out = out.dropna(subset=required_after_coercion)

    if out.empty:
        raise OhlcDataError(
            "No valid OHLC rows remain after datetime/numeric coercion for required columns."
        )

    out = out.sort_values("ts", ascending=True).reset_index(drop=True)
    return out


def resample_ohlc(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample canonical OHLC bars into a target timeframe."""
    if timeframe not in _ALLOWED_TIMEFRAMES:
        raise ValueError(
            "Unsupported timeframe " + repr(timeframe) + ". Allowed: " + str(sorted(_ALLOWED_TIMEFRAMES.keys()))
        )

    required = ["ts", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise OhlcDataError("Missing required canonical column(s) for resampling: " + str(missing))

    rule = _ALLOWED_TIMEFRAMES[timeframe]

    agg: dict[str, str] = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }
    if "volume" in df.columns:
        agg["volume"] = "sum"

    resampled = (
        df.sort_values("ts", ascending=True)
        .set_index("ts")
        .resample(rule, label="right", closed="right")
        .agg(agg)
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )

    if resampled.empty:
        raise OhlcDataError("Resampling produced zero rows for timeframe " + repr(timeframe) + ".")

    return resampled


def _validate_source_columns(df: pd.DataFrame, colmap: dict[str, str]) -> None:
    missing = [src_col for src_col in colmap.values() if src_col not in df.columns]
    if missing:
        raise OhlcDataError("CSV is missing required mapped source column(s): " + str(missing))


def _apply_timezone(ts: pd.Series, timezone: str) -> pd.Series:
    if ts.isna().all():
        return ts

    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize(timezone, nonexistent="NaT", ambiguous="NaT")
    return ts.dt.tz_convert(timezone)


def generate_ema_signals(
    bars: pd.DataFrame,
    *,
    ema_fast_span: int,
    ema_slow_span: int,
    mode: str,
) -> pd.DataFrame:
    """Build EMA signals and tradable position series for a canonical bar frame."""
    if ema_fast_span <= 0 or ema_slow_span <= 0:
        raise ValueError(
            f"EMA spans must be positive. Got fast={ema_fast_span}, slow={ema_slow_span}."
        )
    if ema_fast_span >= ema_slow_span:
        raise ValueError(
            f"EMA span contract requires fast < slow. Got fast={ema_fast_span}, slow={ema_slow_span}."
        )

    valid_modes = {"trend_long_short", "trend_long_only", "trend_short_only"}
    if mode not in valid_modes:
        raise ValueError(f"Unsupported EMA mode {mode!r}. Allowed: {sorted(valid_modes)}")

    required = ["ts", "open", "high", "low", "close"]
    missing = [c for c in required if c not in bars.columns]
    if missing:
        raise OhlcDataError("Missing required bar column(s) for EMA signal generation: " + str(missing))

    out = bars.copy(deep=True).sort_values("ts", ascending=True).reset_index(drop=True)
    out["ema_fast"] = out["close"].ewm(span=ema_fast_span, adjust=False).mean()
    out["ema_slow"] = out["close"].ewm(span=ema_slow_span, adjust=False).mean()
    out["signal"] = np.sign(out["ema_fast"] - out["ema_slow"]).astype(float)

    if mode == "trend_long_only":
        out["signal"] = out["signal"].clip(lower=0.0, upper=1.0)
    elif mode == "trend_short_only":
        out["signal"] = out["signal"].clip(lower=-1.0, upper=0.0)

    out["position"] = out["signal"].shift(1).fillna(0.0)
    return out


def run_point_backtest(bars: pd.DataFrame, *, commission_points: float) -> pd.DataFrame:
    """Run point-based bar-level backtest with explicit next-bar execution semantics.

    Contract:
    - signal is formed on bar close t
    - position[t] is active from open[t] onward because it is signal[t-1]
    - pnl for bar t is measured from open[t] to open[t+1]
    - last bar is force-closed at close[t]
    """
    required = ["ts", "open", "close", "position"]
    missing = [c for c in required if c not in bars.columns]
    if missing:
        raise OhlcDataError("Missing required bar column(s) for backtest: " + str(missing))

    out = bars.copy(deep=True).sort_values("ts", ascending=True).reset_index(drop=True)
    out["trades"] = out["position"].diff().abs().fillna(out["position"].abs())
    out["fee"] = out["trades"] * float(commission_points)
    out["next_open"] = out["open"].shift(-1)

    out["terminal_fee"] = 0.0
    if not out.empty:
        out.loc[out.index[-1], "terminal_fee"] = abs(float(out.iloc[-1]["position"])) * float(commission_points)

    out["pnl_bar"] = np.where(
        out["next_open"].notna(),
        out["position"] * (out["next_open"] - out["open"]),
        out["position"] * (out["close"] - out["open"]),
    )
    out["pnl_bar"] = out["pnl_bar"] - out["fee"] - out["terminal_fee"]
    return out


def summarize_backtest_by_day(bars: pd.DataFrame) -> pd.DataFrame:
    """Aggregate bar-level PnL and trade counts to daily results."""
    required = ["ts", "pnl_bar", "trades"]
    missing = [c for c in required if c not in bars.columns]
    if missing:
        raise OhlcDataError("Missing required bar column(s) for day summary: " + str(missing))

    work = bars.copy(deep=True)
    work["date"] = pd.to_datetime(work["ts"], errors="coerce").dt.normalize()
    work = work.dropna(subset=["date"])

    days = (
        work.groupby("date", as_index=False)
        .agg(pnl_day=("pnl_bar", "sum"), num_trades_day=("trades", "sum"))
        .sort_values("date", ascending=True)
        .reset_index(drop=True)
    )
    days["num_trades_day"] = days["num_trades_day"].astype(float)
    days["cum_pnl_day"] = days["pnl_day"].cumsum()
    days["dd_day"] = days["cum_pnl_day"] - days["cum_pnl_day"].cummax()
    return days


def summarize_day_segment(days: pd.DataFrame, *, near_zero_threshold: float) -> dict[str, Any]:
    """Build segment-level summary metrics from daily backtest output."""
    if days.empty:
        return {
            "pnl_day_mean": 0.0,
            "win_rate": 0.0,
            "near_zero_rate": 0.0,
            "total_pnl": 0.0,
            "num_days": 0,
            "num_trades": 0,
            "max_dd": 0.0,
        }

    pnl = pd.to_numeric(days["pnl_day"], errors="coerce").fillna(0.0)
    trades = pd.to_numeric(days.get("num_trades_day", 0.0), errors="coerce").fillna(0.0)
    dd = pd.to_numeric(days.get("dd_day", 0.0), errors="coerce").fillna(0.0)

    return {
        "pnl_day_mean": float(pnl.mean()),
        "win_rate": float((pnl > 0.0).mean()),
        "near_zero_rate": float((pnl.abs() <= float(near_zero_threshold)).mean()),
        "total_pnl": float(pnl.sum()),
        "num_days": int(len(days)),
        "num_trades": int(round(float(trades.sum()))),
        "max_dd": float(dd.min()),
    }


def summarize_by_day(*args, **kwargs):
    return summarize_backtest_by_day(*args, **kwargs)


def summarize_segment(*args, **kwargs):
    return summarize_day_segment(*args, **kwargs)
