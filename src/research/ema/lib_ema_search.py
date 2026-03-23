"""Foundational OHLC loading, normalization, and resampling utilities for EMA research."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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


def summarize_by_day(*args, **kwargs):
    return summarize_backtest_by_day(*args, **kwargs)


def summarize_segment(*args, **kwargs):
    return summarize_day_segment(*args, **kwargs)
