from __future__ import annotations

from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

INSTRUMENT_ID: Final = "usdrubf"
CALENDAR_CONTRACT: Final = "moex_futures"
TIMEZONE_NAME: Final = "Europe/Moscow"

_REQUIRED_D1_COLUMNS: Final = ("instrument_id", "end", "open", "high", "low", "close")
_PRICE_COLUMNS: Final = ("open", "high", "low", "close")

INDICATOR_COLUMNS: Final = (
    "rsi_14",
    "roc_10",
    "stoch_k_14",
    "stoch_d_3",
    "adx_14",
    "di_spread_14",
    "macd_hist_12_26_9_pct",
    "atr_14_pct",
    "bb_percent_b_20_2",
    "bb_bandwidth_20_2",
)

OUTPUT_COLUMNS: Final = (
    "instrument_id",
    "end",
    "session_index",
    *INDICATOR_COLUMNS,
    "indicator_ready",
)


def _normalize_end(values: pd.Series, *, timezone_name: str) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("invalid timestamp values in D1 column end")

    try:
        timezone = timestamps.dt.tz
    except AttributeError:
        timezone = None
    if timezone is not None:
        timestamps = timestamps.dt.tz_convert(timezone_name).dt.tz_localize(None)
    return timestamps


def normalize_d1_ohlc_frame(
    frame: pd.DataFrame,
    *,
    instrument_id: str = INSTRUMENT_ID,
    timezone_name: str = TIMEZONE_NAME,
) -> pd.DataFrame:
    if instrument_id != INSTRUMENT_ID:
        raise ValueError("instrument_id must equal 'usdrubf'")

    missing = [column for column in _REQUIRED_D1_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("missing required D1 columns: " + ", ".join(missing))
    if frame.empty:
        raise ValueError("D1 input has zero rows")

    selected = list(_REQUIRED_D1_COLUMNS)
    if "volume" in frame.columns:
        selected.append("volume")
    if "finalized" in frame.columns:
        selected.append("finalized")

    work = frame[selected].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != instrument_id).any():
        raise ValueError("all D1 instrument_id values must equal 'usdrubf'")

    work["end"] = _normalize_end(work["end"], timezone_name=timezone_name)
    if work.duplicated(["instrument_id", "end"]).any():
        raise ValueError("duplicate D1 instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("D1 rows must be chronological by finalized end")

    if "finalized" in work.columns:
        finalized = work["finalized"]
        if finalized.isna().any() or not finalized.astype(bool).all():
            raise ValueError("all supplied D1 rows must be finalized")

    for column in _PRICE_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    numeric_prices = work[list(_PRICE_COLUMNS)]
    if numeric_prices.isna().any().any():
        raise ValueError("non-numeric or missing D1 OHLC values found")
    if not np.isfinite(numeric_prices.to_numpy(dtype=float)).all():
        raise ValueError("non-finite D1 OHLC values found")
    if (work["high"] < work["low"]).any():
        raise ValueError("D1 high must be greater than or equal to low")

    if "volume" in work.columns:
        work["volume"] = pd.to_numeric(work["volume"], errors="coerce")
        if work["volume"].isna().any():
            raise ValueError("non-numeric or missing D1 volume values found")
        if not np.isfinite(work["volume"].to_numpy(dtype=float)).all():
            raise ValueError("non-finite D1 volume values found")

    return work.reset_index(drop=True)


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    left = pd.to_numeric(numerator, errors="coerce").astype(float)
    right = pd.to_numeric(denominator, errors="coerce").astype(float)
    result = pd.Series(np.nan, index=left.index, dtype=float)
    valid = left.notna() & right.notna() & np.isfinite(left) & np.isfinite(right) & right.ne(0.0)
    result.loc[valid] = left.loc[valid] / right.loc[valid]
    return result.replace([np.inf, -np.inf], np.nan)


def wilder_average(values: pd.Series, *, period: int) -> pd.Series:
    if period < 1:
        raise ValueError("Wilder period must be positive")

    numeric = pd.to_numeric(values, errors="coerce").astype(float)
    output = pd.Series(np.nan, index=numeric.index, dtype=float)
    if numeric.empty:
        return output

    start_position: int | None = None
    for position in range(period - 1, len(numeric)):
        window = numeric.iloc[position - period + 1 : position + 1]
        if window.notna().all() and np.isfinite(window.to_numpy(dtype=float)).all():
            output.iloc[position] = float(window.mean())
            start_position = position
            break

    if start_position is None:
        return output

    previous = float(output.iloc[start_position])
    for position in range(start_position + 1, len(numeric)):
        current = numeric.iloc[position]
        if pd.isna(current) or not np.isfinite(float(current)):
            output.iloc[position] = np.nan
            previous = np.nan
            continue
        if not np.isfinite(previous):
            output.iloc[position] = np.nan
            continue
        previous = ((previous * float(period - 1)) + float(current)) / float(period)
        output.iloc[position] = previous
    return output


def _rsi_14(close: pd.Series) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0.0)
    losses = (-delta).clip(lower=0.0)
    average_gain = wilder_average(gains, period=14)
    average_loss = wilder_average(losses, period=14)

    relative_strength = _safe_divide(average_gain, average_loss)
    rsi = 100.0 - (100.0 / (1.0 + relative_strength))
    rsi.loc[average_loss.eq(0.0) & average_gain.gt(0.0)] = 100.0
    rsi.loc[average_gain.eq(0.0) & average_loss.gt(0.0)] = 0.0
    rsi.loc[average_gain.eq(0.0) & average_loss.eq(0.0)] = np.nan
    return rsi.replace([np.inf, -np.inf], np.nan)


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    previous_close = close.shift(1)
    components = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    )
    return components.max(axis=1, skipna=True).astype(float)


def _directional_indicators(
    high: pd.Series,
    low: pd.Series,
    true_range: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    upward_move = high.diff()
    downward_move = -low.diff()
    plus_dm = pd.Series(
        np.where((upward_move > downward_move) & (upward_move > 0.0), upward_move, 0.0),
        index=high.index,
        dtype=float,
    )
    minus_dm = pd.Series(
        np.where((downward_move > upward_move) & (downward_move > 0.0), downward_move, 0.0),
        index=high.index,
        dtype=float,
    )
    if len(plus_dm):
        plus_dm.iloc[0] = 0.0
        minus_dm.iloc[0] = 0.0

    smoothed_tr = wilder_average(true_range, period=14)
    smoothed_plus_dm = wilder_average(plus_dm, period=14)
    smoothed_minus_dm = wilder_average(minus_dm, period=14)

    plus_di = 100.0 * _safe_divide(smoothed_plus_dm, smoothed_tr)
    minus_di = 100.0 * _safe_divide(smoothed_minus_dm, smoothed_tr)
    di_spread = plus_di - minus_di
    dx = 100.0 * _safe_divide((plus_di - minus_di).abs(), plus_di + minus_di)
    adx = wilder_average(dx, period=14)
    return plus_di, minus_di, adx


def build_classical_indicators_frame(
    d1_ohlc_frame: pd.DataFrame,
    *,
    instrument_id: str = INSTRUMENT_ID,
    timezone_name: str = TIMEZONE_NAME,
) -> pd.DataFrame:
    work = normalize_d1_ohlc_frame(
        d1_ohlc_frame,
        instrument_id=instrument_id,
        timezone_name=timezone_name,
    )
    close = work["close"].astype(float)
    high = work["high"].astype(float)
    low = work["low"].astype(float)

    indicators = pd.DataFrame(index=work.index)
    indicators["instrument_id"] = work["instrument_id"]
    indicators["end"] = work["end"]
    indicators["session_index"] = pd.Series(range(len(work)), dtype="int64")

    indicators["rsi_14"] = _rsi_14(close)
    indicators["roc_10"] = _safe_divide(close, close.shift(10)) - 1.0

    lowest_low_14 = low.rolling(window=14, min_periods=14).min()
    highest_high_14 = high.rolling(window=14, min_periods=14).max()
    stochastic_range = highest_high_14 - lowest_low_14
    indicators["stoch_k_14"] = 100.0 * _safe_divide(close - lowest_low_14, stochastic_range)
    indicators["stoch_d_3"] = indicators["stoch_k_14"].rolling(window=3, min_periods=3).mean()

    true_range = _true_range(high, low, close)
    plus_di, minus_di, adx = _directional_indicators(high, low, true_range)
    indicators["adx_14"] = adx
    indicators["di_spread_14"] = plus_di - minus_di

    ema_12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema_26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = ema_12 - ema_26
    signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    indicators["macd_hist_12_26_9_pct"] = _safe_divide(macd - signal, close)

    atr_14 = wilder_average(true_range, period=14)
    indicators["atr_14_pct"] = _safe_divide(atr_14, close)

    bb_middle = close.rolling(window=20, min_periods=20).mean()
    bb_std = close.rolling(window=20, min_periods=20).std(ddof=0)
    bb_upper = bb_middle + 2.0 * bb_std
    bb_lower = bb_middle - 2.0 * bb_std
    bb_width = bb_upper - bb_lower
    indicators["bb_percent_b_20_2"] = _safe_divide(close - bb_lower, bb_width)
    indicators["bb_bandwidth_20_2"] = _safe_divide(bb_width, bb_middle)

    indicators[list(INDICATOR_COLUMNS)] = indicators[list(INDICATOR_COLUMNS)].replace(
        [np.inf, -np.inf], np.nan
    )
    indicators["indicator_ready"] = indicators[list(INDICATOR_COLUMNS)].notna().all(axis=1)
    return indicators.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)


def read_d1_ohlc_artifact(path: str | Path) -> pd.DataFrame:
    artifact_path = Path(path)
    suffix = artifact_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(artifact_path)
    if suffix == ".parquet":
        return pd.read_parquet(artifact_path)
    raise ValueError("D1 OHLC artifact path must end with .csv or .parquet")


def materialize_feature_frame(
    *,
    d1_ohlc_frame: pd.DataFrame | None = None,
    d1_ohlc_path: str | Path | None = None,
    instrument_id: str = INSTRUMENT_ID,
    timezone_name: str = TIMEZONE_NAME,
) -> pd.DataFrame:
    if d1_ohlc_frame is None:
        if d1_ohlc_path is None:
            raise ValueError("either d1_ohlc_frame or d1_ohlc_path is required")
        d1_ohlc_frame = read_d1_ohlc_artifact(d1_ohlc_path)
    elif d1_ohlc_path is not None:
        raise ValueError("provide d1_ohlc_frame or d1_ohlc_path, not both")
    return build_classical_indicators_frame(
        d1_ohlc_frame,
        instrument_id=instrument_id,
        timezone_name=timezone_name,
    )
