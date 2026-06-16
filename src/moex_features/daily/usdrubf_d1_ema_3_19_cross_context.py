from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.moex_features.daily.usdrubf_d1_ohlc_from_5m import build_d1_ohlc_from_5m_frame

_REQUIRED_D1_COLUMNS = ("instrument_id", "end", "open", "high", "low", "close")
_PRICE_COLUMNS = ("open", "high", "low", "close")
_LABEL_LIKE_PREFIXES = ("signed_ret_", "allow_trade_", "max_adverse_", "max_favorable_")
SLOW_EMA_WINDOW = 19
FAST_EMA_WINDOW = 3


def ema_alpha(window: int) -> float:
    if window < 1:
        raise ValueError("EMA window must be positive")
    return 2.0 / float(window + 1)


def calculate_ema(values: pd.Series, *, window: int) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.isna().any():
        raise ValueError("EMA input contains non-numeric or missing values")
    return numeric.ewm(alpha=ema_alpha(window), adjust=False).mean()


def _normalize_d1_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in _REQUIRED_D1_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("missing required columns: " + ", ".join(missing))

    selected = list(_REQUIRED_D1_COLUMNS)
    if "volume" in frame.columns:
        selected.append("volume")

    work = frame[selected].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != "usdrubf").any():
        raise ValueError("all instrument_id values must equal 'usdrubf'")

    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    if work["end"].isna().any():
        raise ValueError("invalid timestamp values in column end")
    if work["end"].duplicated().any():
        raise ValueError("duplicate finalized D1 timestamps found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("end timestamps must be strictly increasing finalized D1 bars")

    for column in _PRICE_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    if work[list(_PRICE_COLUMNS)].isna().any().any():
        raise ValueError("non-numeric or missing OHLC values found")

    if "volume" in work.columns:
        work["volume"] = pd.to_numeric(work["volume"], errors="coerce")
        if work["volume"].isna().any():
            raise ValueError("non-numeric or missing volume values found")

    if work.empty:
        raise ValueError("D1 input has zero rows")
    return work.reset_index(drop=True)


def _cross_direction(ema_diff_prev: float, ema_diff: float) -> str | None:
    if ema_diff_prev <= 0.0 and ema_diff > 0.0:
        return "cross_up"
    if ema_diff_prev >= 0.0 and ema_diff < 0.0:
        return "cross_down"
    return None


def build_ema_3_19_cross_context_frame(d1_frame: pd.DataFrame) -> pd.DataFrame:
    work = _normalize_d1_frame(d1_frame)
    work["ema3"] = calculate_ema(work["close"], window=FAST_EMA_WINDOW)
    work["ema19"] = calculate_ema(work["close"], window=SLOW_EMA_WINDOW)
    work["ema_diff"] = work["ema3"] - work["ema19"]
    work["ema_diff_prev"] = work["ema_diff"].shift(1)

    ret_1d_raw = work["close"].pct_change(1)
    work["ret_1d"] = ret_1d_raw
    work["ret_3d"] = work["close"].pct_change(3)
    work["ret_5d"] = work["close"].pct_change(5)
    work["rolling_vol_5d"] = ret_1d_raw.rolling(window=5, min_periods=5).std()
    work["rolling_vol_20d"] = ret_1d_raw.rolling(window=20, min_periods=20).std()

    event_rows: list[dict[str, object]] = []
    previous_cross_index: int | None = None
    for index, row in work.iterrows():
        if index < SLOW_EMA_WINDOW:
            continue
        ema_diff_prev = row["ema_diff_prev"]
        if pd.isna(ema_diff_prev):
            continue
        cross_dir = _cross_direction(float(ema_diff_prev), float(row["ema_diff"]))
        if cross_dir is None:
            continue

        event: dict[str, object] = {
            "instrument_id": row["instrument_id"],
            "end": row["end"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "ema3": float(row["ema3"]),
            "ema19": float(row["ema19"]),
            "ema_diff": float(row["ema_diff"]),
            "ema_diff_prev": float(row["ema_diff_prev"]),
            "cross_dir": cross_dir,
            "bars_since_prev_cross": pd.NA if previous_cross_index is None else int(index - previous_cross_index),
            "ret_1d": row["ret_1d"],
            "ret_3d": row["ret_3d"],
            "ret_5d": row["ret_5d"],
            "rolling_vol_5d": row["rolling_vol_5d"],
            "rolling_vol_20d": row["rolling_vol_20d"],
        }
        if "volume" in work.columns:
            event["volume"] = float(row["volume"])
        for column in event:
            if column.startswith(_LABEL_LIKE_PREFIXES):
                raise ValueError("label-like values must not enter feature rows")
        event_rows.append(event)
        previous_cross_index = int(index)

    columns = [
        "instrument_id",
        "end",
        "open",
        "high",
        "low",
        "close",
    ]
    if "volume" in work.columns:
        columns.append("volume")
    columns.extend(
        [
            "ema3",
            "ema19",
            "ema_diff",
            "ema_diff_prev",
            "cross_dir",
            "bars_since_prev_cross",
            "ret_1d",
            "ret_3d",
            "ret_5d",
            "rolling_vol_5d",
            "rolling_vol_20d",
        ]
    )
    return pd.DataFrame(event_rows, columns=columns)


def materialize_feature_frame(
    *,
    d1_ohlc_frame: pd.DataFrame | None = None,
    dataset_artifact_path: str | Path | None = None,
    instrument_id: str = "usdrubf",
    timezone_name: str = "Europe/Moscow",
) -> pd.DataFrame:
    if d1_ohlc_frame is None:
        if dataset_artifact_path is None:
            raise ValueError("either d1_ohlc_frame or dataset_artifact_path is required")
        source = pd.read_csv(Path(dataset_artifact_path))
        d1_ohlc_frame = build_d1_ohlc_from_5m_frame(
            source,
            instrument_id=instrument_id,
            timezone_name=timezone_name,
        )
    return build_ema_3_19_cross_context_frame(d1_ohlc_frame)
