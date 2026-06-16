from __future__ import annotations

from pathlib import Path

import pandas as pd

_REQUIRED_INTRADAY_COLUMNS = ("end", "open", "high", "low", "close")
_PRICE_COLUMNS = ("open", "high", "low", "close")


def _normalize_timestamps(values: pd.Series, timezone_name: str) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("invalid timestamp values in column end")

    try:
        tz = timestamps.dt.tz
    except AttributeError:
        tz = None

    if tz is not None:
        timestamps = timestamps.dt.tz_convert(timezone_name).dt.tz_localize(None)

    return timestamps


def _validate_strictly_increasing_end(frame: pd.DataFrame) -> None:
    if frame["end"].duplicated().any():
        raise ValueError("duplicate finalized timestamps found in column end")
    if not frame["end"].is_monotonic_increasing:
        raise ValueError("end timestamps must be strictly increasing finalized bars")


def normalize_intraday_5m_frame(
    frame: pd.DataFrame,
    *,
    instrument_id: str = "usdrubf",
    timezone_name: str = "Europe/Moscow",
) -> pd.DataFrame:
    if instrument_id != "usdrubf":
        raise ValueError("instrument_id must equal 'usdrubf'")

    missing = [column for column in _REQUIRED_INTRADAY_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("missing required columns: " + ", ".join(missing))

    selected = list(_REQUIRED_INTRADAY_COLUMNS)
    if "instrument_id" in frame.columns:
        selected.insert(0, "instrument_id")
    if "volume" in frame.columns:
        selected.append("volume")

    work = frame[selected].copy()
    if "instrument_id" not in work.columns:
        work.insert(0, "instrument_id", instrument_id)
    work["instrument_id"] = work["instrument_id"].fillna(instrument_id).astype(str)
    if (work["instrument_id"] != instrument_id).any():
        raise ValueError("all instrument_id values must equal 'usdrubf'")

    work["end"] = _normalize_timestamps(work["end"], timezone_name)
    _validate_strictly_increasing_end(work)

    for column in _PRICE_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    if work[list(_PRICE_COLUMNS)].isna().any().any():
        raise ValueError("non-numeric or missing OHLC values found")

    if "volume" in work.columns:
        work["volume"] = pd.to_numeric(work["volume"], errors="coerce")
        if work["volume"].isna().any():
            raise ValueError("non-numeric or missing volume values found")

    if work.empty:
        raise ValueError("input has zero finalized 5m rows")

    return work.reset_index(drop=True)


def _drop_incomplete_trailing_bucket(work: pd.DataFrame) -> pd.DataFrame:
    bucket_stats = (
        work.groupby("trade_date", sort=True)
        .agg(bar_count=("end", "size"), final_end=("end", "max"))
        .reset_index()
    )
    if len(bucket_stats) < 2:
        return work

    reference = bucket_stats.iloc[:-1]
    expected_count = int(reference["bar_count"].mode().iloc[0])
    expected_final_time = reference["final_end"].dt.time.mode().iloc[0]
    last = bucket_stats.iloc[-1]
    last_count = int(last["bar_count"])
    last_final_time = pd.Timestamp(last["final_end"]).time()

    if last_count < expected_count and last_final_time != expected_final_time:
        last_trade_date = last["trade_date"]
        return work[work["trade_date"] != last_trade_date].reset_index(drop=True)
    return work


def build_d1_ohlc_from_5m_frame(
    frame: pd.DataFrame,
    *,
    instrument_id: str = "usdrubf",
    timezone_name: str = "Europe/Moscow",
) -> pd.DataFrame:
    work = normalize_intraday_5m_frame(frame, instrument_id=instrument_id, timezone_name=timezone_name)
    work["trade_date"] = work["end"].dt.normalize()
    work = _drop_incomplete_trailing_bucket(work)

    rows: list[dict[str, object]] = []
    for _, group in work.groupby("trade_date", sort=True):
        day = group.reset_index(drop=True)
        instrument_values = set(day["instrument_id"].astype(str))
        if instrument_values != {instrument_id}:
            raise ValueError("mixed instrument_id values found inside a D1 aggregation bucket")

        row: dict[str, object] = {
            "instrument_id": instrument_id,
            "end": pd.Timestamp(day.iloc[-1]["end"]),
            "open": float(day.iloc[0]["open"]),
            "high": float(day["high"].max()),
            "low": float(day["low"].min()),
            "close": float(day.iloc[-1]["close"]),
        }
        if "volume" in day.columns:
            row["volume"] = float(day["volume"].sum())
        rows.append(row)

    daily = pd.DataFrame(rows).reset_index(drop=True)
    if daily.empty:
        raise ValueError("D1 aggregation produced zero rows")
    _validate_strictly_increasing_end(daily)
    return daily


def materialize_d1_ohlc_from_5m(
    *,
    dataset_artifact_path: str | Path,
    instrument_id: str = "usdrubf",
    timezone_name: str = "Europe/Moscow",
) -> pd.DataFrame:
    path = Path(dataset_artifact_path)
    frame = pd.read_csv(path)
    return build_d1_ohlc_from_5m_frame(frame, instrument_id=instrument_id, timezone_name=timezone_name)


def materialize_feature_frame(
    *,
    dataset_artifact_path: str | Path,
    instrument_id: str = "usdrubf",
    timezone_name: str = "Europe/Moscow",
) -> pd.DataFrame:
    return materialize_d1_ohlc_from_5m(
        dataset_artifact_path=dataset_artifact_path,
        instrument_id=instrument_id,
        timezone_name=timezone_name,
    )
