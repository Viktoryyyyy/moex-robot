from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.moex_features.daily.usdrubf_d1_ema_3_19_classical_indicators import (
    INDICATOR_COLUMNS,
    INSTRUMENT_ID,
    TIMEZONE_NAME,
    build_classical_indicators_frame,
    read_d1_ohlc_artifact,
)

_KEYS = ("instrument_id", "end")
_REQUIRED_EVENTS = (*_KEYS, "cross_dir")
_DIRECTIONS = {"cross_up", "cross_down"}
_FORBIDDEN_EXACT = {
    "entry_open", "exit_open", "outcome_open", "next_open",
    "reverse_label_censored", "holding_sessions_to_reverse_exit",
}
_FORBIDDEN_PREFIXES = tuple(
    "ret_ return_ signed_ret_ signed_return_ allow_trade max_adverse_ max_favorable_ "
    "entry_session_ exit_session_ completion_ reverse_ opposite_cross_".split()
)
_FORBIDDEN_MARKERS = ("future_outcome", "target_value", "label_censored")


def _end(values: pd.Series, timezone_name: str) -> pd.Series:
    result = pd.to_datetime(values, errors="coerce")
    if result.isna().any():
        raise ValueError("invalid timestamp values in column end")
    if getattr(result.dt, "tz", None) is not None:
        result = result.dt.tz_convert(timezone_name).dt.tz_localize(None)
    return result


def _is_forbidden(column: object) -> bool:
    value = str(column).strip().lower()
    return (
        value in _FORBIDDEN_EXACT
        or value.startswith(_FORBIDDEN_PREFIXES)
        or any(marker in value for marker in _FORBIDDEN_MARKERS)
    )


def forbidden_context_columns(frame: pd.DataFrame) -> list[str]:
    return sorted(str(column) for column in frame.columns if _is_forbidden(column))


def _events(frame: pd.DataFrame, instrument_id: str, timezone_name: str) -> pd.DataFrame:
    missing = [column for column in _REQUIRED_EVENTS if column not in frame.columns]
    if missing:
        raise ValueError("missing required crossover columns: " + ", ".join(missing))
    work = frame.copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    work["end"] = _end(work["end"], timezone_name)
    if (work["instrument_id"] != instrument_id).any():
        raise ValueError("all crossover instrument_id values must equal 'usdrubf'")
    if work.duplicated(list(_KEYS)).any():
        raise ValueError("duplicate crossover instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("crossover rows must be chronological by end")
    invalid = sorted(set(work["cross_dir"].dropna().astype(str)) - _DIRECTIONS)
    if invalid or work["cross_dir"].isna().any():
        raise ValueError("unsupported cross_dir values: " + ", ".join(invalid or ["null"]))
    work["cross_dir"] = work["cross_dir"].astype(str)
    return work.reset_index(drop=True)


def _indicators(frame: pd.DataFrame, instrument_id: str, timezone_name: str) -> pd.DataFrame:
    required = [*_KEYS, "session_index", *INDICATOR_COLUMNS, "indicator_ready"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError("missing required indicator columns: " + ", ".join(missing))
    work = frame[required].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    work["end"] = _end(work["end"], timezone_name)
    if (work["instrument_id"] != instrument_id).any():
        raise ValueError("all indicator instrument_id values must equal 'usdrubf'")
    if work.duplicated(list(_KEYS)).any():
        raise ValueError("duplicate indicator instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("indicator rows must be chronological by end")
    index = pd.to_numeric(work["session_index"], errors="coerce")
    if index.isna().any() or not index.eq(range(len(work))).all():
        raise ValueError("indicator session_index must be zero-based chronological 0..N-1")
    work["session_index"] = index.astype("int64")
    values = work[list(INDICATOR_COLUMNS)].apply(pd.to_numeric, errors="coerce")
    values = values.replace([np.inf, -np.inf], np.nan)
    work[list(INDICATOR_COLUMNS)] = values
    work["indicator_ready"] = values.notna().all(axis=1)
    return work.reset_index(drop=True)


def build_ema_3_19_indicator_context_frame(
    crossover_context_frame: pd.DataFrame,
    indicator_frame: pd.DataFrame,
    *,
    instrument_id: str = INSTRUMENT_ID,
    timezone_name: str = TIMEZONE_NAME,
) -> pd.DataFrame:
    if instrument_id != INSTRUMENT_ID:
        raise ValueError("instrument_id must equal 'usdrubf'")
    events = _events(crossover_context_frame, instrument_id, timezone_name)
    indicators = _indicators(indicator_frame, instrument_id, timezone_name)
    payload = ["session_index", *INDICATOR_COLUMNS, "indicator_ready"]
    columns = [column for column in events.columns if not _is_forbidden(column) and column not in payload]
    if any(column not in columns for column in _REQUIRED_EVENTS):
        raise ValueError("required event column cannot be filtered")
    left = events[columns].copy()
    expected_cross_dir = left["cross_dir"].reset_index(drop=True)
    left["_event_order"] = range(len(left))
    joined = left.merge(
        indicators[[*_KEYS, *payload]],
        on=list(_KEYS), how="left", validate="one_to_one", indicator=True, sort=False,
    )
    if (joined["_merge"] != "both").any():
        raise ValueError("every crossover instrument_id/end key must match exactly one D1 indicator row")
    joined = joined.sort_values("_event_order", kind="stable").drop(columns=["_event_order", "_merge"])
    joined = joined.reset_index(drop=True)
    if not joined["cross_dir"].equals(expected_cross_dir):
        raise ValueError("cross_dir changed during event-to-indicator join")
    forbidden = forbidden_context_columns(joined)
    if forbidden:
        raise ValueError("indicator context contains return, label, or future-outcome columns: " + ", ".join(forbidden))
    return joined


def read_crossover_context_artifact(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise ValueError("crossover context artifact path must end with .csv or .parquet")


def materialize_feature_frame(
    *,
    crossover_context_frame: pd.DataFrame | None = None,
    d1_ohlc_frame: pd.DataFrame | None = None,
    indicator_frame: pd.DataFrame | None = None,
    crossover_context_path: str | Path | None = None,
    d1_ohlc_path: str | Path | None = None,
    instrument_id: str = INSTRUMENT_ID,
    timezone_name: str = TIMEZONE_NAME,
) -> pd.DataFrame:
    if crossover_context_frame is None:
        if crossover_context_path is None:
            raise ValueError("either crossover_context_frame or crossover_context_path is required")
        crossover_context_frame = read_crossover_context_artifact(crossover_context_path)
    elif crossover_context_path is not None:
        raise ValueError("provide crossover_context_frame or crossover_context_path, not both")
    if indicator_frame is None:
        if d1_ohlc_frame is None:
            if d1_ohlc_path is None:
                raise ValueError("D1 OHLC input is required when indicator_frame is not supplied")
            d1_ohlc_frame = read_d1_ohlc_artifact(d1_ohlc_path)
        elif d1_ohlc_path is not None:
            raise ValueError("provide d1_ohlc_frame or d1_ohlc_path, not both")
        indicator_frame = build_classical_indicators_frame(
            d1_ohlc_frame, instrument_id=instrument_id, timezone_name=timezone_name
        )
    return build_ema_3_19_indicator_context_frame(
        crossover_context_frame, indicator_frame,
        instrument_id=instrument_id, timezone_name=timezone_name,
    )
