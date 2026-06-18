from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd

from src.moex_features.daily.usdrubf_d1_classical_indicators import (
    INDICATOR_COLUMNS,
    INSTRUMENT_ID,
    TIMEZONE_NAME,
    build_classical_indicators_frame,
    read_d1_ohlc_artifact,
)

_JOIN_KEYS: Final = ("instrument_id", "end")
_REQUIRED_EVENT_COLUMNS: Final = ("instrument_id", "end", "cross_dir")
_VALID_CROSS_DIRECTIONS: Final = {"cross_up", "cross_down"}

_FORBIDDEN_EXACT_COLUMNS: Final = {
    "entry_open",
    "exit_open",
    "outcome_open",
    "next_open",
    "reverse_label_censored",
    "holding_sessions_to_reverse_exit",
}
_FORBIDDEN_PREFIXES: Final = (
    "ret_",
    "return_",
    "signed_ret_",
    "signed_return_",
    "allow_trade",
    "max_adverse_",
    "max_favorable_",
    "entry_session_",
    "exit_session_",
    "completion_",
    "reverse_",
    "opposite_cross_",
)
_FORBIDDEN_SUBSTRINGS: Final = ("future_outcome", "target_value", "label_censored")


def _normalize_end(values: pd.Series, *, timezone_name: str) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("invalid timestamp values in column end")
    try:
        timezone = timestamps.dt.tz
    except AttributeError:
        timezone = None
    if timezone is not None:
        timestamps = timestamps.dt.tz_convert(timezone_name).dt.tz_localize(None)
    return timestamps


def _is_forbidden_context_column(column: object) -> bool:
    lowered = str(column).strip().lower()
    return (
        lowered in _FORBIDDEN_EXACT_COLUMNS
        or lowered.startswith(_FORBIDDEN_PREFIXES)
        or any(marker in lowered for marker in _FORBIDDEN_SUBSTRINGS)
    )


def forbidden_context_columns(frame: pd.DataFrame) -> list[str]:
    return sorted(str(column) for column in frame.columns if _is_forbidden_context_column(column))


def _normalize_events(
    frame: pd.DataFrame,
    *,
    instrument_id: str,
    timezone_name: str,
) -> pd.DataFrame:
    missing = [column for column in _REQUIRED_EVENT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("missing required crossover columns: " + ", ".join(missing))

    work = frame.copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != instrument_id).any():
        raise ValueError("all crossover instrument_id values must equal 'usdrubf'")
    work["end"] = _normalize_end(work["end"], timezone_name=timezone_name)
    if work.duplicated(list(_JOIN_KEYS)).any():
        raise ValueError("duplicate crossover instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("crossover rows must be chronological by end")

    invalid = sorted(set(work["cross_dir"].dropna().astype(str)) - _VALID_CROSS_DIRECTIONS)
    if invalid or work["cross_dir"].isna().any():
        values = invalid or ["null"]
        raise ValueError("unsupported cross_dir values: " + ", ".join(values))
    work["cross_dir"] = work["cross_dir"].astype(str)
    return work.reset_index(drop=True)


def _normalize_indicators(
    frame: pd.DataFrame,
    *,
    instrument_id: str,
    timezone_name: str,
) -> pd.DataFrame:
    required = ["instrument_id", "end", "session_index", *INDICATOR_COLUMNS, "indicator_ready"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError("missing required indicator columns: " + ", ".join(missing))

    work = frame[required].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != instrument_id).any():
        raise ValueError("all indicator instrument_id values must equal 'usdrubf'")
    work["end"] = _normalize_end(work["end"], timezone_name=timezone_name)
    if work.duplicated(list(_JOIN_KEYS)).any():
        raise ValueError("duplicate indicator instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("indicator rows must be chronological by end")

    session_index = pd.to_numeric(work["session_index"], errors="coerce")
    if session_index.isna().any() or not session_index.eq(range(len(work))).all():
        raise ValueError("indicator session_index must be zero-based chronological 0..N-1")
    work["session_index"] = session_index.astype("int64")
    work["indicator_ready"] = work["indicator_ready"].astype(bool)
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

    events = _normalize_events(
        crossover_context_frame,
        instrument_id=instrument_id,
        timezone_name=timezone_name,
    )
    indicators = _normalize_indicators(
        indicator_frame,
        instrument_id=instrument_id,
        timezone_name=timezone_name,
    )

    safe_event_columns = [column for column in events.columns if not _is_forbidden_context_column(column)]
    for key in _REQUIRED_EVENT_COLUMNS:
        if key not in safe_event_columns:
            raise ValueError(f"required event column {key} cannot be filtered")

    indicator_payload_columns = ["session_index", *INDICATOR_COLUMNS, "indicator_ready"]
    safe_event_columns = [column for column in safe_event_columns if column not in indicator_payload_columns]
    event_payload = events[safe_event_columns].copy()
    original_cross_dir = event_payload["cross_dir"].copy()
    event_payload["_event_order"] = range(len(event_payload))

    joined = event_payload.merge(
        indicators[[*_JOIN_KEYS, *indicator_payload_columns]],
        on=list(_JOIN_KEYS),
        how="left",
        validate="one_to_one",
        indicator=True,
        sort=False,
    )
    unmatched = joined.loc[joined["_merge"] != "both", list(_JOIN_KEYS)]
    if not unmatched.empty:
        raise ValueError("every crossover instrument_id/end key must match exactly one D1 indicator row")

    joined = joined.sort_values("_event_order", kind="stable").drop(columns=["_event_order", "_merge"])
    joined = joined.reset_index(drop=True)
    if not joined["cross_dir"].equals(original_cross_dir.reset_index(drop=True)):
        raise ValueError("cross_dir changed during event-to-indicator join")

    forbidden = forbidden_context_columns(joined)
    if forbidden:
        raise ValueError("indicator context contains return, label, or future-outcome columns: " + ", ".join(forbidden))
    return joined


def read_crossover_context_artifact(path: str | Path) -> pd.DataFrame:
    artifact_path = Path(path)
    suffix = artifact_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(artifact_path)
    if suffix == ".parquet":
        return pd.read_parquet(artifact_path)
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
            d1_ohlc_frame,
            instrument_id=instrument_id,
            timezone_name=timezone_name,
        )

    return build_ema_3_19_indicator_context_frame(
        crossover_context_frame,
        indicator_frame,
        instrument_id=instrument_id,
        timezone_name=timezone_name,
    )
