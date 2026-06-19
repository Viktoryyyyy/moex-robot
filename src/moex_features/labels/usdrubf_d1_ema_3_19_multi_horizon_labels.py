from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd

from src.moex_features.daily.usdrubf_d1_ema_3_19_classical_indicators import (
    INSTRUMENT_ID,
    TIMEZONE_NAME,
    normalize_d1_ohlc_frame,
)

JOIN_KEYS: Final = ("instrument_id", "end")
HORIZONS: Final = {"h1": 1, "h2": 2, "h3": 3, "h5": 5, "h10": 10}
CROSS_DIRECTIONS: Final = {"cross_up": 1.0, "cross_down": -1.0}
BASE_OUTPUT_COLUMNS: Final = (
    "instrument_id", "end", "cross_dir", "event_session_index", "entry_session_index"
)
FIXED_HORIZON_COLUMNS: Final = tuple(
    column
    for horizon in HORIZONS
    for column in (
        f"{horizon}_completion_index",
        f"{horizon}_signed_return",
        f"{horizon}_allow_trade",
        f"{horizon}_opposite_cross_before_exit",
    )
)
REVERSE_COLUMNS: Final = (
    "reverse_event_session_index",
    "reverse_completion_index",
    "holding_sessions_to_reverse_exit",
    "reverse_signed_return",
    "reverse_allow_trade",
    "reverse_label_censored",
)
OUTPUT_COLUMNS: Final = (*BASE_OUTPUT_COLUMNS, *FIXED_HORIZON_COLUMNS, *REVERSE_COLUMNS)


def _normalize_end(values: pd.Series, timezone_name: str) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("invalid timestamp values in crossover column end")
    if getattr(timestamps.dt, "tz", None) is not None:
        timestamps = timestamps.dt.tz_convert(timezone_name).dt.tz_localize(None)
    return timestamps


def _normalize_events(frame: pd.DataFrame, instrument_id: str, timezone_name: str) -> pd.DataFrame:
    required = ["instrument_id", "end", "cross_dir"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError("missing required crossover columns: " + ", ".join(missing))
    work = frame[required].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    work["end"] = _normalize_end(work["end"], timezone_name)
    if (work["instrument_id"] != instrument_id).any():
        raise ValueError("all crossover instrument_id values must equal 'usdrubf'")
    if work.duplicated(list(JOIN_KEYS)).any():
        raise ValueError("duplicate crossover instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("crossover rows must be chronological by end")
    work["cross_dir"] = work["cross_dir"].astype(str)
    invalid = sorted(set(work["cross_dir"]) - set(CROSS_DIRECTIONS))
    if invalid:
        raise ValueError("unsupported cross_dir values: " + ", ".join(invalid))
    return work.reset_index(drop=True)


def _event_session_indices(events: pd.DataFrame, daily: pd.DataFrame) -> list[int]:
    keys = daily[list(JOIN_KEYS)].copy()
    keys["event_session_index"] = range(len(keys))
    joined = events[list(JOIN_KEYS)].merge(
        keys,
        on=list(JOIN_KEYS),
        how="left",
        validate="one_to_one",
        indicator=True,
        sort=False,
    )
    if (joined["_merge"] != "both").any():
        raise ValueError("every supplied crossover key must match exactly one D1 session")
    return joined["event_session_index"].astype("int64").tolist()


def _signed_return(
    daily: pd.DataFrame,
    entry_index: int | None,
    exit_index: int | None,
    direction: float,
) -> float | None:
    if entry_index is None or exit_index is None:
        return None
    entry_open = float(daily.iloc[entry_index]["open"])
    exit_open = float(daily.iloc[exit_index]["open"])
    if entry_open == 0.0 or not np.isfinite(entry_open) or not np.isfinite(exit_open):
        return None
    value = direction * (exit_open / entry_open - 1.0)
    return float(value) if np.isfinite(value) else None


def _allow_trade(value: float | None) -> int | None:
    return None if value is None or pd.isna(value) else int(float(value) > 0.0)


def _first_opposite(
    event_position: int,
    event_index: int,
    cross_dir: str,
    event_indices: list[int],
    directions: list[str],
) -> int | None:
    for later_position in range(event_position + 1, len(event_indices)):
        later_index = event_indices[later_position]
        if later_index <= event_index:
            raise ValueError("later crossover events must have strictly later D1 session indices")
        if directions[later_position] != cross_dir:
            return later_index
    return None


def _coerce(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.reindex(columns=OUTPUT_COLUMNS).copy()
    integer_columns = [
        "event_session_index",
        "entry_session_index",
        *[f"{horizon}_completion_index" for horizon in HORIZONS],
        *[f"{horizon}_allow_trade" for horizon in HORIZONS],
        "reverse_event_session_index",
        "reverse_completion_index",
        "holding_sessions_to_reverse_exit",
        "reverse_allow_trade",
    ]
    float_columns = [*[f"{horizon}_signed_return" for horizon in HORIZONS], "reverse_signed_return"]
    boolean_columns = [
        *[f"{horizon}_opposite_cross_before_exit" for horizon in HORIZONS],
        "reverse_label_censored",
    ]
    for column in integer_columns:
        out[column] = pd.array(out[column], dtype="Int64")
    for column in float_columns:
        out[column] = pd.array(out[column], dtype="Float64")
    for column in boolean_columns:
        out[column] = pd.array(out[column], dtype="boolean")
    return out


def build_multi_horizon_labels_frame(
    crossover_context_frame: pd.DataFrame,
    d1_ohlc_frame: pd.DataFrame,
    *,
    instrument_id: str = INSTRUMENT_ID,
    timezone_name: str = TIMEZONE_NAME,
) -> pd.DataFrame:
    if instrument_id != INSTRUMENT_ID:
        raise ValueError("instrument_id must equal 'usdrubf'")
    daily = normalize_d1_ohlc_frame(
        d1_ohlc_frame, instrument_id=instrument_id, timezone_name=timezone_name
    )
    events = _normalize_events(crossover_context_frame, instrument_id, timezone_name)
    event_indices = _event_session_indices(events, daily)
    directions = events["cross_dir"].tolist()
    rows: list[dict[str, object]] = []

    for event_position, event in events.iterrows():
        event_index = event_indices[event_position]
        entry_index = event_index + 1 if event_index + 1 < len(daily) else None
        cross_dir = str(event["cross_dir"])
        direction = CROSS_DIRECTIONS[cross_dir]
        row: dict[str, object] = {
            "instrument_id": event["instrument_id"],
            "end": pd.Timestamp(event["end"]),
            "cross_dir": cross_dir,
            "event_session_index": event_index,
            "entry_session_index": entry_index,
        }
        opposite_index = _first_opposite(
            event_position, event_index, cross_dir, event_indices, directions
        )
        for horizon, holding_sessions in HORIZONS.items():
            exit_index = (
                entry_index + holding_sessions
                if entry_index is not None and entry_index + holding_sessions < len(daily)
                else None
            )
            value = _signed_return(daily, entry_index, exit_index, direction)
            row[f"{horizon}_completion_index"] = exit_index
            row[f"{horizon}_signed_return"] = value
            row[f"{horizon}_allow_trade"] = _allow_trade(value)
            row[f"{horizon}_opposite_cross_before_exit"] = (
                None if exit_index is None else bool(opposite_index is not None and opposite_index < exit_index)
            )

        reverse_exit_index = (
            opposite_index + 1
            if opposite_index is not None and opposite_index + 1 < len(daily)
            else None
        )
        reverse_value = _signed_return(daily, entry_index, reverse_exit_index, direction)
        censored = entry_index is None or opposite_index is None or reverse_exit_index is None or reverse_value is None
        row.update(
            {
                "reverse_event_session_index": opposite_index,
                "reverse_completion_index": reverse_exit_index,
                "holding_sessions_to_reverse_exit": (
                    reverse_exit_index - entry_index
                    if entry_index is not None and reverse_exit_index is not None
                    else None
                ),
                "reverse_signed_return": None if censored else reverse_value,
                "reverse_allow_trade": None if censored else _allow_trade(reverse_value),
                "reverse_label_censored": censored,
            }
        )
        rows.append(row)
    return _coerce(pd.DataFrame(rows))


def materialize_label_frame(
    *,
    crossover_context_frame: pd.DataFrame,
    d1_ohlc_frame: pd.DataFrame,
    instrument_id: str = INSTRUMENT_ID,
    timezone_name: str = TIMEZONE_NAME,
) -> pd.DataFrame:
    return build_multi_horizon_labels_frame(
        crossover_context_frame,
        d1_ohlc_frame,
        instrument_id=instrument_id,
        timezone_name=timezone_name,
    )
