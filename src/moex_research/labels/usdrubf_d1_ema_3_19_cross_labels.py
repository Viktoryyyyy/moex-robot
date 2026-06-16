from __future__ import annotations

import pandas as pd

_REQUIRED_EVENT_COLUMNS = ("instrument_id", "end", "cross_dir")
_REQUIRED_D1_COLUMNS = ("instrument_id", "end", "open", "high", "low", "close")
_VALID_CROSS_DIRECTIONS = {"cross_up": 1.0, "cross_down": -1.0}


def _normalize_d1_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in _REQUIRED_D1_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("missing required D1 columns: " + ", ".join(missing))

    work = frame[list(_REQUIRED_D1_COLUMNS)].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != "usdrubf").any():
        raise ValueError("all D1 instrument_id values must equal 'usdrubf'")
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    if work["end"].isna().any():
        raise ValueError("invalid timestamp values in D1 column end")
    if work["end"].duplicated().any():
        raise ValueError("duplicate D1 end timestamps found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("D1 end timestamps must be strictly increasing")

    for column in ("open", "high", "low", "close"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    if work[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError("non-numeric or missing D1 OHLC values found")
    return work.reset_index(drop=True)


def _normalize_events(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in _REQUIRED_EVENT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("missing required event columns: " + ", ".join(missing))

    work = frame[list(_REQUIRED_EVENT_COLUMNS)].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != "usdrubf").any():
        raise ValueError("all event instrument_id values must equal 'usdrubf'")
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    if work["end"].isna().any():
        raise ValueError("invalid timestamp values in event column end")
    invalid_cross = sorted(set(work["cross_dir"]) - set(_VALID_CROSS_DIRECTIONS))
    if invalid_cross:
        raise ValueError("unsupported cross_dir values: " + ", ".join(str(item) for item in invalid_cross))
    if not work["end"].is_monotonic_increasing:
        raise ValueError("event end timestamps must be strictly increasing")
    return work.reset_index(drop=True)


def _signed_open_to_open(daily: pd.DataFrame, event_index: int, direction: float, horizon: int) -> float | None:
    entry_index = event_index + 1
    exit_index = event_index + 1 + horizon
    if exit_index >= len(daily):
        return None
    entry_open = float(daily.iloc[entry_index]["open"])
    exit_open = float(daily.iloc[exit_index]["open"])
    if entry_open == 0.0:
        raise ValueError("D+1 open must be non-zero")
    return direction * (exit_open - entry_open) / entry_open


def _excursions_h5(daily: pd.DataFrame, event_index: int, direction: float) -> tuple[float | None, float | None]:
    entry_index = event_index + 1
    final_index = event_index + 5
    if final_index >= len(daily):
        return None, None

    entry_open = float(daily.iloc[entry_index]["open"])
    if entry_open == 0.0:
        raise ValueError("D+1 open must be non-zero")

    horizon = daily.iloc[entry_index : final_index + 1]
    max_high = float(horizon["high"].max())
    min_low = float(horizon["low"].min())

    if direction > 0.0:
        favorable = (max_high - entry_open) / entry_open
        adverse = (min_low - entry_open) / entry_open
    else:
        favorable = (entry_open - min_low) / entry_open
        adverse = (entry_open - max_high) / entry_open
    return adverse, favorable


def build_ema_3_19_cross_labels(event_frame: pd.DataFrame, d1_ohlc_frame: pd.DataFrame) -> pd.DataFrame:
    events = _normalize_events(event_frame)
    daily = _normalize_d1_frame(d1_ohlc_frame)
    index_by_end = {pd.Timestamp(row["end"]): int(index) for index, row in daily.iterrows()}

    rows: list[dict[str, object]] = []
    for _, event in events.iterrows():
        event_end = pd.Timestamp(event["end"])
        if event_end not in index_by_end:
            raise ValueError("event end timestamp not found in D1 OHLC frame")
        event_index = index_by_end[event_end]
        direction = _VALID_CROSS_DIRECTIONS[str(event["cross_dir"])]

        h1 = _signed_open_to_open(daily, event_index, direction, 1)
        h2 = _signed_open_to_open(daily, event_index, direction, 2)
        h5 = _signed_open_to_open(daily, event_index, direction, 5)
        adverse_h5, favorable_h5 = _excursions_h5(daily, event_index, direction)
        allow_trade_h5 = int(h5 is not None and adverse_h5 is not None and favorable_h5 is not None)

        rows.append(
            {
                "instrument_id": event["instrument_id"],
                "end": event_end,
                "cross_dir": event["cross_dir"],
                "signed_ret_o2o_h1": h1,
                "signed_ret_o2o_h2": h2,
                "signed_ret_o2o_h5": h5,
                "allow_trade_h5": allow_trade_h5,
                "max_adverse_excursion_h5": adverse_h5,
                "max_favorable_excursion_h5": favorable_h5,
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "instrument_id",
            "end",
            "cross_dir",
            "signed_ret_o2o_h1",
            "signed_ret_o2o_h2",
            "signed_ret_o2o_h5",
            "allow_trade_h5",
            "max_adverse_excursion_h5",
            "max_favorable_excursion_h5",
        ],
    )


def materialize_label_frame(*, event_frame: pd.DataFrame, d1_ohlc_frame: pd.DataFrame) -> pd.DataFrame:
    return build_ema_3_19_cross_labels(event_frame, d1_ohlc_frame)
