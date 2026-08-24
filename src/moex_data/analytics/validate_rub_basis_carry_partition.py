from __future__ import annotations

import math
from datetime import date
from pathlib import Path

import pandas as pd

RATE_COLUMNS = ("spot_rate", "perpetual_rate", "front_rate", "next_rate")
IDENTITY_COLUMNS = ("instrument_id", "trade_date", "ts", "alignment_policy")


class BasisCarryPartitionValidationError(ValueError):
    pass


def _fail(message: str) -> None:
    raise BasisCarryPartitionValidationError(message)


def validate_partition(
    path: str | Path,
    *,
    expected_instrument_id: str,
    expected_trade_date: str,
    expected_row_count: int,
) -> dict[str, object]:
    partition_path = Path(path)
    if not partition_path.is_file():
        _fail("derived partition must be an existing regular file")
    try:
        frame = pd.read_parquet(partition_path)
    except Exception as exc:
        raise BasisCarryPartitionValidationError("derived partition is not readable parquet: " + str(exc)) from exc

    if len(frame.index) != expected_row_count or expected_row_count <= 0:
        _fail("derived partition row_count mismatch")
    missing = [column for column in (*IDENTITY_COLUMNS, *RATE_COLUMNS) if column not in frame.columns]
    if missing:
        _fail("derived partition missing required columns: " + ",".join(missing))

    instruments = {str(value).strip() for value in frame["instrument_id"].dropna().unique()}
    if instruments != {expected_instrument_id}:
        _fail("derived partition instrument_id mismatch")
    trade_dates = {str(value).strip() for value in frame["trade_date"].dropna().unique()}
    if trade_dates != {expected_trade_date}:
        _fail("derived partition trade_date mismatch")
    try:
        date.fromisoformat(expected_trade_date)
    except ValueError as exc:
        raise BasisCarryPartitionValidationError("expected_trade_date must be YYYY-MM-DD") from exc

    if not frame["alignment_policy"].astype(str).eq("exact_timestamp_inner_join").all():
        _fail("derived partition alignment_policy mismatch")

    parsed_ts = pd.to_datetime(frame["ts"], errors="coerce")
    if parsed_ts.isna().any():
        _fail("derived partition contains invalid ts")
    timezone_value = getattr(parsed_ts.dt, "tz", None)
    if timezone_value is None:
        _fail("derived partition ts must be timezone-aware")
    utc_ts = parsed_ts.dt.tz_convert("UTC")
    if utc_ts.duplicated().any():
        _fail("derived partition contains duplicate ts")
    if not utc_ts.is_monotonic_increasing:
        _fail("derived partition ts must be monotonic increasing")

    for column in RATE_COLUMNS:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if numeric.isna().any():
            _fail("derived partition contains nonnumeric rate: " + column)
        numeric = numeric.astype(float)
        if (numeric <= 0).any() or not numeric.map(math.isfinite).all():
            _fail("derived partition rate must be finite and positive: " + column)

    return {
        "row_count": int(len(frame.index)),
        "instrument_id": expected_instrument_id,
        "trade_date": expected_trade_date,
        "first_ts_utc": utc_ts.iloc[0].isoformat(),
        "last_ts_utc": utc_ts.iloc[-1].isoformat(),
        "duplicate_ts_count": int(utc_ts.duplicated().sum()),
        "rates_valid": True,
        "physical_readback_passed": True,
    }
