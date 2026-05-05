"""Shared on-demand higher-timeframe resampling for continuous MOEX futures bars.

The module is intentionally IO-free: callers pass already selected
futures_continuous_5m.v1 rows and a canonical MOEX futures session calendar
DataFrame covering the same interval.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


SOURCE_SCHEMA_VERSION = "futures_continuous_5m.v1"
OUTPUT_SCHEMA_VERSION = "futures_continuous_htf_ondemand_resampling.v1"
CALENDAR_ID = "moex_futures_session_calendar.v1"
CALENDAR_SCHEMA_VERSION = "moex_futures_session_calendar.v1"
CALENDAR_STATUS_CANONICAL = "canonical"
EXCHANGE_TIMEZONE = "Europe/Moscow"
EXPECTED_BAR_GRANULARITY = "5m"

TIMEFRAME_MINUTES: Mapping[str, int] = {
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
}

REQUIRED_SOURCE_COLUMNS: Sequence[str] = (
    "trade_date",
    "end",
    "session_date",
    "continuous_symbol",
    "family_code",
    "source_secid",
    "source_contract",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "roll_policy_id",
    "adjustment_policy_id",
    "adjustment_factor",
    "is_roll_boundary",
    "roll_map_id",
    "schema_version",
    "ingest_ts",
)

REQUIRED_CALENDAR_COLUMNS: Sequence[str] = (
    "calendar_id",
    "calendar_date",
    "session_date",
    "session_seq",
    "is_trading_day",
    "is_planned_non_trading_day",
    "session_interval_id",
    "session_start",
    "session_end",
    "exchange_timezone",
    "expected_bar_granularity",
    "expected_first_bar_end",
    "expected_last_bar_end",
    "expected_bar_count",
    "source_name",
    "source_endpoint_or_provider",
    "source_retrieved_at",
    "source_range_start",
    "source_range_end",
    "calendar_status",
    "schema_version",
)

OUTPUT_COLUMNS: Sequence[str] = (
    "trade_date",
    "session_date",
    "bucket_end",
    "timeframe",
    "continuous_symbol",
    "family_code",
    "source_secid",
    "source_contract",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "roll_policy_id",
    "adjustment_policy_id",
    "adjustment_factor",
    "contains_roll_boundary",
    "source_bar_count",
    "first_source_end",
    "last_source_end",
    "schema_version",
)

SOURCE_ORDER_GROUP_COLUMNS: Sequence[str] = (
    "continuous_symbol",
    "roll_policy_id",
    "adjustment_policy_id",
    "family_code",
    "trade_date",
    "session_date",
)

SOURCE_DUPLICATE_COLUMNS: Sequence[str] = SOURCE_ORDER_GROUP_COLUMNS + ("end",)

BUCKET_INVARIANT_COLUMNS: Sequence[str] = (
    "trade_date",
    "session_date",
    "continuous_symbol",
    "family_code",
    "source_secid",
    "source_contract",
    "roll_policy_id",
    "adjustment_policy_id",
    "adjustment_factor",
    "roll_map_id",
)

OUTPUT_PRIMARY_KEY_COLUMNS: Sequence[str] = (
    "continuous_symbol",
    "roll_policy_id",
    "adjustment_policy_id",
    "timeframe",
    "trade_date",
    "session_date",
    "bucket_end",
)


class HTFResamplingError(RuntimeError):
    """Fail-closed error raised when contract-safe HTF resampling is impossible."""


@dataclass(frozen=True)
class _CalendarInterval:
    session_date: str
    session_interval_id: str
    session_start: pd.Timestamp
    session_end: pd.Timestamp
    expected_first_bar_end: pd.Timestamp
    expected_last_bar_end: pd.Timestamp
    expected_bar_count: int


def resample_continuous_htf(
    source_bars: pd.DataFrame,
    session_calendar: pd.DataFrame,
    timeframe: str,
    session_calendar_id: str = CALENDAR_ID,
) -> pd.DataFrame:
    """Resample validated futures_continuous_5m.v1 rows into contract-safe HTF bars."""

    timeframe_text = str(timeframe).strip()
    if timeframe_text not in TIMEFRAME_MINUTES:
        raise HTFResamplingError("unsupported_timeframe:" + timeframe_text)

    source = _prepare_source_bars(source_bars)
    calendar = _prepare_session_calendar(session_calendar, session_calendar_id)

    if source.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    interval_table, expected_table = _build_expected_bar_tables(
        calendar=calendar,
        timeframe_minutes=TIMEFRAME_MINUTES[timeframe_text],
    )
    enriched = _attach_calendar_interval(source, interval_table)
    enriched = _attach_expected_bucket(enriched, expected_table)

    bucket_keys = (
        "continuous_symbol",
        "family_code",
        "roll_policy_id",
        "adjustment_policy_id",
        "trade_date",
        "session_date",
        "session_interval_id",
        "bucket_end",
    )

    _validate_bucket_invariants(enriched, bucket_keys)
    _validate_expected_bar_counts(enriched, expected_table, bucket_keys)

    output = _aggregate_buckets(enriched, timeframe_text, bucket_keys)
    _validate_output(output)

    return output.loc[:, list(OUTPUT_COLUMNS)].sort_values(
        list(OUTPUT_PRIMARY_KEY_COLUMNS)
    ).reset_index(drop=True)


def _prepare_source_bars(source_bars: pd.DataFrame) -> pd.DataFrame:
    if source_bars is None:
        raise HTFResamplingError("missing_source_dataframe")
    if not isinstance(source_bars, pd.DataFrame):
        raise HTFResamplingError("source_is_not_dataframe")

    missing = [col for col in REQUIRED_SOURCE_COLUMNS if col not in source_bars.columns]
    if missing:
        raise HTFResamplingError("missing_source_columns:" + ",".join(missing))

    source = source_bars.copy()
    if source[list(REQUIRED_SOURCE_COLUMNS)].isna().any(axis=None):
        null_cols = [col for col in REQUIRED_SOURCE_COLUMNS if source[col].isna().any()]
        raise HTFResamplingError("null_required_source_values:" + ",".join(null_cols))

    source["end"] = pd.to_datetime(source["end"], errors="coerce")
    if source["end"].isna().any():
        raise HTFResamplingError("invalid_source_end_timestamp")

    for col in ("open", "high", "low", "close", "volume", "adjustment_factor"):
        source[col] = pd.to_numeric(source[col], errors="coerce")
    if source[["open", "high", "low", "close", "volume", "adjustment_factor"]].isna().any(axis=None):
        raise HTFResamplingError("invalid_numeric_source_values")

    for col in (
        "trade_date",
        "session_date",
        "continuous_symbol",
        "family_code",
        "source_secid",
        "source_contract",
        "roll_policy_id",
        "adjustment_policy_id",
        "roll_map_id",
        "schema_version",
    ):
        source[col] = source[col].astype(str)

    if not (source["schema_version"] == SOURCE_SCHEMA_VERSION).all():
        raise HTFResamplingError("source_schema_mismatch")

    source["is_roll_boundary"] = source["is_roll_boundary"].map(_to_bool)

    invalid_ohlc = (
        (source["high"] < source["low"])
        | (source["open"] < source["low"])
        | (source["open"] > source["high"])
        | (source["close"] < source["low"])
        | (source["close"] > source["high"])
    )
    if bool(invalid_ohlc.any()):
        raise HTFResamplingError("invalid_source_ohlc")

    if source.duplicated(subset=list(SOURCE_DUPLICATE_COLUMNS)).any():
        raise HTFResamplingError("duplicate_source_timestamps")

    _validate_source_order_before_sort(source)

    return source.sort_values(
        list(SOURCE_ORDER_GROUP_COLUMNS) + ["end"]
    ).reset_index(drop=True)


def _validate_source_order_before_sort(source: pd.DataFrame) -> None:
    for group_key, group in source.groupby(list(SOURCE_ORDER_GROUP_COLUMNS), sort=False):
        if not group["end"].is_monotonic_increasing:
            raise HTFResamplingError(
                "non_monotonic_source_timestamps:" + "|".join(str(item) for item in group_key)
            )


def _prepare_session_calendar(
    session_calendar: pd.DataFrame,
    session_calendar_id: str,
) -> pd.DataFrame:
    if session_calendar is None:
        raise HTFResamplingError("missing_calendar_binding")
    if not isinstance(session_calendar, pd.DataFrame):
        raise HTFResamplingError("calendar_binding_is_not_dataframe")

    missing = [col for col in REQUIRED_CALENDAR_COLUMNS if col not in session_calendar.columns]
    if missing:
        raise HTFResamplingError("missing_calendar_columns:" + ",".join(missing))

    calendar = session_calendar.copy()
    if calendar.empty:
        raise HTFResamplingError("empty_calendar_binding")

    required_non_null = [
        col for col in REQUIRED_CALENDAR_COLUMNS if col != "is_planned_non_trading_day"
    ]
    if calendar[required_non_null].isna().any(axis=None):
        null_cols = [col for col in required_non_null if calendar[col].isna().any()]
        raise HTFResamplingError("null_required_calendar_values:" + ",".join(null_cols))

    for col in (
        "calendar_id",
        "session_date",
        "session_interval_id",
        "exchange_timezone",
        "expected_bar_granularity",
        "calendar_status",
        "schema_version",
    ):
        calendar[col] = calendar[col].astype(str)

    if str(session_calendar_id) != CALENDAR_ID:
        raise HTFResamplingError("unsupported_calendar_id:" + str(session_calendar_id))
    if set(calendar["calendar_id"].unique().tolist()) != {CALENDAR_ID}:
        raise HTFResamplingError("calendar_id_mismatch")
    if set(calendar["schema_version"].unique().tolist()) != {CALENDAR_SCHEMA_VERSION}:
        raise HTFResamplingError("calendar_schema_mismatch")
    if set(calendar["calendar_status"].unique().tolist()) != {CALENDAR_STATUS_CANONICAL}:
        raise HTFResamplingError("calendar_status_not_canonical")
    if set(calendar["exchange_timezone"].unique().tolist()) != {EXCHANGE_TIMEZONE}:
        raise HTFResamplingError("calendar_timezone_mismatch")
    if set(calendar["expected_bar_granularity"].unique().tolist()) != {EXPECTED_BAR_GRANULARITY}:
        raise HTFResamplingError("calendar_granularity_mismatch")

    calendar["is_trading_day"] = calendar["is_trading_day"].map(_to_bool)
    if not bool(calendar["is_trading_day"].all()):
        raise HTFResamplingError("calendar_contains_non_trading_day_interval")

    calendar["session_seq"] = pd.to_numeric(calendar["session_seq"], errors="coerce")
    calendar["expected_bar_count"] = pd.to_numeric(calendar["expected_bar_count"], errors="coerce")
    if calendar[["session_seq", "expected_bar_count"]].isna().any(axis=None):
        raise HTFResamplingError("invalid_calendar_numeric_values")

    for col in ("session_start", "session_end", "expected_first_bar_end", "expected_last_bar_end"):
        calendar[col] = pd.to_datetime(calendar[col], errors="coerce")
    if calendar[["session_start", "session_end", "expected_first_bar_end", "expected_last_bar_end"]].isna().any(axis=None):
        raise HTFResamplingError("invalid_calendar_timestamps")

    if calendar.duplicated(subset=["session_date", "session_interval_id"]).any():
        raise HTFResamplingError("duplicate_calendar_session_interval")

    calendar = calendar.sort_values(["session_date", "session_start", "session_end"]).reset_index(drop=True)
    _validate_calendar_intervals(calendar)
    return calendar


def _validate_calendar_intervals(calendar: pd.DataFrame) -> None:
    seq = calendar[["session_date", "session_seq"]].drop_duplicates().sort_values("session_seq")
    if seq["session_seq"].duplicated().any():
        raise HTFResamplingError("duplicate_calendar_session_seq")
    if not seq["session_seq"].is_monotonic_increasing:
        raise HTFResamplingError("non_monotonic_calendar_session_seq")

    for session_date, group in calendar.groupby("session_date", sort=False):
        previous_end: Optional[pd.Timestamp] = None
        session_seq_values = group["session_seq"].drop_duplicates()
        if len(session_seq_values) != 1:
            raise HTFResamplingError("mixed_calendar_session_seq:" + str(session_date))

        for _, row in group.iterrows():
            if row["session_start"] >= row["session_end"]:
                raise HTFResamplingError("invalid_calendar_session_interval:" + str(session_date))
            if row["expected_first_bar_end"] < row["session_start"]:
                raise HTFResamplingError("calendar_first_bar_before_session_start:" + str(session_date))
            if row["expected_last_bar_end"] > row["session_end"]:
                raise HTFResamplingError("calendar_last_bar_after_session_end:" + str(session_date))
            if int(row["expected_bar_count"]) <= 0:
                raise HTFResamplingError("calendar_non_positive_expected_bar_count:" + str(session_date))
            if previous_end is not None and row["session_start"] <= previous_end:
                raise HTFResamplingError("overlapping_calendar_session_intervals:" + str(session_date))
            previous_end = row["session_end"]


def _build_expected_bar_tables(
    calendar: pd.DataFrame,
    timeframe_minutes: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    interval_rows: List[Dict[str, object]] = []
    expected_rows: List[Dict[str, object]] = []
    source_frequency = pd.Timedelta(minutes=5)

    for _, row in calendar.iterrows():
        interval = _CalendarInterval(
            session_date=str(row["session_date"]),
            session_interval_id=str(row["session_interval_id"]),
            session_start=row["session_start"],
            session_end=row["session_end"],
            expected_first_bar_end=row["expected_first_bar_end"],
            expected_last_bar_end=row["expected_last_bar_end"],
            expected_bar_count=int(row["expected_bar_count"]),
        )
        expected_ends = list(
            pd.date_range(
                interval.expected_first_bar_end,
                interval.expected_last_bar_end,
                freq=source_frequency,
            )
        )
        if len(expected_ends) != interval.expected_bar_count:
            raise HTFResamplingError(
                "calendar_expected_bar_count_mismatch:"
                + interval.session_date
                + ":"
                + interval.session_interval_id
            )

        interval_rows.append(
            {
                "session_date": interval.session_date,
                "session_interval_id": interval.session_interval_id,
                "session_start": interval.session_start,
                "session_end": interval.session_end,
            }
        )

        for bar_end in expected_ends:
            expected_rows.append(
                {
                    "session_date": interval.session_date,
                    "session_interval_id": interval.session_interval_id,
                    "end": bar_end,
                    "bucket_end": _bucket_end_for_timestamp(
                        timestamp=bar_end,
                        session_start=interval.session_start,
                        session_end=interval.session_end,
                        timeframe_minutes=timeframe_minutes,
                    ),
                }
            )

    interval_table = pd.DataFrame(interval_rows)
    expected_table = pd.DataFrame(expected_rows)
    if expected_table.duplicated(subset=["session_date", "session_interval_id", "end"]).any():
        raise HTFResamplingError("duplicate_expected_calendar_bar_end")

    return interval_table, expected_table


def _attach_calendar_interval(source: pd.DataFrame, interval_table: pd.DataFrame) -> pd.DataFrame:
    matched_parts: List[pd.DataFrame] = []

    for _, interval in interval_table.iterrows():
        mask = (
            (source["session_date"] == str(interval["session_date"]))
            & (source["end"] >= interval["session_start"])
            & (source["end"] <= interval["session_end"])
        )
        part = source.loc[mask].copy()
        if part.empty:
            continue
        part["session_interval_id"] = str(interval["session_interval_id"])
        matched_parts.append(part)

    if not matched_parts:
        raise HTFResamplingError("source_range_not_covered_by_calendar")

    enriched = pd.concat(matched_parts, ignore_index=True)
    if len(enriched) != len(source):
        raise HTFResamplingError("source_bars_outside_calendar_intervals")
    if enriched.duplicated(subset=list(SOURCE_DUPLICATE_COLUMNS)).any():
        raise HTFResamplingError("source_bar_maps_to_multiple_calendar_intervals")

    return enriched


def _attach_expected_bucket(source: pd.DataFrame, expected_table: pd.DataFrame) -> pd.DataFrame:
    merged = source.merge(
        expected_table,
        on=["session_date", "session_interval_id", "end"],
        how="left",
        validate="many_to_one",
    )
    if merged["bucket_end"].isna().any():
        raise HTFResamplingError("source_bar_not_expected_by_calendar")
    return merged


def _validate_bucket_invariants(source: pd.DataFrame, bucket_keys: Sequence[str]) -> None:
    for group_key, group in source.groupby(list(bucket_keys), sort=False):
        key_text = "|".join(str(item) for item in group_key)
        for col in BUCKET_INVARIANT_COLUMNS:
            if group[col].nunique(dropna=False) != 1:
                raise HTFResamplingError("mixed_" + col + "_inside_bucket:" + key_text)


def _validate_expected_bar_counts(
    source: pd.DataFrame,
    expected_table: pd.DataFrame,
    bucket_keys: Sequence[str],
) -> None:
    expected_counts = (
        expected_table.groupby(["session_date", "session_interval_id", "bucket_end"], dropna=False)
        .size()
        .rename("expected_source_bar_count")
        .reset_index()
    )
    observed_counts = (
        source.groupby(list(bucket_keys), dropna=False)
        .agg(source_bar_count=("end", "count"))
        .reset_index()
    )
    context_keys = [col for col in bucket_keys if col != "bucket_end"]
    observed_ranges = (
        source.groupby(list(context_keys), dropna=False)
        .agg(
            first_requested_source_end=("end", "min"),
            last_requested_source_end=("end", "max"),
        )
        .reset_index()
    )
    expected_spine = observed_ranges.merge(
        expected_counts,
        on=["session_date", "session_interval_id"],
        how="left",
    )
    if expected_spine["expected_source_bar_count"].isna().any():
        raise HTFResamplingError("bucket_boundary_not_validated_by_calendar")

    expected_spine = expected_spine.loc[
        (expected_spine["bucket_end"] >= expected_spine["first_requested_source_end"])
        & (expected_spine["bucket_end"] <= expected_spine["last_requested_source_end"])
    ].copy()
    coverage = expected_spine.merge(
        observed_counts,
        on=list(bucket_keys),
        how="left",
    )
    if coverage["source_bar_count"].isna().any():
        raise HTFResamplingError("missing_expected_5m_source_bar_inside_bucket")
    if (coverage["source_bar_count"] != coverage["expected_source_bar_count"]).any():
        raise HTFResamplingError("missing_expected_5m_source_bar_inside_bucket")


def _aggregate_buckets(
    source: pd.DataFrame,
    timeframe: str,
    bucket_keys: Sequence[str],
) -> pd.DataFrame:
    sorted_source = source.sort_values(list(bucket_keys) + ["end"]).reset_index(drop=True)
    rows: List[Dict[str, object]] = []

    for _, group in sorted_source.groupby(list(bucket_keys), sort=False):
        first = group.iloc[0]
        last = group.iloc[-1]
        rows.append(
            {
                "trade_date": first["trade_date"],
                "session_date": first["session_date"],
                "bucket_end": first["bucket_end"],
                "timeframe": timeframe,
                "continuous_symbol": first["continuous_symbol"],
                "family_code": first["family_code"],
                "source_secid": first["source_secid"],
                "source_contract": first["source_contract"],
                "open": first["open"],
                "high": group["high"].max(),
                "low": group["low"].min(),
                "close": last["close"],
                "volume": group["volume"].sum(),
                "roll_policy_id": first["roll_policy_id"],
                "adjustment_policy_id": first["adjustment_policy_id"],
                "adjustment_factor": first["adjustment_factor"],
                "contains_roll_boundary": bool(group["is_roll_boundary"].any()),
                "source_bar_count": int(len(group)),
                "first_source_end": first["end"],
                "last_source_end": last["end"],
                "schema_version": OUTPUT_SCHEMA_VERSION,
            }
        )

    return pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))


def _validate_output(output: pd.DataFrame) -> None:
    if output.empty:
        return

    if output[list(OUTPUT_COLUMNS)].isna().any(axis=None):
        null_cols = [col for col in OUTPUT_COLUMNS if output[col].isna().any()]
        raise HTFResamplingError("null_required_output_values:" + ",".join(null_cols))

    invalid_ohlc = (
        (output["high"] < output["low"])
        | (output["open"] < output["low"])
        | (output["open"] > output["high"])
        | (output["close"] < output["low"])
        | (output["close"] > output["high"])
    )
    if bool(invalid_ohlc.any()):
        raise HTFResamplingError("invalid_output_ohlc")

    if output.duplicated(subset=list(OUTPUT_PRIMARY_KEY_COLUMNS)).any():
        raise HTFResamplingError("duplicate_output_primary_key")
    if set(output["schema_version"].unique().tolist()) != {OUTPUT_SCHEMA_VERSION}:
        raise HTFResamplingError("output_schema_mismatch")


def _bucket_end_for_timestamp(
    timestamp: pd.Timestamp,
    session_start: pd.Timestamp,
    session_end: pd.Timestamp,
    timeframe_minutes: int,
) -> pd.Timestamp:
    if timestamp < session_start or timestamp > session_end:
        raise HTFResamplingError("timestamp_outside_session_interval")

    bucket_seconds = timeframe_minutes * 60
    elapsed_seconds = (timestamp - session_start).total_seconds()
    bucket_index = int((elapsed_seconds + bucket_seconds - 1) // bucket_seconds)
    if bucket_index < 1:
        bucket_index = 1

    bucket_end = session_start + pd.Timedelta(minutes=timeframe_minutes * bucket_index)
    if bucket_end > session_end:
        bucket_end = session_end

    return bucket_end


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}
