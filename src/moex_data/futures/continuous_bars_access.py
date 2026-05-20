"""Shared access layer for canonical MOEX futures continuous bars."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence
import pandas as pd
from moex_data.futures.continuous_htf_resampling import HTFResamplingError, resample_continuous_htf

FIVE_MINUTE_SCHEMA_VERSION = "futures_continuous_5m.v1"
D1_SCHEMA_VERSION = "futures_continuous_d1.v1"
W1_SCHEMA_VERSION = "futures_continuous_w1.v1"
HTF_SCHEMA_VERSION = "futures_continuous_htf_ondemand_resampling.v1"
CALENDAR_ID = "moex_futures_session_calendar.v1"
SUPPORTED_TIMEFRAMES = ("5m", "15m", "30m", "1h", "4h", "D1", "W1")
HTF_TIMEFRAMES = ("15m", "30m", "1h", "4h")
FIVE_MINUTE_COLUMNS: Sequence[str] = ("trade_date", "end", "session_date", "continuous_symbol", "family_code", "source_secid", "source_contract", "open", "high", "low", "close", "volume", "roll_policy_id", "adjustment_policy_id", "adjustment_factor", "is_roll_boundary", "roll_map_id", "schema_version", "ingest_ts")
HTF_COLUMNS: Sequence[str] = ("trade_date", "session_date", "bucket_end", "timeframe", "continuous_symbol", "family_code", "source_secid", "source_contract", "open", "high", "low", "close", "volume", "roll_policy_id", "adjustment_policy_id", "adjustment_factor", "contains_roll_boundary", "source_bar_count", "first_source_end", "last_source_end", "schema_version")
D1_COLUMNS: Sequence[str] = ("trade_date", "session_date", "continuous_symbol", "family_code", "source_contracts", "open", "high", "low", "close", "volume", "roll_policy_id", "adjustment_policy_id", "adjustment_factor", "has_roll_boundary", "roll_map_id", "schema_version", "ingest_ts")
W1_COLUMNS: Sequence[str] = ("week_start", "week_end", "iso_year", "iso_week", "continuous_symbol", "family_code", "source_trade_dates", "source_contracts", "open", "high", "low", "close", "volume", "roll_policy_id", "adjustment_policy_id", "adjustment_factor", "has_roll_boundary", "roll_map_id", "source_d1_row_count", "schema_version", "ingest_ts")
FIVE_MINUTE_SORT_COLUMNS: Sequence[str] = ("continuous_symbol", "roll_policy_id", "adjustment_policy_id", "trade_date", "session_date", "end")
HTF_SORT_COLUMNS: Sequence[str] = ("continuous_symbol", "roll_policy_id", "adjustment_policy_id", "timeframe", "trade_date", "session_date", "bucket_end")
D1_SORT_COLUMNS: Sequence[str] = ("continuous_symbol", "roll_policy_id", "adjustment_policy_id", "trade_date", "session_date")
W1_SORT_COLUMNS: Sequence[str] = ("continuous_symbol", "roll_policy_id", "adjustment_policy_id", "week_start", "iso_year", "iso_week")
FIVE_MINUTE_DUPLICATE_COLUMNS: Sequence[str] = FIVE_MINUTE_SORT_COLUMNS
HTF_DUPLICATE_COLUMNS: Sequence[str] = HTF_SORT_COLUMNS
D1_DUPLICATE_COLUMNS: Sequence[str] = D1_SORT_COLUMNS
W1_DUPLICATE_COLUMNS: Sequence[str] = W1_SORT_COLUMNS
NUMERIC_COLUMNS: Sequence[str] = ("open", "high", "low", "close", "volume", "adjustment_factor")

class FuturesContinuousBarsAccessError(RuntimeError):
    """Fail-closed error raised when canonical continuous bars cannot be loaded."""

@dataclass(frozen=True)
class _Request:
    data_root: Path
    family_code: str
    roll_policy_id: str
    adjustment_policy_id: str
    timeframe: str
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp
    trade_dates: Sequence[str]

def load_futures_continuous_bars(*, data_root, family_code: str, roll_policy_id: str, adjustment_policy_id: str, timeframe: str, start, end, session_calendar_id: str | None = None, session_calendar=None, columns=None) -> pd.DataFrame:
    try:
        request = _build_request(data_root=data_root, family_code=family_code, roll_policy_id=roll_policy_id, adjustment_policy_id=adjustment_policy_id, timeframe=timeframe, start=start, end=end)
        if request.timeframe in HTF_TIMEFRAMES:
            _validate_htf_calendar_request(session_calendar_id, session_calendar)
            source = _load_source_partitions(request, "5m")
            source = _filter_intraday_range(source, request)
            source = _validate_5m_source(source, request)
            result = resample_continuous_htf(source, session_calendar, request.timeframe, session_calendar_id=session_calendar_id)
            result = _validate_htf_output(result, request)
            return _select_columns(result, HTF_COLUMNS, columns)
        if request.timeframe == "5m":
            source = _load_source_partitions(request, "5m")
            source = _filter_intraday_range(source, request)
            result = _validate_5m_source(source, request)
            result = _sort_frame(result, FIVE_MINUTE_SORT_COLUMNS)
            return _select_columns(result.loc[:, list(FIVE_MINUTE_COLUMNS)], FIVE_MINUTE_COLUMNS, columns)
        if request.timeframe == "D1":
            source = _load_source_partitions(request, "D1")
            source = _filter_d1_range(source, request)
            result = _validate_d1_source(source, request)
            result = _sort_frame(result, D1_SORT_COLUMNS)
            return _select_columns(result.loc[:, list(D1_COLUMNS)], D1_COLUMNS, columns)
        source = _load_source_partitions(request, "W1")
        source = _filter_w1_range(source, request)
        result = _validate_w1_source(source, request)
        result = _sort_frame(result, W1_SORT_COLUMNS)
        return _select_columns(result.loc[:, list(W1_COLUMNS)], W1_COLUMNS, columns)
    except FuturesContinuousBarsAccessError:
        raise
    except HTFResamplingError as exc:
        raise FuturesContinuousBarsAccessError("htf_resampling_failed:" + str(exc)) from exc
    except Exception as exc:
        raise FuturesContinuousBarsAccessError("access_layer_failed:" + exc.__class__.__name__ + ":" + str(exc)) from exc

def _build_request(*, data_root, family_code: str, roll_policy_id: str, adjustment_policy_id: str, timeframe: str, start, end) -> _Request:
    if data_root is None or str(data_root).strip() == "":
        raise FuturesContinuousBarsAccessError("missing_data_root")
    family = _required_text("family_code", family_code)
    roll_policy = _required_text("roll_policy_id", roll_policy_id)
    adjustment_policy = _required_text("adjustment_policy_id", adjustment_policy_id)
    if not isinstance(timeframe, str) or timeframe not in SUPPORTED_TIMEFRAMES:
        raise FuturesContinuousBarsAccessError("unsupported_timeframe:" + str(timeframe))
    start_ts = _parse_bound("start", start, is_end=False)
    end_ts = _parse_bound("end", end, is_end=True)
    if start_ts > end_ts:
        raise FuturesContinuousBarsAccessError("start_after_end")
    trade_dates = [x.strftime("%Y-%m-%d") for x in pd.date_range(start_ts.normalize(), end_ts.normalize(), freq="D")]
    if not trade_dates:
        raise FuturesContinuousBarsAccessError("empty_requested_trade_date_range")
    return _Request(data_root=Path(data_root), family_code=family, roll_policy_id=roll_policy, adjustment_policy_id=adjustment_policy, timeframe=timeframe, start_ts=start_ts, end_ts=end_ts, trade_dates=trade_dates)

def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or value.strip() == "":
        raise FuturesContinuousBarsAccessError("missing_" + name)
    return value.strip()

def _parse_bound(name: str, value: object, *, is_end: bool) -> pd.Timestamp:
    if value is None or str(value).strip() == "":
        raise FuturesContinuousBarsAccessError("missing_" + name)
    text_value = value if isinstance(value, str) else None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        raise FuturesContinuousBarsAccessError("invalid_" + name)
    timestamp = pd.Timestamp(timestamp)
    if is_end and _is_date_only(text_value, value):
        timestamp = timestamp + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    return timestamp

def _is_date_only(text_value: str | None, value: object) -> bool:
    if text_value is None:
        return not isinstance(value, pd.Timestamp) and not hasattr(value, "hour")
    stripped = text_value.strip()
    return not ("T" in stripped or ":" in stripped) and len(stripped) <= 10

def _validate_htf_calendar_request(session_calendar_id: str | None, session_calendar) -> None:
    if session_calendar_id is None or str(session_calendar_id).strip() == "":
        raise FuturesContinuousBarsAccessError("missing_htf_session_calendar_id")
    if str(session_calendar_id) != CALENDAR_ID:
        raise FuturesContinuousBarsAccessError("unsupported_htf_session_calendar_id:" + str(session_calendar_id))
    if session_calendar is None:
        raise FuturesContinuousBarsAccessError("missing_htf_session_calendar")

def _load_source_partitions(request: _Request, source_timeframe: str) -> pd.DataFrame:
    if source_timeframe == "W1":
        return _load_w1_partitions(request)
    frames: List[pd.DataFrame] = []
    for key in _source_partition_keys(request, source_timeframe):
        path = _source_partition_path(request, source_timeframe, key)
        if not path.is_file():
            raise FuturesContinuousBarsAccessError("missing_source_partition:" + str(path))
        frames.append(pd.read_parquet(path))
    if not frames:
        raise FuturesContinuousBarsAccessError("no_source_partitions")
    return pd.concat(frames, ignore_index=True)

def _load_w1_partitions(request: _Request) -> pd.DataFrame:
    root = request.data_root / "futures" / "continuous_w1" / ("roll_policy=" + request.roll_policy_id) / ("adjustment_policy=" + request.adjustment_policy_id) / ("family=" + request.family_code)
    if not root.is_dir():
        raise FuturesContinuousBarsAccessError("missing_source_family:" + str(root))
    start_date = request.start_ts.strftime("%Y-%m-%d")
    end_date = request.end_ts.strftime("%Y-%m-%d")
    frames: List[pd.DataFrame] = []
    for path in sorted(root.glob("week_start=*/part.parquet")):
        week_start = _partition_value(path, "week_start")
        if not week_start:
            raise FuturesContinuousBarsAccessError("invalid_w1_partition_path:" + str(path))
        week_end = (pd.Timestamp(week_start) + pd.Timedelta(days=6)).date().isoformat()
        if week_end < start_date or week_start > end_date:
            continue
        frames.append(pd.read_parquet(path))
    if not frames:
        raise FuturesContinuousBarsAccessError("no_source_partitions")
    return pd.concat(frames, ignore_index=True)

def _partition_value(path: Path, key: str) -> str:
    prefix = key + "="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""

def _source_partition_keys(request: _Request, source_timeframe: str) -> Sequence[str]:
    if source_timeframe != "W1":
        return request.trade_dates
    out: List[str] = []
    seen = set()
    for trade_date in request.trade_dates:
        ts = pd.Timestamp(trade_date)
        key = (ts - pd.Timedelta(days=int(ts.weekday()))).date().isoformat()
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out

def _source_partition_path(request: _Request, source_timeframe: str, key: str) -> Path:
    if source_timeframe == "5m":
        zone = "continuous_5m"; part = "trade_date=" + key
    elif source_timeframe == "D1":
        zone = "continuous_d1"; part = "trade_date=" + key
    elif source_timeframe == "W1":
        zone = "continuous_w1"; part = "week_start=" + key
    else:
        raise FuturesContinuousBarsAccessError("unsupported_source_timeframe:" + source_timeframe)
    return request.data_root / "futures" / zone / ("roll_policy=" + request.roll_policy_id) / ("adjustment_policy=" + request.adjustment_policy_id) / ("family=" + request.family_code) / part / "part.parquet"

def _filter_intraday_range(source: pd.DataFrame, request: _Request) -> pd.DataFrame:
    _require_columns(source, FIVE_MINUTE_COLUMNS)
    frame = source.copy(); frame["end"] = pd.to_datetime(frame["end"], errors="coerce")
    if frame["end"].isna().any():
        raise FuturesContinuousBarsAccessError("invalid_end_timestamp")
    frame = frame.loc[(frame["end"] >= request.start_ts) & (frame["end"] <= request.end_ts)].copy()
    if frame.empty:
        raise FuturesContinuousBarsAccessError("empty_source_range")
    return frame.reset_index(drop=True)

def _filter_d1_range(source: pd.DataFrame, request: _Request) -> pd.DataFrame:
    _require_columns(source, D1_COLUMNS)
    frame = source.copy(); frame["trade_date"] = frame["trade_date"].astype(str)
    start_date = request.start_ts.strftime("%Y-%m-%d"); end_date = request.end_ts.strftime("%Y-%m-%d")
    frame = frame.loc[(frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)].copy()
    if frame.empty:
        raise FuturesContinuousBarsAccessError("empty_source_range")
    return frame.reset_index(drop=True)

def _filter_w1_range(source: pd.DataFrame, request: _Request) -> pd.DataFrame:
    _require_columns(source, W1_COLUMNS)
    frame = source.copy(); frame["week_start"] = frame["week_start"].astype(str); frame["week_end"] = frame["week_end"].astype(str)
    start_date = request.start_ts.strftime("%Y-%m-%d"); end_date = request.end_ts.strftime("%Y-%m-%d")
    frame = frame.loc[(frame["week_end"] >= start_date) & (frame["week_start"] <= end_date)].copy()
    if frame.empty:
        raise FuturesContinuousBarsAccessError("empty_source_range")
    return frame.reset_index(drop=True)

def _validate_5m_source(source: pd.DataFrame, request: _Request) -> pd.DataFrame:
    frame = _validate_common_source(source=source, request=request, required_columns=FIVE_MINUTE_COLUMNS, schema_version=FIVE_MINUTE_SCHEMA_VERSION, duplicate_columns=FIVE_MINUTE_DUPLICATE_COLUMNS)
    _validate_monotonic(frame, ["continuous_symbol", "trade_date", "session_date"], "end")
    return frame

def _validate_d1_source(source: pd.DataFrame, request: _Request) -> pd.DataFrame:
    frame = _validate_common_source(source=source, request=request, required_columns=D1_COLUMNS, schema_version=D1_SCHEMA_VERSION, duplicate_columns=D1_DUPLICATE_COLUMNS)
    if bool(frame["source_contracts"].map(lambda x: not _items_present(x)).any()):
        raise FuturesContinuousBarsAccessError("empty_source_contracts")
    _validate_monotonic(frame, ["continuous_symbol"], "trade_date")
    return frame

def _validate_w1_source(source: pd.DataFrame, request: _Request) -> pd.DataFrame:
    if not isinstance(source, pd.DataFrame):
        raise FuturesContinuousBarsAccessError("source_is_not_dataframe")
    if source.empty:
        raise FuturesContinuousBarsAccessError("empty_source_range")
    _require_columns(source, W1_COLUMNS)
    frame = source.copy()
    scalar = [x for x in W1_COLUMNS if x not in {"source_trade_dates", "source_contracts"}]
    if frame[scalar].isna().any(axis=None):
        raise FuturesContinuousBarsAccessError("null_required_source_values:" + ",".join([x for x in scalar if frame[x].isna().any()]))
    for col in NUMERIC_COLUMNS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    if frame[list(NUMERIC_COLUMNS)].isna().any(axis=None):
        raise FuturesContinuousBarsAccessError("invalid_numeric_source_values")
    for col in ("week_start", "week_end", "continuous_symbol", "family_code", "roll_policy_id", "adjustment_policy_id", "schema_version"):
        frame[col] = frame[col].astype(str)
    _validate_family_policy_schema(frame, request, W1_SCHEMA_VERSION)
    if bool(frame["source_trade_dates"].map(lambda x: not _items_present(x)).any()):
        raise FuturesContinuousBarsAccessError("empty_source_trade_dates")
    if bool(frame["source_contracts"].map(lambda x: not _items_present(x)).any()):
        raise FuturesContinuousBarsAccessError("empty_source_contracts")
    _validate_ohlc(frame)
    if frame.duplicated(subset=list(W1_DUPLICATE_COLUMNS)).any():
        raise FuturesContinuousBarsAccessError("duplicate_source_primary_key")
    _validate_monotonic(frame, ["continuous_symbol"], "week_start")
    return frame

def _validate_common_source(*, source: pd.DataFrame, request: _Request, required_columns: Sequence[str], schema_version: str, duplicate_columns: Sequence[str]) -> pd.DataFrame:
    if not isinstance(source, pd.DataFrame):
        raise FuturesContinuousBarsAccessError("source_is_not_dataframe")
    if source.empty:
        raise FuturesContinuousBarsAccessError("empty_source_range")
    _require_columns(source, required_columns)
    frame = source.copy()
    if frame[list(required_columns)].isna().any(axis=None):
        raise FuturesContinuousBarsAccessError("null_required_source_values:" + ",".join([x for x in required_columns if frame[x].isna().any()]))
    for col in NUMERIC_COLUMNS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    if frame[list(NUMERIC_COLUMNS)].isna().any(axis=None):
        raise FuturesContinuousBarsAccessError("invalid_numeric_source_values")
    for col in ("trade_date", "session_date", "continuous_symbol", "family_code", "roll_policy_id", "adjustment_policy_id", "schema_version"):
        frame[col] = frame[col].astype(str)
    _validate_family_policy_schema(frame, request, schema_version)
    _validate_ohlc(frame)
    if frame.duplicated(subset=list(duplicate_columns)).any():
        raise FuturesContinuousBarsAccessError("duplicate_source_primary_key")
    return frame

def _validate_family_policy_schema(frame: pd.DataFrame, request: _Request, schema_version: str) -> None:
    if set(frame["family_code"].unique().tolist()) != {request.family_code}:
        raise FuturesContinuousBarsAccessError("family_code_mismatch")
    if set(frame["roll_policy_id"].unique().tolist()) != {request.roll_policy_id}:
        raise FuturesContinuousBarsAccessError("roll_policy_id_mismatch")
    if set(frame["adjustment_policy_id"].unique().tolist()) != {request.adjustment_policy_id}:
        raise FuturesContinuousBarsAccessError("adjustment_policy_id_mismatch")
    if set(frame["schema_version"].unique().tolist()) != {schema_version}:
        raise FuturesContinuousBarsAccessError("source_schema_mismatch")

def _require_columns(frame: pd.DataFrame, required_columns: Sequence[str]) -> None:
    missing = [x for x in required_columns if x not in frame.columns]
    if missing:
        raise FuturesContinuousBarsAccessError("missing_source_columns:" + ",".join(missing))

def _validate_ohlc(frame: pd.DataFrame) -> None:
    invalid = (frame["high"] < frame["low"]) | (frame["open"] < frame["low"]) | (frame["open"] > frame["high"]) | (frame["close"] < frame["low"]) | (frame["close"] > frame["high"])
    if bool(invalid.any()):
        raise FuturesContinuousBarsAccessError("invalid_source_ohlc")

def _validate_monotonic(frame: pd.DataFrame, group_columns: Sequence[str], order_column: str) -> None:
    for group_key, group in frame.groupby(list(group_columns), sort=False):
        if not group[order_column].is_monotonic_increasing:
            suffix = "|".join(str(x) for x in group_key) if isinstance(group_key, tuple) else str(group_key)
            raise FuturesContinuousBarsAccessError("non_monotonic_source_timestamps:" + suffix)

def _items_present(value: object) -> bool:
    if isinstance(value, (list, tuple, set)):
        return len(value) > 0 and all(str(x).strip() != "" for x in value)
    return str(value).strip() not in {"", "[]"}

def _validate_htf_output(output: pd.DataFrame, request: _Request) -> pd.DataFrame:
    if not isinstance(output, pd.DataFrame):
        raise FuturesContinuousBarsAccessError("htf_output_is_not_dataframe")
    if output.empty:
        raise FuturesContinuousBarsAccessError("empty_htf_output")
    _require_columns(output, HTF_COLUMNS)
    frame = output.copy()
    if frame[list(HTF_COLUMNS)].isna().any(axis=None):
        raise FuturesContinuousBarsAccessError("null_required_htf_values:" + ",".join([x for x in HTF_COLUMNS if frame[x].isna().any()]))
    for col in NUMERIC_COLUMNS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    if frame[list(NUMERIC_COLUMNS)].isna().any(axis=None):
        raise FuturesContinuousBarsAccessError("invalid_numeric_htf_values")
    for col in ("trade_date", "session_date", "timeframe", "continuous_symbol", "family_code", "roll_policy_id", "adjustment_policy_id", "schema_version"):
        frame[col] = frame[col].astype(str)
    if set(frame["timeframe"].unique().tolist()) != {request.timeframe}:
        raise FuturesContinuousBarsAccessError("htf_timeframe_mismatch")
    _validate_family_policy_schema(frame, request, HTF_SCHEMA_VERSION)
    frame["bucket_end"] = pd.to_datetime(frame["bucket_end"], errors="coerce")
    if frame["bucket_end"].isna().any():
        raise FuturesContinuousBarsAccessError("invalid_htf_bucket_end")
    _validate_ohlc(frame)
    if frame.duplicated(subset=list(HTF_DUPLICATE_COLUMNS)).any():
        raise FuturesContinuousBarsAccessError("duplicate_htf_primary_key")
    _validate_monotonic(frame, ["continuous_symbol", "timeframe", "trade_date", "session_date"], "bucket_end")
    return _sort_frame(frame.loc[:, list(HTF_COLUMNS)], HTF_SORT_COLUMNS)

def _sort_frame(frame: pd.DataFrame, sort_columns: Sequence[str]) -> pd.DataFrame:
    return frame.sort_values(list(sort_columns)).reset_index(drop=True)

def _select_columns(frame: pd.DataFrame, allowed_columns: Sequence[str], columns) -> pd.DataFrame:
    if columns is None:
        return frame.loc[:, list(allowed_columns)].reset_index(drop=True)
    if isinstance(columns, str):
        raise FuturesContinuousBarsAccessError("invalid_columns_request")
    requested = list(columns)
    if len(requested) != len(set(requested)):
        raise FuturesContinuousBarsAccessError("duplicate_requested_columns")
    unknown = [x for x in requested if x not in allowed_columns]
    if unknown:
        raise FuturesContinuousBarsAccessError("unknown_requested_columns:" + ",".join(unknown))
    return frame.loc[:, requested].reset_index(drop=True)
