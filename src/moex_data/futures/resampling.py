from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final

from moex_core.calendars.futures_session import (
    resolve_futures_bar_session,
    validate_partition_session_metadata,
)
from moex_core.calendars.moex_iss_calendar import MoexIssFuturesCalendar, coerce_date
from moex_data.quality.futures_ohlcv import (
    FuturesOhlcvQualityReport,
    validate_futures_quality_report_values,
    validate_ohlcv_rows,
)

from .manifests import FuturesPartitionManifest, validate_futures_partition_manifest_values
from .validation import (
    DERIVED_TIMEFRAMES,
    FuturesInstrumentIdentity,
    FuturesValidationError,
    guard_external_pattern,
    require_mapping,
    require_text,
    require_text_sequence,
    validate_identifier_values,
    validate_timeframe,
)

RAW_5M_DATASET_ID: Final[str] = "futures_ohlcv_5m"
RAW_5M_TIMEFRAME: Final[str] = "5m"
DERIVED_OHLCV_DATASET_ID: Final[str] = "futures_ohlcv_derived_timeframe"
DERIVED_OHLCV_CONTRACT_ID: Final[str] = "futures_ohlcv_derived_timeframe.v1"
INTRADAY_TIMEFRAME_MINUTES: Final[Mapping[str, int]] = {
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
}
PARENT_ROW_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "ts",
    "trade_date",
    "session_date",
    "FAMILY",
    "SECID",
    "BOARD",
    "MARKET",
    "SERIES_TYPE",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


@dataclass(frozen=True)
class DerivedOhlcvResamplingRequest:
    dataset_id: str
    contract_id: str
    timeframe: str
    identity: FuturesInstrumentIdentity
    partition_key: date
    storage_ref: str
    parent_manifest_ref: str
    calendar_contract_ref: str
    manifest_ref: str
    quality_report_ref: str


@dataclass(frozen=True)
class DerivedOhlcvResamplingResult:
    request: DerivedOhlcvResamplingRequest
    rows: tuple[dict[str, object], ...]
    manifest: FuturesPartitionManifest
    quality_report: FuturesOhlcvQualityReport


def _require_number(value: object, field_name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise FuturesValidationError(field_name + " must be numeric")
    return float(value)


def _require_parent_rows(rows: Sequence[Mapping[str, object]]) -> tuple[Mapping[str, object], ...]:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise FuturesValidationError("parent 5m rows must be a sequence")
    if not rows:
        raise FuturesValidationError("parent 5m rows must be non-empty")
    return tuple(require_mapping(row, "parent_5m_row") for row in rows)


def _validate_core_config_timeframe(values: Mapping[str, object], timeframe: str) -> None:
    config = require_mapping(values, "futures_historical_data_core_config")
    approved = require_text_sequence(config.get("approved_timeframes"), "approved_timeframes")
    for item in approved:
        validate_timeframe(item)
    if "10m" not in approved:
        raise FuturesValidationError("10m timeframe must be declared in approved config")
    if timeframe not in approved:
        raise FuturesValidationError("derived timeframe is absent from approved config")


def validate_derived_ohlcv_resampling_request_values(
    values: Mapping[str, object], *, core_config_values: Mapping[str, object]
) -> DerivedOhlcvResamplingRequest:
    values = require_mapping(values, "derived_ohlcv_resampling_request")
    timeframe = validate_timeframe(values.get("timeframe"), derived_only=True)
    _validate_core_config_timeframe(core_config_values, timeframe)
    dataset_id = require_text(values.get("dataset_id"), "dataset_id")
    if dataset_id != DERIVED_OHLCV_DATASET_ID:
        raise FuturesValidationError("derived resampling boundary received unsupported dataset_id")
    contract_id = require_text(values.get("contract_id"), "contract_id")
    if contract_id != DERIVED_OHLCV_CONTRACT_ID:
        raise FuturesValidationError("derived resampling boundary received unsupported contract_id")
    identity = validate_identifier_values(values)
    if identity.series_type == "continuous":
        raise FuturesValidationError("continuous series is rejected by derived resampling boundary")
    if identity.series_type != "native":
        raise FuturesValidationError("derived resampling boundary requires native SERIES_TYPE")
    return DerivedOhlcvResamplingRequest(
        dataset_id=dataset_id,
        contract_id=contract_id,
        timeframe=timeframe,
        identity=identity,
        partition_key=coerce_date(values.get("partition_key"), "partition_key"),
        storage_ref=guard_external_pattern(values.get("storage_ref"), "storage_ref"),
        parent_manifest_ref=guard_external_pattern(values.get("parent_manifest_ref"), "parent_manifest_ref"),
        calendar_contract_ref=require_text(values.get("calendar_contract_ref"), "calendar_contract_ref"),
        manifest_ref=guard_external_pattern(values.get("manifest_ref"), "manifest_ref"),
        quality_report_ref=guard_external_pattern(values.get("quality_report_ref"), "quality_report_ref"),
    )


def _validate_parent_manifest_values(
    parent_manifest_values: Mapping[str, object],
    *,
    request: DerivedOhlcvResamplingRequest,
    calendar: MoexIssFuturesCalendar,
) -> FuturesPartitionManifest:
    manifest = validate_futures_partition_manifest_values(parent_manifest_values, calendar=calendar)
    if manifest.dataset_id != RAW_5M_DATASET_ID:
        raise FuturesValidationError("derived resampling requires futures_ohlcv_5m parent manifest")
    if manifest.timeframe != RAW_5M_TIMEFRAME:
        raise FuturesValidationError("derived resampling requires 5m parent manifest")
    if manifest.series_type != request.identity.series_type:
        raise FuturesValidationError("parent manifest SERIES_TYPE mismatch")
    if manifest.family != request.identity.family:
        raise FuturesValidationError("parent manifest FAMILY mismatch")
    if manifest.secid != request.identity.secid:
        raise FuturesValidationError("parent manifest SECID mismatch")
    if manifest.board != request.identity.board:
        raise FuturesValidationError("parent manifest BOARD mismatch")
    if manifest.market != request.identity.market:
        raise FuturesValidationError("parent manifest MARKET mismatch")
    return manifest


def _validate_parent_manifests(
    parent_manifest_values: Mapping[str, object] | Sequence[Mapping[str, object]],
    *,
    request: DerivedOhlcvResamplingRequest,
    calendar: MoexIssFuturesCalendar,
) -> tuple[FuturesPartitionManifest, ...]:
    if isinstance(parent_manifest_values, Mapping):
        raw_values = (parent_manifest_values,)
    elif isinstance(parent_manifest_values, Sequence) and not isinstance(parent_manifest_values, (str, bytes)):
        raw_values = tuple(parent_manifest_values)
    else:
        raise FuturesValidationError("parent 5m manifest is required")
    if not raw_values:
        raise FuturesValidationError("parent 5m manifest is required")
    manifests = tuple(
        _validate_parent_manifest_values(require_mapping(item, "parent_5m_manifest"), request=request, calendar=calendar)
        for item in raw_values
    )
    if request.timeframe != "1W":
        if len(manifests) != 1:
            raise FuturesValidationError("non-weekly derived resampling requires exactly one parent 5m manifest")
        if manifests[0].partition_key != request.partition_key:
            raise FuturesValidationError("parent manifest partition_key mismatch")
    else:
        week_start = _trading_week_start(request.partition_key)
        for manifest in manifests:
            if _trading_week_start(manifest.partition_key) != week_start:
                raise FuturesValidationError("weekly parent manifest outside requested trading week")
    return manifests


def _validate_required_parent_row_fields(row: Mapping[str, object]) -> None:
    missing = tuple(field for field in PARENT_ROW_REQUIRED_FIELDS if field not in row)
    if missing:
        raise FuturesValidationError("missing parent 5m row field: " + missing[0])


def _validate_parent_rows(
    rows: tuple[Mapping[str, object], ...],
    *,
    request: DerivedOhlcvResamplingRequest,
    calendar: MoexIssFuturesCalendar,
) -> None:
    resolved_sessions = []
    for row in rows:
        _validate_required_parent_row_fields(row)
        row_identity = validate_identifier_values(row)
        if row_identity != request.identity:
            raise FuturesValidationError("parent 5m row identity does not match request identity")
        if row_identity.series_type == "continuous":
            raise FuturesValidationError("continuous series is rejected by derived resampling boundary")
        resolved_sessions.append(
            resolve_futures_bar_session(row, calendar=calendar, contract_ref=request.calendar_contract_ref)
        )
    if request.timeframe != "1W":
        validate_partition_session_metadata(request.partition_key.isoformat(), tuple(resolved_sessions))
    else:
        week_start = _trading_week_start(request.partition_key)
        for session in resolved_sessions:
            if _trading_week_start(session.session_date) != week_start:
                raise FuturesValidationError("weekly parent row outside requested trading week")
    validate_ohlcv_rows(rows, timeframe=RAW_5M_TIMEFRAME)


def _bucket_start(ts: datetime, minutes: int) -> datetime:
    minute_of_day = ts.hour * 60 + ts.minute
    bucket_minute = minute_of_day - (minute_of_day % minutes)
    return ts.replace(hour=bucket_minute // 60, minute=bucket_minute % 60, second=0, microsecond=0)


def _trading_week_start(value: date) -> date:
    return date.fromordinal(value.toordinal() - value.weekday())


def _group_key(row: Mapping[str, object], timeframe: str) -> object:
    ts = row.get("ts")
    if not isinstance(ts, datetime):
        raise FuturesValidationError("ts must be datetime")
    if timeframe in INTRADAY_TIMEFRAME_MINUTES:
        return row.get("session_date"), _bucket_start(ts, INTRADAY_TIMEFRAME_MINUTES[timeframe])
    if timeframe == "1D":
        return row.get("session_date")
    if timeframe == "1W":
        return _trading_week_start(coerce_date(row.get("session_date"), "session_date"))
    raise FuturesValidationError("unsupported timeframe")


def _aggregate_group(rows: tuple[Mapping[str, object], ...], *, request: DerivedOhlcvResamplingRequest) -> dict[str, object]:
    first = rows[0]
    last = rows[-1]
    output: dict[str, object] = {
        "ts": _output_ts(rows, request.timeframe),
        "trade_date": last["trade_date"],
        "session_date": last["session_date"],
        "FAMILY": request.identity.family,
        "SECID": request.identity.secid,
        "BOARD": request.identity.board,
        "MARKET": request.identity.market,
        "SERIES_TYPE": request.identity.series_type,
        "timeframe": request.timeframe,
        "open": _require_number(first.get("open"), "open"),
        "high": max(_require_number(row.get("high"), "high") for row in rows),
        "low": min(_require_number(row.get("low"), "low") for row in rows),
        "close": _require_number(last.get("close"), "close"),
        "volume": sum(_require_number(row.get("volume"), "volume") for row in rows),
    }
    if any("value" in row for row in rows):
        output["value"] = sum(_require_number(row.get("value"), "value") for row in rows)
    if any("trades" in row for row in rows):
        output["trades"] = sum(_require_number(row.get("trades"), "trades") for row in rows)
    if any("open_interest" in row for row in rows):
        last_valid_oi = None
        for row in rows:
            value = row.get("open_interest")
            if value is None:
                continue
            last_valid_oi = _require_number(value, "open_interest")
        if last_valid_oi is not None:
            output["open_interest"] = last_valid_oi
    if request.timeframe == "1W":
        output["week_start_date"] = _trading_week_start(coerce_date(last["session_date"], "session_date"))
    return output


def _output_ts(rows: tuple[Mapping[str, object], ...], timeframe: str) -> datetime:
    if timeframe in INTRADAY_TIMEFRAME_MINUTES:
        ts = rows[0].get("ts")
        if not isinstance(ts, datetime):
            raise FuturesValidationError("ts must be datetime")
        return _bucket_start(ts, INTRADAY_TIMEFRAME_MINUTES[timeframe])
    ts = rows[-1].get("ts")
    if not isinstance(ts, datetime):
        raise FuturesValidationError("ts must be datetime")
    return ts


def _resample_rows(
    rows: tuple[Mapping[str, object], ...], *, request: DerivedOhlcvResamplingRequest
) -> tuple[dict[str, object], ...]:
    groups: dict[object, list[Mapping[str, object]]] = {}
    for row in rows:
        groups.setdefault(_group_key(row, request.timeframe), []).append(row)
    sorted_keys = sorted(groups)
    return tuple(_aggregate_group(tuple(groups[key]), request=request) for key in sorted_keys)


def _build_manifest(
    request: DerivedOhlcvResamplingRequest, *, row_count: int, calendar: MoexIssFuturesCalendar
) -> FuturesPartitionManifest:
    manifest_values = {
        "dataset_id": request.dataset_id,
        "timeframe": request.timeframe,
        "SERIES_TYPE": request.identity.series_type,
        "FAMILY": request.identity.family,
        "SECID": request.identity.secid,
        "BOARD": request.identity.board,
        "MARKET": request.identity.market,
        "partition_key": request.partition_key.isoformat(),
        "storage_ref": request.storage_ref,
        "row_count": row_count,
        "expected_bar_count": row_count,
        "observed_bar_count": row_count,
        "missing_bar_count": 0,
        "calendar_contract_ref": request.calendar_contract_ref,
        "quality_report_ref": request.quality_report_ref,
    }
    return validate_futures_partition_manifest_values(manifest_values, calendar=calendar)


def _build_quality_report(request: DerivedOhlcvResamplingRequest, *, row_count: int) -> FuturesOhlcvQualityReport:
    quality_values = {
        "dataset_id": request.dataset_id,
        "timeframe": request.timeframe,
        "SERIES_TYPE": request.identity.series_type,
        "FAMILY": request.identity.family,
        "SECID": request.identity.secid,
        "BOARD": request.identity.board,
        "MARKET": request.identity.market,
        "partition_key": request.partition_key.isoformat(),
        "status": "pass",
        "downstream_consumption_allowed": True,
        "row_count": row_count,
        "duplicate_ts_secid_count": 0,
        "non_monotonic_timestamp_count": 0,
        "invalid_ohlc_count": 0,
        "negative_volume_count": 0,
        "negative_value_count": 0,
        "negative_trades_count": 0,
        "parent_manifest_ref": request.parent_manifest_ref,
    }
    return validate_futures_quality_report_values(quality_values)


def resample_ohlcv_5m_partition(
    parent_rows: Sequence[Mapping[str, object]],
    request_values: Mapping[str, object],
    *,
    parent_manifest_values: Mapping[str, object] | Sequence[Mapping[str, object]],
    core_config_values: Mapping[str, object],
    calendar: MoexIssFuturesCalendar,
) -> DerivedOhlcvResamplingResult:
    request = validate_derived_ohlcv_resampling_request_values(
        request_values, core_config_values=core_config_values
    )
    _validate_parent_manifests(parent_manifest_values, request=request, calendar=calendar)
    rows = _require_parent_rows(parent_rows)
    _validate_parent_rows(rows, request=request, calendar=calendar)
    output_rows = _resample_rows(rows, request=request)
    validate_ohlcv_rows(output_rows, timeframe=request.timeframe)
    row_count = len(output_rows)
    manifest = _build_manifest(request, row_count=row_count, calendar=calendar)
    quality_report = _build_quality_report(request, row_count=row_count)
    return DerivedOhlcvResamplingResult(
        request=request,
        rows=output_rows,
        manifest=manifest,
        quality_report=quality_report,
    )
