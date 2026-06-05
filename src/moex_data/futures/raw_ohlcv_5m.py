from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
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
    FuturesInstrumentIdentity,
    FuturesValidationError,
    guard_external_pattern,
    require_mapping,
    require_text,
    validate_identifier_values,
    validate_timeframe,
)

RAW_5M_DATASET_ID: Final[str] = "futures_ohlcv_5m"
RAW_5M_CONTRACT_ID: Final[str] = "futures_ohlcv_5m.v1"
RAW_5M_TIMEFRAME: Final[str] = "5m"
RAW_5M_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
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
    "value",
    "trades",
)


@dataclass(frozen=True)
class FuturesUniverse:
    universe_id: str
    instruments: tuple[FuturesInstrumentIdentity, ...]


@dataclass(frozen=True)
class Raw5mMaterializationRequest:
    dataset_id: str
    contract_id: str
    timeframe: str
    identity: FuturesInstrumentIdentity
    partition_key: date
    storage_ref: str
    calendar_contract_ref: str
    manifest_ref: str
    quality_report_ref: str
    source_contract_ref: str


@dataclass(frozen=True)
class Raw5mPartitionValidation:
    request: Raw5mMaterializationRequest
    row_count: int
    manifest: FuturesPartitionManifest
    quality_report: FuturesOhlcvQualityReport


def validate_futures_universe_values(values: Mapping[str, object]) -> FuturesUniverse:
    values = require_mapping(values, "futures_universe")
    universe_id = require_text(values.get("universe_id"), "universe_id")
    if values.get("dynamic_scan_allowed") is not False:
        raise FuturesValidationError("futures universe dynamic_scan_allowed must be false")
    raw_instruments = values.get("instruments")
    if isinstance(raw_instruments, (str, bytes)) or not isinstance(raw_instruments, Sequence):
        raise FuturesValidationError("instruments must be a sequence")
    instruments = tuple(validate_identifier_values(item) for item in raw_instruments)
    if not instruments:
        raise FuturesValidationError("futures universe instruments must be non-empty")
    identities = tuple((item.family, item.secid, item.board, item.market, item.series_type) for item in instruments)
    if len(set(identities)) != len(identities):
        raise FuturesValidationError("duplicate instrument identity")
    return FuturesUniverse(universe_id=universe_id, instruments=instruments)


def validate_identity_in_universe(identity: FuturesInstrumentIdentity, universe: FuturesUniverse) -> FuturesInstrumentIdentity:
    if identity not in universe.instruments:
        raise FuturesValidationError("instrument is not declared in futures universe")
    return identity


def _require_native_raw_identity(identity: FuturesInstrumentIdentity) -> None:
    if identity.series_type == "continuous":
        raise FuturesValidationError("continuous series is rejected by raw 5m boundary")
    if identity.series_type != "native":
        raise FuturesValidationError("raw 5m boundary requires native SERIES_TYPE")


def validate_raw_5m_materialization_request_values(
    values: Mapping[str, object], *, universe_values: Mapping[str, object] | FuturesUniverse
) -> Raw5mMaterializationRequest:
    values = require_mapping(values, "raw_5m_materialization_request")
    universe = universe_values if isinstance(universe_values, FuturesUniverse) else validate_futures_universe_values(universe_values)
    identity = validate_identifier_values(values)
    validate_identity_in_universe(identity, universe)
    _require_native_raw_identity(identity)
    timeframe = validate_timeframe(values.get("timeframe"))
    if timeframe != RAW_5M_TIMEFRAME:
        raise FuturesValidationError("raw 5m boundary supports only 5m timeframe")
    dataset_id = require_text(values.get("dataset_id"), "dataset_id")
    if dataset_id != RAW_5M_DATASET_ID:
        raise FuturesValidationError("raw 5m boundary received unsupported dataset_id")
    contract_id = require_text(values.get("contract_id"), "contract_id")
    if contract_id != RAW_5M_CONTRACT_ID:
        raise FuturesValidationError("raw 5m boundary received unsupported contract_id")
    partition_key = coerce_date(values.get("partition_key"), "partition_key")
    return Raw5mMaterializationRequest(
        dataset_id=dataset_id,
        contract_id=contract_id,
        timeframe=timeframe,
        identity=identity,
        partition_key=partition_key,
        storage_ref=guard_external_pattern(values.get("storage_ref"), "storage_ref"),
        calendar_contract_ref=require_text(values.get("calendar_contract_ref"), "calendar_contract_ref"),
        manifest_ref=guard_external_pattern(values.get("manifest_ref"), "manifest_ref"),
        quality_report_ref=guard_external_pattern(values.get("quality_report_ref"), "quality_report_ref"),
        source_contract_ref=require_text(values.get("source_contract_ref"), "source_contract_ref"),
    )


def _validate_raw_row_identity(row: Mapping[str, object], identity: FuturesInstrumentIdentity) -> None:
    row_identity = validate_identifier_values(row)
    if row_identity != identity:
        raise FuturesValidationError("raw 5m row identity does not match request identity")


def _validate_required_row_fields(row: Mapping[str, object]) -> None:
    missing = tuple(field for field in RAW_5M_REQUIRED_FIELDS if field not in row)
    if missing:
        raise FuturesValidationError("missing raw 5m row field: " + missing[0])


def validate_raw_5m_partition_rows(
    rows: Sequence[Mapping[str, object]], *, request: Raw5mMaterializationRequest, calendar: MoexIssFuturesCalendar
) -> Raw5mPartitionValidation:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise FuturesValidationError("raw 5m rows must be a sequence")
    if not rows:
        raise FuturesValidationError("raw 5m partition requires at least one row")
    resolved_sessions = []
    for row in rows:
        row = require_mapping(row, "raw_5m_row")
        _validate_required_row_fields(row)
        _validate_raw_row_identity(row, request.identity)
        resolved_sessions.append(
            resolve_futures_bar_session(row, calendar=calendar, contract_ref=request.calendar_contract_ref)
        )
    validate_partition_session_metadata(request.partition_key.isoformat(), tuple(resolved_sessions))
    validate_ohlcv_rows(rows, timeframe=request.timeframe)
    row_count = len(rows)
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
    manifest = validate_futures_partition_manifest_values(manifest_values, calendar=calendar)
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
        "parent_manifest_ref": request.manifest_ref,
    }
    quality_report = validate_futures_quality_report_values(quality_values, parent_manifest=manifest)
    return Raw5mPartitionValidation(
        request=request,
        row_count=row_count,
        manifest=manifest,
        quality_report=quality_report,
    )
