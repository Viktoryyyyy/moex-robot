from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from moex_core.calendars.moex_iss_calendar import MoexIssFuturesCalendar, coerce_date

from .validation import (
    FuturesValidationError,
    guard_external_pattern,
    require_non_negative_int,
    require_text,
    validate_identifier_values,
    validate_timeframe,
)


REQUIRED_MANIFEST_FIELDS: tuple[str, ...] = (
    "dataset_id",
    "timeframe",
    "SERIES_TYPE",
    "FAMILY",
    "SECID",
    "BOARD",
    "MARKET",
    "partition_key",
    "storage_ref",
    "row_count",
    "expected_bar_count",
    "observed_bar_count",
    "missing_bar_count",
    "calendar_contract_ref",
    "quality_report_ref",
)


@dataclass(frozen=True)
class FuturesPartitionManifest:
    dataset_id: str
    timeframe: str
    series_type: str
    family: str
    secid: str
    board: str
    market: str
    partition_key: date
    storage_ref: str
    row_count: int
    expected_bar_count: int
    observed_bar_count: int
    missing_bar_count: int
    calendar_contract_ref: str
    quality_report_ref: str


def validate_storage_ref(value: object, field_name: str = "storage_ref") -> str:
    return guard_external_pattern(value, field_name)


def validate_futures_partition_manifest_values(
    values: Mapping[str, object], *, calendar: MoexIssFuturesCalendar | None = None
) -> FuturesPartitionManifest:
    missing = tuple(field for field in REQUIRED_MANIFEST_FIELDS if field not in values)
    if missing:
        raise FuturesValidationError("missing manifest field: " + missing[0])
    identity = validate_identifier_values(values)
    timeframe = validate_timeframe(values.get("timeframe"))
    partition_key = coerce_date(values.get("partition_key"), "partition_key")
    if calendar is not None:
        calendar.require_trading_day(partition_key)
    storage_ref = validate_storage_ref(values.get("storage_ref"))
    row_count = require_non_negative_int(values.get("row_count"), "row_count")
    expected = require_non_negative_int(values.get("expected_bar_count"), "expected_bar_count")
    observed = require_non_negative_int(values.get("observed_bar_count"), "observed_bar_count")
    missing_count = require_non_negative_int(values.get("missing_bar_count"), "missing_bar_count")
    if observed + missing_count != expected:
        raise FuturesValidationError("observed and missing counts must equal expected_bar_count")
    if row_count != observed:
        raise FuturesValidationError("row_count must match observed_bar_count")
    return FuturesPartitionManifest(
        dataset_id=require_text(values.get("dataset_id"), "dataset_id"),
        timeframe=timeframe,
        series_type=identity.series_type,
        family=identity.family,
        secid=identity.secid,
        board=identity.board,
        market=identity.market,
        partition_key=partition_key,
        storage_ref=storage_ref,
        row_count=row_count,
        expected_bar_count=expected,
        observed_bar_count=observed,
        missing_bar_count=missing_count,
        calendar_contract_ref=require_text(values.get("calendar_contract_ref"), "calendar_contract_ref"),
        quality_report_ref=require_text(values.get("quality_report_ref"), "quality_report_ref"),
    )


def futures_partition_manifest_to_values(manifest: FuturesPartitionManifest) -> dict[str, object]:
    return {
        "dataset_id": manifest.dataset_id,
        "timeframe": manifest.timeframe,
        "SERIES_TYPE": manifest.series_type,
        "FAMILY": manifest.family,
        "SECID": manifest.secid,
        "BOARD": manifest.board,
        "MARKET": manifest.market,
        "partition_key": manifest.partition_key.isoformat(),
        "storage_ref": manifest.storage_ref,
        "row_count": manifest.row_count,
        "expected_bar_count": manifest.expected_bar_count,
        "observed_bar_count": manifest.observed_bar_count,
        "missing_bar_count": manifest.missing_bar_count,
        "calendar_contract_ref": manifest.calendar_contract_ref,
        "quality_report_ref": manifest.quality_report_ref,
    }
