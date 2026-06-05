from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from moex_data.futures.manifests import FuturesPartitionManifest
from moex_data.futures.validation import (
    FuturesValidationError,
    require_bool,
    require_non_negative_int,
    require_text,
    validate_identifier_values,
    validate_timeframe,
)

QUALITY_STATUSES: Final[frozenset[str]] = frozenset({"pass", "warn", "fail"})


@dataclass(frozen=True)
class FuturesOhlcvQualityReport:
    dataset_id: str
    timeframe: str
    series_type: str
    family: str
    secid: str
    board: str
    market: str
    partition_key: str
    status: str
    downstream_consumption_allowed: bool
    row_count: int
    duplicate_ts_secid_count: int
    non_monotonic_timestamp_count: int
    invalid_ohlc_count: int
    negative_volume_count: int
    negative_value_count: int
    negative_trades_count: int
    parent_manifest_ref: str | None


def _require_number(value: object, field_name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise FuturesValidationError(field_name + " must be numeric")
    return float(value)


def validate_ohlcv_rows(rows: Iterable[Mapping[str, object]], *, timeframe: str) -> None:
    validate_timeframe(timeframe)
    seen: set[tuple[datetime, str]] = set()
    last_ts_by_secid: dict[str, datetime] = {}
    for row in rows:
        ts = row.get("ts")
        secid = row.get("SECID")
        if not isinstance(ts, datetime):
            raise FuturesValidationError("ts must be datetime")
        if not isinstance(secid, str) or not secid:
            raise FuturesValidationError("SECID is required")
        key = (ts, secid)
        if key in seen:
            raise FuturesValidationError("duplicate ts/SECID")
        seen.add(key)
        last_ts = last_ts_by_secid.get(secid)
        if last_ts is not None and ts <= last_ts:
            raise FuturesValidationError("non-monotonic timestamps")
        last_ts_by_secid[secid] = ts
        open_ = _require_number(row.get("open"), "open")
        high = _require_number(row.get("high"), "high")
        low = _require_number(row.get("low"), "low")
        close = _require_number(row.get("close"), "close")
        if high < max(open_, close, low) or low > min(open_, close, high):
            raise FuturesValidationError("invalid OHLC")
        for field_name in ("volume", "value", "trades"):
            if _require_number(row.get(field_name), field_name) < 0:
                raise FuturesValidationError(field_name + " must be non-negative")


def validate_futures_quality_report_values(
    values: Mapping[str, object], *, parent_manifest: FuturesPartitionManifest | None = None
) -> FuturesOhlcvQualityReport:
    identity = validate_identifier_values(values)
    timeframe = validate_timeframe(values.get("timeframe"))
    status = require_text(values.get("status"), "status")
    if status not in QUALITY_STATUSES:
        raise FuturesValidationError("unsupported quality status")
    downstream_allowed = require_bool(
        values.get("downstream_consumption_allowed"), "downstream_consumption_allowed"
    )
    if status == "fail" and downstream_allowed:
        raise FuturesValidationError("fail quality status blocks downstream consumption")
    row_count = require_non_negative_int(values.get("row_count"), "row_count")
    duplicate_count = require_non_negative_int(values.get("duplicate_ts_secid_count"), "duplicate_ts_secid_count")
    non_monotonic_count = require_non_negative_int(
        values.get("non_monotonic_timestamp_count"), "non_monotonic_timestamp_count"
    )
    invalid_ohlc_count = require_non_negative_int(values.get("invalid_ohlc_count"), "invalid_ohlc_count")
    negative_volume_count = require_non_negative_int(values.get("negative_volume_count"), "negative_volume_count")
    negative_value_count = require_non_negative_int(values.get("negative_value_count"), "negative_value_count")
    negative_trades_count = require_non_negative_int(values.get("negative_trades_count"), "negative_trades_count")
    if status == "pass" and any(
        (
            duplicate_count,
            non_monotonic_count,
            invalid_ohlc_count,
            negative_volume_count,
            negative_value_count,
            negative_trades_count,
        )
    ):
        raise FuturesValidationError("pass quality report cannot contain failed hard checks")
    partition_key = require_text(values.get("partition_key"), "partition_key")
    parent_manifest_ref = values.get("parent_manifest_ref")
    if parent_manifest_ref is not None:
        parent_manifest_ref = require_text(parent_manifest_ref, "parent_manifest_ref")
    if parent_manifest is not None:
        if parent_manifest.dataset_id != values.get("dataset_id"):
            raise FuturesValidationError("parent manifest dataset_id mismatch")
        if parent_manifest.timeframe != timeframe:
            raise FuturesValidationError("parent manifest timeframe mismatch")
        if parent_manifest.series_type != identity.series_type:
            raise FuturesValidationError("parent manifest SERIES_TYPE mismatch")
        if parent_manifest.family != identity.family:
            raise FuturesValidationError("parent manifest FAMILY mismatch")
        if parent_manifest.secid != identity.secid:
            raise FuturesValidationError("parent manifest SECID mismatch")
        if parent_manifest.board != identity.board or parent_manifest.market != identity.market:
            raise FuturesValidationError("parent manifest market identity mismatch")
        if parent_manifest.partition_key.isoformat() != partition_key:
            raise FuturesValidationError("parent manifest partition_key mismatch")
        if parent_manifest.row_count != row_count:
            raise FuturesValidationError("parent manifest row_count mismatch")
    return FuturesOhlcvQualityReport(
        dataset_id=require_text(values.get("dataset_id"), "dataset_id"),
        timeframe=timeframe,
        series_type=identity.series_type,
        family=identity.family,
        secid=identity.secid,
        board=identity.board,
        market=identity.market,
        partition_key=partition_key,
        status=status,
        downstream_consumption_allowed=downstream_allowed,
        row_count=row_count,
        duplicate_ts_secid_count=duplicate_count,
        non_monotonic_timestamp_count=non_monotonic_count,
        invalid_ohlc_count=invalid_ohlc_count,
        negative_volume_count=negative_volume_count,
        negative_value_count=negative_value_count,
        negative_trades_count=negative_trades_count,
        parent_manifest_ref=parent_manifest_ref,
    )


def futures_quality_report_to_values(report: FuturesOhlcvQualityReport) -> dict[str, object]:
    return {
        "dataset_id": report.dataset_id,
        "timeframe": report.timeframe,
        "SERIES_TYPE": report.series_type,
        "FAMILY": report.family,
        "SECID": report.secid,
        "BOARD": report.board,
        "MARKET": report.market,
        "partition_key": report.partition_key,
        "status": report.status,
        "downstream_consumption_allowed": report.downstream_consumption_allowed,
        "row_count": report.row_count,
        "duplicate_ts_secid_count": report.duplicate_ts_secid_count,
        "non_monotonic_timestamp_count": report.non_monotonic_timestamp_count,
        "invalid_ohlc_count": report.invalid_ohlc_count,
        "negative_volume_count": report.negative_volume_count,
        "negative_value_count": report.negative_value_count,
        "negative_trades_count": report.negative_trades_count,
        "parent_manifest_ref": report.parent_manifest_ref,
    }
