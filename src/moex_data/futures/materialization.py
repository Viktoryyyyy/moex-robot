from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from moex_core.calendars.moex_iss_calendar import MoexIssFuturesCalendar

from .raw_ohlcv_5m import (
    Raw5mMaterializationRequest,
    Raw5mPartitionValidation,
    validate_raw_5m_materialization_request_values,
    validate_raw_5m_partition_rows,
)
from .validation import FuturesValidationError, require_mapping, require_text


class Raw5mSourceAdapter(Protocol):
    def read_rows(self, request: Raw5mMaterializationRequest) -> Sequence[Mapping[str, object]]:
        pass


@dataclass(frozen=True)
class Raw5mMaterializationBoundaryResult:
    request: Raw5mMaterializationRequest
    partition_validation: Raw5mPartitionValidation


_RAW_5M_SOURCE_CONTRACTS: Mapping[str, tuple[str, str]] = {
    "moex_iss_forts_candles_5m": ("MOEX_ISS", "5m"),
    "moex_apim_algopack_fo_tradestats_5m": ("MOEX_APIM_ALGOPACK", "5m"),
}


def validate_raw_5m_source_contract_binding(values: Mapping[str, object]) -> str:
    values = require_mapping(values, "raw_5m_source_contract")
    source_id = require_text(values.get("source_id"), "source_id")
    expected = _RAW_5M_SOURCE_CONTRACTS.get(source_id)
    if expected is None:
        raise FuturesValidationError("raw 5m boundary requires a declared native 5m source")
    expected_system, expected_timeframe = expected
    if require_text(values.get("source_system"), "source_system") != expected_system:
        raise FuturesValidationError("raw 5m source_system does not match declared source_id")
    if require_text(values.get("native_timeframe"), "native_timeframe") != expected_timeframe:
        raise FuturesValidationError("raw 5m boundary requires native 5m source")
    if require_text(values.get("output_contract_ref"), "output_contract_ref") != "contracts/datasets/futures_ohlcv_5m.v1.yaml":
        raise FuturesValidationError("raw 5m source must bind futures_ohlcv_5m contract")
    return source_id


def materialize_raw_5m_boundary(
    request_values: Mapping[str, object],
    *,
    universe_values: Mapping[str, object],
    source_contract_values: Mapping[str, object],
    calendar: MoexIssFuturesCalendar,
    source_adapter: Raw5mSourceAdapter,
) -> Raw5mMaterializationBoundaryResult:
    validate_raw_5m_source_contract_binding(source_contract_values)
    request = validate_raw_5m_materialization_request_values(
        request_values, universe_values=universe_values
    )
    rows = source_adapter.read_rows(request)
    partition_validation = validate_raw_5m_partition_rows(rows, request=request, calendar=calendar)
    return Raw5mMaterializationBoundaryResult(request=request, partition_validation=partition_validation)
