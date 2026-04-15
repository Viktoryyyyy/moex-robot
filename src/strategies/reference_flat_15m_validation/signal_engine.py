from __future__ import annotations

from datetime import datetime
from typing import Mapping

from src.moex_strategy_sdk.errors import InterfaceValidationError
from src.moex_strategy_sdk.interfaces import StrategyInputFrame, StrategySignalFrame
from src.strategies.reference_flat_15m_validation.config import StrategyConfig


def _coerce_bar_end(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise InterfaceValidationError("invalid end timestamp") from exc
    raise InterfaceValidationError("invalid end timestamp")


def _coerce_instrument_id(row: Mapping[str, object], config: StrategyConfig) -> str:
    raw = row.get("instrument_id", config.instrument_id)
    if not isinstance(raw, str) or raw != config.instrument_id:
        raise InterfaceValidationError("invalid instrument_id")
    return raw


def generate_signals(*, inputs: StrategyInputFrame, config: StrategyConfig) -> StrategySignalFrame:
    prev_end: datetime | None = None
    for row in inputs:
        end = _coerce_bar_end(row.get("end"))
        _coerce_instrument_id(row, config)
        if prev_end is not None and end <= prev_end:
            raise InterfaceValidationError("input bars must be strictly increasing by end")
        prev_end = end
    return tuple()
