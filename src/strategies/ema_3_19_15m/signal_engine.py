from __future__ import annotations

from datetime import datetime
from typing import Mapping

from src.moex_strategy_sdk.errors import InterfaceValidationError
from src.moex_strategy_sdk.interfaces import StrategyInputFrame, StrategySignalFrame
from src.strategies.ema_3_19_15m.config import StrategyConfig


def _coerce_bar_end(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise InterfaceValidationError("invalid end timestamp") from exc
    raise InterfaceValidationError("invalid end timestamp")


def _coerce_close(value: object) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise InterfaceValidationError("invalid close")
    close = float(value)
    if close <= 0.0:
        raise InterfaceValidationError("close must be positive")
    return close


def _coerce_instrument_id(row: Mapping[str, object], config: StrategyConfig) -> str:
    raw = row.get("instrument_id", config.instrument_id)
    if not isinstance(raw, str) or not raw.strip():
        raise InterfaceValidationError("invalid instrument_id")
    return raw


def _alpha(window: int) -> float:
    return 2.0 / (float(window) + 1.0)


def generate_signals(*, inputs: StrategyInputFrame, config: StrategyConfig) -> StrategySignalFrame:
    prev_end: datetime | None = None
    prev_fast: float | None = None
    prev_slow: float | None = None
    next_rows: list[dict[str, object]] = []

    for row in inputs:
        end = _coerce_bar_end(row.get("end"))
        close = _coerce_close(row.get("close"))
        instrument_id = _coerce_instrument_id(row, config)

        if prev_end is not None and end <= prev_end:
            raise InterfaceValidationError("input bars must be strictly increasing by end")
        prev_end = end

        next_fast = close if prev_fast is None else (_alpha(config.ema_fast_window) * close) + ((1.0 - _alpha(config.ema_fast_window)) * prev_fast)
        next_slow = close if prev_slow is None else (_alpha(config.ema_slow_window) * close) + ((1.0 - _alpha(config.ema_slow_window)) * prev_slow)

        if prev_fast is not None and prev_slow is not None:
            crossed_up = prev_fast <= prev_slow and next_fast > next_slow
            crossed_down = prev_fast >= prev_slow and next_fast < next_slow

            if crossed_up:
                next_rows.append(
                    {
                        "instrument_id": instrument_id,
                        "decision_ts": end,
                        "desired_position": 1.0,
                        "signal_code": "ema_cross_up",
                        "signal_strength": abs(next_fast - next_slow),
                        "reason_code": "ema_fast_crossed_above_ema_slow",
                    }
                )
            elif crossed_down:
                next_rows.append(
                    {
                        "instrument_id": instrument_id,
                        "decision_ts": end,
                        "desired_position": -1.0,
                        "signal_code": "ema_cross_down",
                        "signal_strength": abs(next_fast - next_slow),
                        "reason_code": "ema_fast_crossed_below_ema_slow",
                    }
                )

        prev_fast = float(next_fast)
        prev_slow = float(next_slow)

    return tuple(next_rows)
