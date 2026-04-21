from __future__ import annotations

from datetime import datetime
from typing import Mapping

from src.moex_strategy_sdk.errors import InterfaceValidationError
from src.moex_strategy_sdk.interfaces import StrategyInputFrame, StrategySignalFrame
from src.strategies.usdrubf_large_day_mr.config import StrategyConfig


def _coerce_date(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            if "T" in text:
                parsed = datetime.fromisoformat(text)
                return parsed.replace(hour=0, minute=0, second=0, microsecond=0)
            return datetime.fromisoformat(text + "T00:00:00")
        except ValueError as exc:
            raise InterfaceValidationError("invalid date") from exc
    raise InterfaceValidationError("invalid date")


def _coerce_numeric(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InterfaceValidationError(name + " must be numeric")
    return float(value)


def _coerce_prior_dir(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InterfaceValidationError("prior_dir must be int")
    if value not in (-1, 0, 1):
        raise InterfaceValidationError("prior_dir must be one of -1, 0, 1")
    return value


def generate_signals(*, inputs: StrategyInputFrame, config: StrategyConfig) -> StrategySignalFrame:
    previous_ts: datetime | None = None
    next_rows: list[dict[str, object]] = []

    for row in inputs:
        if not isinstance(row, Mapping):
            raise InterfaceValidationError("input row must be Mapping")

        decision_ts = _coerce_date(row.get("date"))
        source_trade_date = _coerce_date(row.get("source_trade_date"))
        prior_dir = _coerce_prior_dir(row.get("prior_dir"))
        prior_abs_body_points = _coerce_numeric("prior_abs_body_points", row.get("prior_abs_body_points"))
        prior_rel_range = _coerce_numeric("prior_rel_range", row.get("prior_rel_range"))

        if source_trade_date >= decision_ts:
            raise InterfaceValidationError("source_trade_date must be strictly earlier than date")
        if previous_ts is not None and decision_ts <= previous_ts:
            raise InterfaceValidationError("input rows must be strictly increasing by date")

        previous_ts = decision_ts

        if prior_dir == 0:
            continue
        if config.prior_dir_filter != 0 and prior_dir != config.prior_dir_filter:
            continue
        if prior_abs_body_points < config.min_prior_abs_body_points:
            continue
        if prior_rel_range < config.min_prior_rel_range:
            continue

        desired_position = -1.0 * float(prior_dir)
        signal_code = "fade_prior_up_day" if prior_dir > 0 else "fade_prior_down_day"
        reason_code = "prior_day_filters_passed"

        next_rows.append(
            {
                "instrument_id": config.instrument_id,
                "decision_ts": decision_ts,
                "desired_position": desired_position,
                "signal_code": signal_code,
                "signal_strength": prior_abs_body_points,
                "reason_code": reason_code,
            }
        )

    return tuple(next_rows)
