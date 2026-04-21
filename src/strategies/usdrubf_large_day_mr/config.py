from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from src.moex_strategy_sdk.config_schema import BaseStrategyConfig
from src.moex_strategy_sdk.errors import ConfigValidationError


@dataclass(frozen=True)
class StrategyConfig(BaseStrategyConfig):
    instrument_id: str = "usdrubf"
    timeframe: str = "1d"
    prior_dir_filter: int = 0
    min_prior_abs_body_points: float = 0.0
    min_prior_rel_range: float = 0.0
    holding_period_days: int = 1


def _expect_exact_str(name: str, value: object, expected: str) -> str:
    if not isinstance(value, str) or value != expected:
        raise ConfigValidationError(name + " must equal " + repr(expected))
    return value


def _expect_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigValidationError(name + " must be int")
    return value


def _expect_float(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigValidationError(name + " must be numeric")
    return float(value)


def validate_config(raw_config: Mapping[str, object]) -> StrategyConfig:
    if not isinstance(raw_config, Mapping):
        raise ConfigValidationError("raw_config must be Mapping")

    allowed = {
        "strategy_id",
        "version",
        "instrument_id",
        "timeframe",
        "prior_dir_filter",
        "min_prior_abs_body_points",
        "min_prior_rel_range",
        "holding_period_days",
    }
    unknown = sorted(set(raw_config.keys()) - allowed)
    if unknown:
        raise ConfigValidationError("unknown config field(s): " + ", ".join(unknown))

    data = {
        "strategy_id": "usdrubf_large_day_mr",
        "version": "1.0.0",
        "instrument_id": "usdrubf",
        "timeframe": "1d",
        "prior_dir_filter": 0,
        "min_prior_abs_body_points": 0.0,
        "min_prior_rel_range": 0.0,
        "holding_period_days": 1,
    }
    data.update(dict(raw_config))

    strategy_id = _expect_exact_str("strategy_id", data["strategy_id"], "usdrubf_large_day_mr")
    version = _expect_exact_str("version", data["version"], "1.0.0")
    instrument_id = _expect_exact_str("instrument_id", data["instrument_id"], "usdrubf")
    timeframe = _expect_exact_str("timeframe", data["timeframe"], "1d")
    prior_dir_filter = _expect_int("prior_dir_filter", data["prior_dir_filter"])
    min_prior_abs_body_points = _expect_float("min_prior_abs_body_points", data["min_prior_abs_body_points"])
    min_prior_rel_range = _expect_float("min_prior_rel_range", data["min_prior_rel_range"])
    holding_period_days = _expect_int("holding_period_days", data["holding_period_days"])

    if prior_dir_filter not in (-1, 0, 1):
        raise ConfigValidationError("prior_dir_filter must be one of -1, 0, 1")
    if min_prior_abs_body_points < 0.0:
        raise ConfigValidationError("min_prior_abs_body_points must be >= 0")
    if min_prior_rel_range < 0.0:
        raise ConfigValidationError("min_prior_rel_range must be >= 0")
    if holding_period_days != 1:
        raise ConfigValidationError("holding_period_days must equal 1")

    return StrategyConfig(
        strategy_id=strategy_id,
        version=version,
        instrument_id=instrument_id,
        timeframe=timeframe,
        prior_dir_filter=prior_dir_filter,
        min_prior_abs_body_points=min_prior_abs_body_points,
        min_prior_rel_range=min_prior_rel_range,
        holding_period_days=holding_period_days,
    )
