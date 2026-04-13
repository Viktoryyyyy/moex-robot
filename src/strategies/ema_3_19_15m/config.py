from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from src.moex_strategy_sdk.config_schema import BaseStrategyConfig
from src.moex_strategy_sdk.errors import ConfigValidationError


@dataclass(frozen=True)
class StrategyConfig(BaseStrategyConfig):
    instrument_id: str = "si"
    timeframe: str = "15m"
    ema_fast_window: int = 3
    ema_slow_window: int = 19
    warmup_bars: int = 19


def _expect_exact_str(name: str, value: object, expected: str) -> str:
    if not isinstance(value, str) or value != expected:
        raise ConfigValidationError(name + " must equal " + repr(expected))
    return value


def _expect_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigValidationError(name + " must be int")
    return value


def validate_config(raw_config: Mapping[str, object]) -> StrategyConfig:
    if not isinstance(raw_config, Mapping):
        raise ConfigValidationError("raw_config must be Mapping")

    allowed = {
        "strategy_id",
        "version",
        "instrument_id",
        "timeframe",
        "ema_fast_window",
        "ema_slow_window",
        "warmup_bars",
    }
    unknown = sorted(set(raw_config.keys()) - allowed)
    if unknown:
        raise ConfigValidationError("unknown config field(s): " + ", ".join(unknown))

    data = {
        "strategy_id": "ema_3_19_15m",
        "version": "1.0.0",
        "instrument_id": "si",
        "timeframe": "15m",
        "ema_fast_window": 3,
        "ema_slow_window": 19,
        "warmup_bars": 19,
    }
    data.update(dict(raw_config))

    strategy_id = _expect_exact_str("strategy_id", data["strategy_id"], "ema_3_19_15m")
    version = _expect_exact_str("version", data["version"], "1.0.0")
    instrument_id = _expect_exact_str("instrument_id", data["instrument_id"], "si")
    timeframe = _expect_exact_str("timeframe", data["timeframe"], "15m")
    ema_fast_window = _expect_int("ema_fast_window", data["ema_fast_window"])
    ema_slow_window = _expect_int("ema_slow_window", data["ema_slow_window"])
    warmup_bars = _expect_int("warmup_bars", data["warmup_bars"])

    if ema_fast_window <= 0:
        raise ConfigValidationError("ema_fast_window must be > 0")
    if ema_slow_window <= 0:
        raise ConfigValidationError("ema_slow_window must be > 0")
    if ema_fast_window >= ema_slow_window:
        raise ConfigValidationError("ema_fast_window must be < ema_slow_window")
    if warmup_bars < ema_slow_window:
        raise ConfigValidationError("warmup_bars must be >= ema_slow_window")

    return StrategyConfig(
        strategy_id=strategy_id,
        version=version,
        instrument_id=instrument_id,
        timeframe=timeframe,
        ema_fast_window=ema_fast_window,
        ema_slow_window=ema_slow_window,
        warmup_bars=warmup_bars,
    )
