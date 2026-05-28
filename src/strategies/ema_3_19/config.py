from __future__ import annotations

from dataclasses import dataclass

from moex_strategy_sdk.config_schema import BaseStrategyConfig
from moex_strategy_sdk.errors import ConfigValidationError


@dataclass(frozen=True)
class EMA319Config(BaseStrategyConfig):
    fast_period: int = 3
    slow_period: int = 19

    def __post_init__(self) -> None:
        if self.strategy_id != "ema_3_19":
            raise ConfigValidationError("strategy_id must be ema_3_19")
        if self.version != "0.1.0":
            raise ConfigValidationError("version must be 0.1.0")
        if not isinstance(self.fast_period, int) or not isinstance(self.slow_period, int):
            raise ConfigValidationError("periods must be integers")
        if self.fast_period < 1 or self.slow_period < 1:
            raise ConfigValidationError("periods must be positive")
        if self.fast_period >= self.slow_period:
            raise ConfigValidationError("fast_period must be less than slow_period")


def build_config(fast_period: int = 3, slow_period: int = 19) -> EMA319Config:
    return EMA319Config(
        strategy_id="ema_3_19",
        version="0.1.0",
        fast_period=fast_period,
        slow_period=slow_period,
    )


DEFAULT_CONFIG = build_config()

__all__ = ["DEFAULT_CONFIG", "EMA319Config", "build_config"]
