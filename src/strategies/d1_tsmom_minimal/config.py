from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from moex_strategy_sdk import BaseStrategyConfig, StrategyConfigSchema
from moex_strategy_sdk.errors import ConfigValidationError

from .manifest import STRATEGY_ID


@dataclass(frozen=True)
class D1TSMOMMinimalConfig(BaseStrategyConfig):
    lookback_days: int = 20
    signal_threshold: float = 0.0
    target_abs_position: int = 1
    execution_delay_bars: int = 1

    def __post_init__(self) -> None:
        if self.strategy_id != STRATEGY_ID:
            raise ConfigValidationError("strategy_id mismatch")
        if self.version != "0.1.0":
            raise ConfigValidationError("unsupported strategy config version")
        if isinstance(self.lookback_days, bool) or self.lookback_days != 20:
            raise ConfigValidationError("lookback_days is fixed at 20 for the minimal platform-run ref")
        threshold = float(self.signal_threshold)
        if threshold != 0.0:
            raise ConfigValidationError("signal_threshold is fixed at 0.0 for the minimal platform-run ref")
        if isinstance(self.target_abs_position, bool) or self.target_abs_position != 1:
            raise ConfigValidationError("target_abs_position is fixed at 1 for the minimal platform-run ref")
        if isinstance(self.execution_delay_bars, bool) or self.execution_delay_bars != 1:
            raise ConfigValidationError("execution_delay_bars is fixed at 1 for canonical next-bar execution")
        object.__setattr__(self, "signal_threshold", threshold)


def build_config_schema() -> StrategyConfigSchema:
    return StrategyConfigSchema(
        strategy_id=STRATEGY_ID,
        schema_version="d1_tsmom_minimal_config.v1",
        defaults={
            "strategy_id": STRATEGY_ID,
            "version": "0.1.0",
            "lookback_days": 20,
            "signal_threshold": 0.0,
            "target_abs_position": 1,
            "execution_delay_bars": 1,
        },
        parameter_bounds={
            "lookback_days": (20, 20),
            "signal_threshold": (0.0, 0.0),
            "target_abs_position": (1, 1),
            "execution_delay_bars": (1, 1),
        },
    )


def validate_config(values: Mapping[str, object] | D1TSMOMMinimalConfig | None = None) -> D1TSMOMMinimalConfig:
    if isinstance(values, D1TSMOMMinimalConfig):
        return values
    merged = build_config_schema().merged_config(values)
    return D1TSMOMMinimalConfig(
        strategy_id=str(merged["strategy_id"]),
        version=str(merged["version"]),
        lookback_days=int(merged["lookback_days"]),
        signal_threshold=float(merged["signal_threshold"]),
        target_abs_position=int(merged["target_abs_position"]),
        execution_delay_bars=int(merged["execution_delay_bars"]),
    )


__all__ = ["D1TSMOMMinimalConfig", "build_config_schema", "validate_config"]
