from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from moex_strategy_sdk import BaseStrategyConfig, StrategyConfigSchema
from moex_strategy_sdk.errors import ConfigValidationError

from .manifest import STRATEGY_ID


@dataclass(frozen=True)
class ReferenceFixtureConfig(BaseStrategyConfig):
    default_target_position: int = 0
    allow_non_flat_fixture_positions: bool = False

    def __post_init__(self) -> None:
        if self.strategy_id != STRATEGY_ID:
            raise ConfigValidationError("strategy_id mismatch")
        if not isinstance(self.version, str) or not self.version.strip():
            raise ConfigValidationError("version is required")
        if isinstance(self.default_target_position, bool) or self.default_target_position not in (-1, 0, 1):
            raise ConfigValidationError("default_target_position must be one of -1, 0, 1")
        if not isinstance(self.allow_non_flat_fixture_positions, bool):
            raise ConfigValidationError("allow_non_flat_fixture_positions must be boolean")
        if self.default_target_position != 0 and not self.allow_non_flat_fixture_positions:
            raise ConfigValidationError("non-flat fixture position requires explicit opt-in")


def build_config_schema() -> StrategyConfigSchema:
    return StrategyConfigSchema(
        strategy_id=STRATEGY_ID,
        schema_version="reference_fixture_config.v1",
        defaults={
            "strategy_id": STRATEGY_ID,
            "version": "0.1.0",
            "default_target_position": 0,
            "allow_non_flat_fixture_positions": False,
        },
        parameter_bounds={"default_target_position": (-1, 1)},
    )


def validate_config(values: Mapping[str, object] | ReferenceFixtureConfig | None = None) -> ReferenceFixtureConfig:
    if isinstance(values, ReferenceFixtureConfig):
        return values
    merged = build_config_schema().merged_config(values)
    return ReferenceFixtureConfig(
        strategy_id=str(merged["strategy_id"]),
        version=str(merged["version"]),
        default_target_position=merged["default_target_position"],
        allow_non_flat_fixture_positions=merged["allow_non_flat_fixture_positions"],
    )

__all__ = ["ReferenceFixtureConfig", "build_config_schema", "validate_config"]
