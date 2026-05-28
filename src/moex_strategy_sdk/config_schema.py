from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping

from .errors import ConfigValidationError


@dataclass(frozen=True)
class BaseStrategyConfig:
    strategy_id: str
    version: str

    def to_mapping(self) -> dict[str, object]:
        return dict(asdict(self))


@dataclass(frozen=True)
class StrategyConfigSchema:
    strategy_id: str
    schema_version: str
    defaults: Mapping[str, Any] = field(default_factory=dict)
    parameter_bounds: Mapping[str, tuple[Any, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or not self.strategy_id.strip():
            raise ConfigValidationError("strategy_id is required")
        if not isinstance(self.schema_version, str) or not self.schema_version.strip():
            raise ConfigValidationError("schema_version is required")
        object.__setattr__(self, "defaults", dict(self.defaults))
        object.__setattr__(self, "parameter_bounds", dict(self.parameter_bounds))

    def merged_config(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        merged = dict(self.defaults)
        if overrides:
            merged.update(dict(overrides))
        return merged
