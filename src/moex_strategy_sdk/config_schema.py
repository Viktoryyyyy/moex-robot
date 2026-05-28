from dataclasses import dataclass, field
from typing import Any, Mapping

from .errors import StrategySDKError


@dataclass(frozen=True)
class StrategyConfigSchema:
    strategy_id: str
    schema_version: str
    defaults: Mapping[str, Any] = field(default_factory=dict)
    parameter_bounds: Mapping[str, tuple[Any, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or not self.strategy_id.strip():
            raise StrategySDKError("strategy_id must be a non-empty string")
        if not isinstance(self.schema_version, str) or not self.schema_version.strip():
            raise StrategySDKError("schema_version must be a non-empty string")

    def merged_config(self, values: Mapping[str, Any] | None = None) -> dict[str, Any]:
        merged = dict(self.defaults)
        if values:
            merged.update(dict(values))
        return merged
