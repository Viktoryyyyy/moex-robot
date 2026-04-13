from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class BaseStrategyConfig:
    strategy_id: str
    version: str

    def to_mapping(self) -> dict[str, object]:
        return dict(asdict(self))
