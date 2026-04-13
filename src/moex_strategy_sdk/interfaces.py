from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, Sequence

StrategyInputFrame = Sequence[Mapping[str, object]]
StrategySignalFrame = tuple[dict[str, object], ...]


@dataclass(frozen=True)
class BacktestAdapterRequest:
    strategy_id: str
    strategy_version: str
    normalized_signals: StrategySignalFrame
    hook_overrides: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class LiveStrategyInput:
    instrument_id: str
    decision_ts: datetime | None = None
    state: Mapping[str, object] = field(default_factory=dict)
    runtime_metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class LiveAdapterDecision:
    strategy_id: str
    strategy_version: str
    instrument_id: str
    decision_ts: datetime
    desired_position: float
    reason_code: str
    supports_execution: bool
    state_patch: Mapping[str, object] = field(default_factory=dict)
