from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence

StrategyInputFrame = Sequence[Mapping[str, object]]
StrategySignalFrame = tuple[dict[str, object], ...]


class SignalEngine(Protocol):
    def generate_signals(
        self,
        features: Mapping[str, Any],
        config: Mapping[str, Any] | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        ...


class BacktestAdapter(Protocol):
    def to_backtest_inputs(
        self,
        signals: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        ...


class LiveAdapter(Protocol):
    def to_live_intents(
        self,
        signals: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> Sequence[Mapping[str, Any]]:
        ...


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
