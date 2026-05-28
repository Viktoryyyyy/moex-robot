from typing import Any, Mapping, Protocol, Sequence


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
