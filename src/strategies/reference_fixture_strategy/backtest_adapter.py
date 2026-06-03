from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from moex_backtest.engine.canonical import CanonicalBacktestInput, CostConfig, ExecutionConfig
from moex_strategy_sdk.errors import InterfaceValidationError

from .manifest import MANIFEST, STRATEGY_ID


class ReferenceFixtureBacktestAdapter:
    strategy_id = STRATEGY_ID

    def to_backtest_inputs(
        self,
        signals: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> CanonicalBacktestInput:
        if not MANIFEST.supports_backtest:
            raise InterfaceValidationError("backtest is not supported by manifest")
        if not isinstance(context, Mapping):
            raise InterfaceValidationError("context must be a mapping")
        bars = context.get("bars")
        if isinstance(bars, (str, bytes)) or not isinstance(bars, Sequence):
            raise InterfaceValidationError("context.bars must be a sequence")
        return CanonicalBacktestInput(
            bars=bars,
            signals=tuple(self._normalize_signal(row) for row in signals),
            cost_config=self._cost_config(context.get("cost_config", {})),
            execution_config=self._execution_config(context.get("execution_config", {})),
        )

    def _normalize_signal(self, row: Mapping[str, Any]) -> dict[str, object]:
        if not isinstance(row, Mapping):
            raise InterfaceValidationError("signal row must be a mapping")
        if "timestamp" not in row:
            raise InterfaceValidationError("signal timestamp is required")
        target_position = row.get("target_position")
        if isinstance(target_position, bool) or target_position not in (-1, 0, 1):
            raise InterfaceValidationError("target_position must be one of -1, 0, 1")
        return {"timestamp": row["timestamp"], "target_position": int(target_position)}

    def _cost_config(self, value: object) -> CostConfig:
        if isinstance(value, CostConfig):
            return value
        if isinstance(value, Mapping):
            return CostConfig(
                commission_bps=value.get("commission_bps", 0.0),
                slippage_bps=value.get("slippage_bps", 0.0),
            )
        raise InterfaceValidationError("cost_config must be CostConfig or mapping")

    def _execution_config(self, value: object) -> ExecutionConfig:
        if isinstance(value, ExecutionConfig):
            return value
        if isinstance(value, Mapping):
            return ExecutionConfig(
                fill_model_id=str(value.get("fill_model_id", "next_bar_open")),
                terminal_close=bool(value.get("terminal_close", True)),
                initial_cash=value.get("initial_cash", 0.0),
            )
        raise InterfaceValidationError("execution_config must be ExecutionConfig or mapping")

__all__ = ["ReferenceFixtureBacktestAdapter"]
