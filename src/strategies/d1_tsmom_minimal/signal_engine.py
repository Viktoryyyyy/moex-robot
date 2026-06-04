from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from moex_strategy_sdk.errors import InterfaceValidationError

from .config import validate_config
from .manifest import STRATEGY_ID


class D1TSMOMMinimalSignalEngine:
    strategy_id = STRATEGY_ID

    def generate_signals(
        self,
        features: Mapping[str, Any],
        config: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, object], ...]:
        validated_config = validate_config(config)
        rows = features.get("rows")
        if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
            raise InterfaceValidationError("features.rows must be a sequence of mappings")
        signals: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise InterfaceValidationError("feature row must be a mapping")
            timestamp = row.get("timestamp")
            momentum = row.get("lookback_return")
            if timestamp is None:
                raise InterfaceValidationError("feature row timestamp is required")
            if isinstance(momentum, bool) or momentum is None:
                raise InterfaceValidationError("feature row lookback_return is required")
            momentum_value = float(momentum)
            if momentum_value > validated_config.signal_threshold:
                target_position = validated_config.target_abs_position
            elif momentum_value < -validated_config.signal_threshold:
                target_position = -validated_config.target_abs_position
            else:
                target_position = 0
            signals.append(
                {
                    "strategy_id": self.strategy_id,
                    "timestamp": timestamp,
                    "target_position": target_position,
                    "reason_code": "d1_tsmom_minimal_20d_return_sign",
                    "lookback_return": momentum_value,
                }
            )
        return tuple(signals)


__all__ = ["D1TSMOMMinimalSignalEngine"]
