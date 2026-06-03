from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from moex_strategy_sdk.errors import InterfaceValidationError

from .config import validate_config


class ReferenceFixtureSignalEngine:
    strategy_id = "reference_fixture_strategy"

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
            if "timestamp" not in row:
                raise InterfaceValidationError("feature row timestamp is required")
            signals.append(
                {
                    "strategy_id": self.strategy_id,
                    "timestamp": row["timestamp"],
                    "target_position": validated_config.default_target_position,
                    "reason_code": "reference_fixture_noop",
                }
            )
        return tuple(signals)

__all__ = ["ReferenceFixtureSignalEngine"]
