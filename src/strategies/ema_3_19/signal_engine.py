from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .config import DEFAULT_CONFIG, EMA319Config

SignalRow = Mapping[str, Any]
SignalFrame = tuple[dict[str, Any], ...]


def _direction(fast_value: float, slow_value: float) -> int:
    if fast_value > slow_value:
        return 1
    if fast_value < slow_value:
        return -1
    return 0


def generate_signals(
    features: Sequence[SignalRow],
    config: EMA319Config = DEFAULT_CONFIG,
) -> SignalFrame:
    output: list[dict[str, Any]] = []
    for row in features:
        fast_value = float(row["ema_3"])
        slow_value = float(row["ema_19"])
        output.append(
            {
                "strategy_id": config.strategy_id,
                "signal_name": "ema_3_19_direction",
                "signal_value": _direction(fast_value, slow_value),
                "feature_timestamp": row.get("feature_timestamp"),
            }
        )
    return tuple(output)


__all__ = ["SignalFrame", "SignalRow", "generate_signals"]
