from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def to_backtest_inputs(
    signals: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "strategy_id": "ema_3_19",
        "signal_rows": tuple(dict(row) for row in signals),
        "context": dict(context or {}),
        "adapter_contract": "canonical_backtest_input.v1",
    }


__all__ = ["to_backtest_inputs"]
