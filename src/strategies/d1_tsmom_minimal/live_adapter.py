from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from moex_strategy_sdk.errors import UnsupportedModeError

from .manifest import MANIFEST, STRATEGY_ID


class D1TSMOMMinimalLiveAdapter:
    strategy_id = STRATEGY_ID

    def to_live_intents(
        self,
        signals: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, object], ...]:
        raise UnsupportedModeError("d1_tsmom_minimal live adapter is blocked by default")

    @property
    def supports_live(self) -> bool:
        return MANIFEST.supports_live


__all__ = ["D1TSMOMMinimalLiveAdapter"]
