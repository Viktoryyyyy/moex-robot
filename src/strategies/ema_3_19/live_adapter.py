from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from moex_strategy_sdk.errors import UnsupportedModeError

SUPPORTS_LIVE = False


def assert_live_blocked() -> None:
    raise UnsupportedModeError("ema_3_19 does not permit live mode")


def to_live_intents(
    signals: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any] | None = None,
) -> tuple[Mapping[str, Any], ...]:
    assert_live_blocked()
    return tuple()


__all__ = ["SUPPORTS_LIVE", "assert_live_blocked", "to_live_intents"]
