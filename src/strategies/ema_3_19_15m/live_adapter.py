from __future__ import annotations

from src.moex_strategy_sdk.errors import UnsupportedModeError
from src.moex_strategy_sdk.interfaces import LiveAdapterDecision, LiveStrategyInput, StrategySignalFrame
from src.strategies.ema_3_19_15m.config import StrategyConfig


def build_live_decision(
    *,
    inputs: LiveStrategyInput,
    signals: StrategySignalFrame,
    config: StrategyConfig,
) -> LiveAdapterDecision:
    del inputs
    del signals
    del config
    raise UnsupportedModeError("ema_3_19_15m live mode is unsupported in wave-1")
