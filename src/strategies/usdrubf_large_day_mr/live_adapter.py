from __future__ import annotations

from src.moex_strategy_sdk.errors import UnsupportedModeError
from src.moex_strategy_sdk.interfaces import LiveAdapterDecision, LiveStrategyInput, StrategySignalFrame
from src.strategies.usdrubf_large_day_mr.config import StrategyConfig


def build_live_decision(*, inputs: LiveStrategyInput, signals: StrategySignalFrame, config: StrategyConfig) -> LiveAdapterDecision:
    del inputs
    del signals
    del config
    raise UnsupportedModeError("usdrubf_large_day_mr does not support live mode in phase 9 first migration slice")
