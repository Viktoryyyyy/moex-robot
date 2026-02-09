#!/usr/bin/env python3
"""
Core EMA(5,12) logic for Si robot.

Pure functions:
  - EMA update,
  - target position decision (LONG/SHORT/FLAT) based on EMA(5) vs EMA(12).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class EmaCoreState:
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    bars_seen: int = 0


def update_ema_values(
    state: EmaCoreState,
    close_price: float,
    fast_window: int,
    slow_window: int,
) -> EmaCoreState:
    """
    Update EMA(5,12) given new close price.
    """
    price = float(close_price)

    def _update(prev: Optional[float], p: float, window: int) -> float:
        alpha = 2.0 / (window + 1.0)
        if prev is None:
            return p
        return alpha * p + (1.0 - alpha) * float(prev)

    state.bars_seen += 1
    state.ema_fast = _update(state.ema_fast, price, fast_window)
    state.ema_slow = _update(state.ema_slow, price, slow_window)
    return state


def decide_target_pos(
    state: EmaCoreState,
    current_pos: int,
    slow_window: int,
) -> int:
    """
    Decide target position based on EMA(5,12):

      - Before we have 'slow_window' bars: keep current position.
      - After that:
          diff = ema_fast - ema_slow
          diff > 0 -> LONG (+1)
          diff < 0 -> SHORT (-1)
          diff = 0 -> FLAT (0)
    """
    if state.bars_seen < slow_window:
        return current_pos

    if state.ema_fast is None or state.ema_slow is None:
        return current_pos

    diff = state.ema_fast - state.ema_slow
    if diff > 0.0:
        return 1
    elif diff < 0.0:
        return -1
    else:
        return 0
