"""
Signal logic for EMA(5,12) GOOD-days robot.

This module:
  - updates EMA(5) and EMA(12) on each closed 5m bar,
  - respects daily trade filter via state["trade_today_flag"],
  - generates target position and crossing type information.

It does NOT know anything about order execution, commission or
MOEX APIs. It only works with:
  - state dict (from state_ema_5_12),
  - Bar object (from feed_ema_5_12).
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from .config_ema_5_12 import EMA_FAST_WINDOW, EMA_SLOW_WINDOW
from .feed_ema_5_12 import Bar


# Signal types returned by process_bar
SIGNAL_NO_TRADE = "NO_TRADE"
SIGNAL_TARGET_LONG = "TARGET_LONG"
SIGNAL_TARGET_SHORT = "TARGET_SHORT"
SIGNAL_TARGET_FLAT = "TARGET_FLAT"

# Cross types for diagnostics
CROSS_NONE = "NONE"
CROSS_UP = "CROSS_UP"
CROSS_DOWN = "CROSS_DOWN"


def _update_ema(prev_ema: Any, price: float, window: int) -> float:
    """
    Update EMA with standard formula:

      EMA_t = alpha * price + (1 - alpha) * EMA_{t-1},
      alpha = 2 / (window + 1)

    If prev_ema is None, EMA is initialised as current price.
    """
    alpha = 2.0 / (window + 1.0)
    if prev_ema is None:
        return float(price)
    try:
        prev = float(prev_ema)
    except Exception:
        prev = float(price)
    return alpha * float(price) + (1.0 - alpha) * prev


def _detect_cross(last_signal: str, target_pos: int) -> str:
    """
    Detect crossing direction for logging/diagnostics.

    last_signal:
      "NONE", "LONG", "SHORT"
    target_pos:
      -1, 0, 1
    """
    if target_pos > 0:
        # Target LONG
        if last_signal == "SHORT":
            return CROSS_UP
        return CROSS_NONE
    if target_pos < 0:
        # Target SHORT
        if last_signal == "LONG":
            return CROSS_DOWN
        return CROSS_NONE
    # Target FLAT -> no crossing classification here
    return CROSS_NONE


def process_bar(state: Dict[str, Any], bar: Bar) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Update EMA(5,12) on closed 5m bar and generate signal.

    Parameters
    ----------
    state : dict
        Mutable state dict loaded via state_ema_5_12.load_state().
    bar : Bar
        Closed 5m bar (end, open, high, low, close, volume).

    Returns
    -------
    new_state : dict
        Updated state.
    signal : dict
        Signal description with keys:
          - "type"       : SIGNAL_* constant
          - "target_pos" : -1 / 0 / 1
          - "cross_type" : CROSS_UP / CROSS_DOWN / CROSS_NONE
          - "ema_fast"   : latest EMA(5) value
          - "ema_slow"   : latest EMA(12) value

        For NO_TRADE signal:
          - "target_pos" equals current state["position"]
          - "cross_type" is CROSS_NONE

    Behaviour
    ---------
    - EMA(5) and EMA(12) are always updated on each bar.
    - bars_count is incremented on each bar.
    - If state["trade_today_flag"] != 1:
        no trading signals are generated (type = NO_TRADE).
    - If bars_count < EMA_SLOW_WINDOW:
        no trading signals are generated (type = NO_TRADE).
    - When trading is allowed:
        target position is defined by sign(EMA5 - EMA12):
          EMA5 > EMA12 -> target_pos = +1 (LONG)
          EMA5 < EMA12 -> target_pos = -1 (SHORT)
          otherwise    -> target_pos = 0 (FLAT)
        If target_pos equals current position -> NO_TRADE.
        Otherwise -> SIGNAL_TARGET_LONG / _SHORT / _FLAT.
    """
    new_state = dict(state)  # shallow copy is enough for flat dict
    close_price = float(bar.close)

    # Update EMA values
    ema_fast_prev = new_state.get("ema_fast")
    ema_slow_prev = new_state.get("ema_slow")

    ema_fast = _update_ema(ema_fast_prev, close_price, EMA_FAST_WINDOW)
    ema_slow = _update_ema(ema_slow_prev, close_price, EMA_SLOW_WINDOW)

    new_state["ema_fast"] = ema_fast
    new_state["ema_slow"] = ema_slow

    # Update bars_count
    try:
        bars_count = int(new_state.get("bars_count", 0)) + 1
    except Exception:
        bars_count = 1
    new_state["bars_count"] = bars_count

    # Default signal: no trade, keep current position
    current_pos = int(new_state.get("position", 0))
    signal_type = SIGNAL_NO_TRADE
    target_pos = current_pos
    cross_type = CROSS_NONE

    # Respect daily trade filter (R1)
    trade_today_flag = int(new_state.get("trade_today_flag", 0))
    if trade_today_flag != 1:
        # We still return EMA updates but no trading signal.
        new_state["last_signal"] = new_state.get("last_signal", "NONE")
        return new_state, {
            "type": signal_type,
            "target_pos": target_pos,
            "cross_type": cross_type,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
        }

    # Require at least EMA_SLOW_WINDOW bars before trading
    if bars_count < EMA_SLOW_WINDOW:
        new_state["last_signal"] = new_state.get("last_signal", "NONE")
        return new_state, {
            "type": signal_type,
            "target_pos": target_pos,
            "cross_type": cross_type,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
        }

    # Determine target position by sign of EMA difference
    diff = ema_fast - ema_slow
    if diff > 0.0:
        target_pos = 1
        target_label = "LONG"
    elif diff < 0.0:
        target_pos = -1
        target_label = "SHORT"
    else:
        target_pos = 0
        target_label = "NONE"

    last_signal_label = str(new_state.get("last_signal", "NONE") or "NONE")
    cross_type = _detect_cross(last_signal_label, target_pos)

    if target_pos == current_pos:
        # No change in position -> NO_TRADE
        signal_type = SIGNAL_NO_TRADE
    else:
        if target_pos > 0:
            signal_type = SIGNAL_TARGET_LONG
        elif target_pos < 0:
            signal_type = SIGNAL_TARGET_SHORT
        else:
            signal_type = SIGNAL_TARGET_FLAT

    # Update last_signal in state for next bar
    new_state["last_signal"] = target_label

    return new_state, {
        "type": signal_type,
        "target_pos": target_pos,
        "cross_type": cross_type,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
    }
