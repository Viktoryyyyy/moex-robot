#!/usr/bin/env python3
"""
Execution layer for EMA(5,12) realtime robot.

Responsibilities:
  - On each new bar, execute pending_target_pos (if any) at bar close.
  - Update session state (pos, entry_price, pnl_real, pnl_cum, seq_no, last_bar_end).
  - Produce a single TradeEvent per bar at most.

This module does NOT:
  - generate EMA signals,
  - call MOEX API,
  - touch CSV/log files.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Tuple

from .config_ema_5_12 import COMMISSION_PTS_PER_TRADE
from .session_state import SessionState


TRADE_QTY = 1


@dataclass
class TradeEvent:
    seq_no: int

    bar_end_signal: datetime
    bar_end_exec: datetime

    side_exec: str  # "BUY" / "SELL" / "FLAT"
    qty: int

    price_exec: float
    price_signal_ref: float

    pnl: float
    pnl_cum: float

    pos_before: int
    pos_after: int

    reason_open: str
    reason_close: str


def _commission_points(pos_before: int, target_pos: int) -> float:
    """
    Commission model consistent with strategy config:

      COMMISSION_PTS_PER_TRADE is per full round-trip (open+close).

      - Open (0 -> ±1) or close (±1 -> 0) -> half round-trip.
      - Reverse (±1 -> ∓1) -> full round-trip.
    """
    if pos_before == 0 and target_pos != 0:
        return COMMISSION_PTS_PER_TRADE / 2.0
    if pos_before != 0 and target_pos == 0:
        return COMMISSION_PTS_PER_TRADE / 2.0
    if pos_before != 0 and target_pos != 0 and target_pos != pos_before:
        return COMMISSION_PTS_PER_TRADE
    return 0.0


def execute_on_bar(
    bar: Mapping[str, Any],
    state: SessionState,
) -> Tuple[SessionState, Optional[TradeEvent]]:
    """
    Execute pending_target_pos on this bar, if any.

    Parameters
    ----------
    bar
        Mapping with at least fields:
          - "end": datetime or ISO string
          - "close": price (int/float)
    state
        SessionState, mutated in place and also returned.

    Returns
    -------
    state
        Updated SessionState.
    trade
        TradeEvent if a trade was executed on this bar, else None.
    """
    # Normalize bar fields
    end_raw = bar.get("end")
    if isinstance(end_raw, datetime):
        bar_end = end_raw
    else:
        bar_end = datetime.fromisoformat(str(end_raw))

    close_price = float(bar["close"])

    trade: Optional[TradeEvent] = None

    # If there is no pending target, just update last_bar_end and return
    if state.pending_target_pos is None:
        state.last_bar_end = bar_end
        return state, None

    # Take snapshot of current state
    target_pos = int(state.pending_target_pos)
    bar_end_signal = state.pending_signal_bar_end or bar_end
    price_signal_ref = (
        float(state.pending_signal_price)
        if state.pending_signal_price is not None
        else close_price
    )
    reason = state.pending_reason or ""

    # Clear pending signal in state
    state.pending_target_pos = None
    state.pending_signal_bar_end = None
    state.pending_signal_price = None
    state.pending_reason = None

    pos_before = state.pos
    entry_price = state.entry_price

    commission = _commission_points(pos_before, target_pos)

    pnl_trade = 0.0
    if pos_before != 0 and entry_price is not None:
        ep = float(entry_price)
        if pos_before > 0:
            pnl_trade = close_price - ep
        else:
            pnl_trade = ep - close_price

    pnl_net = pnl_trade - commission

    state.pnl_real += pnl_net
    state.pnl_cum += pnl_net

    pos_after = target_pos
    if pos_after != 0:
        state.entry_price = close_price
    else:
        state.entry_price = None

    state.seq_no += 1
    state.last_bar_end = bar_end

    if pos_after > pos_before:
        side_exec = "BUY"
    elif pos_after < pos_before:
        side_exec = "SELL"
    else:
        side_exec = "FLAT"

    trade = TradeEvent(
        seq_no=state.seq_no,
        bar_end_signal=bar_end_signal,
        bar_end_exec=bar_end,
        side_exec=side_exec,
        qty=TRADE_QTY,
        price_exec=close_price,
        price_signal_ref=price_signal_ref,
        pnl=pnl_net,
        pnl_cum=state.pnl_cum,
        pos_before=pos_before,
        pos_after=pos_after,
        reason_open=reason,
        reason_close=reason,
    )

    return state, trade
