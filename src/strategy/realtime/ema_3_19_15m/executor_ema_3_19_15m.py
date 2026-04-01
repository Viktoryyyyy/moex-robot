
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from src.strategy.realtime.ema_3_19_15m.session_state_ema_3_19_15m import SessionStateEma31915m, validate_state


@dataclass(frozen=True)
class ExecutionEventEma31915m:
    trade_date: str
    seq: int
    bar_end: str
    action: str
    prev_pos: int
    new_pos: int
    price: float
    entry_price: float | None
    realized_pnl: float
    cum_pnl: float


def _bar_end_dt(bar: Mapping[str, Any]) -> datetime:
    raw = bar.get("end")
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str) and raw.strip():
        return datetime.fromisoformat(raw)
    raise RuntimeError("invalid bar end")


def _bar_close(bar: Mapping[str, Any]) -> float:
    raw = bar.get("close")
    if not isinstance(raw, (int, float)):
        raise RuntimeError("invalid bar close")
    value = float(raw)
    if value <= 0.0:
        raise RuntimeError("invalid non-positive bar close")
    return value


def _action(prev_pos: int, new_pos: int) -> str:
    if prev_pos == -1 and new_pos == 1:
        return "REVERSE_TO_LONG"
    if prev_pos == 1 and new_pos == -1:
        return "REVERSE_TO_SHORT"
    if prev_pos == 0 and new_pos == 1:
        return "OPEN_LONG"
    if prev_pos == 0 and new_pos == -1:
        return "OPEN_SHORT"
    if prev_pos == 1 and new_pos == 0:
        return "CLOSE_LONG"
    if prev_pos == -1 and new_pos == 0:
        return "CLOSE_SHORT"
    raise RuntimeError("position change action undefined")


def execute_pending_target_on_closed_bar(state: SessionStateEma31915m, bar: Mapping[str, Any]) -> ExecutionEventEma31915m | None:
    validate_state(state, expected_trade_date=state.trade_date)
    if state.pending_target is None:
        return None
    if state.pending_signal_bar_end is None:
        raise RuntimeError("pending target without pending signal bar end")

    bar_end_dt = _bar_end_dt(bar)
    exec_bar_end = bar_end_dt.isoformat()
    signal_bar_end_dt = datetime.fromisoformat(state.pending_signal_bar_end)
    if bar_end_dt <= signal_bar_end_dt:
        raise RuntimeError("impossible sequencing: execution bar is not strictly later than signal bar")

    new_pos = state.pending_target
    if new_pos not in (-1, 0, 1):
        raise RuntimeError("invalid pending target")
    prev_pos = state.pos
    if prev_pos not in (-1, 0, 1):
        raise RuntimeError("invalid state pos")
    if new_pos == prev_pos:
        raise RuntimeError("impossible sequencing: pending target equals current pos")

    price = _bar_close(bar)
    realized = 0.0
    if prev_pos != 0:
        if state.entry_price is None:
            raise RuntimeError("missing entry price for open position")
        if prev_pos == 1:
            realized = price - float(state.entry_price)
        elif prev_pos == -1:
            realized = float(state.entry_price) - price

    state.realized_pnl = float(realized)
    state.cum_pnl = float(state.cum_pnl + realized)
    state.pos = int(new_pos)
    state.entry_price = None if new_pos == 0 else float(price)
    state.trade_seq = int(state.trade_seq + 1)
    state.pending_target = None
    state.pending_signal_bar_end = None

    validate_state(state, expected_trade_date=state.trade_date)

    return ExecutionEventEma31915m(
        trade_date=state.trade_date,
        seq=state.trade_seq,
        bar_end=exec_bar_end,
        action=_action(prev_pos, new_pos),
        prev_pos=prev_pos,
        new_pos=new_pos,
        price=float(price),
        entry_price=state.entry_price,
        realized_pnl=state.realized_pnl,
        cum_pnl=state.cum_pnl,
    )
