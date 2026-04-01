
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from src.strategy.realtime.ema_3_19_15m.session_state_ema_3_19_15m import SessionStateEma31915m, validate_state


EMA_FAST_WINDOW = 3
EMA_SLOW_WINDOW = 19


def _alpha(window: int) -> float:
    return 2.0 / (float(window) + 1.0)


def _bar_end_str(bar: Mapping[str, Any]) -> str:
    raw = bar.get("end")
    if isinstance(raw, datetime):
        return raw.isoformat()
    if isinstance(raw, str) and raw.strip():
        datetime.fromisoformat(raw)
        return raw
    raise RuntimeError("invalid bar end")


def _bar_close(bar: Mapping[str, Any]) -> float:
    raw = bar.get("close")
    if not isinstance(raw, (int, float)):
        raise RuntimeError("invalid bar close")
    value = float(raw)
    if value <= 0.0:
        raise RuntimeError("invalid non-positive bar close")
    return value


def update_signal_state_on_closed_bar(state: SessionStateEma31915m, bar: Mapping[str, Any]) -> SessionStateEma31915m:
    validate_state(state, expected_trade_date=state.trade_date)
    close_price = _bar_close(bar)

    prev_fast = state.ema_fast
    prev_slow = state.ema_slow

    if prev_fast is None:
        next_fast = close_price
    else:
        next_fast = (_alpha(EMA_FAST_WINDOW) * close_price) + ((1.0 - _alpha(EMA_FAST_WINDOW)) * prev_fast)

    if prev_slow is None:
        next_slow = close_price
    else:
        next_slow = (_alpha(EMA_SLOW_WINDOW) * close_price) + ((1.0 - _alpha(EMA_SLOW_WINDOW)) * prev_slow)

    state.ema_fast = float(next_fast)
    state.ema_slow = float(next_slow)

    if state.pending_target is not None:
        return validate_state(state, expected_trade_date=state.trade_date)

    if prev_fast is None or prev_slow is None:
        return validate_state(state, expected_trade_date=state.trade_date)

    crossed_up = prev_fast <= prev_slow and state.ema_fast > state.ema_slow
    crossed_down = prev_fast >= prev_slow and state.ema_fast < state.ema_slow

    if crossed_up:
        state.pending_target = 1
        state.pending_signal_bar_end = _bar_end_str(bar)
    elif crossed_down:
        state.pending_target = -1
        state.pending_signal_bar_end = _bar_end_str(bar)

    return validate_state(state, expected_trade_date=state.trade_date)
