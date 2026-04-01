
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


STATE_DIR = Path("data") / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class SessionStateEma31915m:
    trade_date: str
    last_bar_end: str | None = None
    pos: int = 0
    entry_price: float | None = None
    realized_pnl: float = 0.0
    cum_pnl: float = 0.0
    trade_seq: int = 0
    ema_fast: float | None = None
    ema_slow: float | None = None
    pending_target: int | None = None
    pending_signal_bar_end: str | None = None


def _state_path(trade_date: str) -> Path:
    return STATE_DIR / ("ema_3_19_15m_session_" + trade_date + ".json")


def _expect_trade_date(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("invalid trade_date")
    try:
        date.fromisoformat(value)
    except Exception as e:
        raise RuntimeError("invalid trade_date format: " + str(e))
    return value


def _expect_bar_end(value: Any, key: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("invalid " + key)
    try:
        datetime.fromisoformat(value)
    except Exception as e:
        raise RuntimeError("invalid " + key + ": " + str(e))
    return value


def _expect_pos(value: Any) -> int:
    if not isinstance(value, int) or value not in (-1, 0, 1):
        raise RuntimeError("invalid pos")
    return value


def _expect_target(value: Any) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or value not in (-1, 0, 1):
        raise RuntimeError("invalid pending_target")
    return value


def _expect_number(value: Any, key: str, allow_none: bool = False) -> float | None:
    if value is None:
        if allow_none:
            return None
        raise RuntimeError("missing " + key)
    if not isinstance(value, (int, float)):
        raise RuntimeError("invalid " + key)
    val = float(value)
    if not math.isfinite(val):
        raise RuntimeError("non-finite " + key)
    return val


def _expect_int(value: Any, key: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError("invalid " + key)
    return value


def validate_state(state: SessionStateEma31915m, expected_trade_date: str | None = None) -> SessionStateEma31915m:
    state.trade_date = _expect_trade_date(state.trade_date)
    state.last_bar_end = _expect_bar_end(state.last_bar_end, "last_bar_end")
    state.pos = _expect_pos(state.pos)
    state.entry_price = _expect_number(state.entry_price, "entry_price", allow_none=True)
    state.realized_pnl = float(_expect_number(state.realized_pnl, "realized_pnl"))
    state.cum_pnl = float(_expect_number(state.cum_pnl, "cum_pnl"))
    state.trade_seq = _expect_int(state.trade_seq, "trade_seq")
    state.ema_fast = _expect_number(state.ema_fast, "ema_fast", allow_none=True)
    state.ema_slow = _expect_number(state.ema_slow, "ema_slow", allow_none=True)
    state.pending_target = _expect_target(state.pending_target)
    state.pending_signal_bar_end = _expect_bar_end(state.pending_signal_bar_end, "pending_signal_bar_end")
    if expected_trade_date is not None and state.trade_date != expected_trade_date:
        raise RuntimeError("trade_date mismatch")
    if state.pos == 0 and state.entry_price is not None:
        raise RuntimeError("entry_price must be null when pos == 0")
    if state.pos != 0 and state.entry_price is None:
        raise RuntimeError("entry_price missing for non-flat pos")
    if state.pending_target is None and state.pending_signal_bar_end is not None:
        raise RuntimeError("pending_signal_bar_end without pending_target")
    if state.pending_target is not None and state.pending_signal_bar_end is None:
        raise RuntimeError("pending_target without pending_signal_bar_end")
    return state


def load_or_init_session_state(trade_date: str) -> SessionStateEma31915m:
    trade_date = _expect_trade_date(trade_date)
    path = _state_path(trade_date)
    if not path.exists():
        return validate_state(SessionStateEma31915m(trade_date=trade_date), expected_trade_date=trade_date)
    try:
        raw = path.read_text(encoding="utf-8")
        obj = json.loads(raw)
    except Exception as e:
        raise RuntimeError("broken session state json: " + str(e))
    if not isinstance(obj, dict):
        raise RuntimeError("session state payload must be object")
    required = {
        "trade_date",
        "last_bar_end",
        "pos",
        "entry_price",
        "realized_pnl",
        "cum_pnl",
        "trade_seq",
        "ema_fast",
        "ema_slow",
        "pending_target",
        "pending_signal_bar_end",
    }
    missing = sorted(required.difference(obj.keys()))
    if missing:
        raise RuntimeError("missing session state fields: " + ",".join(missing))
    state = SessionStateEma31915m(**{k: obj.get(k) for k in required})
    return validate_state(state, expected_trade_date=trade_date)


def save_session_state(state: SessionStateEma31915m) -> None:
    validate_state(state, expected_trade_date=state.trade_date)
    path = _state_path(state.trade_date)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(asdict(state), ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
