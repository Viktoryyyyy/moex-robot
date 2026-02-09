#!/usr/bin/env python3
"""
Session state for EMA(5,12) realtime robot.

One SessionState instance corresponds to one trading date (MSK).
State is persisted as JSON under data/state/.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional


STATE_DIR = Path("data") / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class SessionState:
    trade_date: date

    last_bar_end: Optional[datetime] = None

    pos: int = 0
    entry_price: Optional[float] = None

    pnl_real: float = 0.0
    pnl_cum: float = 0.0

    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    ema_bars_seen: int = 0

    pending_target_pos: Optional[int] = None
    pending_signal_bar_end: Optional[datetime] = None
    pending_signal_price: Optional[float] = None
    pending_reason: Optional[str] = None

    seq_no: int = 0

    version: int = 1


def _state_path(trade_date: date) -> Path:
    name = f"ema_5_12_state_{trade_date.isoformat()}.json"
    return STATE_DIR / name


def session_state_to_dict(state: SessionState) -> Dict[str, Any]:
    d = asdict(state)

    d["trade_date"] = state.trade_date.isoformat()
    d["last_bar_end"] = (
        state.last_bar_end.isoformat() if state.last_bar_end is not None else None
    )
    d["pending_signal_bar_end"] = (
        state.pending_signal_bar_end.isoformat()
        if state.pending_signal_bar_end is not None
        else None
    )

    return d


def session_state_from_dict(d: Dict[str, Any]) -> SessionState:
    td = date.fromisoformat(d["trade_date"])

    last_bar_end_raw = d.get("last_bar_end")
    last_bar_end_dt = (
        datetime.fromisoformat(last_bar_end_raw) if last_bar_end_raw is not None else None
    )

    pending_signal_bar_end_raw = d.get("pending_signal_bar_end")
    pending_signal_bar_end_dt = (
        datetime.fromisoformat(pending_signal_bar_end_raw)
        if pending_signal_bar_end_raw is not None
        else None
    )

    state = SessionState(
        trade_date=td,
        last_bar_end=last_bar_end_dt,
        pos=int(d.get("pos", 0)),
        entry_price=(
            float(d["entry_price"]) if d.get("entry_price") is not None else None
        ),
        pnl_real=float(d.get("pnl_real", 0.0)),
        pnl_cum=float(d.get("pnl_cum", 0.0)),
        ema_fast=(
            float(d["ema_fast"]) if d.get("ema_fast") is not None else None
        ),
        ema_slow=(
            float(d["ema_slow"]) if d.get("ema_slow") is not None else None
        ),
        ema_bars_seen=int(d.get("ema_bars_seen", 0)),
        pending_target_pos=(
            int(d["pending_target_pos"])
            if d.get("pending_target_pos") is not None
            else None
        ),
        pending_signal_bar_end=pending_signal_bar_end_dt,
        pending_signal_price=(
            float(d["pending_signal_price"])
            if d.get("pending_signal_price") is not None
            else None
        ),
        pending_reason=d.get("pending_reason"),
        seq_no=int(d.get("seq_no", 0)),
        version=int(d.get("version", 1)),
    )
    return state


def save_session_state(state: SessionState) -> None:
    path = _state_path(state.trade_date)
    d = session_state_to_dict(state)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(__import__("json").dumps(d, ensure_ascii=False, indent=2))
    tmp.replace(path)


def load_session_state(trade_date: date) -> SessionState:
    path = _state_path(trade_date)
    if not path.is_file():
        return SessionState(trade_date=trade_date)

    raw = path.read_text()
    d = __import__("json").loads(raw)
    return session_state_from_dict(d)
