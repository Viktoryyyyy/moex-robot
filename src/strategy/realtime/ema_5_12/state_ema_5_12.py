"""
State management for EMA(5,12) GOOD-days robot.

State is stored as a single JSON file at config/ema_5_12_state.json.
One file corresponds to a single trading date (MSK). If the file is missing,
corrupted, or contains state for a different date, a fresh default state is
created.

This module is intentionally minimal and pessimistic: any error when reading
state results in a new default state with flat position and zero PnL.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict

from .config_ema_5_12 import STATE_PATH


# Keys expected in state JSON
_STATE_KEYS_DEFAULTS = {
    "trade_date": None,          # str YYYY-MM-DD
    "regime_yday": "BAD",        # "GOOD" or "BAD"
    "trade_today_flag": 0,       # 1 or 0

    "position": 0,               # -1 / 0 / 1
    "entry_price": None,         # float or null
    "pnl_cum_net_pts": 0.0,      # float

    "ema_fast": None,            # float or null
    "ema_slow": None,            # float or null
    "bars_count": 0,             # int

    "last_bar_end": None,        # str "YYYY-MM-DD HH:MM:SS+03:00" or null
    "ready_for_exec": True,      # bool

    "last_signal": "NONE",       # "NONE" / "LONG" / "SHORT"
    "trade_id_last": 0           # int
}


def _default_state(trade_date: date) -> Dict[str, Any]:
    """
    Create a new default state for given trade_date.

    The state is flat, with zero PnL and BAD/0 regime by default.
    Regime and trade_today_flag should be updated by runner after
    calling legacy day gate removed.
    """
    state: Dict[str, Any] = {}

    for key, default_value in _STATE_KEYS_DEFAULTS.items():
        # Copy defaults
        state[key] = default_value

    state["trade_date"] = trade_date.isoformat()
    return state


def _ensure_defaults(state: Dict[str, Any], trade_date: date) -> Dict[str, Any]:
    """
    Ensure that state dict contains all required keys with sane defaults.

    - trade_date is always forced to provided value.
    - Missing keys are filled with defaults.
    - Extra keys are preserved, but not required.
    """
    normalized: Dict[str, Any] = {}

    for key, default_value in _STATE_KEYS_DEFAULTS.items():
        if key in state:
            normalized[key] = state[key]
        else:
            normalized[key] = default_value

    normalized["trade_date"] = trade_date.isoformat()
    return normalized


def load_state(trade_date: date) -> Dict[str, Any]:
    """
    Load JSON state for given trade_date from STATE_PATH.

    Behaviour:
    - If state file does not exist -> return fresh default state.
    - If state file exists but:
        * cannot be parsed as JSON, or
        * does not contain trade_date matching the requested date
      -> return fresh default state.
    - Otherwise -> return state with defaults ensured.

    This function is intentionally pessimistic: any read/parse error
    leads to a clean state with flat position.
    """
    path: Path = STATE_PATH

    # No file -> default clean state
    if not path.is_file():
        return _default_state(trade_date)

    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return _default_state(trade_date)

        raw_trade_date = str(raw.get("trade_date") or "").strip()
        if raw_trade_date != trade_date.isoformat():
            # State belongs to different day -> do not reuse
            return _default_state(trade_date)

        state = _ensure_defaults(raw, trade_date)
        return state
    except Exception:
        # Any error -> safe fallback
        return _default_state(trade_date)


def save_state(state: Dict[str, Any]) -> None:
    """
    Persist state dict to STATE_PATH as JSON.

    The function does not modify the state, it only writes it to disk.
    Directory for STATE_PATH is created if needed.
    """
    path: Path = STATE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    # We write as compact JSON, pretty-print is not required for robot,
    # but 2-space indent keeps it readable.
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)

    # Atomic-ish replace: write to tmp first, then replace target.
    tmp_path.replace(path)
