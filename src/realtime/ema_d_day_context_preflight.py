from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class EmaDDayContextResult:
    allowed: bool
    blocked: bool
    band: str
    decision: str
    status: str
    target_day: str
    source_trade_date: str
    payload: Dict[str, Any]


def _expect_str(obj: Dict[str, Any], key: str) -> str:
    val = obj.get(key)
    if not isinstance(val, str) or not val.strip():
        raise RuntimeError("invalid or missing " + key)
    return val.strip()


def preflight(path: str = "data/state/ema_d_day_context_latest.json") -> EmaDDayContextResult:
    if not os.path.exists(path):
        raise RuntimeError("context file not found: " + path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception as e:
        raise RuntimeError("failed to read context json: " + path + " err=" + str(e))

    if not isinstance(obj, dict):
        raise RuntimeError("context payload must be an object: " + path)

    target_day = _expect_str(obj, "target_day")
    source_trade_date = _expect_str(obj, "source_trade_date")
    band = _expect_str(obj, "band")
    decision = _expect_str(obj, "decision")
    status = _expect_str(obj, "status")

    blocked_raw = obj.get("blocked")
    if not isinstance(blocked_raw, bool):
        raise RuntimeError("invalid or missing blocked")
    blocked = bool(blocked_raw)

    if band not in ("adverse", "neutral", "favorable"):
        raise RuntimeError("invalid band: " + band)
    if decision not in ("allowed", "blocked"):
        raise RuntimeError("invalid decision: " + decision)
    if status != "ok":
        raise RuntimeError("status is not ok: " + status)
    if blocked:
        raise RuntimeError("context blocked=true")
    if decision != "allowed":
        raise RuntimeError("context decision is not allowed: " + decision)

    return EmaDDayContextResult(
        allowed=True,
        blocked=False,
        band=band,
        decision=decision,
        status=status,
        target_day=target_day,
        source_trade_date=source_trade_date,
        payload=obj,
    )
