from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict


@dataclass(frozen=True)
class GateResult:
    risk: int
    payload: Dict[str, Any]


def _die(msg: str) -> None:
    raise RuntimeError(msg)


def _parse_iso_z(ts: str) -> datetime:
    t = ts.strip()
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    return datetime.fromisoformat(t)


def _yday_iso() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def _read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        _die("failed to read json: " + path + " err=" + str(e))
    return {}


def _history_last_date(path: str) -> str:
    if not os.path.exists(path):
        _die("history file not found: " + path)
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            _die("history file empty: " + path)
        last = (rows[-1].get("date") or "").strip()
        if not last:
            _die("history missing last date: " + path)
        return last
    except Exception as e:
        _die("failed to read history csv: " + path + " err=" + str(e))
    return ""


def preflight(path: str = "data/gate/phase_transition_risk.json") -> GateResult:
    if not os.path.exists(path):
        _die("gate file not found: " + path)
    obj = _read_json(path)

    risk = obj.get("phase_transition_risk")
    if risk is None:
        risk = obj.get("risk")
    if risk is None:
        _die("missing phase_transition_risk in gate json: " + path)
    try:
        r = int(risk)
    except Exception:
        _die("invalid phase_transition_risk in gate json: " + str(risk))
    if r not in (0, 1):
        _die("phase_transition_risk must be 0 or 1, got: " + str(r))

    asof = (obj.get("asof_date") or "").strip()
    if not asof:
        _die("missing asof_date in gate json: " + path)
    yday = _yday_iso()
    if asof != yday:
        _die("gate asof_date mismatch: asof=" + asof + " expected_yday=" + yday)

    updated_at = (obj.get("updated_at") or "").strip()
    if not updated_at:
        _die("missing updated_at in gate json: " + path)
    try:
        upd = _parse_iso_z(updated_at)
    except Exception as e:
        _die("invalid updated_at in gate json: " + updated_at + " err=" + str(e))
    if upd.tzinfo is None:
        upd = upd.replace(tzinfo=timezone.utc)
    ttl_h = os.getenv("GATE_TTL_HOURS")
    try:
        ttl = int(ttl_h) if ttl_h else 24
    except Exception:
        ttl = 24
    now = datetime.now(timezone.utc)
    if now - upd > timedelta(hours=ttl):
        _die("gate updated_at stale: updated_at=" + updated_at + " ttl_hours=" + str(ttl))

    stamp_path = "data/gate/phase_transition_gate_daily.last_success.json"
    if not os.path.exists(stamp_path):
        _die("gate daily last-success stamp missing: " + stamp_path)
    stamp = _read_json(stamp_path)
    stamp_asof = (stamp.get("asof_date") or "").strip()
    if stamp_asof != asof:
        _die("gate daily stamp asof mismatch: stamp_asof=" + stamp_asof + " gate_asof=" + asof)

    hist_path = "data/state/rel_range_history.csv"
    hist_last = _history_last_date(hist_path)
    if hist_last != asof:
        _die("history last_date mismatch: hist_last=" + hist_last + " gate_asof=" + asof)

    return GateResult(risk=r, payload=obj)
