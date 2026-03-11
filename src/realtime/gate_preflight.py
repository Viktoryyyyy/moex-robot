from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class GateResult:
    risk: int
    payload: Dict[str, Any]


def preflight(path: str = "data/state/phase_transition_risk.json") -> GateResult:
    if not os.path.exists(path):
        raise RuntimeError("gate file not found: " + path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception as e:
        raise RuntimeError("failed to read gate json: " + path + " err=" + str(e))

    risk = obj.get("phase_transition_risk")
    if risk is None:
        # backward compat (if someone used a different key earlier)
        risk = obj.get("risk")
    if risk is None:
        raise RuntimeError("missing phase_transition_risk in gate json: " + path)

    try:
        r = int(risk)
    except Exception:
        raise RuntimeError("invalid phase_transition_risk in gate json: " + str(risk))

    if r not in (0, 1):
        raise RuntimeError("phase_transition_risk must be 0 or 1, got: " + str(r))

    return GateResult(risk=r, payload=obj)

