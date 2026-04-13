from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Mapping

from src.moex_strategy_sdk.errors import StrategyRegistrationError


FIELDNAMES = ["trade_date", "seq", "bar_end", "action", "prev_pos", "new_pos", "price", "reason_code"]


def load_runtime_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("runtime state payload must be object")
    return dict(payload)


def save_runtime_state(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.replace(path)


def read_last_trade_log_row(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return None
    return dict(rows[-1])


def next_trade_seq(*, prior_state: Mapping[str, object], last_trade_log_row: Mapping[str, str] | None) -> int:
    if last_trade_log_row is not None:
        raw = last_trade_log_row.get("seq")
        if raw is None:
            raise StrategyRegistrationError("runtime trade log last row missing seq")
        try:
            return int(raw) + 1
        except ValueError as exc:
            raise StrategyRegistrationError("runtime trade log seq must be int") from exc
    raw_state_seq = prior_state.get("last_trade_seq", 0)
    if isinstance(raw_state_seq, bool) or not isinstance(raw_state_seq, int) or raw_state_seq < 0:
        raise StrategyRegistrationError("runtime state last_trade_seq must be non-negative int")
    return raw_state_seq + 1


def append_trade_log_row(path: Path, row: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerow(dict(row))
