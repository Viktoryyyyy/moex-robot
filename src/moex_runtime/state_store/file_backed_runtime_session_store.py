from __future__ import annotations

import csv
import json
import os
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
        handle.flush()
        os.fsync(handle.fileno())
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
        handle.flush()
        os.fsync(handle.fileno())


def transition_journal_path(state_path: Path) -> Path:
    return state_path.with_suffix(state_path.suffix + ".pending")


def prepare_runtime_transition(
    state_path: Path, row: Mapping[str, object], updated_state: Mapping[str, object],
) -> None:
    # Persist the complete adapter patch before committing its event to CSV.
    save_runtime_state(transition_journal_path(state_path),
                       {"trade": dict(row), "state": dict(updated_state)})


def recover_runtime_state(
    state_path: Path, prior_state: Mapping[str, object], last_trade_log_row: Mapping[str, str] | None,
) -> dict[str, object]:
    from src.moex_runtime.execution.runtime_position_transition import recover_position_state

    pending = load_runtime_state(transition_journal_path(state_path))
    if pending and last_trade_log_row is not None:
        row = pending["trade"]
        if str(row["seq"]) == last_trade_log_row.get("seq"):
            if any(str(row[key]) != last_trade_log_row.get(key) for key in FIELDNAMES):
                raise StrategyRegistrationError("runtime recovery journal disagrees with committed event")
            if int(row["seq"]) > int(prior_state.get("last_trade_seq", 0)):
                prior_state = pending["state"]
    return recover_position_state(prior_state, last_trade_log_row)
