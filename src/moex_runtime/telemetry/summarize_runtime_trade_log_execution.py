from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from src.moex_strategy_sdk.errors import StrategyRegistrationError


_SUMMARY_SCHEMA_VERSION = 1
_OPEN_LONG = "OPEN_LONG"
_OPEN_SHORT = "OPEN_SHORT"
_CLOSE_LONG = "CLOSE_LONG"
_CLOSE_SHORT = "CLOSE_SHORT"
_REVERSE_TO_LONG = "REVERSE_TO_LONG"
_REVERSE_TO_SHORT = "REVERSE_TO_SHORT"


def _default_summary() -> dict[str, object]:
    return {
        "execution_summary_schema_version": _SUMMARY_SCHEMA_VERSION,
        "execution_event_count_day": 0,
        "last_execution_seq": None,
        "last_execution_bar_end": None,
        "last_execution_action": None,
        "last_closed_trade_pnl_points": None,
        "current_day_realized_pnl_points": 0.0,
    }


def _parse_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or value is None:
        raise StrategyRegistrationError("runtime trade log " + field_name + " must be int")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise StrategyRegistrationError("runtime trade log " + field_name + " must be int") from exc


def _parse_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or value is None:
        raise StrategyRegistrationError("runtime trade log " + field_name + " must be float")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise StrategyRegistrationError("runtime trade log " + field_name + " must be float") from exc


def _load_trade_log_rows(*, trade_log_path: Path, trade_date: str) -> list[dict[str, Any]]:
    if not trade_log_path.exists():
        return []
    with trade_log_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("trade_date") != trade_date:
            continue
        action = row.get("action")
        if not isinstance(action, str) or not action:
            raise StrategyRegistrationError("runtime trade log action must be non-empty string")
        filtered_rows.append(
            {
                "seq": _parse_int(row.get("seq"), field_name="seq"),
                "bar_end": row.get("bar_end"),
                "action": action,
                "price": _parse_float(row.get("price"), field_name="price"),
            }
        )
    filtered_rows.sort(key=lambda row: int(row["seq"]))
    return filtered_rows


def summarize_runtime_trade_log_execution(*, trade_log_path: Path, trade_date: str) -> dict[str, object]:
    rows = _load_trade_log_rows(trade_log_path=trade_log_path, trade_date=trade_date)
    summary = _default_summary()
    if not rows:
        return summary
    current_day_realized_pnl_points = 0.0
    last_closed_trade_pnl_points: float | None = None
    tracked_open_side: float | None = None
    tracked_open_price: float | None = None
    for row in rows:
        action = str(row["action"])
        price = float(row["price"])
        if action == _OPEN_LONG:
            tracked_open_side = 1.0
            tracked_open_price = price
            continue
        if action == _OPEN_SHORT:
            tracked_open_side = -1.0
            tracked_open_price = price
            continue
        if action == _CLOSE_LONG:
            if tracked_open_side == 1.0 and tracked_open_price is not None:
                last_closed_trade_pnl_points = price - tracked_open_price
                current_day_realized_pnl_points += last_closed_trade_pnl_points
            tracked_open_side = None
            tracked_open_price = None
            continue
        if action == _CLOSE_SHORT:
            if tracked_open_side == -1.0 and tracked_open_price is not None:
                last_closed_trade_pnl_points = tracked_open_price - price
                current_day_realized_pnl_points += last_closed_trade_pnl_points
            tracked_open_side = None
            tracked_open_price = None
            continue
        if action == _REVERSE_TO_LONG:
            if tracked_open_side == -1.0 and tracked_open_price is not None:
                last_closed_trade_pnl_points = tracked_open_price - price
                current_day_realized_pnl_points += last_closed_trade_pnl_points
            tracked_open_side = 1.0
            tracked_open_price = price
            continue
        if action == _REVERSE_TO_SHORT:
            if tracked_open_side == 1.0 and tracked_open_price is not None:
                last_closed_trade_pnl_points = price - tracked_open_price
                current_day_realized_pnl_points += last_closed_trade_pnl_points
            tracked_open_side = -1.0
            tracked_open_price = price
            continue
        raise StrategyRegistrationError("unsupported runtime trade log action for execution summary: " + action)
    last_row = rows[-1]
    summary.update(
        {
            "execution_event_count_day": len(rows),
            "last_execution_seq": int(last_row["seq"]),
            "last_execution_bar_end": last_row["bar_end"],
            "last_execution_action": last_row["action"],
            "last_closed_trade_pnl_points": last_closed_trade_pnl_points,
            "current_day_realized_pnl_points": float(current_day_realized_pnl_points),
        }
    )
    return summary
