from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict


PILOT_JOURNAL_PATH = Path("data/state/ema_3_19_15m_pilot_journal.csv")
PILOT_DAY_STATUS_PATH = Path("data/state/ema_3_19_15m_pilot_day_status.csv")

PILOT_JOURNAL_HEADER = [
    "trading_day",
    "event_time",
    "branch_name",
    "strategy_id",
    "ticker",
    "timeframe",
    "context_band",
    "context_decision",
    "context_source_trade_date",
    "signal_state",
    "action",
    "side",
    "price_in",
    "price_out",
    "pnl_points",
    "pnl_rub",
    "trade_status",
    "block_reason",
    "artifact_date",
    "artifact_status",
    "note",
    "source",
]

PILOT_DAY_STATUS_HEADER = [
    "trading_day",
    "branch_name",
    "strategy_id",
    "ticker",
    "timeframe",
    "artifact_date",
    "artifact_status",
    "context_band",
    "context_decision",
    "source_trade_date",
    "day_status",
    "block_reason",
    "note",
    "generated_at",
]


def _append_csv(path: Path, header: list[str], row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    write_header = True
    if exists and path.stat().st_size > 0:
        write_header = False
    normalized = {}
    for key in header:
        value = row.get(key, "")
        if value is None:
            value = ""
        normalized[key] = value
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(normalized)


def append_pilot_journal(row: Dict[str, object]) -> None:
    _append_csv(PILOT_JOURNAL_PATH, PILOT_JOURNAL_HEADER, row)


def append_pilot_day_status(row: Dict[str, object]) -> None:
    _append_csv(PILOT_DAY_STATUS_PATH, PILOT_DAY_STATUS_HEADER, row)
