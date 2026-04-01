
from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path

from src.strategy.realtime.ema_3_19_15m.executor_ema_3_19_15m import ExecutionEventEma31915m


SIGNALS_DIR = Path("data") / "signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

FIELDNAMES = [
    "trade_date",
    "seq",
    "bar_end",
    "action",
    "prev_pos",
    "new_pos",
    "price",
    "entry_price",
    "realized_pnl",
    "cum_pnl",
]


def _file_path(trade_date: str) -> Path:
    return SIGNALS_DIR / ("ema_3_19_15m_realtime_" + trade_date + ".csv")


def append_execution_event(event: ExecutionEventEma31915m) -> None:
    path = _file_path(event.trade_date)
    is_new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerow(asdict(event))
