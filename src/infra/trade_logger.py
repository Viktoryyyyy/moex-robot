#!/usr/bin/env python3
"""
Trade logger for realtime strategies.

For EMA(5,12) robot we log TradeEvent into:
  data/signals/ema_5_12_realtime_<YYYY-MM-DD>.csv
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from src.strategy.realtime.ema_5_12.executor_ema_5_12 import TradeEvent


SIGNALS_DIR = Path("data") / "signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)


FIELDNAMES_EMA_5_12 = [
    "seq_no",
    "bar_end_signal",
    "bar_end_exec",
    "side_exec",
    "qty",
    "price_exec",
    "price_signal_ref",
    "pnl",
    "pnl_cum",
    "pos_before",
    "pos_after",
    "reason_open",
    "reason_close",
]


def _file_path_ema_5_12(trade_date: date) -> Path:
    name = f"ema_5_12_realtime_{trade_date.isoformat()}.csv"
    return SIGNALS_DIR / name


def ensure_ema_5_12_file(trade_date: date) -> None:
    """
    Ensure that EMA(5,12) signals file exists with header.

    Called once at robot startup. If file already exists, nothing is changed.
    """
    path = _file_path_ema_5_12(trade_date)
    if path.exists():
        return

    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES_EMA_5_12)
        writer.writeheader()
    tmp.replace(path)


def append_trade_ema_5_12(trade_date: date, trade: TradeEvent) -> None:
    """
    Append single TradeEvent to EMA(5,12) signals file for given trade_date.
    """
    path = _file_path_ema_5_12(trade_date)
    is_new = not path.exists()

    row = {
        "seq_no": trade.seq_no,
        "bar_end_signal": trade.bar_end_signal.isoformat(),
        "bar_end_exec": trade.bar_end_exec.isoformat(),
        "side_exec": trade.side_exec,
        "qty": trade.qty,
        "price_exec": f"{trade.price_exec:.1f}",
        "price_signal_ref": f"{trade.price_signal_ref:.1f}",
        "pnl": f"{trade.pnl:.1f}",
        "pnl_cum": f"{trade.pnl_cum:.1f}",
        "pos_before": trade.pos_before,
        "pos_after": trade.pos_after,
        "reason_open": trade.reason_open,
        "reason_close": trade.reason_close,
    }

    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES_EMA_5_12)
        if is_new:
            writer.writeheader()
        writer.writerow(row)
    tmp.replace(path)
