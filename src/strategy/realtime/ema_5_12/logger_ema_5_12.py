"""
Logging for EMA(5,12) GOOD-days robot.

This module:
  - prepares CSV file data/signals/ema_5_12_realtime_<date>.csv,
  - writes header once (if file is new/empty),
  - appends trade records produced by executor_ema_5_12.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from .config_ema_5_12 import SIGNALS_DIR


# Fixed order of CSV columns (must match trade_record from executor_ema_5_12)
CSV_FIELDS: List[str] = [
    "trade_date",
    "trade_id",
    "bar_end_signal",
    "bar_end_exec",
    "action",
    "pos_before",
    "pos_after",
    "side_exec",
    "price_signal_ref",
    "price_exec",
    "qty",
    "commission_pts",
    "pnl_gross_pts",
    "pnl_net_pts",
    "pnl_cum_net_pts",
    "ema_fast",
    "ema_slow",
    "cross_type",
    "regime_yday",
    "trade_today_flag",
]


@dataclass
class Logger:
    """
    Simple logger holder for EMA(5,12) robot.

    Attributes
    ----------
    path : Path
        Path to CSV file for this trading date.
    fields : list[str]
        Ordered list of CSV column names.
    """
    path: Path
    fields: List[str]


def _ensure_header(path: Path, fields: List[str]) -> None:
    """
    Ensure that CSV file exists and has a header row.

    If file does not exist or is empty, header is written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # If file exists and is non-empty -> assume header is already there.
    if path.is_file() and path.stat().st_size > 0:
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()


def init_logger(trade_date: date) -> Logger:
    """
    Initialise CSV logger for given trading date.

    Parameters
    ----------
    trade_date : datetime.date

    Returns
    -------
    Logger
        Logger instance with path and fieldnames.
    """
    filename = f"ema_5_12_realtime_{trade_date.isoformat()}.csv"
    path = SIGNALS_DIR / filename
    _ensure_header(path, CSV_FIELDS)
    return Logger(path=path, fields=list(CSV_FIELDS))


def log_trade(logger: Logger, trade: Dict[str, Any]) -> None:
    """
    Append single trade record to CSV file.

    Parameters
    ----------
    logger : Logger
        Logger instance returned by init_logger.
    trade : dict
        Trade record from executor_ema_5_12.execute_signal().
        Keys must match CSV_FIELDS. Missing keys will be written as empty.
    """
    row: Dict[str, Any] = {}

    for field in logger.fields:
        row[field] = trade.get(field, "")

    with logger.path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=logger.fields)
        writer.writerow(row)
