#!/usr/bin/env python3
"""
CLI entrypoint for EMA(5,12) GOOD-days realtime robot.

Usage (from project root):
  python src/cli/loop_ema_5_12_realtime.py
  python src/cli/loop_ema_5_12_realtime.py --date 2025-12-01
"""

from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path
import sys
from typing import Optional


def _setup_sys_path() -> None:
    """
    Ensure that src/ is on sys.path so that imports like:

        from strategy.realtime.ema_5_12.runner_ema_5_12 import run_ema_5_12_loop

    work reliably when this script is executed as:

        python src/cli/loop_ema_5_12_realtime.py
    """
    here = Path(__file__).resolve()
    src_dir = here.parents[1]  # .../src
    src_str = str(src_dir)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


_setup_sys_path()

from strategy.realtime.ema_5_12.runner_ema_5_12 import run_ema_5_12_loop  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EMA(5,12) GOOD-days realtime robot (dry-run)."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Trading date in format YYYY-MM-DD (MSK). "
             "If omitted, current MSK calendar date is used.",
    )
    return parser.parse_args()


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.date()
    except ValueError:
        raise SystemExit(f"Invalid --date format: {date_str!r}, expected YYYY-MM-DD")


def main() -> None:
    args = _parse_args()
    trade_date = _parse_date(args.date)
    run_ema_5_12_loop(trade_date=trade_date)


if __name__ == "__main__":
    main()
