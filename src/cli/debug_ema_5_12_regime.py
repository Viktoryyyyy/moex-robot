#!/usr/bin/env python3
"""
Debug helper for EMA(5,12) GOOD-days regime R1.

Usage (from project root):

  python src/cli/debug_ema_5_12_regime.py
  python src/cli/debug_ema_5_12_regime.py --today 2025-12-01

It will:
  - take 'today' (MSK calendar date, or from --today),
  - call get_trade_flag_for_date(today),
  - scan DAY_REGIME_CSV and show which last date T < today is used,
    and what regime is stored there.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from zoneinfo import ZoneInfo


def _setup_sys_path() -> None:
    import sys
    here = Path(__file__).resolve()
    src_dir = here.parents[1]  # .../src
    src_str = str(src_dir)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


import sys  # noqa: E402
_setup_sys_path()

from strategy.realtime.ema_5_12.config_ema_5_12 import MSK_TZ, DAY_REGIME_CSV  # noqa: E402
from strategy.realtime.ema_5_12.regime_day_loader import get_trade_flag_for_date  # noqa: E402


def _current_date_msk() -> date:
    tz = ZoneInfo(MSK_TZ)
    now_msk = datetime.now(tz)
    return now_msk.date()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Debug R1 regime for EMA(5,12) GOOD-days robot."
    )
    p.add_argument(
        "--today",
        type=str,
        default=None,
        help="Today date in format YYYY-MM-DD (MSK). "
             "If omitted, current MSK calendar date is used.",
    )
    return p.parse_args()


def _parse_date(s: Optional[str]) -> date:
    if not s:
        return _current_date_msk()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit(f"Invalid --today format: {s!r}, expected YYYY-MM-DD")


def _find_last_regime_row_before(trade_date: date) -> Optional[dict]:
    """
    Scan DAY_REGIME_CSV and return row for maximum TRADEDATE < trade_date.

    Returns dict or None if not found / file missing / parse errors.
    """
    path = DAY_REGIME_CSV
    if not path.is_file():
        return None

    best_date: Optional[date] = None
    best_row: Optional[dict] = None

    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                td_raw = (row.get("TRADEDATE") or "").strip()
                if not td_raw:
                    continue
                try:
                    d = date.fromisoformat(td_raw)
                except Exception:
                    continue
                if d >= trade_date:
                    continue
                if best_date is None or d > best_date:
                    best_date = d
                    best_row = row
    except Exception:
        return None

    return best_row


def main() -> None:
    args = _parse_args()
    today = _parse_date(args.today)

    print(f"Today (D) : {today.isoformat()}")
    print()

    # R1 decision using the same loader as robot
    trade_today, regime_yday = get_trade_flag_for_date(today)
    print("R1 decision (get_trade_flag_for_date):")
    print(f"  trade_today : {trade_today}")
    print(f"  regime_yday : {regime_yday}")
    print()

    print(f"CSV file : {DAY_REGIME_CSV}")
    row = _find_last_regime_row_before(today)

    if row is None:
        print("Last regime row with TRADEDATE < D: NOT FOUND")
        return

    td_raw = (row.get("TRADEDATE") or "").strip()
    regime_raw = (row.get("regime_day_ema_5_12_D5000") or "").strip()

    print("Last regime row with TRADEDATE < D:")
    print(f"  TRADEDATE                         : {td_raw}")
    print(f"  regime_day_ema_5_12_D5000         : {regime_raw}")
    # Print any additional columns for completeness
    for k, v in row.items():
        if k in ("TRADEDATE", "regime_day_ema_5_12_D5000"):
            continue
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
