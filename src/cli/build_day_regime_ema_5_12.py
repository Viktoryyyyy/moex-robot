#!/usr/bin/env python3
"""
Build daily regime file for EMA(5,12) GOOD-days robot from master CSV.

Expected master file:
  - contains column 'regime_day_ema_5_12_D5000'
  - has either:
      * column 'TRADEDATE' with YYYY-MM-DD, or
      * column 'end' (datetime), from which date can be derived.

Output:
  data/master/day_regime_ema_5_12_D5000.csv

Columns:
  TRADEDATE, regime_day_ema_5_12_D5000

Usage (from project root):
  python src/cli/build_day_regime_ema_5_12.py --master path/to/master.csv
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


def _setup_sys_path() -> None:
    import sys
    here = Path(__file__).resolve()
    src_dir = here.parents[1]  # .../src
    src_str = str(src_dir)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


import sys  # noqa: E402
_setup_sys_path()

from strategy.realtime.ema_5_12.config_ema_5_12 import DAY_REGIME_CSV  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build daily regime CSV for EMA(5,12) GOOD-days robot."
    )
    p.add_argument(
        "--master",
        type=str,
        required=True,
        help="Path to master CSV with column 'regime_day_ema_5_12_D5000'.",
    )
    return p.parse_args()


def _detect_tradedate_column(df: pd.DataFrame) -> pd.Series:
    """
    Detect or derive TRADEDATE column.

    Priority:
      1) existing 'TRADEDATE' column;
      2) parse 'end' as datetime and take .date().

    Raises SystemExit on failure.
    """
    if "TRADEDATE" in df.columns:
        s = df["TRADEDATE"].astype(str).str.strip()
        try:
            _ = pd.to_datetime(s, format="%Y-%m-%d")
        except Exception as e:
            raise SystemExit(f"Invalid TRADEDATE format in master: {e}")
        return s

    if "end" in df.columns:
        try:
            dt = pd.to_datetime(df["end"])
        except Exception as e:
            raise SystemExit(f"Cannot parse 'end' column as datetime: {e}")
        return dt.dt.date.astype(str)

    raise SystemExit(
        "Master file must contain either 'TRADEDATE' or 'end' column."
    )


def main() -> None:
    args = _parse_args()
    master_path = Path(args.master)

    if not master_path.is_file():
        raise SystemExit(f"Master file not found: {master_path}")

    print(f"[build_day_regime] Reading master: {master_path}")
    df = pd.read_csv(master_path)

    if "regime_day_ema_5_12_D5000" not in df.columns:
        raise SystemExit(
            "Master file has no column 'regime_day_ema_5_12_D5000'. "
            "Make sure you pass the _with_regime version."
        )

    tradedate = _detect_tradedate_column(df)
    df["TRADEDATE"] = tradedate

    # Group by TRADEDATE and check that regime is unique per day
    col_regime = "regime_day_ema_5_12_D5000"
    grp = df.groupby("TRADEDATE")[col_regime]

    regimes_per_day = grp.nunique(dropna=True)
    bad_days = regimes_per_day[regimes_per_day > 1]
    if not bad_days.empty:
        print("[build_day_regime] ERROR: multiple regime values per day:")
        for td, cnt in bad_days.items():
            print(f"  {td}: unique regimes = {cnt}")
        raise SystemExit(
            "Aborting: regime_day_ema_5_12_D5000 must be unique per TRADEDATE."
        )

    # Take first regime per day (all are identical per day due to check above)
    daily = grp.first().reset_index()

    out_path = DAY_REGIME_CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)

    daily.to_csv(out_path, index=False)
    print(f"[build_day_regime] Saved daily regime to: {out_path}")
    print(f"[build_day_regime] Rows: {len(daily)}")
    print(f"[build_day_regime] First date: {daily['TRADEDATE'].min()}")
    print(f"[build_day_regime] Last  date: {daily['TRADEDATE'].max()}")


if __name__ == "__main__":
    main()
