#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timedelta

from lib_moex_api import resolve_fut_by_key
from fo_5m_day import load_tradestats


def date_range(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    cur = d0
    while cur <= d1:
        yield cur.isoformat()
        cur += timedelta(days=1)


def main():
    key = os.getenv("FO_KEY") or (sys.argv[1] if len(sys.argv) > 1 else "Si")
    start = os.getenv("FO_START") or (sys.argv[2] if len(sys.argv) > 2 else "")
    end   = os.getenv("FO_END")   or (sys.argv[3] if len(sys.argv) > 3 else "")

    if not start or not end:
        print(
            "Usage: FO_KEY=Si FO_START=YYYY-MM-DD FO_END=YYYY-MM-DD python fo_tradestats_coverage_probe.py",
            file=sys.stderr,
        )
        sys.exit(2)

    # Заголовок CSV
    print("date,secid,rows")

    for day in date_range(start, end):
        # Прогресс в stderr, чтобы сразу было видно, что скрипт живой
        print(f"[{day}] probing...", file=sys.stderr)

        secid = resolve_fut_by_key(key, board="rfud", limit_probe_day=day)
        if not secid:
            print(f"{day},<none>,0")
            continue

        df = load_tradestats(secid, day)
        rows = len(df) if df is not None else 0

        # Итог по дню — в CSV (stdout)
        print(f"{day},{secid},{rows}", flush=True)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
