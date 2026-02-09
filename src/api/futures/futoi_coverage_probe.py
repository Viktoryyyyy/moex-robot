#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timedelta

import pandas as pd

from futoi_day import build_rows as futoi_build_rows


def date_range(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    cur = d0
    while cur <= d1:
        yield cur.isoformat()
        cur += timedelta(days=1)


def main():
    asset = os.getenv("FUTOI_ASSET") or (sys.argv[1] if len(sys.argv) > 1 else "si")
    start = os.getenv("FUTOI_START") or (sys.argv[2] if len(sys.argv) > 2 else "")
    end   = os.getenv("FUTOI_END")   or (sys.argv[3] if len(sys.argv) > 3 else "")

    if not start or not end:
        print(
            "Usage: FUTOI_ASSET=si FUTOI_START=YYYY-MM-DD FUTOI_END=YYYY-MM-DD python futoi_coverage_probe.py",
            file=sys.stderr,
        )
        sys.exit(2)

    # Заголовок CSV
    print("date,asset,rows,first_end,last_end")

    for day in date_range(start, end):
        # Прогресс в stderr, чтобы было видно, что скрипт живой
        print(f"[{day}] probing FUTOI for asset={asset}...", file=sys.stderr)

        try:
            rows = futoi_build_rows(asset, day)
        except Exception as e:
            # Не валим весь диапазон из-за одной ошибки
            print(f"[{day}] ERROR: {e}", file=sys.stderr)
            print(f"{day},{asset},0,,")
            continue

        if not rows:
            print(f"{day},{asset},0,,")
            continue

        df = pd.DataFrame(rows)

        # Определяем временной диапазон внутри дня
        if "end" in df.columns:
            # Мы в futoi_day уже формировали end (datetime c MSK)
            # Приводим к строке ISO, чтобы не таскать таймзоны как объект
            df["end"] = pd.to_datetime(df["end"])
            first_end = df["end"].min().isoformat()
            last_end  = df["end"].max().isoformat()
        else:
            # fallback: если по какой-то причине нет end, пробуем tradedate+tradetime
            if {"tradedate", "tradetime"} <= set(df.columns):
                dt = pd.to_datetime(df["tradedate"] + " " + df["tradetime"])
                first_end = dt.min().isoformat()
                last_end  = dt.max().isoformat()
            else:
                first_end = ""
                last_end  = ""

        print(f"{day},{asset},{len(df)},{first_end},{last_end}", flush=True)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
