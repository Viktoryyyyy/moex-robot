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


def load_futoi_5m_for_day(asset: str, day: str) -> pd.DataFrame:
    """
    Грузим FUTOI по базовому активу (например, 'si') за один день
    и агрегируем до 5-минутных слотов:
      - 'end' -> datetime
      - floor до 5 минут
      - по каждой 5-минутке берём последнюю запись (самую свежую).
    """
    rows = futoi_build_rows(asset, day)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "end" not in df.columns:
        print(f"[{day}] FUTOI: no 'end' column in rows", file=sys.stderr)
        return pd.DataFrame()

    df = df.copy()
    df["end"] = pd.to_datetime(df["end"])

    # Метрики FUTOI, которые нас интересуют
    metric_cols = [
        "pos_fiz",
        "pos_yur",
        "pos_long_fiz",
        "pos_short_fiz",
        "pos_long_yur",
        "pos_short_yur",
        "pos_long_num_fiz",
        "pos_short_num_fiz",
        "pos_long_num_yur",
        "pos_short_num_yur",
    ]
    metric_existing = [c for c in metric_cols if c in df.columns]

    if not metric_existing:
        # Если по какой-то причине build_rows вернул другой формат — просто вернём end + всё, что есть
        keep = [c for c in df.columns if c != "end"]
        df["end_5m"] = df["end"].dt.floor("5min")
        work = (
            df.sort_values("end")
              .groupby("end_5m")[keep]
              .last()
              .reset_index()
              .rename(columns={"end_5m": "end"})
        )
        return work

    # Нормальный путь: работаем только с end + FUTOI-метриками
    work = df[["end"] + metric_existing].copy()
    work["end_5m"] = work["end"].dt.floor("5min")

    # Для каждой 5-минутки берём последнюю запись по времени
    work = (
        work.sort_values("end")
            .groupby("end_5m")[metric_existing]
            .last()
            .reset_index()
            .rename(columns={"end_5m": "end"})
    )

    # Добавим колонку с базовым активом для удобства
    work.insert(1, "asset", asset.lower())

    # Убираем возможные дубликаты колонок (на всякий случай)
    work = work.loc[:, ~work.columns.duplicated()]

    return work[["end", "asset"] + metric_existing]


def main():
    asset = os.getenv("FUTOI_ASSET") or (sys.argv[1] if len(sys.argv) > 1 else "si")
    start = os.getenv("FUTOI_START") or (sys.argv[2] if len(sys.argv) > 2 else "")
    end   = os.getenv("FUTOI_END")   or (sys.argv[3] if len(sys.argv) > 3 else "")

    if not start or not end:
        print(
            "Usage: FUTOI_ASSET=si FUTOI_START=YYYY-MM-DD FUTOI_END=YYYY-MM-DD python futoi_5m_period.py",
            file=sys.stderr,
        )
        print("   or: python futoi_5m_period.py si 2024-01-01 2024-12-31", file=sys.stderr)
        sys.exit(2)

    all_days: list[pd.DataFrame] = []
    total_rows = 0

    for day in date_range(start, end):
        print(f"[{day}] FUTOI {asset}...", file=sys.stderr)
        df_day = load_futoi_5m_for_day(asset, day)
        if df_day.empty:
            print(f"[{day}] no rows, skipping", file=sys.stderr)
            continue

        rows = len(df_day)
        total_rows += rows
        print(f"[{day}] ok, rows={rows}", file=sys.stderr)
        all_days.append(df_day)

    if not all_days:
        print("No FUTOI data for given period", file=sys.stderr)
        sys.exit(0)

    result = pd.concat(all_days, ignore_index=True)
    result = result.sort_values("end").reset_index(drop=True)

    out_asset = asset.lower()
    out_name = f"futoi_{out_asset}_5m_{start}_{end}.csv"
    result.to_csv(out_name, index=False)

    print(f"\nAsset:  {asset}")
    print(f"Period: {start}..{end}")
    print(f"Rows:   {len(result)} (sum of per-day={total_rows})")
    print(f"Saved:  {out_name}")

    print("\nHead:")
    print(result.head(5).to_csv(index=False))
    print("Tail:")
    print(result.tail(5).to_csv(index=False))


if __name__ == "__main__":
    main()
