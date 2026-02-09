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
        # fallback: если формат поменялся, просто агрегируем все колонки, кроме end
        keep = [c for c in df.columns if c != "end"]
        df["end_5m"] = df["end"].dt.floor("5min")
        work = (
            df.sort_values("end")
              .groupby("end_5m")[keep]
              .last()
              .reset_index()
              .rename(columns={"end_5m": "end"})
        )
        work.insert(1, "asset", asset.lower())
        work = work.loc[:, ~work.columns.duplicated()]
        return work

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

    work.insert(1, "asset", asset.lower())
    work = work.loc[:, ~work.columns.duplicated()]

    return work[["end", "asset"] + metric_existing]


def main():
    """
    Определяет период по master-файлу Si/CNY и выгружает FUTOI за весь этот период.
    """
    asset = os.getenv("FUTOI_ASSET") or "si"
    master_path = os.getenv("MASTER_PATH") or "si_cny_5m_2020-01-01_2025-11-13.csv"

    if not os.path.exists(master_path):
        print(f"Master file not found: {master_path}", file=sys.stderr)
        print("Укажи путь через MASTER_PATH=/path/to/file.csv", file=sys.stderr)
        sys.exit(1)

    print(f"Reading master file: {master_path}", file=sys.stderr)
    try:
        master = pd.read_csv(master_path, usecols=["end"])
    except ValueError:
        # если колонка называется иначе, пробуем без usecols
        master = pd.read_csv(master_path)

    if "end" not in master.columns:
        print("Master file does not contain 'end' column", file=sys.stderr)
        sys.exit(1)

    master["end"] = pd.to_datetime(master["end"])
    start_date = master["end"].min().date().isoformat()
    end_date   = master["end"].max().date().isoformat()

    print(f"Detected period from master: {start_date}..{end_date}", file=sys.stderr)
    print(f"Asset (FUTOI): {asset}", file=sys.stderr)

    all_days: list[pd.DataFrame] = []
    total_rows = 0

    for day in date_range(start_date, end_date):
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
        print("No FUTOI data for detected period", file=sys.stderr)
        sys.exit(0)

    result = pd.concat(all_days, ignore_index=True)
    result = result.sort_values("end").reset_index(drop=True)

    out_asset = asset.lower()
    out_name = f"futoi_{out_asset}_5m_{start_date}_{end_date}.csv"
    result.to_csv(out_name, index=False)

    print(f"\nAsset:  {asset}")
    print(f"Period: {start_date}..{end_date}")
    print(f"Rows:   {len(result)} (sum of per-day={total_rows})")
    print(f"Saved:  {out_name}")

    print("\nHead:")
    print(result.head(5).to_csv(index=False))
    print("Tail:")
    print(result.tail(5).to_csv(index=False))


if __name__ == "__main__":
    main()
