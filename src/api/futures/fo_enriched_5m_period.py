#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from lib_moex_api import resolve_fut_by_key
from fo_5m_day import load_tradestats
from futoi_day import build_rows as futoi_build_rows
from obstats_5m_day import build_rows as ob_build_rows

TZ_MSK = ZoneInfo("Europe/Moscow")


def _to_dt(series: pd.Series) -> pd.Series:
    """Унифицированное преобразование end в pandas datetime."""
    return pd.to_datetime(series)


# ---------- Загрузка данных за 1 день ----------

def load_fo_for_day(key: str, day: str) -> tuple[str | None, pd.DataFrame]:
    """
    FO 5m (tradestats) по ключу key и дате day.
    Не делает sys.exit — при отсутствии данных возвращает (None, empty_df),
    чтобы периодовый скрипт мог просто пропустить день.
    """
    secid = resolve_fut_by_key(key, board="rfud", limit_probe_day=day)
    if not secid:
        print(f"[{day}] WARN: no futures match key='{key}'", file=sys.stderr)
        return None, pd.DataFrame()

    df = load_tradestats(secid, day)
    if df.empty:
        print(f"[{day}] WARN: no tradestats data for {secid}", file=sys.stderr)
        return secid, pd.DataFrame()

    if "end" not in df.columns:
        print(f"[{day}] ERROR: FO dataframe for {secid} has no 'end' column", file=sys.stderr)
        return secid, pd.DataFrame()

    df = df.copy()
    df["end"] = _to_dt(df["end"])
    df["ticker"] = secid  # чтобы в итоговом CSV видно было контракт
    return secid, df


def load_futoi_for_si(day: str) -> pd.DataFrame:
    """
    FUTOI для базового актива Si.
    Нормализация:
      - end -> datetime
      - округление вниз до 5 минут
      - для каждого 5-минутного слота берём последнюю запись (самое свежее значение OI).
    """
    rows = futoi_build_rows("si", day)
    cols = [
        "end",
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
    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)

    if "end" not in df.columns:
        print(f"[{day}] FUTOI: no 'end' column in rows", file=sys.stderr)
        return pd.DataFrame(columns=cols)

    df = df.copy()
    df["end"] = _to_dt(df["end"])

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
        return pd.DataFrame(columns=cols)

    work = df[["end"] + metric_existing].copy()
    work["end_5m"] = work["end"].dt.floor("5min")

    # Для каждой 5-минутки берём последнюю запись по времени
    work = work.sort_values("end").groupby("end_5m")[metric_existing].last().reset_index()
    work = work.rename(columns={"end_5m": "end"})
    work = work.loc[:, ~work.columns.duplicated()]

    return work[["end"] + metric_existing]


def load_obstats_for_secid(secid: str, day: str) -> pd.DataFrame:
    """
    OBSTATS для конкретного фьючерса (например, SiZ5).
    Нормализация:
      - end -> datetime
      - убираем служебные поля.
    """
    rows = ob_build_rows(secid, day)
    metric_cols = [
        "mid_price",
        "micro_price",
        "spread_l1",
        "spread_l2",
        "spread_l3",
        "spread_l5",
        "spread_l10",
        "spread_l20",
        "levels_b",
        "levels_s",
        "vol_b_l1",
        "vol_b_l2",
        "vol_b_l3",
        "vol_b_l5",
        "vol_b_l10",
        "vol_b_l20",
        "vol_s_l1",
        "vol_s_l2",
        "vol_s_l3",
        "vol_s_l5",
        "vol_s_l10",
        "vol_s_l20",
        "vwap_b_l3",
        "vwap_b_l5",
        "vwap_b_l10",
        "vwap_b_l20",
        "vwap_s_l3",
        "vwap_s_l5",
        "vwap_s_l10",
        "vwap_s_l20",
    ]

    if not rows:
        return pd.DataFrame(columns=["end"] + metric_cols)

    df = pd.DataFrame(rows)

    if "end" not in df.columns:
        print(f"[{day}] OBSTATS: no 'end' column in rows", file=sys.stderr)
        return pd.DataFrame(columns=["end"] + metric_cols)

    df = df.copy()
    df["end"] = _to_dt(df["end"])

    drop_cols = {"tradedate", "tradetime", "secid", "asset_code", "SYSTIME"}
    keep = ["end"] + [c for c in df.columns if c not in drop_cols and c != "end"]

    work = df[keep].copy()
    work = work.loc[:, ~work.columns.duplicated()]
    return work


def enrich_day(key: str, day: str) -> pd.DataFrame:
    """
    Собирает полный набор FO+FUTOI+OBSTATS за 1 день.
    При проблемах возвращает пустой DataFrame.
    """
    secid, fo_df = load_fo_for_day(key, day)
    if secid is None or fo_df.empty:
        return pd.DataFrame()

    futoi_df = load_futoi_for_si(day)
    ob_df = load_obstats_for_secid(secid, day)

    # Проверки на структуру
    for name, df in [("FO", fo_df), ("FUTOI", futoi_df), ("OBSTATS", ob_df)]:
        if not df.empty:
            if df.columns.duplicated().any():
                dups = list(df.columns[df.columns.duplicated()])
                print(f"[{day}] ERROR: '{name}' dataframe has duplicated columns: {dups}", file=sys.stderr)
                return pd.DataFrame()
            if "end" not in df.columns:
                print(f"[{day}] ERROR: '{name}' dataframe has no 'end' column", file=sys.stderr)
                return pd.DataFrame()

    enriched = fo_df.copy()

    if not futoi_df.empty:
        enriched = enriched.merge(futoi_df, on="end", how="left")

    if not ob_df.empty:
        enriched = enriched.merge(ob_df, on="end", how="left")

    return enriched


# ---------- Период ----------

def date_range(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    cur = d0
    while cur <= d1:
        yield cur.isoformat()
        cur += timedelta(days=1)


def main():
    key = os.getenv("FO_KEY") or (sys.argv[1] if len(sys.argv) > 1 else "")
    start = os.getenv("FO_START") or (sys.argv[2] if len(sys.argv) > 2 else "")
    end = os.getenv("FO_END") or (sys.argv[3] if len(sys.argv) > 3 else "")

    if not key or not start or not end:
        print(
            "Usage: FO_KEY=Si FO_START=YYYY-MM-DD FO_END=YYYY-MM-DD python fo_enriched_5m_period.py",
            file=sys.stderr,
        )
        print("   or: python fo_enriched_5m_period.py Si 2025-01-01 2025-11-13", file=sys.stderr)
        sys.exit(2)

    all_days: list[pd.DataFrame] = []
    total_rows = 0

    for day in date_range(start, end):
        print(f"[{day}] processing...", file=sys.stderr)
        df_day = enrich_day(key, day)
        if df_day.empty:
            print(f"[{day}] no enriched data, skipping", file=sys.stderr)
            continue

        rows = len(df_day)
        total_rows += rows
        print(f"[{day}] ok, rows={rows}", file=sys.stderr)
        all_days.append(df_day)

    if not all_days:
        print("No data for given period", file=sys.stderr)
        sys.exit(0)

    result = pd.concat(all_days, ignore_index=True)

    out_key = key.lower()
    out_name = f"{out_key}_5m_enriched_{start}_{end}.csv"
    result.to_csv(out_name, index=False)

    print(f"\nKey:    {key}")
    print(f"Period: {start}..{end}")
    print(f"Rows:   {len(result)} (sum of per-day)")
    print(f"Saved:  {out_name}")

    print("\nHead:")
    print(result.head(3).to_csv(index=False))
    print("Tail:")
    print(result.tail(3).to_csv(index=False))


if __name__ == "__main__":
    main()
