#!/usr/bin/env python3
import os
import sys
from datetime import datetime
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


def load_fo(key: str, day: str) -> tuple[str, pd.DataFrame]:
    """
    FO 5m (tradestats) по ключу key и дате day.
    """
    secid = resolve_fut_by_key(key, board="rfud", limit_probe_day=day)
    if not secid:
        print(f"ERROR: no futures match key='{key}'", file=sys.stderr)
        sys.exit(3)

    df = load_tradestats(secid, day)
    if df.empty:
        print(f"WARN: no tradestats data for {secid} {day}", file=sys.stderr)
        sys.exit(0)

    if "end" not in df.columns:
        print("ERROR: FO dataframe has no 'end' column", file=sys.stderr)
        sys.exit(1)

    df = df.copy()
    df["end"] = _to_dt(df["end"])
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
    if not rows:
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
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)

    if "end" not in df.columns:
        print("FUTOI: no 'end' column in rows", file=sys.stderr)
        return pd.DataFrame(columns=[])

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
        return pd.DataFrame(columns=["end"])

    # Берём только end + метрики, работаем на копии
    work = df[["end"] + metric_existing].copy()
    work["end_5m"] = work["end"].dt.floor("5min")

    # Для каждой 5-минутки берём последнюю запись по времени
    work = work.sort_values("end").groupby("end_5m")[metric_existing].last().reset_index()

    work = work.rename(columns={"end_5m": "end"})
    # Гарантируем отсутствие дубликатов колонок
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
    if not rows:
        cols = [
            "end",
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
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)

    if "end" not in df.columns:
        print("OBSTATS: no 'end' column in rows", file=sys.stderr)
        return pd.DataFrame(columns=[])

    df = df.copy()
    df["end"] = _to_dt(df["end"])

    drop_cols = {"tradedate", "tradetime", "secid", "asset_code", "SYSTIME"}
    metric_cols = [c for c in df.columns if c not in drop_cols and c != "end"]

    if not metric_cols:
        return pd.DataFrame(columns=["end"])

    work = df[["end"] + metric_cols].copy()
    work = work.loc[:, ~work.columns.duplicated()]

    return work


def main():
    key = os.getenv("FO_KEY") or (sys.argv[1] if len(sys.argv) > 1 else "")
    day = (
        os.getenv("FO_DAY")
        or (sys.argv[2] if len(sys.argv) > 2 else "")
        or datetime.now(TZ_MSK).date().isoformat()
    )

    if not key:
        print(
            "Usage: FO_KEY=<substr> [FO_DAY=YYYY-MM-DD] python fo_enriched_5m_day.py",
            file=sys.stderr,
        )
        sys.exit(2)

    # 1) FO (5m OHLCV)
    secid, fo_df = load_fo(key, day)

    # 2) FUTOI по Si
    futoi_df = load_futoi_for_si(day)

    # 3) OBSTATS по контракту
    ob_df = load_obstats_for_secid(secid, day)

    # Проверки на уникальность end
    for name, df in [("FO", fo_df), ("FUTOI", futoi_df), ("OBSTATS", ob_df)]:
        if not df.empty:
            if df.columns.duplicated().any():
                dups = df.columns[df.columns.duplicated()]
                print(f"ERROR: '{name}' dataframe has duplicated columns: {list(dups)}", file=sys.stderr)
                sys.exit(1)
            if "end" not in df.columns:
                print(f"ERROR: '{name}' dataframe has no 'end' column", file=sys.stderr)
                sys.exit(1)

    # 4) Mердж по end (datetime)
    enriched = fo_df.copy()

    if not futoi_df.empty:
        enriched = enriched.merge(futoi_df, on="end", how="left")

    if not ob_df.empty:
        enriched = enriched.merge(ob_df, on="end", how="left")

    out_name = f"{secid.lower()}_5m_enriched_{day}.csv"
    enriched.to_csv(out_name, index=False)

    print(f"Key:    {key}")
    print(f"Ticker: {secid}")
    print(f"Date:   {day}")
    print(f"Rows:   {len(enriched)}")
    print(f"Saved:  {out_name}")

    if not enriched.empty:
        print("\nHead:")
        print(enriched.head(3).to_csv(index=False))
        print("Tail:")
        print(enriched.tail(3).to_csv(index=False))


if __name__ == "__main__":
    main()
