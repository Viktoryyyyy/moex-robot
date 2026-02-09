#!/usr/bin/env python3
import os
import sys
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from lib_moex_api import get_json

TZ_MSK = ZoneInfo("Europe/Moscow")


def date_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def fetch_obstats_raw(secid: str, day: str, timeout: float = 30.0) -> pd.DataFrame:
    path = f"/iss/datashop/algopack/fo/obstats/{secid}.json"

    all_rows = []
    all_cols = None
    start_param = 0

    while True:
        params = {"from": day, "till": day}
        if start_param:
            params["start"] = start_param

        j = get_json(path, params=params, timeout=timeout)
        data_block = j.get("data") or {}
        cols = data_block.get("columns") or []
        rows = data_block.get("data") or []

        if not cols or not rows:
            break

        if all_cols is None:
            all_cols = cols

        all_rows.extend(rows)
        got = len(rows)

        cursor_block = j.get("data.cursor") or {}
        c_cols = cursor_block.get("columns") or []
        c_rows = cursor_block.get("data") or []

        if not c_cols or not c_rows:
            if got == 0:
                break
            if got < 100:
                break
            start_param += got
            continue

        c_map = {name: idx for idx, name in enumerate(c_cols)}
        first = c_rows[0]
        total = first[c_map.get("TOTAL")] if "TOTAL" in c_map else None
        page_index = first[c_map.get("PAGEINDEX")] if "PAGEINDEX" in c_map else None
        page_size = first[c_map.get("PAGESIZE")] if "PAGESIZE" in c_map else None
        pages = first[c_map.get("PAGES")] if "PAGES" in c_map else None

        if pages is not None and page_index is not None:
            if page_index >= pages - 1:
                break

        if total is not None and len(all_rows) >= total:
            break

        if got == 0:
            break

        if page_size is not None:
            start_param += page_size
        else:
            start_param += got

    if not all_rows or not all_cols:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=all_cols)
    return df


def build_obstats_5m_for_day(secid: str, day: str) -> pd.DataFrame:
    raw = fetch_obstats_raw(secid, day)
    if raw.empty:
        return pd.DataFrame()

    if "tradedate" in raw.columns:
        raw = raw[raw["tradedate"] == day]
    if "secid" in raw.columns:
        raw = raw[raw["secid"] == secid]

    if raw.empty:
        return pd.DataFrame()

    if not {"tradedate", "tradetime"} <= set(raw.columns):
        print(f"[{day}] OBSTATS {secid}: no tradedate/tradetime in columns", file=sys.stderr)
        return pd.DataFrame()

    dt = pd.to_datetime(raw["tradedate"] + " " + raw["tradetime"])
    raw = raw.copy()
    raw["end"] = dt.dt.tz_localize(TZ_MSK, nonexistent="shift_forward", ambiguous="NaT")

    base_cols = [
        "end",
        "secid",
        "asset_code",
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

    existing_base = [c for c in base_cols if c in raw.columns]
    other_cols = [c for c in raw.columns if c not in existing_base and c not in ("tradedate", "tradetime")]

    cols_order = existing_base + other_cols

    work = raw[["end"] + [c for c in cols_order if c != "end"]].copy()

    work["end_5m"] = work["end"].dt.floor("5min")
    group_cols = [c for c in work.columns if c not in ("end", "end_5m")]

    work = (
        work.sort_values("end")
            .groupby("end_5m")[group_cols]
            .last()
            .reset_index()
            .rename(columns={"end_5m": "end"})
    )

    work = work.loc[:, ~work.columns.duplicated()]

    return work


def main():
    master_path = os.getenv("MASTER_PATH") or "si_cny_5m_2020-01-01_2025-11-13.csv"

    if not os.path.exists(master_path):
        print(f"Master file not found: {master_path}", file=sys.stderr)
        print("Укажи путь через MASTER_PATH=/path/to/file.csv", file=sys.stderr)
        sys.exit(1)

    print(f"Reading master file: {master_path}", file=sys.stderr)
    master = pd.read_csv(master_path)

    if "end" not in master.columns:
        print("Master file must contain 'end' column", file=sys.stderr)
        sys.exit(1)

    # Ищем корректное поле с тикером: сначала ticker_si, потом ticker
    ticker_col_candidates = [c for c in ("ticker_si", "ticker") if c in master.columns]
    if not ticker_col_candidates:
        print("Master file must contain 'ticker_si' or 'ticker' column", file=sys.stderr)
        sys.exit(1)

    ticker_col = ticker_col_candidates[0]
    print(f"Using ticker column: {ticker_col}", file=sys.stderr)

    master["end"] = pd.to_datetime(master["end"])
    master["date"] = master["end"].dt.date

    start_date = master["date"].min()
    end_date = master["date"].max()

    print(f"Detected period from master: {start_date}..{end_date}", file=sys.stderr)

    day_secids = {}
    for d, df_day in master.groupby("date"):
        tickers = df_day[ticker_col].dropna().astype(str)
        if tickers.empty:
            continue
        secid = tickers.value_counts().idxmax()
        day_secids[d] = secid

    if not day_secids:
        print("No ticker mapping per day derived from master file", file=sys.stderr)
        sys.exit(1)

    all_days = []
    total_rows = 0

    for d in date_range(start_date, end_date):
        if d not in day_secids:
            print(f"[{d}] no secid in master (no trades?), skipping", file=sys.stderr)
            continue

        day_str = d.isoformat()
        secid = day_secids[d]

        print(f"[{day_str}] OBSTATS {secid}...", file=sys.stderr)
        df_day = build_obstats_5m_for_day(secid, day_str)
        if df_day.empty:
            print(f"[{day_str}] no OBSTATS rows for {secid}, skipping", file=sys.stderr)
            continue

        rows = len(df_day)
        total_rows += rows
        print(f"[{day_str}] ok, rows={rows}", file=sys.stderr)
        all_days.append(df_day)

    if not all_days:
        print("No OBSTATS data for detected period", file=sys.stderr)
        sys.exit(0)

    result = pd.concat(all_days, ignore_index=True)
    result = result.sort_values("end").reset_index(drop=True)

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    out_name = f"obstats_si_5m_{start_str}_{end_str}.csv"
    result.to_csv(out_name, index=False)

    print(f"\nBase asset: Si (OBSTATS via per-sec series)")
    print(f"Period:     {start_str}..{end_str}")
    print(f"Rows:       {len(result)} (sum of per-day={total_rows})")
    print(f"Saved:      {out_name}")

    print("\nHead:")
    print(result.head(5).to_csv(index=False))
    print("Tail:")
    print(result.tail(5).to_csv(index=False))


if __name__ == "__main__":
    main()
