#!/usr/bin/env python3
import os
import sys
import csv
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

TZ_MSK = ZoneInfo("Europe/Moscow")


def fetch_obstats_rows(secid: str, day: str) -> tuple[list[str], list[list]]:
    """
    Загружает ВСЕ страницы OBSTATS для заданного secid и даты day
    через корректный эндпоинт:
      /iss/datashop/algopack/fo/obstats/{secid}.json?from=...&till=...

    Возвращает (columns, all_rows_raw), без фильтрации.
    """
    base = os.getenv("MOEX_API_BASE", "https://apim.moex.com")
    # ВАЖНО: используем инструментно-специфичный endpoint, а не общий .json
    url = base.rstrip("/") + f"/iss/datashop/algopack/fo/obstats/{secid}.json"

    token = os.getenv("MOEX_API_KEY")
    if not token:
        print("MOEX_API_KEY не найден в окружении", file=sys.stderr)
        sys.exit(1)

    headers = {"Authorization": f"Bearer {token}"}

    # Только from/till, без 'ticker' в query
    base_params = {
        "from": day,
        "till": day,
    }

    all_rows: list[list] = []
    cols: list[str] | None = None

    start = 0
    page = 0

    while True:
        params = dict(base_params)
        params["start"] = start

        r = requests.get(url, params=params, headers=headers, timeout=25.0)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            print(f"HTTP error on page {page} (start={start}):", e, file=sys.stderr)
            print("Response text (first 500 chars):", r.text[:500], file=sys.stderr)
            sys.exit(1)

        j = r.json()

        block = j.get("data") or {}
        page_cols = block.get("columns", [])
        page_data = block.get("data", [])

        if cols is None:
            cols = page_cols
        elif page_cols != cols:
            print("WARNING: columns changed between pages", file=sys.stderr)

        cursor = j.get("data.cursor") or {}
        cur_cols = cursor.get("columns", [])
        cur_data = cursor.get("data", [])

        total = pagesize = index = None
        if cur_cols and cur_data:
            cur_rec = dict(zip(cur_cols, cur_data[0]))
            total = cur_rec.get("TOTAL")
            pagesize = cur_rec.get("PAGESIZE")
            index = cur_rec.get("INDEX")

        all_rows.extend(page_data)

        if not page_data:
            break

        if total is not None and pagesize is not None and index is not None:
            try:
                total_i = int(total)
                pagesize_i = int(pagesize)
                index_i = int(index)
            except (TypeError, ValueError):
                print("WARNING: cursor values are not ints, stopping after this page", file=sys.stderr)
                break

            if index_i + pagesize_i >= total_i:
                break

            start = index_i + pagesize_i
            page += 1
        else:
            if page > 0:
                print("WARNING: missing cursor fields on page > 0, stopping", file=sys.stderr)
            break

    return cols or [], all_rows


def build_rows(secid: str, day: str) -> list[dict]:
    cols, data = fetch_obstats_rows(secid, day)

    if not cols or not data:
        return []

    idx = {name: i for i, name in enumerate(cols)}

    required = [
        "tradedate",
        "tradetime",
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
        "SYSTIME",
    ]
    missing = [c for c in required if c not in idx]
    if missing:
        print("Отсутствуют обязательные поля в data:", ", ".join(missing), file=sys.stderr)
        sys.exit(1)

    # Шаг 1: фильтруем только наш secid, собираем даты
    rows_for_secid: list[dict] = []
    dates_for_secid: set[str] = set()

    for row in data:
        tradedate_raw = str(row[idx["tradedate"]])
        tradedate = tradedate_raw[:10]
        tradetime = str(row[idx["tradetime"]])
        secid_row = str(row[idx["secid"]])

        if secid_row != secid:
            continue

        dates_for_secid.add(tradedate)

        rec = {
            "tradedate": tradedate,
            "tradetime": tradetime,
            "secid": secid_row,
            "asset_code": row[idx["asset_code"]],
            "mid_price": row[idx["mid_price"]],
            "micro_price": row[idx["micro_price"]],
            "spread_l1": row[idx["spread_l1"]],
            "spread_l2": row[idx["spread_l2"]],
            "spread_l3": row[idx["spread_l3"]],
            "spread_l5": row[idx["spread_l5"]],
            "spread_l10": row[idx["spread_l10"]],
            "spread_l20": row[idx["spread_l20"]],
            "levels_b": row[idx["levels_b"]],
            "levels_s": row[idx["levels_s"]],
            "vol_b_l1": row[idx["vol_b_l1"]],
            "vol_b_l2": row[idx["vol_b_l2"]],
            "vol_b_l3": row[idx["vol_b_l3"]],
            "vol_b_l5": row[idx["vol_b_l5"]],
            "vol_b_l10": row[idx["vol_b_l10"]],
            "vol_b_l20": row[idx["vol_b_l20"]],
            "vol_s_l1": row[idx["vol_s_l1"]],
            "vol_s_l2": row[idx["vol_s_l2"]],
            "vol_s_l3": row[idx["vol_s_l3"]],
            "vol_s_l5": row[idx["vol_s_l5"]],
            "vol_s_l10": row[idx["vol_s_l10"]],
            "vol_s_l20": row[idx["vol_s_l20"]],
            "vwap_b_l3": row[idx["vwap_b_l3"]],
            "vwap_b_l5": row[idx["vwap_b_l5"]],
            "vwap_b_l10": row[idx["vwap_b_l10"]],
            "vwap_b_l20": row[idx["vwap_b_l20"]],
            "vwap_s_l3": row[idx["vwap_s_l3"]],
            "vwap_s_l5": row[idx["vwap_s_l5"]],
            "vwap_s_l10": row[idx["vwap_s_l10"]],
            "vwap_s_l20": row[idx["vwap_s_l20"]],
            "SYSTIME": row[idx["SYSTIME"]],
        }

        rows_for_secid.append(rec)

    if not rows_for_secid:
        print(f"Нет строк OBSTATS для secid={secid} в ответе сервера", file=sys.stderr)
        return []

    dates_sorted = sorted(dates_for_secid)
    print(f"Даты в OBSTATS для {secid}: {', '.join(dates_sorted)}", file=sys.stderr)
    if day not in dates_for_secid:
        print(
            f"ВНИМАНИЕ: запрошенный день {day} отсутствует в данных OBSTATS для {secid}. "
            f"Сервер вернул данные только за: {', '.join(dates_sorted)}",
            file=sys.stderr,
        )

    # Шаг 2: режем по нужному дню
    rows: list[dict] = [r for r in rows_for_secid if r["tradedate"] == day]

    if not rows:
        return []

    def parse_dt(r: dict) -> datetime:
        dt_str = f"{r['tradedate']} {r['tradetime']}"
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(dt_str, fmt)
                return dt.replace(tzinfo=TZ_MSK)
            except ValueError:
                continue
        raise ValueError(f"Не удалось разобрать datetime: {dt_str}")

    rows.sort(key=parse_dt)

    for r in rows:
        dt = parse_dt(r)
        r["end"] = dt.isoformat()

    return rows


def save_csv(secid: str, day: str, rows: list[dict]) -> str:
    fieldnames = [
        "end",
        "tradedate",
        "tradetime",
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
        "SYSTIME",
    ]

    out_name = f"obstats_{secid}_{day}.csv"
    with open(out_name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})
    return out_name


def main():
    secid = os.getenv("OB_TICKER") or (sys.argv[1] if len(sys.argv) > 1 else "")
    day = os.getenv("OB_DATE") or (sys.argv[2] if len(sys.argv) > 2 else "")

    if not secid or not day:
        print("Usage: OB_TICKER=SiZ5 OB_DATE=YYYY-MM-DD python obstats_5m_day.py", file=sys.stderr)
        print("   or: python obstats_5m_day.py SiZ5 YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    rows = build_rows(secid, day)
    out_path = save_csv(secid, day, rows)

    print(f"Secid: {secid}")
    print(f"Date:  {day}")
    print(f"Rows:  {len(rows)}")
    print(f"Saved: {out_path}")

    if rows:
        print("\nHead:")
        for r in rows[:3]:
            print(r["end"], r["mid_price"], r["spread_l1"])
        print("\nTail:")
        for r in rows[-3:]:
            print(r["end"], r["mid_price"], r["spread_l1"])
    else:
        print("No rows for this secid/date after filtering.")

if __name__ == "__main__":
    main()
