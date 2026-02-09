#!/usr/bin/env python3
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

TZ_MSK = ZoneInfo("Europe/Moscow")


def get_json_obstats(ticker: str, day: str) -> dict:
    """
    Минимальный клиент для OBSTATS (datashop/algopack/fo/obstats.json).
    Используем только реальные блоки: data, data.cursor, data.dates.
    """
    base = os.getenv("MOEX_API_BASE", "https://apim.moex.com")
    url = base.rstrip("/") + "/iss/datashop/algopack/fo/obstats.json"

    params = {
        "from": day,
        "till": day,
        "ticker": ticker,
    }

    token = os.getenv("MOEX_API_KEY")
    if not token:
        print("MOEX_API_KEY не найден в окружении", file=sys.stderr)
        sys.exit(1)

    headers = {"Authorization": f"Bearer {token}"}

    print("Request URL:", url)
    print("Params:", params)

    r = requests.get(url, params=params, headers=headers, timeout=25.0)
    print("HTTP status:", r.status_code)

    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print("HTTP error:", e, file=sys.stderr)
        print("Response text (first 500 chars):", r.text[:500], file=sys.stderr)
        sys.exit(1)

    return r.json()


def main():
    # Для OBSTATS нужен конкретный контракт (SiZ5 и т.п.)
    ticker = os.getenv("OB_TICKER") or (sys.argv[1] if len(sys.argv) > 1 else "")
    day = os.getenv("OB_DATE") or (sys.argv[2] if len(sys.argv) > 2 else "")

    if not ticker or not day:
        print("Usage: OB_TICKER=SiZ5 OB_DATE=YYYY-MM-DD python obstats_probe_fields.py", file=sys.stderr)
        print("   or: python obstats_probe_fields.py SiZ5 YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    j = get_json_obstats(ticker, day)

    # Печатаем список блоков
    print("\nTop-level blocks:")
    for k in j.keys():
        print(" -", k)

    # Реальный основной блок: data
    b = j.get("data") or {}
    cols = b.get("columns", [])
    rows = b.get("data", [])

    print("\n[data] columns ({}):".format(len(cols)))
    if cols:
        print(", ".join(cols))
    else:
        print("NO COLUMNS")

    print("rows =", len(rows))

    # Печатаем первые несколько строк как dict, чтобы увидеть реальные поля
    for row in rows[:5]:
        rec = dict(zip(cols, row))
        print(rec)

    # Блок data.dates — смотрим даты в ответе
    dd = j.get("data.dates") or {}
    dd_cols = dd.get("columns", [])
    dd_data = dd.get("data", [])

    print("\n[data.dates]:")
    if dd_cols and dd_data:
        for r in dd_data:
            print(dict(zip(dd_cols, r)))
    else:
        print("NO DATA.DATES DATA")

if __name__ == "__main__":
    main()
