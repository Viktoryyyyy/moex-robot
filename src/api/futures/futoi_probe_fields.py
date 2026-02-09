#!/usr/bin/env python3
import os, sys, requests
from datetime import datetime
from zoneinfo import ZoneInfo

TZ_MSK = ZoneInfo("Europe/Moscow")

def get_json_futoi(asset: str, day: str) -> dict:
    """
    Минимальный клиент специально для FUTOI.
    Обходит lib_moex_api, чтобы исключить возможные баги с авторизацией.
    """
    base = os.getenv("MOEX_API_BASE", "https://apim.moex.com")
    url = base.rstrip("/") + f"/iss/analyticalproducts/futoi/securities/{asset}.json"
    params = {"from": day, "till": day}

    token = os.getenv("MOEX_API_KEY")
    if not token:
        print("MOEX_API_KEY не найден в окружении", file=sys.stderr)
        sys.exit(1)

    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, params=params, headers=headers, timeout=25.0)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print("HTTP error:", e, file=sys.stderr)
        print("Response text (first 500 chars):", r.text[:500], file=sys.stderr)
        sys.exit(1)

    return r.json()

def main():
    # Базовый актив для FUTOI: по нашей конвенции 'si' для фьючерсов Si
    asset = os.getenv("FO_FUTOI_ASSET") or (sys.argv[1] if len(sys.argv) > 1 else "si")
    day   = os.getenv("FUTOI_DATE") or (sys.argv[2] if len(sys.argv) > 2 else "")

    if not day:
        print("Usage: FUTOI_DATE=YYYY-MM-DD [FO_FUTOI_ASSET=si] python futoi_probe_fields.py", file=sys.stderr)
        sys.exit(2)

    j = get_json_futoi(asset, day)

    # Печатаем список блоков, чтобы видеть полную структуру ответа
    print("Top-level blocks:")
    for k in j.keys():
        print(" -", k)

    # Блок FUTOI (основные данные)
    b = j.get("futoi") or {}
    cols = b.get("columns", [])
    data = b.get("data", [])

    print("\n[futoi] columns ({}):".format(len(cols)))
    if cols:
        print(", ".join(cols))
    else:
        print("NO COLUMNS")

    print("rows =", len(data))

    # Печатаем первые несколько строк как dict, чтобы увидеть реальные поля
    for row in data[:5]:
        rec = dict(zip(cols, row))
        print(rec)

    # Блок dataversion — проверяем торговую дату, сессию и т.п.
    dv = j.get("dataversion") or {}
    dv_cols = dv.get("columns", [])
    dv_data = dv.get("data", [])

    print("\n[dataversion]:")
    if dv_cols and dv_data:
        dv_rec = dict(zip(dv_cols, dv_data[0]))
        print(dv_rec)
    else:
        print("NO DATAVERSION DATA")

if __name__ == "__main__":
    main()
