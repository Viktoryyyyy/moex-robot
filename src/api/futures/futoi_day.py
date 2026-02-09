#!/usr/bin/env python3
import os
import sys
import csv
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

TZ_MSK = ZoneInfo("Europe/Moscow")


def get_json_futoi(asset: str, day: str) -> dict:
    """
    Минимальный клиент специально для FUTOI.
    Использует тот же подход, что и futoi_probe_fields.py.
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


def build_rows(asset: str, day: str) -> list[dict]:
    j = get_json_futoi(asset, day)
    b = j.get("futoi") or {}
    cols = b.get("columns", [])
    data = b.get("data", [])

    if not cols or not data:
        return []

    # Индексы колонок для скорости и защиты от изменений порядка
    idx = {name: i for i, name in enumerate(cols)}

    required = [
        "sess_id",
        "seqnum",
        "tradedate",
        "tradetime",
        "ticker",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "systime",
        "trade_session_date",
    ]
    missing = [c for c in required if c not in idx]
    if missing:
        print("Отсутствуют обязательные поля в futoi:", ", ".join(missing), file=sys.stderr)
        sys.exit(1)

    grouped: dict[tuple[str, str], dict] = {}

    for row in data:
        tradedate = row[idx["tradedate"]]
        tradetime = row[idx["tradetime"]]
        ticker = row[idx["ticker"]]
        clgroup = row[idx["clgroup"]]  # ожидаем "FIZ" или "YUR"

        key = (tradedate, tradetime)

        rec = grouped.get(key)
        if rec is None:
            # базовая часть, общая для FIZ и YUR
            rec = {
                "tradedate": tradedate,
                "tradetime": tradetime,
                "sess_id": row[idx["sess_id"]],
                "seqnum": row[idx["seqnum"]],
                "ticker": ticker,
                "trade_session_date": row[idx["trade_session_date"]],
                "systime": row[idx["systime"]],
                # заранее заполняем None для всех полей FIZ/YUR
                "pos_fiz": None,
                "pos_yur": None,
                "pos_long_fiz": None,
                "pos_short_fiz": None,
                "pos_long_yur": None,
                "pos_short_yur": None,
                "pos_long_num_fiz": None,
                "pos_short_num_fiz": None,
                "pos_long_num_yur": None,
                "pos_short_num_yur": None,
            }
            grouped[key] = rec

        # заполняем данные для конкретного clgroup
        if clgroup == "FIZ":
            rec["pos_fiz"] = row[idx["pos"]]
            rec["pos_long_fiz"] = row[idx["pos_long"]]
            rec["pos_short_fiz"] = row[idx["pos_short"]]
            rec["pos_long_num_fiz"] = row[idx["pos_long_num"]]
            rec["pos_short_num_fiz"] = row[idx["pos_short_num"]]
        elif clgroup == "YUR":
            rec["pos_yur"] = row[idx["pos"]]
            rec["pos_long_yur"] = row[idx["pos_long"]]
            rec["pos_short_yur"] = row[idx["pos_short"]]
            rec["pos_long_num_yur"] = row[idx["pos_long_num"]]
            rec["pos_short_num_yur"] = row[idx["pos_short_num"]]
        else:
            # На всякий случай игнорируем другие группы, если появятся
            continue

    # Приводим к списку и сортируем по времени
    rows = list(grouped.values())

    def parse_dt(r: dict) -> datetime:
        # Используем точное время, как пришло (включая секунды).
        dt_str = f"{r['tradedate']} {r['tradetime']}"
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # На всякий случай поддержка формата без секунд
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=TZ_MSK)

    rows.sort(key=parse_dt)

    # Добавляем колонку end в формате ISO (как в FO/FX 5m)
    for r in rows:
        dt = parse_dt(r)
        r["end"] = dt.isoformat()

    return rows


def save_csv(asset: str, day: str, rows: list[dict]) -> str:
    if not rows:
        out_name = f"futoi_{asset}_{day}.csv"
        with open(out_name, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "end",
                    "tradedate",
                    "tradetime",
                    "sess_id",
                    "seqnum",
                    "ticker",
                    "trade_session_date",
                    "systime",
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
            )
        return out_name

    fieldnames = [
        "end",
        "tradedate",
        "tradetime",
        "sess_id",
        "seqnum",
        "ticker",
        "trade_session_date",
        "systime",
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

    out_name = f"futoi_{asset}_{day}.csv"
    with open(out_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in fieldnames})
    return out_name


def main():
    asset = os.getenv("FO_FUTOI_ASSET") or (sys.argv[1] if len(sys.argv) > 1 else "si")
    day = os.getenv("FUTOI_DATE") or (sys.argv[2] if len(sys.argv) > 2 else "")

    if not day:
        print("Usage: FUTOI_DATE=YYYY-MM-DD [FO_FUTOI_ASSET=si] python futoi_day.py", file=sys.stderr)
        sys.exit(2)

    rows = build_rows(asset, day)
    out_path = save_csv(asset, day, rows)

    print(f"Asset: {asset}")
    print(f"Date:  {day}")
    print(f"Rows:  {len(rows)}")
    print(f"Saved: {out_path}")

    if rows:
        print("\nHead:")
        for r in rows[:3]:
            print(r["end"], r["pos_fiz"], r["pos_yur"])
        print("\nTail:")
        for r in rows[-3:]:
            print(r["end"], r["pos_fiz"], r["pos_yur"])


if __name__ == "__main__":
    main()
