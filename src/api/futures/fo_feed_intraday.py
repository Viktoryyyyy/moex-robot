#!/usr/bin/env python3
"""
FO intraday feed (5m tradestats) for a given SECID and date.

This module hides MOEX ISS/APIM details from strategy code.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List

from src.api.utils.lib_moex_api import get_json


def load_fo_5m_day(secid: str, trade_date: date) -> List[Dict[str, Any]]:
    """
    Load 5m bars for given futures SECID and trade_date from MOEX datashop/fo/tradestats.

    Returns
    -------
    rows : list of dict
        Each dict has keys: end, open, high, low, close, volume.
        Sorted by end ascending. Empty list -> no data.
    """
    secid = (secid or "").strip()
    if not secid:
        return []

    day_str = trade_date.isoformat()

    try:
        j = get_json(
            f"/iss/datashop/algopack/fo/tradestats/{secid}.json",
            {"from": day_str, "till": day_str},
            timeout=25.0,
        )
    except Exception as e:
        print(f"[FO_FEED] get_json tradestats failed for {secid} {day_str}: {e}")
        return []

    b = j.get("data") or {}
    cols = b.get("columns", [])
    data = b.get("data", [])
    if not cols or not data:
        print(f"[FO_FEED] tradestats empty for {secid} {day_str}")
        return []

    idx = {name: i for i, name in enumerate(cols)}
    need = ["tradedate", "tradetime", "pr_open", "pr_high", "pr_low", "pr_close", "vol"]
    if not all(k in idx for k in need):
        print(f"[FO_FEED] tradestats missing columns for {secid} {day_str}")
        return []

    rows: List[Dict[str, Any]] = []
    for rec in data:
        try:
            end_str = f"{rec[idx['tradedate']]} {rec[idx['tradetime']]}+03:00"
            end_dt = datetime.fromisoformat(end_str)
            rows.append(
                {
                    "end": end_dt,
                    "open": float(rec[idx["pr_open"]]),
                    "high": float(rec[idx["pr_high"]]),
                    "low": float(rec[idx["pr_low"]]),
                    "close": float(rec[idx["pr_close"]]),
                    "volume": float(rec[idx["vol"]]),
                }
            )
        except Exception:
            continue

    rows.sort(key=lambda r: r["end"])
    print(f"[FO_FEED] {day_str}: loaded {len(rows)} bars for SECID={secid}")
    return rows
