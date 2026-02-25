#!/usr/bin/env python3
"""
FO intraday feed (5m tradestats) for a given SECID and date.

This module hides MOEX ISS/APIM details from strategy code.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Union

from src.api.utils.lib_moex_api import get_json, blocks, to_rows, resolve_fut_by_key


def load_fo_5m_day(secid: str, trade_date: Union[str, date] = "AUTO") -> List[Dict[str, Any]]:
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

    if isinstance(trade_date, date):
        day_str = trade_date.isoformat()
    else:
        td = str(trade_date).strip()
        if td.upper() == "AUTO":
            from datetime import timedelta
            today = (datetime.utcnow() + timedelta(hours=3)).date()
            day_str = None
            for d in [today, today - timedelta(days=1), today - timedelta(days=2), today - timedelta(days=3)]:
                ds = d.isoformat()
                sec = resolve_fut_by_key(secid, limit_probe_day=ds)
                if not sec:
                    continue
                try:
                    j0 = get_json(f"/iss/datashop/algopack/fo/tradestats/{sec}.json", {"from": ds, "till": ds}, timeout=15.0)
                    b0 = j0.get("data") or {}
                    if isinstance(b0, dict) and b0.get("data"):
                        day_str = ds
                        break
                except Exception:
                    continue
            if not day_str:
                print("[FO_FEED] AUTO trade_date resolve failed (no tradestats in last 4 days)")
                return []
        else:
            day_str = td

    resolved = resolve_fut_by_key(secid, limit_probe_day=day_str)
    if not resolved:
        print("[FO_FEED] resolve_fut_by_key failed for key=" + str(secid) + " day=" + str(day_str))
        return []
    secid = resolved

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
