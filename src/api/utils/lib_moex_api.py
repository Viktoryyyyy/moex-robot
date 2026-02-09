#!/usr/bin/env python3
import os, requests, typing as T
import pandas as pd

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_api_base/1.3").strip()
TK  = os.getenv("MOEX_API_KEY", "").strip()

def _headers() -> dict:
    return {"Authorization": "Bearer " + TK, "User-Agent": UA}

def get_json(path: str, params: dict | None = None, timeout: float = 20.0) -> dict:
    """GET {API}/{path} -> JSON dict (raise_for_status on HTTP errors)."""
    url = f"{API}/{path.lstrip('/')}"
    r = requests.get(url, headers=_headers(), params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def blocks(j: dict) -> list[str]:
    return [k for k,v in j.items() if isinstance(v, dict) and "columns" in v and "data" in v]

def to_rows(j: dict, block: str) -> tuple[list[str], list[list[T.Any]]]:
    b = j.get(block, {})
    cols = b.get("columns", []) if isinstance(b, dict) else []
    data = b.get("data", []) if isinstance(b, dict) else []
    if not isinstance(cols, list): cols = []
    if not isinstance(data, list): data = []
    return cols, data

def resolve_fut_by_key(key: str, board: str = "rfud", limit_probe_day: str | None = None) -> str | None:
    """
    Находит актуальный фьючерс по подстроке `key` на доске `board` (без регистра).
    Если задан limit_probe_day — выбирает того кандидата, у кого на эту дату есть tradestats.
    """
    key_low = key.lower()
    j = get_json(f"/iss/engines/futures/markets/forts/boards/{board}/securities.json")
    b = j.get("securities") or {}
    cols, data = b.get("columns", []), b.get("data", [])
    if not cols or not data: return None
    df = pd.DataFrame(data, columns=cols)
    if "SECID" not in df.columns: return None
    mask = df["SECID"].astype(str).str.lower().str.contains(key_low, na=False)
    cands = df.loc[mask, "SECID"].drop_duplicates().tolist()
    if not cands: return None
    if not limit_probe_day:
        return cands[0]
    best, best_rows = None, -1
    for sec in cands:
        try:
            j2 = get_json(f"/iss/datashop/algopack/fo/tradestats/{sec}.json",
                          {"from": limit_probe_day, "till": limit_probe_day}, timeout=25.0)
            b2 = j2.get("data") or {}
            rows = len(b2.get("data", [])) if isinstance(b2, dict) else 0
            if rows > best_rows:
                best, best_rows = sec, rows
        except Exception:
            continue
    return best or cands[0]
