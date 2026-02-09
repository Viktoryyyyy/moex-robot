#!/usr/bin/env python3
import os
from typing import Any, Dict, Optional

import requests
from mcp.server.fastmcp import FastMCP

API_URL = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
API_KEY = os.getenv("MOEX_API_KEY", "").strip()
USER_AGENT = os.getenv("MOEX_UA", "moex_mcp/1.0").strip() or "moex_mcp/1.0"


def _headers() -> Dict[str, str]:
    h = {"User-Agent": USER_AGENT}
    if API_KEY:
        h["Authorization"] = f"Bearer {API_KEY}"
    return h


def get_json(path: str, params: Optional[Dict[str, Any]] = None, timeout: float = 20.0) -> Dict[str, Any]:
    if not API_KEY:
        raise RuntimeError("MOEX_API_KEY не задан в окружении")
    url = f"{API_URL}{path}"
    resp = requests.get(url, headers=_headers(), params=params or {}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


mcp = FastMCP("moex")


@mcp.tool()
def ping() -> str:
    return "pong from moex_mcp"


@mcp.tool()
def fo_marketdata(ticker: str) -> Dict[str, Any]:
    path = f"/iss/engines/futures/markets/forts/boards/rfud/securities/{ticker}.json"
    j = get_json(path, params={"iss.meta": "off"})
    block = j.get("marketdata") or j.get("data") or {}
    cols = block.get("columns") or []
    data = block.get("data") or []
    if not cols or not data:
        raise RuntimeError(f"Пустой marketdata для тикера {ticker}")
    row = data[0]

    def pick(name: str):
        return row[cols.index(name)] if name in cols else None

    return {
        "secid": pick("SECID") or ticker,
        "bid": pick("BID"),
        "offer": pick("OFFER"),
        "spread": pick("SPREAD"),
        "last": pick("LAST"),
        "open": pick("OPEN"),
        "high": pick("HIGH"),
        "low": pick("LOW"),
        "numtrades": pick("NUMTRADES"),
        "vol_today": pick("VOLTODAY"),
        "val_today": pick("VALTODAY"),
        "open_position": pick("OPENPOSITION"),
        "time": pick("TIME") or pick("UPDATETIME"),
    }


@mcp.tool()
def fo_tradestats_raw(ticker: str, tradedate: str) -> Dict[str, Any]:
    """
    Сырые Super Candles TradeStats за один день.
    Пытаемся оба варианта эндпоинта, как в fo_tradestats_chain.py.
    Возвращаем блок columns+data как есть.
    """
    day = tradedate
    urls = [
        f"{API_URL}/iss/datashop/algopack/fo/tradestats/{ticker}.json?from={day}&till={day}&iss.meta=off",
        f"{API_URL}/iss/datashop/algopack/fo/tradestats.json?ticker={ticker}&from={day}&till={day}&iss.meta=off",
    ]
    last_err: Optional[str] = None

    for u in urls:
        try:
            r = requests.get(u, headers=_headers(), timeout=25.0)
        except Exception as e:
            last_err = f"req_error={e}"
            continue

        if not r.ok or "application/json" not in r.headers.get("content-type", ""):
            last_err = f"http={r.status_code}, ct={r.headers.get('content-type')}"
            continue

        try:
            j = r.json()
        except Exception as e:
            last_err = f"json_error={e}"
            continue

        blk = j.get("tradestats") or j.get("data") or {}

        # В некоторых ответах tradestats — это список блоков
        if not isinstance(blk, dict) and "tradestats" in j and isinstance(j["tradestats"], list) and j["tradestats"]:
            blk = j["tradestats"][0]

        if not isinstance(blk, dict):
            last_err = "tradestats_block_not_dict"
            continue

        cols = blk.get("columns") or []
        data = blk.get("data") or []

        # Если совсем пусто — возвращаем пустой результат без ошибки
        if not cols or not data:
            return {"columns": cols, "data": data, "ticker": ticker, "tradedate": tradedate}

        return {
            "columns": cols,
            "data": data,
            "ticker": ticker,
            "tradedate": tradedate,
        }

    raise RuntimeError(f"fo_tradestats_raw: не удалось получить данные для {ticker} {tradedate}: {last_err}")


@mcp.tool()
def fo_5m_day(ticker: str, tradedate: str) -> Dict[str, Any]:
    """
    Нормализованные 5m-бары за день в формате:
      end, open, high, low, close, volume, ticker

    end — строка вида "YYYY-MM-DD HH:MM:SS+03:00" (московское время).
    Данные берутся из fo_tradestats_raw().
    """
    raw = fo_tradestats_raw(ticker, tradedate)
    cols = raw.get("columns") or []
    data = raw.get("data") or []

    if not cols or not data:
        return {"bars": [], "ticker": ticker, "tradedate": tradedate}

    name_to_idx = {name: i for i, name in enumerate(cols)}

    required = ["tradedate", "tradetime", "pr_open", "pr_high", "pr_low", "pr_close"]
    missing = [n for n in required if n not in name_to_idx]
    if missing:
        raise RuntimeError(f"В tradestats отсутствуют необходимые колонки: {missing}")

    vol_col = None
    for cand in ["vol", "volume", "VOLUME"]:
        if cand in name_to_idx:
            vol_col = name_to_idx[cand]
            break

    bars = []
    for row in data:
        d = row[name_to_idx["tradedate"]]
        t = row[name_to_idx["tradetime"]]

        # MOEX иногда отдаёт HH:MM, иногда HH:MM:SS
        t_str = str(t)
        if len(t_str) == 5:  # "HH:MM"
            t_str = f"{t_str}:00"

        end = f"{d} {t_str}+03:00"

        bar = {
            "end": end,
            "open": row[name_to_idx["pr_open"]],
            "high": row[name_to_idx["pr_high"]],
            "low": row[name_to_idx["pr_low"]],
            "close": row[name_to_idx["pr_close"]],
            "volume": row[vol_col] if vol_col is not None else None,
            "ticker": ticker,
        }
        bars.append(bar)

    return {
        "bars": bars,
        "ticker": ticker,
        "tradedate": tradedate,
    }


if __name__ == "__main__":
    mcp.run(transport="stdio")
