"""
Data feed (5m bars) for EMA(5,12) GOOD-days robot.

Step 14:
  - Define a strict Bar structure.
  - Provide a generic helper to iterate closed 5m bars from
    already prepared rows (e.g. from CSV, DataFrame, MOEX 5m candles).
  - Implement iter_new_bars(state) using MOEX FO tradestats endpoint
    via lib_moex_api in a conservative polling loop.

The feed:
  - works per trading date state["trade_date"] (YYYY-MM-DD),
  - resolves current Si futures contract via resolve_fut_by_key(),
  - repeatedly polls /iss/datashop/algopack/fo/tradestats/<SECID>.json
    with from=till=<trade_date>,
  - converts rows to internal Bar objects,
  - yields only NEW bars with end > state["last_bar_end"].

Anti-cheat:
  - Bar objects are created only from already closed 5m candles
    returned by MOEX (no extrapolation).
  - Filtering by state["last_bar_end"] ensures that each bar is
    processed at most once across restarts.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional

import pandas as pd

from api.utils.lib_moex_api import get_json, resolve_fut_by_key
from .config_ema_5_12 import FO_KEY


POLL_SECONDS = 30.0  # delay between tradestats polls in realtime loop


@dataclass(frozen=True)
class Bar:
    """
    Internal 5m bar representation used by EMA(5,12) robot.

    All times must be timezone-aware (MSK, +03:00).
    Prices and volume are represented as floats for simplicity.
    """
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


def _parse_end(value: Any) -> Optional[datetime]:
    """
    Try to convert value to datetime.

    Expected formats:
      - datetime instance (returned as-is)
      - ISO string "YYYY-MM-DD HH:MM:SS+03:00" or "YYYY-MM-DDTHH:MM:SS+03:00"

    Returns None if parsing fails.
    """
    if isinstance(value, datetime):
        return value

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    # Accept both " " and "T" between date and time.
    text = text.replace("T", " ")

    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _get_last_bar_end_from_state(state: Dict[str, Any]) -> Optional[datetime]:
    """
    Extract last_bar_end from state dict as datetime.

    Returns None if field is missing, empty or cannot be parsed.
    """
    raw = state.get("last_bar_end")
    if raw is None:
        return None
    return _parse_end(raw)


def iter_bars_from_rows(
    rows: Iterable[Mapping[str, Any]],
    state: Dict[str, Any],
) -> Iterator[Bar]:
    """
    Generic helper: iterate 5m bars from an iterable of row-like objects.

    Each row must provide at least the following keys:
      - "end"
      - "open"
      - "high"
      - "low"
      - "close"
      - "volume"

    All rows must be sorted by "end" in ascending order.

    Anti-cheat:
      - Only rows with end > state.last_bar_end are yielded.
      - If last_bar_end is None, all rows are eligible.

    This function does not access network or disk. It is intended to be
    used by higher-level code that already obtained 5m data from some
    concrete source (CSV, DataFrame, MOEX candles, etc.).
    """
    last_end = _get_last_bar_end_from_state(state)

    for row in rows:
        end_dt = _parse_end(row.get("end"))
        if end_dt is None:
            # Skip invalid row silently; caller can validate upstream.
            continue

        if last_end is not None and end_dt <= last_end:
            # Already processed or older bar.
            continue

        try:
            o = float(row.get("open"))
            h = float(row.get("high"))
            l = float(row.get("low"))
            c = float(row.get("close"))
            v = float(row.get("volume"))
        except Exception:
            # Any conversion error -> skip this row, do not break the loop.
            continue

        yield Bar(end=end_dt, open=o, high=h, low=l, close=c, volume=v)


def _load_tradestats_for_day(secid: str, day: str) -> pd.DataFrame:
    """
    Load FO 5m tradestats for given SECID and day (YYYY-MM-DD).

    This mirrors the logic of fo_5m_day.py: we use the same endpoint and
    map fields to standard OHLCV 5m structure.
    """
    j = get_json(
        f"/iss/datashop/algopack/fo/tradestats/{secid}.json",
        {"from": day, "till": day},
        timeout=25.0,
    )
    b = j.get("data") or {}
    cols, data = b.get("columns", []), b.get("data", [])
    if not cols or not data:
        return pd.DataFrame(columns=["end", "open", "high", "low", "close", "volume"])

    raw = pd.DataFrame(data, columns=cols)
    need = {"tradedate", "tradetime", "pr_open", "pr_high", "pr_low", "pr_close", "vol"}
    if not need.issubset(raw.columns):
        return pd.DataFrame(columns=["end", "open", "high", "low", "close", "volume"])

    raw["end"] = raw["tradedate"] + " " + raw["tradetime"] + "+03:00"
    df = pd.DataFrame(
        {
            "end": raw["end"],
            "open": pd.to_numeric(raw["pr_open"], errors="coerce"),
            "high": pd.to_numeric(raw["pr_high"], errors="coerce"),
            "low": pd.to_numeric(raw["pr_low"], errors="coerce"),
            "close": pd.to_numeric(raw["pr_close"], errors="coerce"),
            "volume": pd.to_numeric(raw["vol"], errors="coerce"),
        }
    ).sort_values("end").reset_index(drop=True)
    return df


def _resolve_fo_secid(trade_date: date) -> Optional[str]:
    """
    Resolve current FO SECID for Si by key FO_KEY for given trade_date.

    Uses lib_moex_api.resolve_fut_by_key with board='rfud' and
    limit_probe_day=trade_date.isoformat().
    """
    day_str = trade_date.isoformat()
    try:
        secid = resolve_fut_by_key(FO_KEY, board="rfud", limit_probe_day=day_str)
    except Exception:
        return None
    if not secid:
        return None
    return str(secid).strip()


def iter_new_bars(state: Dict[str, Any]) -> Iterator[Bar]:
    """
    Realtime generator for closed 5m bars.

    Strategy:
      - trade_date is taken from state["trade_date"] (YYYY-MM-DD).
      - Resolve SECID once at start.
      - In a loop:
          * load full tradestats 5m history for this SECID and date,
          * convert to rows and pass through iter_bars_from_rows(),
          * yield only bars with end > state["last_bar_end"],
          * sleep POLL_SECONDS and repeat.

    This is a polling model on MOEX datashop/fo/tradestats, with strictly
    non-anticipative behaviour: we only see bars that MOEX already
    сформировал и выдал.
    """
    trade_date_raw = state.get("trade_date")
    if trade_date_raw:
        try:
            trade_date_obj = date.fromisoformat(str(trade_date_raw))
        except Exception:
            trade_date_obj = date.today()
    else:
        trade_date_obj = date.today()
    day_str = trade_date_obj.isoformat()

    secid = _resolve_fo_secid(trade_date_obj)
    if not secid:
        # No suitable futures found -> no data.
        return

    # Main polling loop
    while True:
        try:
            df = _load_tradestats_for_day(secid, day_str)
            if not df.empty:
                rows = df.to_dict("records")
                for bar in iter_bars_from_rows(rows, state):
                    yield bar
        except Exception:
            # Any error from MOEX or parsing -> just wait and try again.
            pass

        time.sleep(POLL_SECONDS)
