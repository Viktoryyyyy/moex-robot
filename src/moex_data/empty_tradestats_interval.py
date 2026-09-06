"""Corroborate isolated empty TradeStats intervals against bounded ISS minute queries."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

MOSCOW = ZoneInfo("Europe/Moscow")


class EmptyIntervalError(ValueError):
    pass


def reconcile_empty_interval(*, missing_end, neighboring_bars, minute_payload):
    """Return evidence only when both adjacent OHLCV buckets match exactly.

    Caller must query the exact 15-minute window [end-10m, end+5m).
    The bounded query is smaller than one ISS candle page. Absence alone, a
    failed/empty response, a present TradeStats row, or a mismatch is insufficient.
    No missing OHLC or OI value is produced by this function.
    """
    if not isinstance(missing_end, datetime) or missing_end.tzinfo is None:
        raise EmptyIntervalError("missing interval end must be aware")
    end = missing_end.astimezone(MOSCOW)
    if end.minute % 5 or end.second or end.microsecond:
        raise EmptyIntervalError("missing end must be 5-minute aligned")
    by_end = {bar["end"]: bar for bar in neighboring_bars}
    if len(by_end) != len(neighboring_bars) or end in by_end:
        raise EmptyIntervalError("duplicate bars or allegedly missing interval is present")
    left, right = end - timedelta(minutes=5), end + timedelta(minutes=5)
    if left not in by_end or right not in by_end:
        raise EmptyIntervalError("both adjacent TradeStats bars are required")
    block = minute_payload.get("candles")
    if not isinstance(block, dict):
        raise EmptyIntervalError("ISS candles block missing")
    columns = block.get("columns", [])
    required = {"begin", "end", "open", "high", "low", "close", "volume"}
    if len(set(columns)) != len(columns) or not required.issubset(columns):
        raise EmptyIntervalError("ISS candle schema mismatch")
    rows = []
    seen = set()
    for values in block.get("data", []):
        if len(values) != len(columns):
            raise EmptyIntervalError("ISS candle row width mismatch")
        row = dict(zip(columns, values))
        begin = datetime.fromisoformat(row["begin"])
        close = datetime.fromisoformat(row["end"])
        if begin.tzinfo is None:
            begin = begin.replace(tzinfo=MOSCOW)
        if close.tzinfo is None:
            close = close.replace(tzinfo=MOSCOW)
        if begin.second or begin.microsecond or close != begin + timedelta(seconds=59):
            raise EmptyIntervalError("ISS candle is not a complete minute")
        if not end - timedelta(minutes=10) <= begin < right or begin in seen:
            raise EmptyIntervalError("duplicate or out-of-window ISS candle")
        seen.add(begin)
        for field in ("open", "high", "low", "close", "volume"):
            if isinstance(row[field], bool):
                raise EmptyIntervalError("invalid candle number")
            value = Decimal(str(row[field]))
            if not value.is_finite() or value <= 0:
                raise EmptyIntervalError("non-positive or non-finite candle field")
            row[field] = value
        if not row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]:
            raise EmptyIntervalError("invalid candle OHLC")
        rows.append((begin, row))
    rows.sort(key=lambda item: item[0])
    if any(left <= begin < end for begin, _ in rows):
        raise EmptyIntervalError("ISS contains trades in the missing interval")
    reconciled = []
    for bucket_end in (left, right):
        candles = [row for begin, row in rows if bucket_end - timedelta(minutes=5) <= begin < bucket_end]
        if not candles:
            raise EmptyIntervalError("ISS adjacent interval is empty")
        observed = {"open": candles[0]["open"], "close": candles[-1]["close"],
                    "high": max(r["high"] for r in candles), "low": min(r["low"] for r in candles),
                    "volume": sum(r["volume"] for r in candles)}
        if any(value != Decimal(str(by_end[bucket_end][key])) for key, value in observed.items()):
            raise EmptyIntervalError("adjacent OHLCV does not reconcile")
        reconciled.append(bucket_end.isoformat())
    return {"status": "CORROBORATED_EMPTY", "interval_begin": left.isoformat(),
            "interval_end": end.isoformat(), "matched_adjacent_bucket_ends": reconciled,
            "minute_candle_count": len(rows), "synthetic_ohlc_created": False,
            "oi_inferred": False, "timestamp_policy": "tradetime_is_interval_end",
            "minute_candle_evidence": {"columns": list(columns), "data": [list(row) for row in block['data']]},
            "tradestats_evidence": [{"end": t.isoformat(), **{key: by_end[t][key]
                                    for key in ("open", "high", "low", "close", "volume")}}
                                   for t in (left, right)]}
