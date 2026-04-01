from __future__ import annotations

import csv
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from statistics import mean, pstdev
from typing import Dict, List, Tuple

MIN_HISTORY_DAYS = 39
ADVERSE_MAX = 0.416309
NEUTRAL_MAX = 0.594050
B0 = 1.0114798400503273
B1 = -0.3139576453893631
B2 = -2.0548119736854127


class ContextBuildError(RuntimeError):
    pass


@dataclass(frozen=True)
class DailyBar:
    trade_date: date
    open_: float
    high: float
    low: float
    close: float


def _die(msg: str) -> None:
    raise ContextBuildError(msg)


def _finite(x: float) -> bool:
    return math.isfinite(float(x))


def _resolve_ohlc_columns(fieldnames: List[str]) -> Tuple[str, str, str, str, str]:
    got = set(fieldnames)
    end_col = "end" if "end" in got else ""
    if not end_col:
        _die("missing required column: end")
    candidates = [
        ("open_fo", "high_fo", "low_fo", "close_fo"),
        ("open", "high", "low", "close"),
        ("OPEN", "HIGH", "LOW", "CLOSE"),
    ]
    for o, h, l, c in candidates:
        if o in got and h in got and l in got and c in got:
            return end_col, o, h, l, c
    _die("missing required OHLC columns")


def _load_daily_bars(master_path: str) -> List[DailyBar]:
    if not os.path.exists(master_path):
        _die("master path not found: " + master_path)
    with open(master_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if r.fieldnames is None:
            _die("master has empty header: " + master_path)
        end_col, open_col, high_col, low_col, close_col = _resolve_ohlc_columns(list(r.fieldnames))
        daily: Dict[date, Dict[str, float | str]] = {}
        for row in r:
            try:
                end_raw = (row.get(end_col) or "").strip()
                trade_day = date.fromisoformat(end_raw[:10])
                o = float((row.get(open_col) or "").strip())
                h = float((row.get(high_col) or "").strip())
                l = float((row.get(low_col) or "").strip())
                c = float((row.get(close_col) or "").strip())
            except Exception:
                _die("invalid master row")
            if not (_finite(o) and _finite(h) and _finite(l) and _finite(c)):
                _die("non-finite OHLC in master row")
            cur = daily.get(trade_day)
            if cur is None:
                daily[trade_day] = {"open": o, "high": h, "low": l, "close": c, "first_end": end_raw, "last_end": end_raw}
            else:
                if end_raw < str(cur["first_end"]):
                    cur["first_end"] = end_raw
                    cur["open"] = o
                if end_raw >= str(cur["last_end"]):
                    cur["last_end"] = end_raw
                    cur["close"] = c
                cur["high"] = max(float(cur["high"]), h)
                cur["low"] = min(float(cur["low"]), l)
    out: List[DailyBar] = []
    for d in sorted(daily.keys()):
        cur = daily[d]
        out.append(DailyBar(trade_date=d, open_=float(cur["open"]), high=float(cur["high"]), low=float(cur["low"]), close=float(cur["close"])))
    if not out:
        _die("master has no valid rows: " + master_path)
    return out


def _previous_trade_day(days: List[date], target_day: date) -> date:
    prev = [d for d in days if d < target_day]
    if not prev:
        _die("previous completed trading day not found for target day: " + target_day.isoformat())
    return prev[-1]


def _body_ratio(bar: DailyBar) -> float:
    rng = bar.high - bar.low
    if not _finite(rng) or rng <= 0.0:
        _die("invalid day range for body_ratio: " + bar.trade_date.isoformat())
    val = abs(bar.close - bar.open_) / rng
    if not _finite(val):
        _die("invalid body_ratio: " + bar.trade_date.isoformat())
    return float(val)


def _true_range(cur: DailyBar, prev_close: float | None) -> float:
    parts = [cur.high - cur.low]
    if prev_close is not None:
        parts.append(abs(cur.high - prev_close))
        parts.append(abs(cur.low - prev_close))
    tr = max(parts)
    if not _finite(tr):
        _die("invalid true_range: " + cur.trade_date.isoformat())
    return float(tr)


def _build_rel_range_series(daily: List[DailyBar]) -> List[Tuple[date, float]]:
    trs: List[float] = []
    rels: List[Tuple[date, float]] = []
    prev_close: float | None = None
    for bar in daily:
        tr = _true_range(bar, prev_close)
        trs.append(tr)
        prev_close = bar.close
        if len(trs) < 20:
            continue
        atr20 = mean(trs[-20:])
        if not _finite(atr20) or atr20 <= 0.0:
            _die("invalid atr20: " + bar.trade_date.isoformat())
        rel = tr / atr20
        if not _finite(rel):
            _die("invalid rel_range: " + bar.trade_date.isoformat())
        rels.append((bar.trade_date, float(rel)))
    return rels


def _vol_z_for_source_day(source_day: date, rel_series: List[Tuple[date, float]]) -> float:
    rel_map = {d: v for d, v in rel_series}
    if source_day not in rel_map:
        _die("source day missing rel_range/atr20 readiness: " + source_day.isoformat())
    hist = [v for d, v in rel_series if d < source_day]
    if len(hist) < 20:
        _die("insufficient seeded history for vol_z: " + source_day.isoformat())
    mu = mean(hist)
    sd = pstdev(hist)
    if not _finite(mu) or not _finite(sd) or sd <= 0.0:
        _die("invalid vol_z history statistics: " + source_day.isoformat())
    val = (rel_map[source_day] - mu) / sd
    if not _finite(val):
        _die("invalid vol_z value: " + source_day.isoformat())
    return float(val)


def compute_context_for_target_day(master_path: str, target_day: str) -> Dict[str, object]:
    try:
        d_target = date.fromisoformat(str(target_day))
    except Exception:
        _die("invalid target day: " + str(target_day))
    daily = _load_daily_bars(master_path)
    if len(daily) < MIN_HISTORY_DAYS:
        _die("insufficient daily history: have=" + str(len(daily)) + " required>=" + str(MIN_HISTORY_DAYS))
    days = [x.trade_date for x in daily]
    source_day = _previous_trade_day(days, d_target)
    bar_map = {x.trade_date: x for x in daily}
    src = bar_map.get(source_day)
    if src is None:
        _die("source trade day not found in daily map: " + source_day.isoformat())
    d1_body_ratio = _body_ratio(src)
    rel_series = _build_rel_range_series(daily)
    d1_vol_z = _vol_z_for_source_day(source_day, rel_series)
    z = B0 + B1 * d1_vol_z + B2 * d1_body_ratio
    if not _finite(z):
        _die("invalid logistic z-score")
    score = 1.0 / (1.0 + math.exp(-z))
    if not _finite(score):
        _die("invalid logistic score")
    if score < ADVERSE_MAX:
        band = "adverse"
        decision = "blocked"
        blocked = True
    elif score < NEUTRAL_MAX:
        band = "neutral"
        decision = "allowed"
        blocked = False
    else:
        band = "favorable"
        decision = "allowed"
        blocked = False
    return {
        "target_day": d_target.isoformat(),
        "source_trade_date": source_day.isoformat(),
        "features": {
            "d1_vol_z": d1_vol_z,
            "d1_body_ratio": d1_body_ratio,
        },
        "score": score,
        "band": band,
        "decision": decision,
        "blocked": blocked,
        "status": "ok",
        "reason": "band=" + band,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_context_payload(master_path: str, target_day: str) -> Dict[str, object]:
    try:
        return compute_context_for_target_day(master_path=master_path, target_day=target_day)
    except ContextBuildError as e:
        try:
            d_target = date.fromisoformat(str(target_day)).isoformat()
        except Exception:
            d_target = str(target_day)
        return {
            "target_day": d_target,
            "source_trade_date": None,
            "features": {
                "d1_vol_z": None,
                "d1_body_ratio": None,
            },
            "score": None,
            "band": None,
            "decision": "blocked",
            "blocked": True,
            "status": "error",
            "reason": str(e),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
