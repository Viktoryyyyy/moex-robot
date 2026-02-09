"""
Regime-day loader for EMA(5,12) GOOD-days robot (online R1).

Логика R1:
  - Используем фиксированный контракт FO_R1_SECID (например, "SiZ5").
  - Для торгового дня D ищем последний день T < D, где по FO_R1_SECID
    есть 5m tradestats.
  - Качаем 5m бары за T.
  - Считаем dd_max базовой EMA(5,12) next-bar стратегии.
  - Если dd_max(T) <= DRAWDOWN_LIMIT_PTS -> режим "GOOD", иначе "BAD".
  - Если данных нет или ошибка -> "BAD" (пессимистично).

Таким образом, фильтр R1 в онлайне смотрит ровно тот же контракт,
на котором тестировалась стратегия, без auto-resolve.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.api.utils.lib_moex_api import get_json

from .config_ema_5_12 import (
    FO_R1_SECID,
    EMA_FAST_WINDOW,
    EMA_SLOW_WINDOW,
    COMMISSION_PTS_PER_TRADE,
)

MAX_LOOKBACK_DAYS = 30
DRAWDOWN_LIMIT_PTS = 5000.0


def _load_fo_5m_for_day(trade_date: date) -> List[Dict[str, Any]]:
    """
    Загружаем 5m tradestats по фиксированному SECID (FO_R1_SECID) за день.

    Возвращаем список баров с ключами:
      "end", "open", "high", "low", "close", "volume"
    Отсортированный по end по возрастанию.
    """
    secid = (FO_R1_SECID or "").strip()
    day_str = trade_date.isoformat()

    if not secid:
        print(f"[R1] FO_R1_SECID пуст, не можем загрузить данные за {day_str}")
        return []

    try:
        j = get_json(
            f"/iss/datashop/algopack/fo/tradestats/{secid}.json",
            {"from": day_str, "till": day_str},
            timeout=25.0,
        )
    except Exception as e:
        print(f"[R1] get_json tradestats failed for {secid} {day_str}: {e}")
        return []

    b = j.get("data") or {}
    cols, data = b.get("columns", []), b.get("data", [])
    if not cols or not data:
        print(f"[R1] tradestats empty for {secid} {day_str}")
        return []

    idx = {name: i for i, name in enumerate(cols)}
    need = ["tradedate", "tradetime", "pr_open", "pr_high", "pr_low", "pr_close", "vol"]
    if not all(k in idx for k in need):
        print(f"[R1] tradestats missing columns for {secid} {day_str}")
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
    print(f"[R1] {day_str}: loaded {len(rows)} bars for SECID={secid}")
    return rows


def _commission_points(pos_before: int, target_pos: int) -> float:
    """
    Модель комиссии, согласованная с executor_ema_5_12:

      - COMMISSION_PTS_PER_TRADE задаётся на полный круг (open+close).
      - Открытие (0 -> ±1) или закрытие (±1 -> 0) -> половина круга.
      - Реверс (±1 -> ∓1) -> полный круг.
    """
    if pos_before == 0 and target_pos != 0:
        return COMMISSION_PTS_PER_TRADE / 2
    if pos_before != 0 and target_pos == 0:
        return COMMISSION_PTS_PER_TRADE / 2
    if pos_before != 0 and target_pos != 0 and target_pos != pos_before:
        return COMMISSION_PTS_PER_TRADE
    return 0.0


def _update_ema(prev: Optional[float], price: float, window: int) -> float:
    alpha = 2.0 / (window + 1)
    if prev is None:
        return price
    return alpha * price + (1 - alpha) * prev


def _simulate_ema_day_ddmax(rows: List[Dict[str, Any]]) -> float:
    """
    Базовая EMA(5,12) стратегия с исполнением на следующем баре.
    Считаем максимальную дневную просадку dd_max (в пунктах).
    """
    if not rows:
        return 0.0

    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    bars = 0

    pos = 0
    entry: Optional[float] = None
    pnl_real = 0.0

    equity = 0.0
    peak = 0.0
    dd_max = 0.0

    pending: Optional[int] = None

    for i, r in enumerate(rows):
        close_i = float(r["close"])

        # 1) Исполняем отложенный сигнал (target от предыдущего бара)
        if pending is not None:
            target = pending
            pending = None

            commission = _commission_points(pos, target)

            pnl_trade = 0.0
            if pos != 0 and entry is not None:
                if pos > 0:
                    pnl_trade = close_i - entry
                else:
                    pnl_trade = entry - close_i

            pnl_real += pnl_trade - commission

            pos = target
            entry = close_i if pos != 0 else None

        # 2) Обновляем EMA по текущему close
        bars += 1
        ema_fast = _update_ema(ema_fast, close_i, EMA_FAST_WINDOW)
        ema_slow = _update_ema(ema_slow, close_i, EMA_SLOW_WINDOW)

        # 3) Целевая позиция по сигналу
        if bars < EMA_SLOW_WINDOW:
            target = pos
        else:
            diff = ema_fast - ema_slow
            if diff > 0:
                target = 1
            elif diff < 0:
                target = -1
            else:
                target = 0

        # 4) Ставим отложенный сигнал на следующий бар
        if i < len(rows) - 1 and target != pos:
            pending = target

        # 5) Марк-то-маркет по текущему бару
        if pos != 0 and entry is not None:
            m2m = (close_i - entry) if pos > 0 else (entry - close_i)
        else:
            m2m = 0.0

        equity = pnl_real + m2m
        if equity > peak:
            peak = equity

        dd = peak - equity
        if dd > dd_max:
            dd_max = dd

    return dd_max


def _find_last_trading_day(trade_date: date) -> Tuple[Optional[date], List[Dict[str, Any]]]:
    """
    Ищем последний день T < D, на который есть 5m данные по FO_R1_SECID.
    """
    print(
        f"[R1] Searching last trading day before {trade_date} "
        f"(SECID={FO_R1_SECID}, lookback {MAX_LOOKBACK_DAYS} days)"
    )
    for offset in range(1, MAX_LOOKBACK_DAYS + 1):
        cand = trade_date - timedelta(days=offset)
        rows = _load_fo_5m_for_day(cand)
        if rows:
            print(f"[R1] -> candidate {cand} ACCEPTED with {len(rows)} bars")
            return cand, rows
        else:
            print(f"[R1] -> candidate {cand} has no bars")
    return None, []


def get_trade_flag_for_date(trade_date: date) -> Tuple[bool, str]:
    """
    Возвращает:
      trade_today : bool
      regime_yday : "GOOD" или "BAD"
    """
    try:
        last_day, rows = _find_last_trading_day(trade_date)
        if last_day is None:
            print(f"[R1] No trading days found before {trade_date}")
            return False, "BAD"

        print(f"[R1] Last trading day before {trade_date}: {last_day}")

        dd_max = _simulate_ema_day_ddmax(rows)
        print(f"[R1] dd_max({last_day}) = {dd_max:.1f} pts")

        if dd_max <= DRAWDOWN_LIMIT_PTS:
            print(f"[R1] → GOOD (dd_max <= {DRAWDOWN_LIMIT_PTS})")
            return True, "GOOD"
        else:
            print(f"[R1] → BAD (dd_max > {DRAWDOWN_LIMIT_PTS})")
            return False, "BAD"

    except Exception as e:
        print(f"[R1] ERROR during R1 calculation: {e}")
        return False, "BAD"
