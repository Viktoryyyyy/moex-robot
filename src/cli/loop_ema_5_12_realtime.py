#!/usr/bin/env python3
import argparse
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from src.strategy.realtime.ema_5_12.config_ema_5_12 import (
    EMA_FAST_WINDOW,
    EMA_SLOW_WINDOW,
)
from src.strategy.realtime.ema_5_12.executor_ema_5_12 import execute_on_bar
from src.strategy.realtime.ema_5_12.session_state import (
    load_session_state,
    save_session_state,
)
from src.infra.trade_logger import append_trade_ema_5_12, ensure_ema_5_12_file
from src.infra.single_instance import acquire_lock, release_lock
from src.api.futures.fo_feed_intraday import load_fo_5m_day


# ===== CONFIG =====
SECID = "Si"
PHASE_TRANSITION_RISK_CSV = Path("data") / "research" / "phase_transition_risk.csv"
# ==================


def load_phase_transition_risk(trade_date: date) -> int:
    if not PHASE_TRANSITION_RISK_CSV.exists():
        raise FileNotFoundError(f"Missing {PHASE_TRANSITION_RISK_CSV}")

    target = trade_date.isoformat()
    with open(PHASE_TRANSITION_RISK_CSV, "r", encoding="utf-8") as f:
        next(f, None)  # header
        for line in f:
            d, v = line.strip().split(",")
            if d == target:
                if v not in ("0", "1"):
                    raise ValueError(f"Bad PhaseTransitionRisk value: {v}")
                return int(v)

    raise KeyError(f"PhaseTransitionRisk not found for {target}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", type=str, default=None)
    p.add_argument("--sleep-sec", type=float, default=2.0)
    return p.parse_args()


def _resolve_trade_date(date_arg: Optional[str]) -> date:
    if date_arg:
        return datetime.strptime(date_arg, "%Y-%m-%d").date()
    return datetime.now().date()


def main() -> None:
    args = _parse_args()
    trade_date = _resolve_trade_date(args.date)
    sleep_sec = max(1.0, float(args.sleep_sec))

    print(f"[EMA_5_12_LOOP] Trade date: {trade_date}")

    lock_name = f"ema_5_12_{trade_date.isoformat()}"
    lock_path: Optional[Path] = None

    try:
        lock_path = acquire_lock(lock_name)

        # ===== Phase Transition Gate (READ ONCE) =====
        risk = load_phase_transition_risk(trade_date)
        if risk == 0:
            print(f"[EMA_5_12_LOOP] PhaseTransitionRisk({trade_date})=0 -> EMA DISABLED")
            ensure_ema_5_12_file(trade_date)
            return

        print(f"[EMA_5_12_LOOP] PhaseTransitionRisk({trade_date})=1 -> EMA ENABLED")
        # ============================================

        ensure_ema_5_12_file(trade_date)
        state = load_session_state(trade_date)

        print(
            f"[EMA_5_12_LOOP] Loaded session state: "
            f"pos={state.pos}, pnl_cum={state.pnl_cum}, last_bar_end={state.last_bar_end}"
        )

        while True:
            bars = load_fo_5m_day(SECID, trade_date)
            if not bars:
                time.sleep(sleep_sec)
                continue

            for bar in bars:
                if state.last_bar_end and bar["end"] <= state.last_bar_end:
                    continue

                trade = execute_on_bar(
                    bar=bar,
                    state=state,
                    ema_fast_window=EMA_FAST_WINDOW,
                    ema_slow_window=EMA_SLOW_WINDOW,
                )

                if trade is not None:
                    append_trade_ema_5_12(trade_date, trade)

                state.last_bar_end = bar["end"]
                save_session_state(trade_date, state)

            time.sleep(sleep_sec)

    finally:
        if lock_path is not None:
            release_lock(lock_path)


if __name__ == "__main__":
    main()
