#!/usr/bin/env python3
import argparse
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from src.realtime.gate_preflight import preflight
from src.infra.trade_logger import append_trade_ema_5_12, ensure_ema_5_12_file
from src.infra.single_instance import acquire_lock, release_lock
from src.api.futures.fo_feed_intraday import load_fo_5m_day


SECID = "Si"


def main() -> None:
    # =============================
    # Infra pre-checks (FAIL-CLOSED)
    # =============================
    import os
    if not os.getenv("MOEX_API_KEY"):
        print("[CRIT] MOEX_API_KEY missing")
        raise SystemExit(2)

    # =============================
    # Gate preflight (FAIL-CLOSED)
    # =============================
    gate = preflight()

    if gate.risk == 1:
        print("[Gate] status=BLOCK reason=phase_transition_risk==1")
        raise SystemExit(2)

    # Import EMA modules only AFTER Gate PASS and risk==0
    from src.strategy.realtime.ema_5_12.config_ema_5_12 import (
        EMA_FAST_WINDOW,
        EMA_SLOW_WINDOW,
    )
    from src.strategy.realtime.ema_5_12.executor_ema_5_12 import execute_on_bar
    from src.strategy.realtime.ema_5_12.session_state import (
        load_session_state,
        save_session_state,
    )

    lock = acquire_lock("ema_5_12_realtime")
    try:
        ensure_ema_5_12_file()

        trade_date = date.today()

        session = load_session_state(trade_date)

        while True:
            
            try:
                bars = load_fo_5m_day(secid=SECID, trade_date=trade_date)
            except Exception as e:
                if "401" in str(e):
                    print("[CRIT] MOEX 401 Unauthorized")
                    raise SystemExit(2)
                raise



            if not bars:
                time.sleep(5)
                continue

            last_bar = bars[-1]

            signal = execute_on_bar(
                bar=last_bar,
                session=session,
                fast_window=EMA_FAST_WINDOW,
                slow_window=EMA_SLOW_WINDOW,
            )

            if signal is not None:
                append_trade_ema_5_12(signal)

            save_session_state(trade_date, session)

            time.sleep(5)

    finally:
        release_lock(lock)


if __name__ == "__main__":
    main()

