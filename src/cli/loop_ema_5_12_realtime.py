#!/usr/bin/env python3
import time
from datetime import date

from src.realtime.gate_preflight import preflight
from src.infra.trade_logger import append_trade_ema_5_12, ensure_ema_5_12_file
from src.infra.single_instance import acquire_lock, release_lock


SECID = "Si"


def _bar_ts(bar):
    try:
        if isinstance(bar, dict):
            return bar.get("end") or bar.get("ts") or bar.get("datetime")
        return getattr(bar, "end", None) or getattr(bar, "ts", None) or getattr(bar, "datetime", None)
    except Exception:
        return None


def _bar_price(bar):
    try:
        if isinstance(bar, dict):
            return bar.get("close") or bar.get("CLOSE") or bar.get("price")
        return getattr(bar, "close", None) or getattr(bar, "CLOSE", None) or getattr(bar, "price", None)
    except Exception:
        return None


def _signal_side(signal):
    side = getattr(signal, "side_exec", None)
    if side == "BUY":
        return "LONG"
    if side == "SELL":
        return "SHORT"
    if side is None:
        return ""
    return str(side)


def main() -> None:
    # =============================
    # Infra pre-checks (FAIL-CLOSED)
    # =============================
    import os
    if not os.getenv("MOEX_API_KEY"):
        print("[CRIT] MOEX_API_KEY missing")
        raise SystemExit(2)

    shadow_mode = str(os.getenv("EMA_SHADOW_MODE", "0")).strip() == "1"

    # =============================
    # Gate preflight (FAIL-CLOSED)
    # =============================
    try:
        gate = preflight()
    except Exception as e:
        print("[Gate] status=BLOCK reason=" + str(e))
        raise SystemExit(2)
    if gate.risk == 1:
        print("[Gate] status=BLOCK reason=phase_transition_risk==1")
        raise SystemExit(2)

    # Import API + EMA only AFTER Gate PASS and risk==0
    from src.api.futures.fo_feed_intraday import load_fo_5m_day
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
        trade_date = date.today()
        if not shadow_mode:
            ensure_ema_5_12_file(trade_date)
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
            session, signal = execute_on_bar(
                bar=last_bar,
                state=session,
            )

            if signal is not None:
                if shadow_mode:
                    ts = _bar_ts(last_bar)
                    px = _bar_price(last_bar)
                    sig = _signal_side(signal)
                    print("[SHADOW] ts=" + str(ts) + " price=" + str(px) + " signal=" + str(sig))
                else:
                    append_trade_ema_5_12(trade_date, signal)

            save_session_state(session)
            time.sleep(5)

    finally:
        release_lock(lock)


if __name__ == "__main__":
    main()
