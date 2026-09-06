#!/usr/bin/env python3
import time
from datetime import date, datetime, timezone
from types import SimpleNamespace

from src.realtime.gate_preflight import preflight
from src.infra.trade_logger import append_trade_ema_5_12, ensure_ema_5_12_file


SECID = "Si"


def process_closed_bar(*, bar, state, now):
    """Advance one closed bar once; execute the previous signal before updating EMA.

    The caller must pass the daily gate before invoking this function.
    """
    from src.strategy.realtime.ema_5_12.executor_ema_5_12 import execute_on_bar
    from src.strategy.realtime.ema_5_12.signals_ema_5_12 import process_bar, SIGNAL_NO_TRADE

    end = bar["end"]
    if not isinstance(end, datetime):
        end = datetime.fromisoformat(str(end))
    if end.tzinfo is None or now.tzinfo is None:
        raise ValueError("closed bars and clock must be timezone-aware")
    if end > now or (state.last_bar_end is not None and end <= state.last_bar_end):
        return state, None
    last_label = "NONE"
    if state.ema_fast is not None and state.ema_slow is not None:
        last_label = "LONG" if state.ema_fast > state.ema_slow else "SHORT" if state.ema_fast < state.ema_slow else "NONE"
    state, trade = execute_on_bar(bar, state)
    updated, signal = process_bar({
        "trade_today_flag": 1, "bars_count": state.ema_bars_seen,
        "position": state.pos, "ema_fast": state.ema_fast,
        "ema_slow": state.ema_slow, "last_signal": last_label,
    }, SimpleNamespace(close=bar["close"]))
    state.ema_fast = updated["ema_fast"]
    state.ema_slow = updated["ema_slow"]
    state.ema_bars_seen = updated["bars_count"]
    if signal["type"] != SIGNAL_NO_TRADE:
        state.pending_target_pos = signal["target_pos"]
        state.pending_signal_bar_end = end
        state.pending_signal_price = float(bar["close"])
        state.pending_reason = signal["type"] + ":" + signal["cross_type"]
    return state, trade


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
    from src.infra.single_instance import acquire_lock, release_lock
    from src.strategy.realtime.ema_5_12.session_state import (
        load_session_state,
        save_session_state,
    )

    lock = acquire_lock("ema_5_12_realtime")
    try:
        trade_date = date.today()
        ensure_ema_5_12_file(trade_date)
        session = load_session_state(trade_date)

        while True:
            try:
                gate = preflight()
            except Exception as e:
                print("[Gate] status=BLOCK reason=" + str(e))
                raise SystemExit(2)
            if gate.risk == 1:
                print("[Gate] status=BLOCK reason=phase_transition_risk==1")
                raise SystemExit(2)

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

            now = datetime.now(timezone.utc)
            # Feed polls may contain an entire day, duplicates and a forming tail.
            def bar_end(bar):
                value = bar["end"]
                return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
            for bar in sorted(bars, key=bar_end):
                end = bar_end(bar)
                if end > now or (session.last_bar_end is not None and end <= session.last_bar_end):
                    continue
                session, trade = process_closed_bar(bar=bar, state=session, now=now)
                if trade is not None:
                    append_trade_ema_5_12(trade_date, trade)
                save_session_state(session)
            time.sleep(5)

    finally:
        release_lock(lock)


if __name__ == "__main__":
    main()
