#!/usr/bin/env python3
import time
from dataclasses import dataclass
from datetime import date, datetime

from src.realtime.gate_preflight import preflight
from src.infra.trade_logger import append_trade_ema_5_12, ensure_ema_5_12_file
from src.infra.single_instance import acquire_lock, release_lock


SECID = "Si"


@dataclass(frozen=True)
class Bar:
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


def _to_dt(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _signal_state_from_session(session):
    if session.pos > 0:
        last_signal = "LONG"
    elif session.pos < 0:
        last_signal = "SHORT"
    else:
        last_signal = "NONE"
    return {
        "ema_fast": session.ema_fast,
        "ema_slow": session.ema_slow,
        "bars_count": session.ema_bars_seen,
        "position": session.pos,
        "last_signal": last_signal,
        "trade_today_flag": 1,
    }


def main() -> None:
    import os

    if not os.getenv("MOEX_API_KEY"):
        print("[CRIT] MOEX_API_KEY missing")
        raise SystemExit(2)

    try:
        gate = preflight()
    except Exception as e:
        print("[Gate] status=BLOCK reason=" + str(e))
        raise SystemExit(2)
    if gate.risk == 1:
        print("[Gate] status=BLOCK reason=phase_transition_risk==1")
        raise SystemExit(2)

    from src.api.futures.fo_feed_intraday import load_fo_5m_day
    from src.strategy.realtime.ema_5_12.executor_ema_5_12 import execute_on_bar
    from src.strategy.realtime.ema_5_12.signals_ema_5_12 import SIGNAL_NO_TRADE, process_bar
    from src.strategy.realtime.ema_5_12.session_state import load_session_state, save_session_state

    lock = acquire_lock("ema_5_12_realtime")
    try:
        trade_date = date.today()
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
            last_bar_end = _to_dt(last_bar["end"])

            if session.last_bar_end is not None and last_bar_end <= session.last_bar_end:
                time.sleep(5)
                continue

            session, signal = execute_on_bar(
                bar=last_bar,
                state=session,
            )

            signal_state = _signal_state_from_session(session)
            signal_bar = Bar(
                end=last_bar_end,
                open=float(last_bar["open"]),
                high=float(last_bar["high"]),
                low=float(last_bar["low"]),
                close=float(last_bar["close"]),
                volume=float(last_bar["volume"]),
            )
            signal_state, sig = process_bar(signal_state, signal_bar)

            session.ema_fast = signal_state["ema_fast"]
            session.ema_slow = signal_state["ema_slow"]
            session.ema_bars_seen = signal_state["bars_count"]

            print("[EMA] fast=%.2f slow=%.2f" % (session.ema_fast or 0, session.ema_slow or 0))

            if sig["type"] != SIGNAL_NO_TRADE:
                session.pending_target_pos = int(sig["target_pos"])
                session.pending_signal_bar_end = last_bar_end
                session.pending_signal_price = float(last_bar["close"])
                session.pending_reason = str(sig["type"]) + ":" + str(sig["cross_type"])

            if signal is not None:
                append_trade_ema_5_12(trade_date, signal)

            save_session_state(session)
            time.sleep(5)

    finally:
        release_lock(lock)


if __name__ == "__main__":
    main()
