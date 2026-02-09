"""
Main runner/orchestrator for EMA(5,12) GOOD-days robot.

Responsibilities:
  - determine trading date (MSK),
  - apply R1 rule using regime_day_loader (trade today or not),
  - load and update state,
  - in GOOD days: iterate closed 5m bars, run signals + execution + logging
    with strict "next-bar" execution (no same-bar decisions).

This module does NOT know anything about MOEX API details. It relies on:
  - feed_ema_5_12.iter_new_bars(state) to provide Bar objects,
  - signals_ema_5_12.process_bar(state, bar) to produce signals,
  - executor_ema_5_12.execute_signal(state, bar_for_entry, signal) to
    model fills and update PnL,
  - logger_ema_5_12 to write trades to CSV.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional

from zoneinfo import ZoneInfo

from .config_ema_5_12 import MSK_TZ
from .regime_day_loader import get_trade_flag_for_date
from .state_ema_5_12 import load_state, save_state
from .feed_ema_5_12 import Bar, iter_new_bars
from .signals_ema_5_12 import process_bar
from .executor_ema_5_12 import execute_signal
from .logger_ema_5_12 import init_logger, log_trade, Logger


def _current_trade_date_msk() -> date:
    """
    Resolve current trading date as calendar date in MSK timezone.

    For now we use simple calendar date in Europe/Moscow. Exchange
    calendar (holidays, shortened sessions) can be added later.
    """
    tz = ZoneInfo(MSK_TZ)
    now_msk = datetime.now(tz)
    return now_msk.date()


def run_ema_5_12_loop(trade_date: Optional[date] = None) -> None:
    """
    Main loop for EMA(5,12) GOOD-days robot.

    Parameters
    ----------
    trade_date : datetime.date, optional
        Trading date D to run the robot for. If None, current MSK
        calendar date is used.

    Behaviour (high-level)
    ----------------------
    1. Determine trading date D.
    2. Apply R1 rule via get_trade_flag_for_date(D):
         - decide whether to trade today,
         - get regime for D-1 (GOOD/BAD).
    3. Load state for D and update:
         - trade_date
         - regime_yday
         - trade_today_flag
    4. If trade_today_flag == 0 -> exit (no EMA, no signals).
    5. If trade_today_flag == 1:
         - init CSV logger for D,
         - iterate over closed 5m bars from iter_new_bars(state),
         - for each bar:
             a) if there is pending signal from previous bar t:
                  execute it on current bar (t+1),
                  log trade and save state;
             b) process current bar to produce new signal,
                  update EMA / bars_count / last_bar_end,
                  save state;
             c) store new signal as pending for next bar.
    """
    # 1. Trading date
    if trade_date is None:
        trade_date = _current_trade_date_msk()

    # 2. R1 rule: trade only if yesterday's regime is GOOD
    trade_today, regime_yday = get_trade_flag_for_date(trade_date)

    # 3. Load and update state
    state: Dict[str, Any] = load_state(trade_date)
    state["trade_date"] = trade_date.isoformat()
    state["regime_yday"] = regime_yday
    state["trade_today_flag"] = 1 if trade_today else 0

    # Ensure last_signal has a safe value
    last_signal = str(state.get("last_signal") or "NONE")
    if last_signal not in ("NONE", "LONG", "SHORT"):
        state["last_signal"] = "NONE"
    else:
        state["last_signal"] = last_signal

    save_state(state)

    # Информативный вывод по дню (GOOD/BAD)
    if not trade_today:
        print(
            f"[EMA_5_12] {trade_date.isoformat()} DISABLED by R1 filter: "
            f"regime_yday={regime_yday}"
        )
        # Консервативно: не считаем EMA, не обрабатываем бары.
        return

    print(
        f"[EMA_5_12] {trade_date.isoformat()} ENABLED by R1 filter: "
        f"regime_yday={regime_yday}"
    )

    # 5. GOOD day: prepare logger and start main loop
    logger: Logger = init_logger(trade_date)

    # Pending signal from previous bar (t) to be executed on next bar (t+1)
    pending_signal: Optional[Dict[str, Any]] = None

    # Iterate over closed 5m bars strictly after state["last_bar_end"]
    for bar in iter_new_bars(state):
        # Step a) execute pending signal (if any) on current bar (t+1)
        if pending_signal is not None:
            state, trade = execute_signal(
                state=state,
                bar_for_entry=bar,
                signal=pending_signal,
            )
            if trade is not None:
                log_trade(logger, trade)
            save_state(state)

        # Step b) process current bar to produce new signal
        state, signal = process_bar(state, bar)

        # Enrich signal with metadata required by executor for logging
        signal["bar_end_signal"] = bar.end
        signal["price_signal_ref"] = float(bar.close)

        # Update last_bar_end in state for anti-cheat
        state["last_bar_end"] = bar.end.isoformat()

        save_state(state)

        # Step c) store signal as pending for next bar
        pending_signal = signal

    # End of bars: any pending signal cannot be executed without bar t+1.
    # We deliberately ignore it to preserve strict "next-bar" execution.
    return
