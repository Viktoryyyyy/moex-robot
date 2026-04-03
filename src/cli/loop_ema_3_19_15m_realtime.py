import os
import signal
import time
from datetime import date, datetime, timezone

from src.api.futures.fo_feed_intraday import load_fo_5m_day
from src.applied.journal.ema_pilot_journal import append_pilot_day_status, append_pilot_journal
from src.infra.single_instance import acquire_lock, release_lock
from src.realtime.ema_d_day_context_preflight import preflight
from src.strategy.realtime.ema_3_19_15m.executor_ema_3_19_15m import execute_pending_target_on_closed_bar
from src.strategy.realtime.ema_3_19_15m.session_state_ema_3_19_15m import load_or_init_session_state, save_session_state
from src.strategy.realtime.ema_3_19_15m.signal_engine_ema_3_19_15m import update_signal_state_on_closed_bar
from src.strategy.realtime.ema_3_19_15m.trade_logger_ema_3_19_15m import append_execution_event


SECID = "Si"
EMA_FAST_WINDOW = 3
EMA_SLOW_WINDOW = 19
BAR_INTERVAL_SECONDS = 15 * 60
BRANCH_NAME = "EMA Applied Context Filter"
STRATEGY_ID = "ema_3_19_15m_block_adverse"
TIMEFRAME = "15m"


def _ts() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _event_row(trading_day: str, action: str, trade_status: str, source: str, note: str, block_reason: str, artifact_date: str, artifact_status: str, context_band: str, context_decision: str, context_source_trade_date: str) -> dict:
    return {
        "trading_day": trading_day,
        "event_time": _ts(),
        "branch_name": BRANCH_NAME,
        "strategy_id": STRATEGY_ID,
        "ticker": SECID,
        "timeframe": TIMEFRAME,
        "context_band": context_band,
        "context_decision": context_decision,
        "context_source_trade_date": context_source_trade_date,
        "signal_state": "",
        "action": action,
        "side": "",
        "price_in": "",
        "price_out": "",
        "pnl_points": "",
        "pnl_rub": "",
        "trade_status": trade_status,
        "block_reason": block_reason,
        "artifact_date": artifact_date,
        "artifact_status": artifact_status,
        "note": note,
        "source": source,
    }


def _day_row(trading_day: str, day_status: str, block_reason: str, note: str, artifact_date: str, artifact_status: str, context_band: str, context_decision: str, context_source_trade_date: str) -> dict:
    return {
        "trading_day": trading_day,
        "branch_name": BRANCH_NAME,
        "strategy_id": STRATEGY_ID,
        "ticker": SECID,
        "timeframe": TIMEFRAME,
        "artifact_date": artifact_date,
        "artifact_status": artifact_status,
        "context_band": context_band,
        "context_decision": context_decision,
        "source_trade_date": context_source_trade_date,
        "day_status": day_status,
        "block_reason": block_reason,
        "note": note,
        "generated_at": _ts(),
    }


def _ctx_fields(ctx) -> tuple[str, str, str, str]:
    return ctx.target_day, ctx.status, ctx.band, ctx.source_trade_date


def _write_block(trading_day: str, source: str, reason: str) -> None:
    append_pilot_journal(_event_row(trading_day, "BLOCK", "blocked", source, "runtime blocked", reason, "", "error", "", "", ""))
    append_pilot_day_status(_day_row(trading_day, "BLOCKED", reason, "runtime blocked", "", "error", "", "", ""))


def _filter_new_bars(bars: list[dict], last_bar_end: str | None) -> list[dict]:
    new_bars = []
    seen = set()
    for bar in bars:
        raw_end = bar.get("end")
        if isinstance(raw_end, datetime):
            bar_end = raw_end.isoformat()
        elif isinstance(raw_end, str) and raw_end.strip():
            datetime.fromisoformat(raw_end)
            bar_end = raw_end
        else:
            raise RuntimeError("invalid bar end in batch")
        if last_bar_end is not None and bar_end <= last_bar_end:
            continue
        if bar_end in seen:
            raise RuntimeError("duplicate bar_end inside one batch: " + bar_end)
        seen.add(bar_end)
        new_bars.append(bar)
    return new_bars


def main() -> int:
    trade_date = date.today()
    trade_date_str = trade_date.isoformat()

    if not os.getenv("MOEX_API_KEY"):
        reason = "MOEX_API_KEY missing"
        _write_block(trade_date_str, "realtime_loop", reason)
        print("[CRIT] " + reason)
        raise SystemExit(2)

    try:
        ctx = preflight()
    except Exception as e:
        reason = str(e)
        _write_block(trade_date_str, "realtime_loop", reason)
        print("[Context] status=BLOCK reason=" + reason)
        raise SystemExit(2)
    if not ctx.allowed:
        reason = "allowed!=true"
        _write_block(trade_date_str, "realtime_loop", reason)
        print("[Context] status=BLOCK reason=" + reason)
        raise SystemExit(2)

    lock = acquire_lock("ema_3_19_15m_realtime")
    exit_code = 0
    stop_requested = False

    def _request_stop(signum, frame):
        nonlocal stop_requested
        stop_requested = True
        print("[STOP] signal=" + str(signum))

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    try:
        artifact_date, artifact_status, context_band, context_source_trade_date = _ctx_fields(ctx)
        append_pilot_journal(_event_row(trade_date_str, "START", "running", "realtime_loop", "runtime active", "", artifact_date, artifact_status, context_band, ctx.decision, context_source_trade_date))
        append_pilot_day_status(_day_row(trade_date_str, "RUNNING", "", "runtime active", artifact_date, artifact_status, context_band, ctx.decision, context_source_trade_date))

        session = load_or_init_session_state(trade_date_str)

        while True:
            if stop_requested:
                break
            try:
                ctx = preflight()
            except Exception as e:
                reason = str(e)
                append_pilot_journal(_event_row(trade_date_str, "BLOCK", "blocked", "realtime_loop", "runtime blocked", reason, "", "error", "", "", ""))
                append_pilot_day_status(_day_row(trade_date_str, "BLOCKED", reason, "runtime blocked", "", "error", "", "", ""))
                print("[Context] status=BLOCK reason=" + reason)
                exit_code = 2
                break
            if not ctx.allowed:
                reason = "allowed!=true"
                append_pilot_journal(_event_row(trade_date_str, "BLOCK", "blocked", "realtime_loop", "runtime blocked", reason, "", "error", "", "", ""))
                append_pilot_day_status(_day_row(trade_date_str, "BLOCKED", reason, "runtime blocked", "", "error", "", "", ""))
                print("[Context] status=BLOCK reason=" + reason)
                exit_code = 2
                break

            bars = load_fo_5m_day(secid=SECID, trade_date=trade_date)
            if not isinstance(bars, list):
                raise RuntimeError("invalid bars payload")

            new_bars = _filter_new_bars(bars, session.last_bar_end)
            if not new_bars:
                time.sleep(5)
                continue

            new_bars.sort(key=lambda x: x["end"])

            for bar in new_bars:
                raw_end = bar["end"]
                bar_end = raw_end.isoformat() if isinstance(raw_end, datetime) else raw_end
                if session.last_bar_end is not None and bar_end <= session.last_bar_end:
                    raise RuntimeError("duplicate or non-increasing bar_end: " + str(bar_end))

                session = update_signal_state_on_closed_bar(session, bar)

                event = None
                if session.pending_target is not None:
                    if session.pending_signal_bar_end is None:
                        raise RuntimeError("pending_target without pending_signal_bar_end")
                    if bar_end > session.pending_signal_bar_end:
                        event = execute_pending_target_on_closed_bar(session, bar)

                if event is not None:
                    append_execution_event(event)

                session.last_bar_end = bar_end
                save_session_state(session)

                if stop_requested:
                    break

            if stop_requested:
                break

            time.sleep(5)
    except Exception:
        exit_code = 2
        raise
    finally:
        try:
            append_pilot_journal(_event_row(trade_date_str, "STOP", "stopped", "realtime_loop", "runtime stopped", "", "", "ok", "", "", ""))
        finally:
            release_lock(lock)
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
