import os
import time
from datetime import date, datetime

from src.applied.journal.ema_pilot_journal import append_pilot_day_status, append_pilot_journal
from src.infra.single_instance import acquire_lock, release_lock
from src.realtime.ema_d_day_context_preflight import preflight


SECID = "Si"
EMA_FAST_WINDOW = 3
EMA_SLOW_WINDOW = 19
BAR_INTERVAL_SECONDS = 15 * 60
BRANCH_NAME = "EMA Applied Context Filter"
STRATEGY_ID = "ema_3_19_15m_block_adverse"
TIMEFRAME = "15m"


def _ts() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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


def main() -> int:
    trade_date = date.today().isoformat()

    if not os.getenv("MOEX_API_KEY"):
        reason = "MOEX_API_KEY missing"
        _write_block(trade_date, "realtime_loop", reason)
        print("[CRIT] " + reason)
        raise SystemExit(2)

    try:
        ctx = preflight()
    except Exception as e:
        reason = str(e)
        _write_block(trade_date, "realtime_loop", reason)
        print("[Context] status=BLOCK reason=" + reason)
        raise SystemExit(2)
    if not ctx.allowed:
        reason = "allowed!=true"
        _write_block(trade_date, "realtime_loop", reason)
        print("[Context] status=BLOCK reason=" + reason)
        raise SystemExit(2)

    lock = acquire_lock("ema_3_19_15m_realtime")
    try:
        artifact_date, artifact_status, context_band, context_source_trade_date = _ctx_fields(ctx)
        append_pilot_journal(_event_row(trade_date, "START", "running", "realtime_loop", "runtime stub active", "", artifact_date, artifact_status, context_band, ctx.decision, context_source_trade_date))
        append_pilot_day_status(_day_row(trade_date, "RUNNING", "", "runtime stub active", artifact_date, artifact_status, context_band, ctx.decision, context_source_trade_date))
        print("[EMA_3_19_15M] status=ALLOW target_day=" + ctx.target_day + " source_trade_date=" + ctx.source_trade_date + " band=" + ctx.band)
        print("[EMA_3_19_15M] runtime stub active")
        print("[EMA_3_19_15M] secid=" + SECID + " ema_fast=" + str(EMA_FAST_WINDOW) + " ema_slow=" + str(EMA_SLOW_WINDOW) + " bar_interval_seconds=" + str(BAR_INTERVAL_SECONDS) + " trade_date=" + trade_date)
        while True:
            try:
                ctx = preflight()
            except Exception as e:
                reason = str(e)
                append_pilot_journal(_event_row(trade_date, "BLOCK", "blocked", "realtime_loop", "runtime blocked", reason, "", "error", "", "", ""))
                append_pilot_day_status(_day_row(trade_date, "BLOCKED", reason, "runtime blocked", "", "error", "", "", ""))
                print("[Context] status=BLOCK reason=" + reason)
                raise SystemExit(2)
            if not ctx.allowed:
                reason = "allowed!=true"
                append_pilot_journal(_event_row(trade_date, "BLOCK", "blocked", "realtime_loop", "runtime blocked", reason, "", "error", "", "", ""))
                append_pilot_day_status(_day_row(trade_date, "BLOCKED", reason, "runtime blocked", "", "error", "", "", ""))
                print("[Context] status=BLOCK reason=" + reason)
                raise SystemExit(2)
            time.sleep(5)
    finally:
        append_pilot_journal(_event_row(trade_date, "STOP", "stopped", "realtime_loop", "runtime stopped", "", "", "ok", "", "", ""))
        release_lock(lock)


if __name__ == "__main__":
    raise SystemExit(main())
