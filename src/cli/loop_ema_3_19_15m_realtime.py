import math
import os
import signal
import time
from datetime import date, datetime, timedelta, timezone

from src.api.futures.fo_feed_intraday import load_fo_5m_day
from src.applied.journal.ema_pilot_journal import append_pilot_day_status, append_pilot_journal
from src.infra.single_instance import acquire_lock, release_lock
from src.realtime.ema_d_day_context_preflight import preflight
from src.strategy.realtime.ema_3_19_15m.executor_ema_3_19_15m import execute_pending_target_on_closed_bar
from src.strategy.realtime.ema_3_19_15m.session_state_ema_3_19_15m import load_or_init_session_state, save_session_state
from src.strategy.realtime.ema_3_19_15m.signal_engine_ema_3_19_15m import update_signal_state_on_closed_bar
from src.strategy.realtime.ema_3_19_15m.trade_logger_ema_3_19_15m import append_execution_event


SECID = "Si"
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


def _bar_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        return datetime.fromisoformat(value)
    raise RuntimeError("invalid bar end")


def _bar_num(bar: dict, key: str, allow_zero: bool = False) -> float:
    raw = bar.get(key)
    if not isinstance(raw, (int, float)):
        raise RuntimeError("invalid bar field: " + key)
    value = float(raw)
    if not math.isfinite(value):
        raise RuntimeError("non-finite bar field: " + key)
    if allow_zero:
        if value < 0.0:
            raise RuntimeError("negative bar field: " + key)
    else:
        if value <= 0.0:
            raise RuntimeError("non-positive bar field: " + key)
    return value


def _normalize_bars(bars: list[dict]) -> list[dict]:
    normalized = []
    seen = set()
    for bar in bars:
        if not isinstance(bar, dict):
            raise RuntimeError("invalid bar payload item")
        end_dt = _bar_dt(bar.get("end"))
        end_iso = end_dt.isoformat()
        if end_iso in seen:
            raise RuntimeError("duplicate bar_end inside one batch: " + end_iso)
        seen.add(end_iso)
        normalized.append(
            {
                "end": end_dt,
                "open": _bar_num(bar, "open"),
                "high": _bar_num(bar, "high"),
                "low": _bar_num(bar, "low"),
                "close": _bar_num(bar, "close"),
                "volume": _bar_num(bar, "volume", allow_zero=True),
            }
        )
    normalized.sort(key=lambda x: x["end"])
    for i in range(1, len(normalized)):
        if normalized[i]["end"] <= normalized[i - 1]["end"]:
            raise RuntimeError("non-increasing bar_end sequence")
    return normalized


def _bucket_label_15m(dt: datetime) -> datetime:
    return dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)


def _build_closed_15m_bar(bars: list[dict], idx: int):
    if idx < 2:
        return None

    b0 = bars[idx - 2]
    b1 = bars[idx - 1]
    b2 = bars[idx]

    t0 = b0["end"]
    t1 = b1["end"]
    t2 = b2["end"]

    label = _bucket_label_15m(t2)
    slot0 = label
    slot1 = label + timedelta(minutes=5)
    slot2 = label + timedelta(minutes=10)

    if t2 != slot2:
        return None

    if t0 != slot0 or t1 != slot1:
        raise RuntimeError("broken 15m bucket aligned to broker label " + label.isoformat())

    high_val = max(b0["high"], b1["high"], b2["high"])
    low_val = min(b0["low"], b1["low"], b2["low"])
    if low_val <= 0.0 or high_val < low_val:
        raise RuntimeError("invalid synthetic 15m range at " + label.isoformat())

    synthetic_bar = {
        "end": label.isoformat(),
        "open": b0["open"],
        "high": high_val,
        "low": low_val,
        "close": b2["close"],
        "volume": b0["volume"] + b1["volume"] + b2["volume"],
    }

    execution_gate_bar_end = t2.isoformat()
    return synthetic_bar, execution_gate_bar_end


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

            full_bars = _normalize_bars(bars)
            processed_any = False

            for idx, raw_bar in enumerate(full_bars):
                raw_bar_end = raw_bar["end"].isoformat()
                if session.last_bar_end is not None and raw_bar_end <= session.last_bar_end:
                    continue

                processed_any = True

                built = _build_closed_15m_bar(full_bars, idx)
                if built is not None:
                    synthetic_bar, execution_gate_bar_end = built
                    prev_pending_target = session.pending_target
                    prev_pending_signal_bar_end = session.pending_signal_bar_end

                    session = update_signal_state_on_closed_bar(session, synthetic_bar)

                    if prev_pending_target is None and session.pending_target is not None:
                        if session.pending_signal_bar_end != synthetic_bar["end"]:
                            raise RuntimeError("unexpected pending_signal_bar_end after 15m signal update")
                        if prev_pending_signal_bar_end is not None and prev_pending_signal_bar_end == session.pending_signal_bar_end:
                            raise RuntimeError("stale pending_signal_bar_end on new signal")
                        session.pending_signal_bar_end = execution_gate_bar_end

                event = None
                if session.pending_target is not None:
                    if session.pending_signal_bar_end is None:
                        raise RuntimeError("pending_target without pending_signal_bar_end")
                    if raw_bar_end > session.pending_signal_bar_end:
                        event = execute_pending_target_on_closed_bar(session, raw_bar)

                session.last_bar_end = raw_bar_end
                save_session_state(session)

                if event is not None:
                    append_execution_event(event)

                if stop_requested:
                    break

            if stop_requested:
                break

            if not processed_any:
                time.sleep(5)
                continue

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
