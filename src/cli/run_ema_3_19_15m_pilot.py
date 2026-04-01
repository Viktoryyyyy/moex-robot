from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from src.applied.journal.ema_pilot_journal import append_pilot_day_status, append_pilot_journal
from src.cli.loop_ema_3_19_15m_realtime import main as loop_main
from src.realtime.ema_d_day_context_preflight import preflight


BRANCH_NAME = "EMA Applied Context Filter"
STRATEGY_ID = "ema_3_19_15m_block_adverse"
TICKER = "Si"
TIMEFRAME = "15m"
OUT_JSON = "data/state/ema_d_day_context_latest.json"
OUT_CSV = "data/state/ema_d_day_context_history.csv"
LOCK_PATH = Path("data/state/ema_3_19_15m_realtime.lock")


def _ts() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _event_row(trading_day: str, action: str, trade_status: str, source: str, note: str, block_reason: str, artifact_date: str, artifact_status: str, context_band: str, context_decision: str, context_source_trade_date: str) -> dict:
    return {
        "trading_day": trading_day,
        "event_time": _ts(),
        "branch_name": BRANCH_NAME,
        "strategy_id": STRATEGY_ID,
        "ticker": TICKER,
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
        "ticker": TICKER,
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master-path", required=True)
    ap.add_argument("--target-day", required=True)
    args = ap.parse_args()

    if not os.getenv("MOEX_API_KEY"):
        reason = "MOEX_API_KEY missing"
        append_pilot_journal(_event_row(args.target_day, "BLOCK", "blocked", "pilot_wrapper", "wrapper precheck failed", reason, "", "error", "", "", ""))
        append_pilot_day_status(_day_row(args.target_day, "BLOCKED", reason, "wrapper precheck failed", "", "error", "", "", ""))
        print("[CRIT] " + reason)
        return 2

    if LOCK_PATH.exists():
        reason = "lock conflict: " + str(LOCK_PATH)
        append_pilot_journal(_event_row(args.target_day, "BLOCK", "blocked", "pilot_wrapper", "wrapper precheck failed", reason, "", "error", "", "", ""))
        append_pilot_day_status(_day_row(args.target_day, "BLOCKED", reason, "wrapper precheck failed", "", "error", "", "", ""))
        print("[CRIT] " + reason)
        return 2

    cmd = [
        sys.executable,
        "-m",
        "src.cli.build_ema_d_day_context",
        "--master-path",
        args.master_path,
        "--target-day",
        args.target_day,
        "--out-json",
        OUT_JSON,
        "--out-csv",
        OUT_CSV,
    ]
    built = subprocess.run(cmd)
    if built.returncode != 0:
        reason = "context build failed"
        append_pilot_journal(_event_row(args.target_day, "BLOCK", "blocked", "pilot_wrapper", "builder failed", reason, args.target_day, "error", "", "", ""))
        append_pilot_day_status(_day_row(args.target_day, "BLOCKED", reason, "builder failed", args.target_day, "error", "", "", ""))
        return built.returncode

    try:
        ctx = preflight()
    except Exception as e:
        reason = str(e)
        append_pilot_journal(_event_row(args.target_day, "BLOCK", "blocked", "pilot_wrapper", "preflight failed", reason, args.target_day, "error", "", "", ""))
        append_pilot_day_status(_day_row(args.target_day, "BLOCKED", reason, "preflight failed", args.target_day, "error", "", "", ""))
        print("[CRIT] " + reason)
        return 2

    artifact_date, artifact_status, context_band, context_source_trade_date = _ctx_fields(ctx)
    append_pilot_journal(_event_row(args.target_day, "READY", "ready", "pilot_wrapper", "wrapper checks passed", "", artifact_date, artifact_status, context_band, ctx.decision, context_source_trade_date))
    append_pilot_day_status(_day_row(args.target_day, "READY", "", "wrapper checks passed", artifact_date, artifact_status, context_band, ctx.decision, context_source_trade_date))

    return loop_main()


if __name__ == "__main__":
    raise SystemExit(main())
