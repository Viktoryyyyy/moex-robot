"""Rebuild the isolated future holdout ledger for the frozen research candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from research.usdrubf_multisession_sequence_20261110 import rework, study


def _candidate(payload: Mapping[str, Any]) -> rework.Variant:
    values = payload["frozen_candidate"]
    return rework.Variant(
        pattern=str(values["pattern"]),
        formation=int(values["formation"]),
        confirmation=str(values["confirmation"]),
        life=int(values["life"]),
        stop_buffer=float(values["stop_buffer"]),
        exit_mode=str(values["exit_mode"]),
        trail_atr=float(values["trail_atr"]),
        structure_lookback=int(values["structure_lookback"]),
    )


def _signal_id(trade: Mapping[str, Any]) -> str:
    raw = "|".join(
        str(trade[key])
        for key in ("variant", "side", "setup_date", "confirmation_ts", "entry_ts")
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def evaluate_gate(
    trades: Sequence[Mapping[str, Any]], gate: Mapping[str, Any]
) -> dict[str, Any]:
    value = rework.metrics(trades)
    enough_total = int(value["trades"]) >= int(gate["minimum_closed_trades"])
    enough_sides = (
        int(value["long_trades"]) >= int(gate["minimum_each_direction"])
        and int(value["short_trades"]) >= int(gate["minimum_each_direction"])
    )
    positive_without_best = float(value["without_best_r"]) > 0
    if enough_total and enough_sides and positive_without_best:
        status = "READY_FOR_USER_REVIEW"
    elif enough_total:
        status = "GATE_FAILED_CONTINUE_COLLECTING"
    else:
        status = "COLLECTING_NOT_ELIGIBLE"
    return {
        **dict(gate),
        "status": status,
        "observed_closed_trades": int(value["trades"]),
        "observed_long_trades": int(value["long_trades"]),
        "observed_short_trades": int(value["short_trades"]),
        "observed_net_r": float(value["net_r"]),
        "observed_without_best_r": float(value["without_best_r"]),
        "observed_max_drawdown_r": float(value["max_drawdown_r"]),
        "automatic_review_ready": status == "READY_FOR_USER_REVIEW",
        # Promotion always requires a separate user decision and code review.
        "promotion_allowed": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tradestats", action="append", required=True)
    parser.add_argument("--history", action="append", required=True)
    parser.add_argument("--gate", required=True)
    parser.add_argument("--output-ledger", required=True)
    parser.add_argument("--output-gate", required=True)
    args = parser.parse_args(argv)
    gate = json.loads(Path(args.gate).read_text(encoding="utf-8"))
    candidate = _candidate(gate)
    bars, daily = study.build_market([Path(value) for value in args.tradestats])
    funding = study.base.load_funding([Path(value) for value in args.history])
    setups = study.setup_catalog(daily, candidate.pattern, candidate.formation)
    trades, _signals, incomplete = rework.run_variant(
        bars,
        daily,
        funding,
        candidate,
        setups,
        simulation_start=str(gate["start"]),
        close_data_end=False,
    )
    ledger = pd.DataFrame([{**trade, "signal_id": _signal_id(trade)} for trade in trades])
    if not ledger.empty:
        ledger = ledger.sort_values(["entry_ts", "signal_id"]).drop_duplicates("signal_id")
    else:
        ledger = pd.DataFrame(
            columns=[
                "signal_id", "variant", "side", "formation_start_date", "setup_date",
                "confirmation_ts", "entry_ts", "entry_raw", "entry", "initial_stop",
                "exit_ts", "exit_raw", "exit", "exit_reason", "commission_points",
                "half_spread_points", "slippage_points", "funding_cost_points",
                "net_points", "net_r",
            ]
        )
    ledger_path = Path(args.output_ledger)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger.to_csv(ledger_path, index=False)
    updated = evaluate_gate(trades, gate)
    updated["open_positions_excluded"] = incomplete
    updated["ledger"] = str(ledger_path.name)
    Path(args.output_gate).write_text(
        json.dumps(rework._json_value(updated), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(rework._json_value(updated), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
