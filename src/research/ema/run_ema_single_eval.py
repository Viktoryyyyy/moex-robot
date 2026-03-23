"""CLI single-run EMA evaluator for research.

Builds one deterministic EMA evaluation run from explicit CLI inputs and writes
three output artifacts: bars, days, and segment summary.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

from src.research.ema import lib_ema_search

MODES = ("trend_long_short", "trend_long_only", "trend_short_only")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Single-run EMA evaluator (research-only)")
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--schema-json", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--timeframe", required=True)
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--fast", required=True, type=int)
    parser.add_argument("--slow", required=True, type=int)
    parser.add_argument("--commission-points", required=True, type=float)
    parser.add_argument("--near-zero-threshold", required=True, type=float)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _require_lib_function(name: str) -> Callable[..., Any]:
    fn = getattr(lib_ema_search, name, None)
    if fn is None or not callable(fn):
        raise RuntimeError(f"Required Step 2 function is missing in lib_ema_search.py: {name}")
    return fn


def _normalize_summary(summary: dict[str, Any]) -> dict[str, Any]:
    out = dict(summary)
    max_dd = float(out.get("max_dd", 0.0))
    out["max_dd"] = abs(max_dd)
    num_trades = out.get("num_trades", 0)
    out["num_trades"] = int(round(float(num_trades)))
    return out


def main() -> None:
    args = parse_args()

    schema = lib_ema_search.load_ohlc_schema(args.schema_json)
    source = lib_ema_search.load_source_ohlc_csv(args.input_csv, schema)
    bars = lib_ema_search.resample_ohlc(source, args.timeframe)

    generate_ema_signals = _require_lib_function("generate_ema_signals")
    run_point_backtest = _require_lib_function("run_point_backtest")
    summarize_by_day = _require_lib_function("summarize_by_day")
    summarize_segment = _require_lib_function("summarize_segment")

    bars = generate_ema_signals(bars, ema_fast_span=args.fast, ema_slow_span=args.slow, mode=args.mode)
    bars = run_point_backtest(bars, commission_points=args.commission_points)
    days = summarize_by_day(bars)

    summary = summarize_segment(days, near_zero_threshold=args.near_zero_threshold)
    summary = _normalize_summary(summary)
    summary.update({
        "instrument": args.instrument,
        "timeframe": args.timeframe,
        "mode": args.mode,
        "fast": int(args.fast),
        "slow": int(args.slow),
        "commission_points": float(args.commission_points),
        "near_zero_threshold": float(args.near_zero_threshold),
    })

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bars_path = out_dir / "ema_single_eval_bars.csv"
    days_path = out_dir / "ema_single_eval_days.csv"
    summary_path = out_dir / "ema_single_eval_summary.json"

    bars_export = bars[["ts", "open", "high", "low", "close", "ema_fast", "ema_slow", "signal", "position", "dclose", "trades", "fee", "pnl_bar"]].copy()
    bars_export.to_csv(bars_path, index=False)

    days_export = days[["date", "pnl_day", "num_trades_day", "cum_pnl_day", "dd_day"]].copy()
    days_export.to_csv(days_path, index=False)

    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"bars_csv={bars_path}")
    print(f"days_csv={days_path}")
    print(f"summary_json={summary_path}")


if __name__ == "__main__":
    main()
