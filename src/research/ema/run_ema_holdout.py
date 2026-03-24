"""Research-only EMA final holdout month runner.

Selects EMA fast/slow pairs on a pre-holdout train segment and evaluates the
frozen selected pair on a final untouched holdout segment built from last N
full calendar months only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.research.ema import lib_ema_search

MODES = ("trend_long_short", "trend_long_only", "trend_short_only")
RANKING_RULE = "pnl_day_mean desc, win_rate desc, max_dd asc"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministic EMA holdout runner (research-only)")
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--schema-json", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--timeframe", required=True)
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--fast-min", required=True, type=int)
    parser.add_argument("--fast-max", required=True, type=int)
    parser.add_argument("--slow-min", required=True, type=int)
    parser.add_argument("--slow-max", required=True, type=int)
    parser.add_argument("--commission-points", required=True, type=float)
    parser.add_argument("--near-zero-threshold", required=True, type=float)
    parser.add_argument("--holdout-months", required=True, type=int)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _require_lib_function(name: str) -> Callable[..., Any]:
    fn = getattr(lib_ema_search, name, None)
    if fn is None or not callable(fn):
        raise RuntimeError("Required function is missing in lib_ema_search.py: " + name)
    return fn


def _validate_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(name + " must be > 0, got " + str(value))


def _validate_bounds(name: str, min_value: int, max_value: int) -> None:
    _validate_positive(name + "-min", min_value)
    _validate_positive(name + "-max", max_value)
    if min_value > max_value:
        raise ValueError(name + " bounds must satisfy min <= max, got min=" + str(min_value) + ", max=" + str(max_value))


def _build_pair_grid(fast_min: int, fast_max: int, slow_min: int, slow_max: int) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    for fast in range(fast_min, fast_max + 1):
        for slow in range(slow_min, slow_max + 1):
            if fast < slow:
                pairs.append((fast, slow))
    return pairs


def _normalize_summary(summary: dict[str, Any]) -> dict[str, Any]:
    out = dict(summary)
    out["pnl_day_mean"] = float(out.get("pnl_day_mean", 0.0))
    out["win_rate"] = float(out.get("win_rate", 0.0))
    out["near_zero_rate"] = float(out.get("near_zero_rate", 0.0))
    out["total_pnl"] = float(out.get("total_pnl", 0.0))
    out["num_days"] = int(round(float(out.get("num_days", 0))))
    out["num_trades"] = int(round(float(out.get("num_trades", 0))))
    out["max_dd"] = abs(float(out.get("max_dd", 0.0)))
    return out


def _month_labels(df: pd.DataFrame) -> list[pd.Period]:
    return list(pd.Index(df["ts"].dt.to_period("M").dropna().unique()).sort_values())


def _is_full_month(df: pd.DataFrame, month: pd.Period) -> bool:
    if df.empty:
        return False

    month_start = month.start_time
    next_month_start = (month + 1).start_time
    ts_min = df["ts"].min()
    ts_max = df["ts"].max()

    if getattr(ts_min, "tzinfo", None) is not None:
        month_start = month_start.tz_localize(ts_min.tzinfo)
        next_month_start = next_month_start.tz_localize(ts_min.tzinfo)

    has_coverage_start = ts_min <= month_start
    has_coverage_end = ts_max >= next_month_start
    return bool(has_coverage_start and has_coverage_end)


def _full_months(df: pd.DataFrame) -> list[str]:
    months = _month_labels(df)
    full: list[str] = []
    for month in months:
        if _is_full_month(df, month):
            full.append(str(month))
    return full


def _segment_from_months(df: pd.DataFrame, start_month: str, end_month: str) -> pd.DataFrame:
    month_key = df["ts"].dt.to_period("M").astype(str)
    mask = (month_key >= start_month) & (month_key <= end_month)
    return df.loc[mask].copy(deep=True)


def _rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: (-float(r["pnl_day_mean"]), -float(r["win_rate"]), float(r["max_dd"])))


def main() -> None:
    args = parse_args()

    _validate_bounds("fast", args.fast_min, args.fast_max)
    _validate_bounds("slow", args.slow_min, args.slow_max)
    _validate_positive("holdout-months", args.holdout_months)

    pairs = _build_pair_grid(args.fast_min, args.fast_max, args.slow_min, args.slow_max)
    if not pairs:
        raise ValueError("No valid EMA pairs found in range. Requirement is fast > 0, slow > 0, fast < slow. Got fast=[" + str(args.fast_min) + "," + str(args.fast_max) + "], slow=[" + str(args.slow_min) + "," + str(args.slow_max) + "]")

    schema = lib_ema_search.load_ohlc_schema(args.schema_json)
    source = lib_ema_search.load_source_ohlc_csv(args.input_csv, schema)
    base_bars = lib_ema_search.resample_ohlc(source, args.timeframe)

    full_months = _full_months(base_bars)
    if len(full_months) < args.holdout_months + 1:
        raise ValueError("Not enough full calendar months for train plus requested holdout. available_full_months=" + str(len(full_months)) + ", requested_holdout_months=" + str(args.holdout_months))

    holdout_months = full_months[-args.holdout_months :]
    train_months = full_months[: -args.holdout_months]
    if not train_months:
        raise ValueError("Train segment is empty after holdout split")

    train_start_month = train_months[0]
    train_end_month = train_months[-1]
    holdout_start_month = holdout_months[0]
    holdout_end_month = holdout_months[-1]

    train_bars = _segment_from_months(base_bars, train_start_month, train_end_month)
    holdout_bars = _segment_from_months(base_bars, holdout_start_month, holdout_end_month)

    generate_ema_signals = _require_lib_function("generate_ema_signals")
    run_point_backtest = _require_lib_function("run_point_backtest")
    summarize_by_day = _require_lib_function("summarize_by_day")
    summarize_segment = _require_lib_function("summarize_segment")

    selection_rows: list[dict[str, Any]] = []
    for fast, slow in pairs:
        bars = generate_ema_signals(train_bars.copy(deep=True), ema_fast_span=fast, ema_slow_span=slow, mode=args.mode)
        bars = run_point_backtest(bars, commission_points=args.commission_points)
        days = summarize_by_day(bars)
        summary = summarize_segment(days, near_zero_threshold=args.near_zero_threshold)
        metrics = _normalize_summary(summary)

        selection_rows.append(
            {
                "selection_start_month": train_start_month,
                "selection_end_month": train_end_month,
                "segment_role": "selection_train",
                "instrument": args.instrument,
                "timeframe": args.timeframe,
                "mode": args.mode,
                "fast": int(fast),
                "slow": int(slow),
                "commission_points": float(args.commission_points),
                "near_zero_threshold": float(args.near_zero_threshold),
                "pnl_day_mean": metrics["pnl_day_mean"],
                "win_rate": metrics["win_rate"],
                "near_zero_rate": metrics["near_zero_rate"],
                "total_pnl": metrics["total_pnl"],
                "num_days": metrics["num_days"],
                "num_trades": metrics["num_trades"],
                "max_dd": metrics["max_dd"],
            }
        )

    ranked = _rank_rows(selection_rows)
    best = ranked[0]
    best_fast = int(best["fast"])
    best_slow = int(best["slow"])

    holdout_eval_bars = generate_ema_signals(holdout_bars.copy(deep=True), ema_fast_span=best_fast, ema_slow_span=best_slow, mode=args.mode)
    holdout_eval_bars = run_point_backtest(holdout_eval_bars, commission_points=args.commission_points)
    holdout_days = summarize_by_day(holdout_eval_bars)
    holdout_summary = summarize_segment(holdout_days, near_zero_threshold=args.near_zero_threshold)
    holdout_metrics = _normalize_summary(holdout_summary)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results_path = out_dir / "ema_holdout_train_selection_results.csv"
    best_path = out_dir / "ema_holdout_best.json"

    pd.DataFrame(ranked).to_csv(results_path, index=False)

    payload = {
        "selection_start_month": train_start_month,
        "selection_end_month": train_end_month,
        "holdout_start_month": holdout_start_month,
        "holdout_end_month": holdout_end_month,
        "instrument": args.instrument,
        "timeframe": args.timeframe,
        "mode": args.mode,
        "fast_min": int(args.fast_min),
        "fast_max": int(args.fast_max),
        "slow_min": int(args.slow_min),
        "slow_max": int(args.slow_max),
        "commission_points": float(args.commission_points),
        "near_zero_threshold": float(args.near_zero_threshold),
        "selection_rule": RANKING_RULE,
        "selected_pair": {"fast": best_fast, "slow": best_slow},
        "train_metrics_for_selected_pair": {
            "pnl_day_mean": float(best["pnl_day_mean"]),
            "win_rate": float(best["win_rate"]),
            "near_zero_rate": float(best["near_zero_rate"]),
            "total_pnl": float(best["total_pnl"]),
            "num_days": int(best["num_days"]),
            "num_trades": int(best["num_trades"]),
            "max_dd": float(best["max_dd"]),
        },
        "holdout_metrics_for_selected_pair": {
            "pnl_day_mean": float(holdout_metrics["pnl_day_mean"]),
            "win_rate": float(holdout_metrics["win_rate"]),
            "near_zero_rate": float(holdout_metrics["near_zero_rate"]),
            "total_pnl": float(holdout_metrics["total_pnl"]),
            "num_days": int(holdout_metrics["num_days"]),
            "num_trades": int(holdout_metrics["num_trades"]),
            "max_dd": float(holdout_metrics["max_dd"]),
        },
    }
    best_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("selection_start_month=" + train_start_month)
    print("selection_end_month=" + train_end_month)
    print("holdout_start_month=" + holdout_start_month)
    print("holdout_end_month=" + holdout_end_month)
    print("pairs_ranked_on_train=" + str(len(ranked)))
    print("selected_pair_fast=" + str(best_fast))
    print("selected_pair_slow=" + str(best_slow))
    print("results_csv=" + str(results_path))
    print("best_json=" + str(best_path))


if __name__ == "__main__":
    main()
