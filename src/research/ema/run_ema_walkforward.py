"""Research-only EMA walk-forward month-split runner.

Builds deterministic rolling train/validation windows using whole calendar
months and evaluates EMA fast/slow grids per window for one
dataset/timeframe/mode/instrument configuration.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.research.ema import lib_ema_search

MODES = ("trend_long_short", "trend_long_only", "trend_short_only")
SELECTION_RULE = "train pnl_day_mean desc, train win_rate desc, train max_dd asc"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministic EMA walk-forward month split (research-only)")
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
    parser.add_argument("--train-months", required=True, type=int)
    parser.add_argument("--valid-months", required=True, type=int)
    parser.add_argument("--step-months", required=True, type=int)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _require_lib_function(name: str) -> Callable[..., Any]:
    fn = getattr(lib_ema_search, name, None)
    if fn is None or not callable(fn):
        raise RuntimeError(f"Required Step 2 function is missing in lib_ema_search.py: {name}")
    return fn


def _validate_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def _validate_bounds(name: str, min_value: int, max_value: int) -> None:
    _validate_positive(f"{name}-min", min_value)
    _validate_positive(f"{name}-max", max_value)
    if min_value > max_value:
        raise ValueError(f"{name} bounds must satisfy min <= max, got min={min_value}, max={max_value}")


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
    months = df["ts"].dt.to_period("M")
    return list(pd.Index(months.dropna().unique()).sort_values())


def _is_full_month(df: pd.DataFrame, month: pd.Period) -> bool:
    month_mask = df["ts"].dt.to_period("M") == month
    month_bars = df.loc[month_mask]
    if month_bars.empty:
        return False

    coverage_min = df["ts"].min()
    coverage_max = df["ts"].max()
    month_start = month.start_time
    next_month_start = (month + 1).start_time

    if coverage_min.tzinfo is not None and month_start.tzinfo is None:
        month_start = month_start.tz_localize(coverage_min.tzinfo)
        next_month_start = next_month_start.tz_localize(coverage_min.tzinfo)

    has_coverage_start = coverage_min <= month_start
    has_coverage_end = coverage_max >= next_month_start
    return bool(has_coverage_start and has_coverage_end)


def _full_months(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    full: list[str] = []
    incomplete: list[str] = []
    for month in _month_labels(df):
        month_str = str(month)
        if _is_full_month(df, month):
            full.append(month_str)
        else:
            incomplete.append(month_str)
    return full, incomplete


def _build_windows(months: list[str], train_months: int, valid_months: int, step_months: int) -> list[dict[str, str]]:
    windows: list[dict[str, str]] = []
    total = train_months + valid_months
    start = 0
    window_id = 1
    while start + total <= len(months):
        train_slice = months[start : start + train_months]
        valid_slice = months[start + train_months : start + total]
        windows.append({
            "window_id": f"w{window_id:03d}",
            "train_start_month": train_slice[0],
            "train_end_month": train_slice[-1],
            "valid_start_month": valid_slice[0],
            "valid_end_month": valid_slice[-1],
        })
        window_id += 1
        start += step_months
    return windows


def _segment_from_months(df: pd.DataFrame, start_month: str, end_month: str) -> pd.DataFrame:
    month_key = df["ts"].dt.to_period("M").astype(str)
    mask = (month_key >= start_month) & (month_key <= end_month)
    return df.loc[mask].copy(deep=True)


def _evaluate_segment(bars: pd.DataFrame, pairs: list[tuple[int, int]], mode: str, commission_points: float, near_zero_threshold: float, generate_ema_signals: Callable[..., Any], run_point_backtest: Callable[..., Any], summarize_by_day: Callable[..., Any], summarize_segment: Callable[..., Any]) -> dict[tuple[int, int], dict[str, Any]]:
    out: dict[tuple[int, int], dict[str, Any]] = {}
    for fast, slow in pairs:
        seg = generate_ema_signals(bars.copy(deep=True), ema_fast_span=fast, ema_slow_span=slow, mode=mode)
        seg = run_point_backtest(seg, commission_points=commission_points)
        days = summarize_by_day(seg)
        summary = summarize_segment(days, near_zero_threshold=near_zero_threshold)
        out[(fast, slow)] = _normalize_summary(summary)
    return out


def _rank_train_rows(train_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(train_rows, key=lambda r: (-float(r["pnl_day_mean"]), -float(r["win_rate"]), float(r["max_dd"])))


def main() -> None:
    args = parse_args()

    _validate_bounds("fast", args.fast_min, args.fast_max)
    _validate_bounds("slow", args.slow_min, args.slow_max)
    _validate_positive("train-months", args.train_months)
    _validate_positive("valid-months", args.valid_months)
    _validate_positive("step-months", args.step_months)

    pairs = _build_pair_grid(args.fast_min, args.fast_max, args.slow_min, args.slow_max)
    if not pairs:
        raise ValueError("No valid EMA pairs found in range. Requirement is fast > 0, slow > 0, fast < slow. " f"Got fast=[{args.fast_min},{args.fast_max}], slow=[{args.slow_min},{args.slow_max}]")

    schema = lib_ema_search.load_ohlc_schema(args.schema_json)
    source = lib_ema_search.load_source_ohlc_csv(args.input_csv, schema)
    base_bars = lib_ema_search.resample_ohlc(source, args.timeframe)

    full_months, incomplete_months = _full_months(base_bars)
    windows = _build_windows(full_months, args.train_months, args.valid_months, args.step_months)
    if not windows:
        raise ValueError(
            "No strict full-month walk-forward windows can be built. "
            + "available_full_months=" + str(len(full_months)) + ", "
            + "excluded_incomplete_months=" + str(len(incomplete_months)) + ", "
            + "train_months=" + str(args.train_months) + ", "
            + "valid_months=" + str(args.valid_months) + ", "
            + "step_months=" + str(args.step_months)
        )

    generate_ema_signals = _require_lib_function("generate_ema_signals")
    run_point_backtest = _require_lib_function("run_point_backtest")
    summarize_by_day = _require_lib_function("summarize_by_day")
    summarize_segment = _require_lib_function("summarize_segment")

    all_rows: list[dict[str, Any]] = []
    best_rows: list[dict[str, Any]] = []

    for window in windows:
        train_bars = _segment_from_months(base_bars, start_month=window["train_start_month"], end_month=window["train_end_month"])
        valid_bars = _segment_from_months(base_bars, start_month=window["valid_start_month"], end_month=window["valid_end_month"])

        train_metrics = _evaluate_segment(bars=train_bars, pairs=pairs, mode=args.mode, commission_points=args.commission_points, near_zero_threshold=args.near_zero_threshold, generate_ema_signals=generate_ema_signals, run_point_backtest=run_point_backtest, summarize_by_day=summarize_by_day, summarize_segment=summarize_segment)
        valid_metrics = _evaluate_segment(bars=valid_bars, pairs=pairs, mode=args.mode, commission_points=args.commission_points, near_zero_threshold=args.near_zero_threshold, generate_ema_signals=generate_ema_signals, run_point_backtest=run_point_backtest, summarize_by_day=summarize_by_day, summarize_segment=summarize_segment)

        train_rows_for_rank: list[dict[str, Any]] = []
        for fast, slow in pairs:
            common = {
                "window_id": window["window_id"],
                "train_start_month": window["train_start_month"],
                "train_end_month": window["train_end_month"],
                "valid_start_month": window["valid_start_month"],
                "valid_end_month": window["valid_end_month"],
                "instrument": args.instrument,
                "timeframe": args.timeframe,
                "mode": args.mode,
                "fast": int(fast),
                "slow": int(slow),
                "commission_points": float(args.commission_points),
                "near_zero_threshold": float(args.near_zero_threshold),
            }

            train_row = {**common, "split_role": "train", **train_metrics[(fast, slow)]}
            valid_row = {**common, "split_role": "valid", **valid_metrics[(fast, slow)]}

            all_rows.append(train_row)
            all_rows.append(valid_row)
            train_rows_for_rank.append(train_row)

        ranked = _rank_train_rows(train_rows_for_rank)
        best = ranked[0]
        best_key = (int(best["fast"]), int(best["slow"]))
        best_valid = valid_metrics[best_key]

        best_rows.append({
            "window_id": window["window_id"],
            "train_start_month": window["train_start_month"],
            "train_end_month": window["train_end_month"],
            "valid_start_month": window["valid_start_month"],
            "valid_end_month": window["valid_end_month"],
            "instrument": args.instrument,
            "timeframe": args.timeframe,
            "mode": args.mode,
            "best_fast": int(best["fast"]),
            "best_slow": int(best["slow"]),
            "train_pnl_day_mean": float(best["pnl_day_mean"]),
            "train_win_rate": float(best["win_rate"]),
            "train_max_dd": float(best["max_dd"]),
            "valid_pnl_day_mean": float(best_valid["pnl_day_mean"]),
            "valid_win_rate": float(best_valid["win_rate"]),
            "valid_near_zero_rate": float(best_valid["near_zero_rate"]),
            "valid_total_pnl": float(best_valid["total_pnl"]),
            "valid_num_days": int(best_valid["num_days"]),
            "valid_num_trades": int(best_valid["num_trades"]),
            "valid_max_dd": float(best_valid["max_dd"]),
        })

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results_path = out_dir / "ema_walkforward_results.csv"
    best_by_window_path = out_dir / "ema_walkforward_best_by_window.csv"
    summary_path = out_dir / "ema_walkforward_summary.json"

    pd.DataFrame(all_rows).to_csv(results_path, index=False)
    pd.DataFrame(best_rows).to_csv(best_by_window_path, index=False)

    summary_payload = {
        "instrument": args.instrument,
        "timeframe": args.timeframe,
        "mode": args.mode,
        "fast_min": int(args.fast_min),
        "fast_max": int(args.fast_max),
        "slow_min": int(args.slow_min),
        "slow_max": int(args.slow_max),
        "commission_points": float(args.commission_points),
        "near_zero_threshold": float(args.near_zero_threshold),
        "train_months": int(args.train_months),
        "valid_months": int(args.valid_months),
        "step_months": int(args.step_months),
        "strict_full_months_enforced": True,
        "available_full_months": int(len(full_months)),
        "excluded_incomplete_months": [str(m) for m in incomplete_months],
        "windows_built": int(len(windows)),
        "pairs_per_window": int(len(pairs)),
        "selection_rule": SELECTION_RULE,
        "artifacts": {
            "ema_walkforward_results_csv": str(results_path),
            "ema_walkforward_best_by_window_csv": str(best_by_window_path),
            "ema_walkforward_summary_json": str(summary_path),
        },
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"windows_built={len(windows)}")
    print(f"pairs_per_window={len(pairs)}")
    print(f"results_csv={results_path}")
    print(f"best_by_window_csv={best_by_window_path}")
    print(f"summary_json={summary_path}")


if __name__ == "__main__":
    main()
