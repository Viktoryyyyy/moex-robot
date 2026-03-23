"""Research-only EMA multi-timeframe final holdout runner.

Evaluates EMA fast/slow pairs on final untouched holdout segments built from the
last N full calendar months for each requested timeframe, then produces a
consolidated cross-timeframe comparison.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.research.ema import lib_ema_search

MODES = ("trend_long_short", "trend_long_only", "trend_short_only")
ALLOWED_TIMEFRAMES = ("5m", "30m", "1h", "4h", "1d")
RANKING_RULE = "pnl_day_mean desc, win_rate desc, max_dd asc"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministic EMA multi-timeframe holdout runner (research-only)"
    )
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--schema-json", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--timeframes", required=True)
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
        raise RuntimeError(f"Required function is missing in lib_ema_search.py: {name}")
    return fn


def _validate_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def _validate_bounds(name: str, min_value: int, max_value: int) -> None:
    _validate_positive(f"{name}-min", min_value)
    _validate_positive(f"{name}-max", max_value)
    if min_value > max_value:
        raise ValueError(f"{name} bounds must satisfy min <= max, got min={min_value}, max={max_value}")


def _parse_timeframes(raw: str) -> list[str]:
    parts = [item.strip().lower() for item in raw.split(",") if item.strip()]
    if not parts:
        raise ValueError("timeframes must contain at least one value from: " + ", ".join(ALLOWED_TIMEFRAMES))

    invalid = sorted({item for item in parts if item not in ALLOWED_TIMEFRAMES})
    if invalid:
        raise ValueError(
            "Unsupported timeframe(s): "
            + str(invalid)
            + ". Allowed: "
            + str(list(ALLOWED_TIMEFRAMES))
        )

    seen: set[str] = set()
    normalized: list[str] = []
    duplicates: list[str] = []
    for item in parts:
        if item in seen:
            duplicates.append(item)
            continue
        seen.add(item)
        normalized.append(item)

    if duplicates:
        raise ValueError("Duplicate timeframe(s) after normalization are not allowed: " + str(sorted(set(duplicates))))

    return normalized


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
    return sorted(
        rows,
        key=lambda r: (
            -float(r["pnl_day_mean"]),
            -float(r["win_rate"]),
            float(r["max_dd"]),
            int(r["fast"]),
            int(r["slow"]),
        ),
    )


def main() -> None:
    args = parse_args()

    timeframes = _parse_timeframes(args.timeframes)
    _validate_bounds("fast", args.fast_min, args.fast_max)
    _validate_bounds("slow", args.slow_min, args.slow_max)
    _validate_positive("holdout-months", args.holdout_months)

    pairs = _build_pair_grid(args.fast_min, args.fast_max, args.slow_min, args.slow_max)
    if not pairs:
        raise ValueError(
            "No valid EMA pairs found in range. Requirement is fast > 0, slow > 0, fast < slow. "
            f"Got fast=[{args.fast_min},{args.fast_max}], slow=[{args.slow_min},{args.slow_max}]"
        )

    generate_ema_signals = _require_lib_function("generate_ema_signals")
    run_point_backtest = _require_lib_function("run_point_backtest")
    summarize_by_day = _require_lib_function("summarize_by_day")
    summarize_segment = _require_lib_function("summarize_segment")

    schema = lib_ema_search.load_ohlc_schema(args.schema_json)
    source = lib_ema_search.load_source_ohlc_csv(args.input_csv, schema)

    all_ranked_rows: list[dict[str, Any]] = []
    best_rows: list[dict[str, Any]] = []
    evaluated_timeframes: list[str] = []

    for timeframe in timeframes:
        bars = lib_ema_search.resample_ohlc(source, timeframe)
        full_months = _full_months(bars)
        if len(full_months) < args.holdout_months:
            raise ValueError(
                "Not enough full calendar months for requested holdout for timeframe "
                f"{timeframe!r}. available_full_months={len(full_months)}, "
                f"requested_holdout_months={args.holdout_months}"
            )

        holdout_months = full_months[-args.holdout_months :]
        holdout_start_month = holdout_months[0]
        holdout_end_month = holdout_months[-1]
        holdout_bars = _segment_from_months(bars, holdout_start_month, holdout_end_month)

        timeframe_rows: list[dict[str, Any]] = []
        for fast, slow in pairs:
            scored = generate_ema_signals(
                holdout_bars.copy(deep=True),
                ema_fast_span=fast,
                ema_slow_span=slow,
                mode=args.mode,
            )
            scored = run_point_backtest(scored, commission_points=args.commission_points)
            days = summarize_by_day(scored)
            summary = summarize_segment(days, near_zero_threshold=args.near_zero_threshold)
            metrics = _normalize_summary(summary)

            timeframe_rows.append(
                {
                    "holdout_start_month": holdout_start_month,
                    "holdout_end_month": holdout_end_month,
                    "instrument": args.instrument,
                    "timeframe": timeframe,
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

        ranked = _rank_rows(timeframe_rows)
        for rank_idx, row in enumerate(ranked, start=1):
            row["rank_in_timeframe"] = rank_idx

        best = ranked[0]
        best_rows.append(
            {
                "holdout_start_month": best["holdout_start_month"],
                "holdout_end_month": best["holdout_end_month"],
                "instrument": best["instrument"],
                "timeframe": best["timeframe"],
                "mode": best["mode"],
                "best_fast": int(best["fast"]),
                "best_slow": int(best["slow"]),
                "pnl_day_mean": float(best["pnl_day_mean"]),
                "win_rate": float(best["win_rate"]),
                "near_zero_rate": float(best["near_zero_rate"]),
                "total_pnl": float(best["total_pnl"]),
                "num_days": int(best["num_days"]),
                "num_trades": int(best["num_trades"]),
                "max_dd": float(best["max_dd"]),
            }
        )

        all_ranked_rows.extend(ranked)
        evaluated_timeframes.append(timeframe)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results_path = out_dir / "ema_multitimeframe_holdout_results.csv"
    best_path = out_dir / "ema_multitimeframe_holdout_best.csv"
    summary_path = out_dir / "ema_multitimeframe_holdout_summary.json"

    pd.DataFrame(all_ranked_rows).to_csv(results_path, index=False)
    pd.DataFrame(best_rows).to_csv(best_path, index=False)

    summary_payload = {
        "instrument": args.instrument,
        "mode": args.mode,
        "requested_timeframes": timeframes,
        "evaluated_timeframes": evaluated_timeframes,
        "fast_min": int(args.fast_min),
        "fast_max": int(args.fast_max),
        "slow_min": int(args.slow_min),
        "slow_max": int(args.slow_max),
        "commission_points": float(args.commission_points),
        "near_zero_threshold": float(args.near_zero_threshold),
        "holdout_months": int(args.holdout_months),
        "ranking_rule": RANKING_RULE,
        "artifacts": {
            "ema_multitimeframe_holdout_results_csv": str(results_path),
            "ema_multitimeframe_holdout_best_csv": str(best_path),
            "ema_multitimeframe_holdout_summary_json": str(summary_path),
        },
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"results_csv={results_path}")
    print(f"best_csv={best_path}")
    print(f"summary_json={summary_path}")
    print(f"evaluated_timeframes_count={len(evaluated_timeframes)}")


if __name__ == "__main__":
    main()
