#!/usr/bin/env python3
import argparse
import hashlib
import json
import math
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from research import build_d1_tsmom_research_package_blocked_safe as blocked_safe

base = blocked_safe.base

RUNNER_VERSION = "d1_tsmom_fragility_inference_package.v1"
CANONICAL_RUN_ID = "d1_tsmom_minimal_2026-05-25_394b9d5f14e4197f"
PRIMARY_LABEL_CLASS = "primary_research_close_to_close"
SECONDARY_LABEL_CLASS = "secondary_execution_compatible_next_open"
KNOWN_BY_WHEN = "setup_day_S_post_close"
EARLIEST_EXECUTABLE_POINT = "S_plus_1_open"
RANDOM_SEED = 394193
BOOTSTRAP_REPS_DEFAULT = 1000
HAC_LAG_5D = 4
BLOCK_LENGTH_5D = 5
STRICT_MIN_INSTRUMENTS = 2

ARTIFACT_SPECS = [
    ("d1_tsmom_fragility_metadata", "metadata_table", "json", "d1_tsmom_fragility_metadata.json"),
    ("d1_tsmom_fragility_parameter_snapshot", "metadata_table", "json", "d1_tsmom_fragility_parameter_snapshot.json"),
    ("d1_tsmom_primary_label_table", "experiment_table", "parquet", "d1_tsmom_primary_label_table.parquet"),
    ("d1_tsmom_secondary_execution_label_table", "experiment_table", "parquet", "d1_tsmom_secondary_execution_label_table.parquet"),
    ("d1_tsmom_inference_1d_summary", "summary_table", "parquet", "d1_tsmom_inference_1d_summary.parquet"),
    ("d1_tsmom_inference_5d_overlap_adjusted_summary", "summary_table", "parquet", "d1_tsmom_inference_5d_overlap_adjusted_summary.parquet"),
    ("d1_tsmom_yearly_subperiod_table", "fragility_table", "parquet", "d1_tsmom_yearly_subperiod_table.parquet"),
    ("d1_tsmom_long_short_decomposition_table", "fragility_table", "parquet", "d1_tsmom_long_short_decomposition_table.parquet"),
    ("d1_tsmom_si_usdrubf_diagnostics_table", "diagnostics_table", "parquet", "d1_tsmom_si_usdrubf_diagnostics_table.parquet"),
    ("d1_tsmom_cost_turnover_sensitivity_table", "fragility_table", "parquet", "d1_tsmom_cost_turnover_sensitivity_table.parquet"),
    ("d1_tsmom_strict_valid_vs_full_valid_table", "fragility_table", "parquet", "d1_tsmom_strict_valid_vs_full_valid_table.parquet"),
    ("d1_tsmom_fragility_report", "report_artifact", "markdown", "d1_tsmom_fragility_report.md"),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_text(value: Any) -> str:
    return base.clean_text(value)


def parse_date(value: Any, field: str) -> str:
    return base.parse_date(value, field)


def stable_id(values: Iterable[Any]) -> str:
    raw = json.dumps(list(values), ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    return base.sha256_file(path)


def finite_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def series_values(frame: pd.DataFrame, column: str) -> List[float]:
    if frame.empty or column not in frame.columns:
        return []
    vals: List[float] = []
    for value in pd.to_numeric(frame[column], errors="coerce").tolist():
        num = finite_float(value)
        if num is not None:
            vals.append(num)
    return vals


def mean_value(values: List[float]) -> float:
    if not values:
        return math.nan
    return float(sum(values) / len(values))


def median_value(values: List[float]) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def sample_std(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return math.nan
    mu = mean_value(values)
    var = sum((x - mu) ** 2 for x in values) / float(n - 1)
    return float(math.sqrt(max(var, 0.0)))


def quantile(values: List[float], q: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ordered[lo])
    weight = pos - lo
    return float(ordered[lo] * (1.0 - weight) + ordered[hi] * weight)


def bootstrap_mean_ci(values: List[float], reps: int, seed: int) -> Tuple[float, float]:
    n = len(values)
    if n == 0:
        return math.nan, math.nan
    rng = random.Random(seed)
    means: List[float] = []
    for _ in range(max(1, reps)):
        sample_sum = 0.0
        for _j in range(n):
            sample_sum += values[rng.randrange(n)]
        means.append(sample_sum / float(n))
    return quantile(means, 0.025), quantile(means, 0.975)


def moving_block_bootstrap_mean_ci(values: List[float], block_length: int, reps: int, seed: int) -> Tuple[float, float]:
    n = len(values)
    if n == 0:
        return math.nan, math.nan
    if n <= block_length:
        return bootstrap_mean_ci(values, reps, seed)
    blocks = [values[i:i + block_length] for i in range(0, n - block_length + 1)]
    rng = random.Random(seed)
    means: List[float] = []
    for _ in range(max(1, reps)):
        sample: List[float] = []
        while len(sample) < n:
            sample.extend(blocks[rng.randrange(len(blocks))])
        means.append(sum(sample[:n]) / float(n))
    return quantile(means, 0.025), quantile(means, 0.975)


def hac_stats(values: List[float], lag: int) -> Dict[str, Any]:
    n = len(values)
    if n == 0:
        return {"hac_lag": lag, "hac_se": math.nan, "hac_t_stat": math.nan, "effective_n": 0.0}
    mu = mean_value(values)
    centered = [x - mu for x in values]
    gamma0 = sum(x * x for x in centered) / float(n)
    long_var = gamma0
    max_lag = min(lag, n - 1)
    for item_lag in range(1, max_lag + 1):
        gamma = 0.0
        for i in range(item_lag, n):
            gamma += centered[i] * centered[i - item_lag]
        gamma = gamma / float(n)
        weight = 1.0 - (float(item_lag) / float(max_lag + 1))
        long_var += 2.0 * weight * gamma
    long_var = max(long_var, 0.0)
    hac_se = math.sqrt(long_var / float(n)) if n else math.nan
    hac_t = mu / hac_se if hac_se and not math.isnan(hac_se) else math.nan
    iid_std = sample_std(values)
    iid_se = iid_std / math.sqrt(n) if n > 1 and not math.isnan(iid_std) else math.nan
    effective_n = float(n)
    if hac_se and not math.isnan(hac_se) and iid_se and not math.isnan(iid_se):
        effective_n = min(float(n), max(1.0, float(n) * (iid_se * iid_se) / (hac_se * hac_se)))
    return {"hac_lag": int(lag), "hac_se": float(hac_se), "hac_t_stat": float(hac_t), "effective_n": float(effective_n)}


def infer_basic(scope: str, label_class: str, label_name: str, values: List[float], reps: int, seed: int) -> Dict[str, Any]:
    n = len(values)
    mu = mean_value(values)
    std = sample_std(values)
    se = std / math.sqrt(n) if n > 1 and not math.isnan(std) else math.nan
    t_stat = mu / se if se and not math.isnan(se) else math.nan
    ci_low, ci_high = bootstrap_mean_ci(values, reps, seed)
    return {
        "scope": scope,
        "label_class": label_class,
        "label_name": label_name,
        "n": int(n),
        "mean": mu,
        "median": median_value(values),
        "std": std,
        "standard_error_iid": se,
        "t_stat_iid": t_stat,
        "hit_rate_positive": float(sum(1 for x in values if x > 0.0) / n) if n else math.nan,
        "bootstrap_reps": int(reps),
        "bootstrap_ci95_low": ci_low,
        "bootstrap_ci95_high": ci_high,
        "method_note": "IID mean summary with deterministic non-parametric bootstrap; used for 1D only",
    }


def outcome_window_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start <= b_end and b_start <= a_end


def overlap_pair_count(indices: List[int], horizon: int) -> int:
    ordered = sorted(indices)
    count = 0
    for pos, idx in enumerate(ordered):
        for later in ordered[pos + 1:]:
            if later <= idx + horizon - 1:
                count += 1
            else:
                break
    return int(count)


def infer_overlap_5d(scope: str, frame: pd.DataFrame, reps: int, seed: int) -> Dict[str, Any]:
    work = frame.loc[frame["has_primary_5d"]].sort_values(["setup_index", "setup_day_s"]).copy()
    values = series_values(work, "y_primary_5d_signed_cc")
    pair_count = overlap_pair_count([int(x) for x in work["setup_index"].tolist()], 5) if not work.empty else 0
    possible_pairs = int(len(work) * (len(work) - 1) / 2)
    ci_low, ci_high = moving_block_bootstrap_mean_ci(values, BLOCK_LENGTH_5D, reps, seed)
    stats = hac_stats(values, HAC_LAG_5D)
    out = {
        "scope": scope,
        "label_class": PRIMARY_LABEL_CLASS,
        "label_name": "y_primary_5d_signed_cc",
        "n": int(len(values)),
        "mean": mean_value(values),
        "median": median_value(values),
        "std": sample_std(values),
        "hit_rate_positive": float(sum(1 for x in values if x > 0.0) / len(values)) if values else math.nan,
        "overlap_horizon_sessions": 5,
        "overlap_pair_count": int(pair_count),
        "overlap_pair_share": float(pair_count / possible_pairs) if possible_pairs else 0.0,
        "block_bootstrap_block_length": int(BLOCK_LENGTH_5D),
        "block_bootstrap_reps": int(reps),
        "block_bootstrap_ci95_low": ci_low,
        "block_bootstrap_ci95_high": ci_high,
        "method_note": "5D overlap handled with HAC lag 4 plus moving-block bootstrap block length 5; IID t-stat is intentionally not reported as decision statistic",
    }
    out.update(stats)
    return out


def build_positions(all_bars: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    blockers: List[Dict[str, Any]] = []
    for instrument, part in all_bars.groupby("instrument", sort=True):
        ordered = part.sort_values("trade_date").reset_index(drop=True).copy()
        if len(ordered) < base.MIN_HISTORY:
            blockers.append({"instrument": str(instrument), "blocker": "available_d1_rows_below_min_history", "available_rows": int(len(ordered)), "min_history": int(base.MIN_HISTORY)})
            continue
        bars = base.make_signal_frame(ordered)
        for i in range(base.LOOKBACK, len(bars)):
            h1 = i + 1
            h5 = i + 5
            if h1 >= len(bars):
                continue
            row_s = bars.iloc[i]
            signal = int(row_s["signal"])
            if signal == 0:
                continue
            row_h1 = bars.iloc[h1]
            row_h5 = bars.iloc[h5] if h5 < len(bars) else None
            side = "long" if signal > 0 else "short"
            payload: Dict[str, Any] = {
                "instrument": str(instrument),
                "setup_day_s": str(row_s["trade_date"]),
                "setup_index": int(row_s["setup_index"]),
                "signal": signal,
                "signal_side": side,
                "known_by_when": KNOWN_BY_WHEN,
                "earliest_executable_point": EARLIEST_EXECUTABLE_POINT,
                "d_day_execution_forbidden": True,
                "close_s": float(row_s["close"]),
                "open_s_plus_1": float(row_h1["open"]),
                "close_s_plus_1": float(row_h1["close"]),
                "outcome_day_s_plus_1": str(row_h1["trade_date"]),
                "outcome_day_s_plus_5": str(row_h5["trade_date"]) if row_h5 is not None else None,
                "close_s_plus_5": float(row_h5["close"]) if row_h5 is not None else math.nan,
                "primary_label_class": PRIMARY_LABEL_CLASS,
                "secondary_label_class": SECONDARY_LABEL_CLASS,
                "y_primary_1d_signed_cc": signal * (float(row_h1["close"]) / float(row_s["close"]) - 1.0),
                "y_primary_5d_signed_cc": signal * (float(row_h5["close"]) / float(row_s["close"]) - 1.0) if row_h5 is not None else math.nan,
                "y_exec_1d_next_open_gross": signal * (float(row_h1["close"]) / float(row_h1["open"]) - 1.0),
                "y_exec_5d_next_open_gross": signal * (float(row_h5["close"]) / float(row_h1["open"]) - 1.0) if row_h5 is not None else math.nan,
                "has_primary_1d": True,
                "has_primary_5d": bool(row_h5 is not None),
                "has_secondary_1d": True,
                "has_secondary_5d": bool(row_h5 is not None),
                "strict_valid_1d": True,
                "strict_valid_5d": bool(row_h5 is not None),
            }
            if instrument == "Si":
                payload["source_contracts"] = str(row_s.get("source_contracts", ""))
                payload["roll_map_id"] = str(row_s.get("roll_map_id", ""))
                payload["roll_policy_id"] = str(row_s.get("roll_policy_id", ""))
                payload["adjustment_policy_id"] = str(row_s.get("adjustment_policy_id", ""))
                payload["has_roll_boundary_s"] = bool(row_s.get("has_roll_boundary", False))
                if row_h5 is not None and "has_roll_boundary" in bars.columns:
                    window_roll = bool(bars.iloc[i:h5 + 1]["has_roll_boundary"].astype(bool).any())
                else:
                    window_roll = bool(row_s.get("has_roll_boundary", False))
                payload["has_roll_boundary_window_5d"] = window_roll
                payload["strict_valid_1d"] = not bool(payload["has_roll_boundary_s"])
                payload["strict_valid_5d"] = bool(row_h5 is not None) and not window_roll
            else:
                payload["source_contracts"] = ""
                payload["roll_map_id"] = ""
                payload["roll_policy_id"] = ""
                payload["adjustment_policy_id"] = ""
                payload["has_roll_boundary_s"] = False
                payload["has_roll_boundary_window_5d"] = False
            rows.append(payload)
    out = pd.DataFrame(rows)
    if out.empty:
        base.die("positions table is empty after fragility signal construction")
    return out.sort_values(["instrument", "setup_day_s"]).reset_index(drop=True), blockers


def build_primary_label_table(positions: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "instrument", "setup_day_s", "setup_index", "signal", "signal_side", "known_by_when", "d_day_execution_forbidden",
        "primary_label_class", "outcome_day_s_plus_1", "outcome_day_s_plus_5", "y_primary_1d_signed_cc", "y_primary_5d_signed_cc",
        "has_primary_1d", "has_primary_5d", "strict_valid_1d", "strict_valid_5d", "has_roll_boundary_s", "has_roll_boundary_window_5d",
    ]
    return positions[cols].copy()


def build_secondary_label_table(positions: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "instrument", "setup_day_s", "setup_index", "signal", "signal_side", "known_by_when", "earliest_executable_point", "d_day_execution_forbidden",
        "secondary_label_class", "outcome_day_s_plus_1", "outcome_day_s_plus_5", "y_exec_1d_next_open_gross", "y_exec_5d_next_open_gross",
        "has_secondary_1d", "has_secondary_5d",
    ]
    return positions[cols].copy()


def scoped_frames(positions: pd.DataFrame) -> List[Tuple[str, pd.DataFrame]]:
    out: List[Tuple[str, pd.DataFrame]] = []
    for instrument, part in positions.groupby("instrument", sort=True):
        out.append((str(instrument), part.copy()))
    out.append(("pooled", positions.copy()))
    return out


def build_inference_1d_summary(positions: pd.DataFrame, reps: int) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for scope, part in scoped_frames(positions):
        work = part.loc[part["has_primary_1d"]].copy()
        rows.append(infer_basic(scope, PRIMARY_LABEL_CLASS, "y_primary_1d_signed_cc", series_values(work, "y_primary_1d_signed_cc"), reps, RANDOM_SEED + len(rows)))
    return pd.DataFrame(rows)


def build_inference_5d_summary(positions: pd.DataFrame, reps: int) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for scope, part in scoped_frames(positions):
        rows.append(infer_overlap_5d(scope, part, reps, RANDOM_SEED + 100 + len(rows)))
    return pd.DataFrame(rows)


def split_id_for_date(date_text: str, midpoint: str) -> str:
    return "first_half_locked" if date_text <= midpoint else "second_half_locked"


def metric_row(scope: str, split_type: str, split_id: str, label_class: str, label_name: str, values: List[float]) -> Dict[str, Any]:
    n = len(values)
    return {
        "scope": scope,
        "split_type": split_type,
        "split_id": split_id,
        "label_class": label_class,
        "label_name": label_name,
        "n": int(n),
        "mean": mean_value(values),
        "median": median_value(values),
        "hit_rate_positive": float(sum(1 for x in values if x > 0.0) / n) if n else math.nan,
        "interpretation_policy": "locked_before_execution_no_post_selection",
    }


def build_yearly_subperiod_table(positions: pd.DataFrame, from_date: str, till: str) -> pd.DataFrame:
    labels = [
        (PRIMARY_LABEL_CLASS, "y_primary_1d_signed_cc", "has_primary_1d"),
        (PRIMARY_LABEL_CLASS, "y_primary_5d_signed_cc", "has_primary_5d"),
        (SECONDARY_LABEL_CLASS, "y_exec_1d_next_open_gross", "has_secondary_1d"),
        (SECONDARY_LABEL_CLASS, "y_exec_5d_next_open_gross", "has_secondary_5d"),
    ]
    midpoint = pd.to_datetime(from_date) + (pd.to_datetime(till) - pd.to_datetime(from_date)) / 2
    midpoint_text = midpoint.date().isoformat()
    work = positions.copy()
    work["setup_year"] = work["setup_day_s"].astype(str).str.slice(0, 4)
    work["locked_half"] = work["setup_day_s"].map(lambda x: split_id_for_date(str(x), midpoint_text))
    rows: List[Dict[str, Any]] = []
    for scope, part in scoped_frames(work):
        for label_class, label_name, valid_col in labels:
            for year, ypart in part.groupby("setup_year", sort=True):
                rows.append(metric_row(scope, "calendar_year", str(year), label_class, label_name, series_values(ypart.loc[ypart[valid_col]], label_name)))
            for half, hpart in part.groupby("locked_half", sort=True):
                rows.append(metric_row(scope, "locked_date_half", str(half), label_class, label_name, series_values(hpart.loc[hpart[valid_col]], label_name)))
    return pd.DataFrame(rows)


def build_long_short_decomposition_table(positions: pd.DataFrame) -> pd.DataFrame:
    labels = [
        (PRIMARY_LABEL_CLASS, "y_primary_1d_signed_cc", "has_primary_1d"),
        (PRIMARY_LABEL_CLASS, "y_primary_5d_signed_cc", "has_primary_5d"),
        (SECONDARY_LABEL_CLASS, "y_exec_1d_next_open_gross", "has_secondary_1d"),
        (SECONDARY_LABEL_CLASS, "y_exec_5d_next_open_gross", "has_secondary_5d"),
    ]
    rows: List[Dict[str, Any]] = []
    for scope, part in scoped_frames(positions):
        for side, side_part in part.groupby("signal_side", sort=True):
            for label_class, label_name, valid_col in labels:
                vals = series_values(side_part.loc[side_part[valid_col]], label_name)
                rows.append(metric_row(scope, "signal_side", str(side), label_class, label_name, vals))
    return pd.DataFrame(rows)


def build_diagnostics_table(all_bars: pd.DataFrame, positions: pd.DataFrame, baseline_diag: List[Dict[str, Any]], blockers: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    total_abs_primary = sum(abs(x) for x in series_values(positions, "y_primary_1d_signed_cc"))
    for instrument, bars in all_bars.groupby("instrument", sort=True):
        part = positions.loc[positions["instrument"] == instrument].copy()
        abs_primary = sum(abs(x) for x in series_values(part, "y_primary_1d_signed_cc"))
        rows.append({
            "scope": str(instrument),
            "diagnostic_type": "instrument_contribution",
            "d1_rows": int(len(bars)),
            "event_rows": int(len(part)),
            "long_rows": int((part["signal"] > 0).sum()),
            "short_rows": int((part["signal"] < 0).sum()),
            "primary_1d_abs_contribution_share": float(abs_primary / total_abs_primary) if total_abs_primary else math.nan,
            "primary_1d_mean": mean_value(series_values(part, "y_primary_1d_signed_cc")),
            "primary_5d_mean": mean_value(series_values(part.loc[part["has_primary_5d"]], "y_primary_5d_signed_cc")),
            "concentration_note": "pooled conclusion must be checked after this per-instrument row",
        })
    for item in baseline_diag:
        row = dict(item)
        row.setdefault("diagnostic_type", "baseline_exclusion")
        rows.append(row)
    for blocker in blockers:
        row = dict(blocker)
        row["scope"] = row.pop("instrument", "unknown")
        row["diagnostic_type"] = "instrument_blocker"
        rows.append(row)
    return pd.DataFrame(rows)


def build_cost_turnover_sensitivity_table(positions: pd.DataFrame, cost_bps_values: List[float]) -> pd.DataFrame:
    labels = [("y_exec_1d_next_open_gross", "has_secondary_1d"), ("y_exec_5d_next_open_gross", "has_secondary_5d")]
    rows: List[Dict[str, Any]] = []
    for scope, part in scoped_frames(positions):
        ordered = part.sort_values("setup_day_s").copy()
        signal_changes = int((ordered["signal"].shift(1).fillna(0) != ordered["signal"]).sum()) if not ordered.empty else 0
        for label_name, valid_col in labels:
            gross_values = series_values(ordered.loc[ordered[valid_col]], label_name)
            for cost_bps in cost_bps_values:
                cost = float(cost_bps) / 10000.0
                adjusted = [x - cost for x in gross_values]
                rows.append({
                    "scope": scope,
                    "label_class": SECONDARY_LABEL_CLASS,
                    "label_name": label_name,
                    "cost_bps_per_event": float(cost_bps),
                    "n": int(len(gross_values)),
                    "mean_gross": mean_value(gross_values),
                    "mean_cost_adjusted": mean_value(adjusted),
                    "event_count_turnover_proxy": int(len(gross_values)),
                    "signal_change_count": int(signal_changes),
                    "boundary_note": "cost sensitivity applies only to secondary execution-compatible labels",
                })
    return pd.DataFrame(rows)


def build_strict_valid_vs_full_valid_table(positions: pd.DataFrame) -> pd.DataFrame:
    specs = [
        ("y_primary_1d_signed_cc", "has_primary_1d", "strict_valid_1d"),
        ("y_primary_5d_signed_cc", "has_primary_5d", "strict_valid_5d"),
    ]
    rows: List[Dict[str, Any]] = []
    for scope, part in scoped_frames(positions):
        for label_name, full_col, strict_col in specs:
            full_values = series_values(part.loc[part[full_col]], label_name)
            strict_values = series_values(part.loc[part[full_col] & part[strict_col]], label_name)
            for mode, values in [("full_valid", full_values), ("strict_valid", strict_values)]:
                rows.append({
                    "scope": scope,
                    "validity_mode": mode,
                    "label_class": PRIMARY_LABEL_CLASS,
                    "label_name": label_name,
                    "n": int(len(values)),
                    "mean": mean_value(values),
                    "median": median_value(values),
                    "hit_rate_positive": float(sum(1 for x in values if x > 0.0) / len(values)) if values else math.nan,
                    "confidence_policy": "strict_valid_primary_for_confidence_full_valid_for_coverage_context",
                })
    return pd.DataFrame(rows)


def build_baseline_exclusion_diagnostics(all_bars: pd.DataFrame, positions: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for instrument, part in all_bars.groupby("instrument", sort=True):
        bars = base.make_signal_frame(part.sort_values("trade_date").reset_index(drop=True).copy())
        events = positions.loc[positions["instrument"] == instrument].copy()
        event_setups = set(int(x) for x in events["setup_index"].tolist())
        event_windows_1d = [(int(x) + 1, int(x) + 1) for x in event_setups]
        event_windows_5d = [(int(x) + 1, int(x) + 5) for x in event_setups]
        for horizon, event_windows in [(1, event_windows_1d), (5, event_windows_5d)]:
            candidate_count = 0
            excluded_event = 0
            excluded_overlap = 0
            kept = 0
            for i in range(base.LOOKBACK, len(bars)):
                h_end = i + horizon
                if h_end >= len(bars):
                    continue
                if int(bars.iloc[i]["signal"]) != 0:
                    continue
                candidate_count += 1
                if i in event_setups:
                    excluded_event += 1
                    continue
                if any(outcome_window_overlap(i + 1, h_end, start, end) for start, end in event_windows):
                    excluded_overlap += 1
                    continue
                kept += 1
            rows.append({
                "scope": str(instrument),
                "diagnostic_type": "baseline_exclusion",
                "baseline_horizon_sessions": int(horizon),
                "baseline_candidate_non_event_rows": int(candidate_count),
                "excluded_event_rows": int(excluded_event),
                "excluded_overlapping_outcome_window_rows": int(excluded_overlap),
                "baseline_rows_after_exclusion": int(kept),
                "baseline_rule": "non_event_rows_exclude_event_rows_and_rows_whose_outcome_windows_overlap_event_outcome_windows",
            })
    return rows


def table_to_markdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_empty_"
    try:
        return frame.to_markdown(index=False)
    except Exception:
        return frame.to_string(index=False)


def make_artifact_manifest(out_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for artifact_id, artifact_class, fmt, filename in ARTIFACT_SPECS:
        rows.append({
            "artifact_id": artifact_id,
            "artifact_class": artifact_class,
            "producer": "src/research/build_d1_tsmom_fragility_inference_package.py",
            "consumer": "PM_L3_research_closeout",
            "format": fmt,
            "path": str(out_dir / filename),
        })
    return rows


def conservative_top_line(strict_table: pd.DataFrame) -> str:
    pooled = strict_table.loc[(strict_table["scope"] == "pooled") & (strict_table["validity_mode"] == "strict_valid")].copy()
    if pooled.empty:
        return "insufficient_strict_valid_evidence"
    means = {str(row["label_name"]): finite_float(row["mean"]) for _, row in pooled.iterrows()}
    ns = {str(row["label_name"]): int(row["n"]) for _, row in pooled.iterrows()}
    if ns.get("y_primary_1d_signed_cc", 0) <= 0 or ns.get("y_primary_5d_signed_cc", 0) <= 0:
        return "insufficient_strict_valid_evidence"
    if (means.get("y_primary_1d_signed_cc") or 0.0) > 0.0 and (means.get("y_primary_5d_signed_cc") or 0.0) > 0.0:
        return "strict_valid_pooled_means_positive_check_instrument_tables_before_interpretation"
    return "strict_valid_pooled_means_not_consistently_positive"


def write_report(path: Path, metadata: Dict[str, Any], params: Dict[str, Any], inference_1d: pd.DataFrame, inference_5d: pd.DataFrame, diagnostics: pd.DataFrame, strict_table: pd.DataFrame) -> None:
    lines: List[str] = []
    lines.append("# D1 TSMOM Fragility & Inference Package")
    lines.append("")
    lines.append("## Run binding")
    lines.append("")
    for key in ["run_id", "canonical_run_id", "run_status", "result_status", "created_ts", "output_dir"]:
        lines.append("- " + key + ": " + str(metadata.get(key)))
    lines.append("")
    lines.append("## Time and label semantics")
    lines.append("")
    lines.append("- known_by_when: " + KNOWN_BY_WHEN)
    lines.append("- D-day execution: forbidden")
    lines.append("- earliest executable point for secondary labels: " + EARLIEST_EXECUTABLE_POINT)
    lines.append("- primary label class: " + PRIMARY_LABEL_CLASS)
    lines.append("- secondary label class: " + SECONDARY_LABEL_CLASS)
    lines.append("- primary labels are close-to-close research outcomes and are the confidence anchor")
    lines.append("- secondary labels are next-open execution-compatible sensitivity outcomes")
    lines.append("- cost sensitivity is reported only for secondary labels")
    lines.append("- yearly and subperiod splits are locked before execution and must not be interpreted post-selection")
    lines.append("")
    lines.append("## Parameter snapshot")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(params, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## 1D primary inference")
    lines.append("")
    lines.append(table_to_markdown(inference_1d))
    lines.append("")
    lines.append("## 5D overlap-adjusted primary inference")
    lines.append("")
    lines.append(table_to_markdown(inference_5d))
    lines.append("")
    lines.append("## Per-instrument and baseline diagnostics")
    lines.append("")
    lines.append(table_to_markdown(diagnostics))
    lines.append("")
    lines.append("## Strict-valid vs full-valid")
    lines.append("")
    lines.append(table_to_markdown(strict_table))
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_cost_bps(raw: str) -> List[float]:
    out: List[float] = []
    for item in str(raw).split(","):
        text = item.strip()
        if not text:
            continue
        out.append(float(text))
    return out or [0.0]


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="")
    parser.add_argument("--manifest-ref", required=True)
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--till", required=True)
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--roll-map-path", default="")
    parser.add_argument("--bootstrap-reps", type=int, default=BOOTSTRAP_REPS_DEFAULT)
    parser.add_argument("--cost-bps", default="0,1,3,5,10")
    args = parser.parse_args()

    data_root = base.resolve_data_root(args.data_root)
    from_date = parse_date(args.from_date, "from")
    till = parse_date(args.till, "till")
    if from_date > till:
        base.die("--from is after --till")
    manifest_path = base.resolve_manifest(data_root, args.manifest_ref)
    manifest_sha = sha256_file(manifest_path)
    manifest = base.read_json(manifest_path)
    run_id = clean_text(args.run_id) or ("d1_tsmom_fragility_" + till + "_" + stable_id([manifest_sha, from_date, till, RUNNER_VERSION]))
    out_dir = Path(clean_text(args.out_dir)).expanduser().resolve() if clean_text(args.out_dir) else (data_root / "futures" / "research" / "d1_tsmom_fragility" / ("run_id=" + run_id))

    usdrubf = base.prepare_usdrubf(base.read_parquet_parts(base.discover_usdrubf_paths(data_root, from_date, till), "USDRUBF D1"))
    si = base.prepare_si(base.read_parquet_parts(base.discover_si_paths(data_root, from_date, till), "Si continuous D1"))
    all_bars = pd.concat([usdrubf, si], ignore_index=True).sort_values(["instrument", "trade_date"]).reset_index(drop=True)
    positions, blockers = build_positions(all_bars)
    baseline_diag = build_baseline_exclusion_diagnostics(all_bars, positions)
    roll_map_path_resolved, roll_map_sha = base.resolve_roll_map_path(data_root, args.roll_map_path, manifest)
    base_quality = base.build_quality_summary(all_bars, positions, manifest_path, manifest_sha, roll_map_sha)

    primary_table = build_primary_label_table(positions)
    secondary_table = build_secondary_label_table(positions)
    inference_1d = build_inference_1d_summary(positions, int(args.bootstrap_reps))
    inference_5d = build_inference_5d_summary(positions, int(args.bootstrap_reps))
    yearly_subperiod = build_yearly_subperiod_table(positions, from_date, till)
    long_short = build_long_short_decomposition_table(positions)
    diagnostics = build_diagnostics_table(all_bars, positions, baseline_diag, blockers)
    cost_turnover = build_cost_turnover_sensitivity_table(positions, parse_cost_bps(args.cost_bps))
    strict_vs_full = build_strict_valid_vs_full_valid_table(positions)

    result_status = "canonical" if base_quality.get("final_quality_gate_verdict") == "pass" and not blockers else "provisional"
    params = {
        "runner_version": RUNNER_VERSION,
        "canonical_run_id": CANONICAL_RUN_ID,
        "instruments": ["Si", "USDRUBF"],
        "timeframe": "D1",
        "known_by_when": KNOWN_BY_WHEN,
        "d_day_execution_forbidden": True,
        "earliest_executable_point": EARLIEST_EXECUTABLE_POINT,
        "primary_research_labels": ["y_primary_1d_signed_cc", "y_primary_5d_signed_cc"],
        "secondary_execution_compatible_labels": ["y_exec_1d_next_open_gross", "y_exec_5d_next_open_gross"],
        "primary_1d_definition": "signal(S) * return_close_to_close(S, S+1)",
        "primary_5d_definition": "signal(S) * return_close_to_close(S, S+5)",
        "secondary_1d_definition": "signal(S) * return_open_to_close(S+1, S+1)",
        "secondary_5d_definition": "signal(S) * return_open_to_close(S+1, S+5)",
        "signal_definition": "sign(close[S] / close[S-20] - 1)",
        "lookback": int(base.LOOKBACK),
        "min_history": int(base.MIN_HISTORY),
        "five_day_overlap_handling": {"hac_lag": HAC_LAG_5D, "block_bootstrap_block_length": BLOCK_LENGTH_5D, "effective_n_reported": True, "overlap_diagnostics_reported": True},
        "baseline_exclusion_rule": "non-event baseline rows exclude event rows and rows whose outcome windows overlap event outcome windows",
        "split_policy": {"calendar_year": "locked_before_execution", "date_halves": "locked_by_from_till_midpoint_before_execution", "post_selection_interpretation": "forbidden"},
        "strict_valid_policy": "strict_valid primary for confidence; full_valid coverage and fragility context only",
        "cost_sensitivity_boundary": "secondary_execution_compatible_labels_only",
        "bootstrap_reps": int(args.bootstrap_reps),
        "cost_bps_values": parse_cost_bps(args.cost_bps),
    }

    artifacts = make_artifact_manifest(out_dir)
    metadata = {
        "run_id": run_id,
        "canonical_run_id": CANONICAL_RUN_ID,
        "runner_version": RUNNER_VERSION,
        "run_status": "executed",
        "result_status": result_status,
        "created_ts": utc_now_iso(),
        "output_dir": str(out_dir),
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "manifest_schema_version": manifest.get("schema_version"),
        "roll_map_path": roll_map_path_resolved or None,
        "roll_map_sha256": roll_map_sha or None,
        "base_quality_summary": base_quality,
        "instrument_blockers": blockers,
        "artifact_manifest": artifacts,
        "top_line_result_summary": conservative_top_line(strict_vs_full),
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(out_dir / "d1_tsmom_fragility_metadata.json", metadata)
    write_json(out_dir / "d1_tsmom_fragility_parameter_snapshot.json", params)
    primary_table.to_parquet(out_dir / "d1_tsmom_primary_label_table.parquet", index=False)
    secondary_table.to_parquet(out_dir / "d1_tsmom_secondary_execution_label_table.parquet", index=False)
    inference_1d.to_parquet(out_dir / "d1_tsmom_inference_1d_summary.parquet", index=False)
    inference_5d.to_parquet(out_dir / "d1_tsmom_inference_5d_overlap_adjusted_summary.parquet", index=False)
    yearly_subperiod.to_parquet(out_dir / "d1_tsmom_yearly_subperiod_table.parquet", index=False)
    long_short.to_parquet(out_dir / "d1_tsmom_long_short_decomposition_table.parquet", index=False)
    diagnostics.to_parquet(out_dir / "d1_tsmom_si_usdrubf_diagnostics_table.parquet", index=False)
    cost_turnover.to_parquet(out_dir / "d1_tsmom_cost_turnover_sensitivity_table.parquet", index=False)
    strict_vs_full.to_parquet(out_dir / "d1_tsmom_strict_valid_vs_full_valid_table.parquet", index=False)
    write_report(out_dir / "d1_tsmom_fragility_report.md", metadata, params, inference_1d, inference_5d, diagnostics, strict_vs_full)

    summary = {
        "run_id": run_id,
        "result_status": result_status,
        "output_dir": str(out_dir),
        "artifact_count": int(len(artifacts)),
        "primary_label_rows_1d": int(primary_table["has_primary_1d"].sum()),
        "primary_label_rows_5d": int(primary_table["has_primary_5d"].sum()),
        "secondary_label_rows_1d": int(secondary_table["has_secondary_1d"].sum()),
        "secondary_label_rows_5d": int(secondary_table["has_secondary_5d"].sum()),
        "top_line_result_summary": metadata["top_line_result_summary"],
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0 if result_status == "canonical" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
