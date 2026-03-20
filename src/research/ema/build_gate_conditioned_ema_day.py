#!/usr/bin/env python3
"""
Research-only utility: build gate-conditioned EMA day comparison.

Purpose
-------
Create deterministic day-level artifacts for EMA-vs-Gate analysis without touching
production/runtime code.

Date semantics (explicit)
-------------------------
- EMA day CSV `date` is interpreted as the trading day for which `pnl_day` and
  `EMA_EDGE_DAY` were computed.
- Gate label `date` is interpreted as the decision/trading day `d`.
- Gate label for day `d` is computed using D-1 features exactly as in
  `src.cli.phase_transition_risk`:
    * `trend_ratio_yday[d] = trend_ratio[d-1]`
    * `rel_range_yday[d]   = rel_range[d-1]`
    * `vol_z_yday[d]` from prior history before `d`
  and threshold rule:
    risk = 1 if any(yday_feature <= p10_threshold), else 0.
- Join key is exact calendar `date` (YYYY-MM-DD):
    EMA(date=d) INNER JOIN GateLabel(date=d)

Inputs
------
1) EMA day-level CSV from existing EMA contract, required columns:
   - date, pnl_day, EMA_EDGE_DAY
2) Day metrics CSV with base day metrics, required columns:
   - date, trend_ratio, rel_range
3) Threshold config JSON (default: config/phase_transition_p10.json)

Outputs
-------
1) Joined day-level CSV (deterministic ordering by date):
   date, pnl_day, EMA_EDGE_DAY, gate_state
2) Aggregate comparison CSV for gate_state in {0,1}:
   gate_state, rows, pnl_day_sum, pnl_day_mean, ema_edge_rate
"""

from __future__ import annotations

import argparse
import os
from typing import List

import pandas as pd

from src.cli.phase_transition_risk import build_yday_features, compute_phase_transition_risk


EMA_REQUIRED_COLS: List[str] = ["date", "pnl_day", "EMA_EDGE_DAY"]
METRICS_REQUIRED_COLS: List[str] = ["date", "trend_ratio", "rel_range"]


def _die(msg: str) -> None:
    raise SystemExit("ERROR: " + msg)


def _ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _normalize_date_col(df: pd.DataFrame, col: str, ctx: str) -> pd.Series:
    out = pd.to_datetime(df[col], errors="coerce").dt.date.astype("string")
    if out.isna().any():
        bad_n = int(out.isna().sum())
        _die(f"{ctx}: invalid date values in column {col}, bad_rows={bad_n}")
    return out


def _validate_columns(df: pd.DataFrame, required: List[str], ctx: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        _die(f"{ctx}: missing required columns: {missing}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ema-day-csv", required=True, help="Input EMA day CSV with date,pnl_day,EMA_EDGE_DAY")
    ap.add_argument(
        "--day-metrics-csv",
        required=True,
        help="Input day metrics CSV with date,trend_ratio,rel_range (base day metrics)",
    )
    ap.add_argument("--config", default="config/phase_transition_p10.json", help="Gate thresholds config JSON")
    ap.add_argument("--out-joined-csv", required=True, help="Output joined day-level CSV")
    ap.add_argument("--out-agg-csv", required=True, help="Output gate aggregate comparison CSV")
    args = ap.parse_args()

    if not os.path.exists(args.ema_day_csv):
        _die(f"ema csv not found: {args.ema_day_csv}")
    if not os.path.exists(args.day_metrics_csv):
        _die(f"day metrics csv not found: {args.day_metrics_csv}")
    if not os.path.exists(args.config):
        _die(f"config not found: {args.config}")

    ema = pd.read_csv(args.ema_day_csv)
    _validate_columns(ema, EMA_REQUIRED_COLS, "ema")
    ema = ema[EMA_REQUIRED_COLS].copy()
    ema["date"] = _normalize_date_col(ema, "date", "ema")
    ema["pnl_day"] = pd.to_numeric(ema["pnl_day"], errors="coerce")
    ema["EMA_EDGE_DAY"] = pd.to_numeric(ema["EMA_EDGE_DAY"], errors="coerce")

    if ema["pnl_day"].isna().any():
        _die(f"ema: non-numeric pnl_day rows={int(ema[pnl_day].isna().sum())}")
    if ema["EMA_EDGE_DAY"].isna().any():
        _die(f"ema: non-numeric EMA_EDGE_DAY rows={int(ema[EMA_EDGE_DAY].isna().sum())}")

    if ema["date"].duplicated().any():
        _die(f"ema: duplicate date rows={int(ema[date].duplicated().sum())}")

    metrics = pd.read_csv(args.day_metrics_csv)
    _validate_columns(metrics, METRICS_REQUIRED_COLS, "day_metrics")
    metrics = metrics[METRICS_REQUIRED_COLS].copy()
    metrics["date"] = _normalize_date_col(metrics, "date", "day_metrics")

    for c in ["trend_ratio", "rel_range"]:
        metrics[c] = pd.to_numeric(metrics[c], errors="coerce")
        if metrics[c].isna().any():
            _die(f"day_metrics: non-numeric {c} rows={int(metrics[c].isna().sum())}")

    if metrics["date"].duplicated().any():
        _die(f"day_metrics: duplicate date rows={int(metrics[date].duplicated().sum())}")

    d = metrics.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = build_yday_features(d, "date")
    d["PhaseTransitionRisk"] = compute_phase_transition_risk(d, args.config)

    gate = d[["date", "PhaseTransitionRisk"]].copy()
    gate["date"] = pd.to_datetime(gate["date"], errors="coerce").dt.date.astype("string")
    gate = gate.rename(columns={"PhaseTransitionRisk": "gate_state"})

    if gate["gate_state"].isna().any():
        _die(f"gate: NaN gate_state rows={int(gate[gate_state].isna().sum())}")

    joined = ema.merge(gate, on="date", how="inner", validate="one_to_one")
    if joined.empty:
        _die("joined result is empty (no overlapping dates)")

    joined = joined.sort_values("date").reset_index(drop=True)
    joined = joined[["date", "pnl_day", "EMA_EDGE_DAY", "gate_state"]]

    agg = (
        joined.groupby("gate_state", as_index=False)
        .agg(
            rows=("date", "count"),
            pnl_day_sum=("pnl_day", "sum"),
            pnl_day_mean=("pnl_day", "mean"),
            ema_edge_rate=("EMA_EDGE_DAY", "mean"),
        )
        .sort_values("gate_state")
        .reset_index(drop=True)
    )

    _ensure_parent_dir(args.out_joined_csv)
    _ensure_parent_dir(args.out_agg_csv)
    joined.to_csv(args.out_joined_csv, index=False)
    agg.to_csv(args.out_agg_csv, index=False)

    print(f"OK: joined_rows={len(joined)} out={args.out_joined_csv}")
    print(f"OK: agg_rows={len(agg)} out={args.out_agg_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
