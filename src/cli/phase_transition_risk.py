import argparse
import json
import os
from typing import Tuple

import pandas as pd


DATE_CANDIDATES = ["date", "TRADEDATE", "tradedate", "day"]

# Physical columns in day_metrics_from_master.csv
BASE_COLS = ["trend_ratio", "rel_range"]

# Logical D-1 features required by spec (not stored physically)
YDAY_COLS = ["trend_ratio_yday", "rel_range_yday", "vol_z_yday"]


def _pick_date_col(df: pd.DataFrame) -> str:
    for c in DATE_CANDIDATES:
        if c in df.columns:
            return c
    raise SystemExit(f"no date column found; tried: {DATE_CANDIDATES}")


def _load_thresholds(path: str) -> Tuple[float, float, float]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    for k in ["p10_trend_ratio", "p10_rel_range", "p10_vol_z"]:
        if k not in cfg:
            raise SystemExit(f"missing key in config: {k}")

    t = cfg["p10_trend_ratio"]
    r = cfg["p10_rel_range"]
    v = cfg["p10_vol_z"]

    if t is None or r is None or v is None:
        raise SystemExit(
            "thresholds are null in config; fit once on train (2020-2023) and freeze. "
            "keys: p10_trend_ratio, p10_rel_range, p10_vol_z"
        )

    return float(t), float(r), float(v)


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def build_yday_features(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    # Strict ex-ante: sort by date, then shift(1)
    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    d = d.sort_values(date_col).reset_index(drop=True)

    for c in BASE_COLS:
        if c not in d.columns:
            raise SystemExit(f"missing required base column: {c}")

    trend_ratio = _to_num(d["trend_ratio"])
    rel_range = _to_num(d["rel_range"])

    # D-1 physical shift
    d["trend_ratio_yday"] = trend_ratio.shift(1)
    d["rel_range_yday"] = rel_range.shift(1)

    # vol_z_yday: deterministic from rel_range using expanding history BEFORE day d
    # Spec:
    # X_d = rel_range[d]
    # mu_d = mean(X[all days < d])
    # sigma_d = std(X[all days < d])
    # vol_z_yday[d] = (X_{d-1} - mean(X[all days < d-1])) / std(X[all days < d-1])
    x_yday = rel_range.shift(1)
    mu_yday = x_yday.expanding().mean()
    sigma_yday = x_yday.expanding().std(ddof=0)
    d["vol_z_yday"] = (x_yday - mu_yday) / sigma_yday

    return d


def compute_phase_transition_risk(df: pd.DataFrame, cfg_path: str) -> pd.Series:
    p10_tr, p10_rr, p10_vz = _load_thresholds(cfg_path)

    tr = _to_num(df["trend_ratio_yday"])
    rr = _to_num(df["rel_range_yday"])
    vz = _to_num(df["vol_z_yday"])

    risk = (tr <= p10_tr) | (rr <= p10_rr) | (vz <= p10_vz)

    # NaN => cannot assert risk => 0 (strategies OFF)
    return risk.fillna(False).astype(int)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--config", default="config/phase_transition_p10.json")
    ap.add_argument("--output", required=True)
    ap.add_argument("--append-output", default="")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    date_col = _pick_date_col(df)

    df = build_yday_features(df, date_col)

    for c in YDAY_COLS:
        if c not in df.columns:
            raise SystemExit(f"missing required derived column: {c}")

    df["PhaseTransitionRisk"] = compute_phase_transition_risk(df, args.config)

    out = df[[date_col, "PhaseTransitionRisk"]].copy()
    out = out.rename(columns={date_col: "date"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date

    _ensure_dir(args.output)
    out.to_csv(args.output, index=False)

    if args.append_output:
        _ensure_dir(args.append_output)
        df.to_csv(args.append_output, index=False)

    print(f"OK: wrote {args.output} rows={len(out)}")
    if args.append_output:
        print(f"OK: wrote {args.append_output} rows={len(df)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
