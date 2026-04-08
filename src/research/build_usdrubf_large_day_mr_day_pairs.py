#!/usr/bin/env python3
import argparse
import os
import sys

import numpy as np
import pandas as pd


DEFAULT_IN_CSV = "data/master/usdrubf_5m_2022-04-26_2026-04-06.csv"
DEFAULT_OUT_CSV = "data/research/usdrubf_large_day_mr_day_pairs.csv"
REQUIRED_COLS = ["end", "open", "high", "low", "close"]


def _die(msg: str) -> None:
    raise SystemExit("ERROR: " + msg)


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _load_intraday(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        _die("input csv not found: " + path)

    df = pd.read_csv(path)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        _die("missing required columns: " + str(missing))

    work = df[REQUIRED_COLS].copy()
    work["end"] = pd.to_datetime(work["end"], errors="coerce")

    for c in ["open", "high", "low", "close"]:
        work[c] = pd.to_numeric(work[c], errors="coerce")

    if work["end"].isna().any():
        _die("invalid timestamp values in column end")
    if work[["open", "high", "low", "close"]].isna().any().any():
        _die("non-numeric or missing OHLC values found")

    if work["end"].duplicated().any():
        _die("duplicate intraday timestamps found")

    work = work.sort_values("end", ascending=True).reset_index(drop=True)
    if work.empty:
        _die("input csv has zero valid rows")

    work["trade_date"] = work["end"].dt.normalize()
    return work


def _build_daily(work: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for trade_date, g in work.groupby("trade_date", sort=True):
        g = g.sort_values("end", ascending=True).reset_index(drop=True)

        day_open = float(g.iloc[0]["open"])
        day_high = float(g["high"].max())
        day_low = float(g["low"].min())
        day_close = float(g.iloc[-1]["close"])

        vals = [day_open, day_high, day_low, day_close]
        if not all(np.isfinite(vals)):
            _die("non-finite aggregated OHLC for trade_date=" + str(trade_date.date()))

        rows.append(
            {
                "trade_date": pd.Timestamp(trade_date),
                "open": day_open,
                "high": day_high,
                "low": day_low,
                "close": day_close,
            }
        )

    daily = pd.DataFrame(rows).sort_values("trade_date", ascending=True).reset_index(drop=True)
    if len(daily) < 2:
        _die("need at least 2 completed trading days after aggregation")

    if daily["trade_date"].duplicated().any():
        _die("duplicate aggregated trade_date rows found")

    return daily


def _build_pairs(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for i in range(1, len(daily)):
        src = daily.iloc[i - 1]
        out = daily.iloc[i]

        source_trade_date = pd.Timestamp(src["trade_date"]).date().isoformat()
        date = pd.Timestamp(out["trade_date"]).date().isoformat()

        if source_trade_date >= date:
            _die("invalid day ordering: source_trade_date must be strictly earlier than date")

        prior_open = float(src["open"])
        prior_high = float(src["high"])
        prior_low = float(src["low"])
        prior_close = float(src["close"])
        outcome_open = float(out["open"])
        outcome_close = float(out["close"])

        prior_body_points = prior_close - prior_open
        prior_abs_body_points = abs(prior_body_points)
        prior_range_points = prior_high - prior_low

        if not np.isfinite(prior_range_points) or prior_range_points <= 0.0:
            _die("invalid prior_range_points for source_trade_date=" + source_trade_date)
        if not np.isfinite(prior_close) or prior_close == 0.0:
            _die("invalid prior_close denominator for source_trade_date=" + source_trade_date)

        prior_rel_range = prior_range_points / prior_close
        if not np.isfinite(prior_rel_range):
            _die("invalid prior_rel_range for source_trade_date=" + source_trade_date)

        prior_dir = _sign(prior_body_points)
        outcome_oc_points = outcome_close - outcome_open
        mr_outcome_points = -_sign(prior_close - prior_open) * outcome_oc_points
        mr_edge_day = int(mr_outcome_points > 0.0)

        row = {
            "date": date,
            "source_trade_date": source_trade_date,
            "prior_open": prior_open,
            "prior_high": prior_high,
            "prior_low": prior_low,
            "prior_close": prior_close,
            "prior_body_points": prior_body_points,
            "prior_abs_body_points": prior_abs_body_points,
            "prior_range_points": prior_range_points,
            "prior_rel_range": prior_rel_range,
            "prior_dir": prior_dir,
            "outcome_open": outcome_open,
            "outcome_close": outcome_close,
            "outcome_oc_points": outcome_oc_points,
            "mr_outcome_points": mr_outcome_points,
            "MR_EDGE_DAY": mr_edge_day,
        }

        for k, v in row.items():
            if k in ["date", "source_trade_date"]:
                continue
            if not np.isfinite(v):
                _die("non-finite output value in row date=" + date + " field=" + k)

        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        _die("pair output is empty")

    expected_cols = [
        "date",
        "source_trade_date",
        "prior_open",
        "prior_high",
        "prior_low",
        "prior_close",
        "prior_body_points",
        "prior_abs_body_points",
        "prior_range_points",
        "prior_rel_range",
        "prior_dir",
        "outcome_open",
        "outcome_close",
        "outcome_oc_points",
        "mr_outcome_points",
        "MR_EDGE_DAY",
    ]
    out = out[expected_cols].sort_values("date", ascending=True).reset_index(drop=True)

    if out["date"].duplicated().any():
        _die("duplicate outcome date rows found")
    if (out["source_trade_date"] >= out["date"]).any():
        _die("source_trade_date must be strictly earlier than date for all rows")

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", default=DEFAULT_IN_CSV, help="Canonical USDRUBF 5m input CSV")
    ap.add_argument("--out_csv", default=DEFAULT_OUT_CSV, help="Output CSV path")
    args = ap.parse_args()

    work = _load_intraday(args.in_csv)
    daily = _build_daily(work)
    out = _build_pairs(daily)

    _ensure_parent_dir(args.out_csv)
    out.to_csv(args.out_csv, index=False)

    print("IN:   " + args.in_csv)
    print("OUT:  " + args.out_csv)
    print("DAYS: " + str(len(out)))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        print("ERROR: " + str(e), file=sys.stderr)
        raise SystemExit(1)
