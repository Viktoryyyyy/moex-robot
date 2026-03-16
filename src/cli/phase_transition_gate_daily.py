#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys

import pandas as pd


DATE_CANDIDATES = ["date", "TRADEDATE", "tradedate", "day"]


def _pick_date_col(df: pd.DataFrame) -> str:
    for c in DATE_CANDIDATES:
        if c in df.columns:
            return c
    raise SystemExit("no date column found in bootstrap csv; tried: %s" % DATE_CANDIDATES)


def _bootstrap_history_if_missing(history_path: str, bootstrap_path: str) -> None:
    if os.path.exists(history_path):
        return

    if not os.path.exists(bootstrap_path):
        subprocess.run([sys.executable, "-m", "src.research.regimes.build_day_metrics_from_master"], check=True)

    if not os.path.exists(bootstrap_path):
        raise SystemExit(
            "missing rel_range history and bootstrap source not found after builder run: history=%s bootstrap=%s"
            % (history_path, bootstrap_path)
        )

    df = pd.read_csv(bootstrap_path)
    date_col = _pick_date_col(df)
    if "rel_range" not in df.columns:
        raise SystemExit("bootstrap csv missing rel_range column: %s" % bootstrap_path)

    out = df[[date_col, "rel_range"]].copy()
    out = out.rename(columns={date_col: "date"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["rel_range"] = pd.to_numeric(out["rel_range"], errors="coerce")
    out = out.dropna(subset=["date", "rel_range"])
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last")

    os.makedirs(os.path.dirname(history_path), exist_ok=True)
    out.to_csv(history_path, index=False)


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", default="Si")
    ap.add_argument("--date", default="D-1")
    ap.add_argument("--out-5m", default="data/realtime/fo_5m_D-1.csv")
    ap.add_argument("--out-day", default="data/state/day_metrics_D-1.csv")
    ap.add_argument("--history", default="data/state/rel_range_history.csv")
    ap.add_argument("--bootstrap-metrics", default="data/research/day_metrics_from_master.csv")
    ap.add_argument("--config", default="config/phase_transition_p10.json")
    ap.add_argument("--out-json", default="data/state/phase_transition_risk.json")
    args = ap.parse_args()

    if args.date != "D-1":
        raise SystemExit("only --date D-1 is supported")

    _bootstrap_history_if_missing(args.history, args.bootstrap_metrics)

    _run(
        [
            sys.executable,
            "-m",
            "src.cli.daily_metrics_builder",
            "--key",
            args.key,
            "--date",
            args.date,
            "--out-5m",
            args.out_5m,
            "--out-day",
            args.out_day,
        ]
    )

    _run(
        [
            sys.executable,
            "-m",
            "src.cli.phase_transition_gate",
            "--in-day",
            args.out_day,
            "--in-history",
            args.history,
            "--config",
            args.config,
            "--out-json",
            args.out_json,
            "--out-history",
            args.history,
        ]
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
