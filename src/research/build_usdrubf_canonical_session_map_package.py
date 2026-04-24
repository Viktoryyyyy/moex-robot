#!/usr/bin/env python3
import argparse
import json
import os
import sys

import pandas as pd


INSTRUMENT = "USDRUBF"
TZ = "Europe/Moscow"
DEFAULT_IN_CSV = "data/master/usdrubf_5m_2022-04-26_2026-04-06.csv"
DEFAULT_OUT_DIR = "data/research/usdrubf_canonical_session_map_package"
REQUIRED_BAR_COLS = ["end", "open", "high", "low", "close"]


def _die(msg: str) -> None:
    raise SystemExit("ERROR: " + msg)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_bars(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        _die("input csv not found: " + path)
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_BAR_COLS if c not in df.columns]
    if missing:
        _die("input csv missing required columns: " + str(missing))
    work = df[REQUIRED_BAR_COLS].copy()
    ts = pd.to_datetime(work["end"], errors="coerce")
    if ts.isna().any():
        _die("invalid bar end timestamps")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize(TZ)
    else:
        ts = ts.dt.tz_convert(TZ)
    work["bar_end_moscow"] = ts
    work["calendar_session_date"] = ts.dt.date.astype(str)
    for c in ["open", "high", "low", "close"]:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    if work[["open", "high", "low", "close"]].isna().any().any():
        _die("invalid OHLC values")
    if work["bar_end_moscow"].duplicated().any():
        _die("duplicate bar end timestamps")
    return work.sort_values("bar_end_moscow").reset_index(drop=True)


def _aggregate_sessions(work: pd.DataFrame) -> pd.DataFrame:
    rows = []
    dates = work["calendar_session_date"].drop_duplicates().tolist()
    date_to_idx = {d: i for i, d in enumerate(dates)}
    for session_date, g in work.groupby("calendar_session_date", sort=True):
        g = g.sort_values("bar_end_moscow").reset_index(drop=True)
        rows.append({
            "calendar_session_index": int(date_to_idx[str(session_date)]),
            "calendar_session_date": str(session_date),
            "weekday": int(pd.Timestamp(str(session_date)).weekday()),
            "is_saturday": int(pd.Timestamp(str(session_date)).weekday() == 5),
            "session_start": pd.Timestamp(g.iloc[0]["bar_end_moscow"]).isoformat(),
            "session_end": pd.Timestamp(g.iloc[-1]["bar_end_moscow"]).isoformat(),
            "bar_count": int(len(g)),
            "open": float(g.iloc[0]["open"]),
            "high": float(g["high"].max()),
            "low": float(g["low"].min()),
            "close": float(g.iloc[-1]["close"]),
        })
    if not rows:
        _die("calendar session table is empty")
    return pd.DataFrame(rows).sort_values("calendar_session_index").reset_index(drop=True)


def _build_calendar_session_map(bars: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    included = bars.copy()
    ordered_dates = included["calendar_session_date"].drop_duplicates().tolist()
    date_to_idx = {d: i for i, d in enumerate(ordered_dates)}
    included["calendar_session_index"] = included["calendar_session_date"].map(date_to_idx).astype(int)
    included["session_rule"] = "bar_end_moscow_calendar_date"
    session_map = included[[
        "bar_end_moscow",
        "calendar_session_date",
        "calendar_session_index",
        "session_rule",
        "open",
        "high",
        "low",
        "close",
    ]].copy()
    sessions = _aggregate_sessions(included)
    return session_map, sessions


def _build_summary(bars: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    saturday_sessions = int(sessions["is_saturday"].sum())
    rows = [
        {"metric": "instrument", "value": INSTRUMENT},
        {"metric": "timezone", "value": TZ},
        {"metric": "session_rule", "value": "date(bar_end in Europe/Moscow)"},
        {"metric": "gap_based_partition_used", "value": 0},
        {"metric": "trade_session_date_remap_used", "value": 0},
        {"metric": "bar_count", "value": int(len(bars))},
        {"metric": "calendar_session_count", "value": int(len(sessions))},
        {"metric": "saturday_session_count", "value": saturday_sessions},
        {"metric": "first_session_date", "value": str(sessions["calendar_session_date"].min())},
        {"metric": "last_session_date", "value": str(sessions["calendar_session_date"].max())},
        {"metric": "decision", "value": "canonical_calendar_date_session_map_materialized"},
    ]
    return pd.DataFrame(rows)


def _write_metadata(path: str, args: argparse.Namespace, summary: pd.DataFrame) -> None:
    payload = {
        "instrument": INSTRUMENT,
        "timezone": TZ,
        "input_csv": args.in_csv,
        "out_dir": args.out_dir,
        "session_rule": "date(bar_end in Europe/Moscow)",
        "gap_based_partition_used": False,
        "trade_session_date_remap_used": False,
        "saturday_rule": "each traded Saturday calendar date is a separate D1 session",
        "summary": {str(r["metric"]): r["value"] for _, r in summary.iterrows()},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _write_report(path: str, summary: pd.DataFrame) -> None:
    lines = []
    lines.append("# USDRUBF calendar-date session map package")
    lines.append("")
    lines.append("## Contract applied")
    lines.append("")
    lines.append("- D1 session rule: group bars by date(bar_end in Europe/Moscow)")
    lines.append("- Saturday bars, when present, remain a separate D1 session for that Saturday")
    lines.append("- trade_session_date remapping is not used in this branch")
    lines.append("- gap > 6h segmentation is not used for D1 construction in this branch")
    lines.append("- scope: research-only session map materialization")
    lines.append("")
    lines.append("## Decision")
    lines.append("")
    for _, row in summary.iterrows():
        lines.append("- " + str(row["metric"]) + ": " + str(row["value"]))
    lines.append("")
    lines.append("## Label note")
    lines.append("")
    lines.append("This package materializes the branch-approved D1 session map only. It does not recompute primary event-anchored labels or secondary delayed execution-compatible labels.")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", default=DEFAULT_IN_CSV)
    ap.add_argument("--out_dir", default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    bars = _load_bars(args.in_csv)
    session_map, sessions = _build_calendar_session_map(bars)
    summary = _build_summary(bars, sessions)

    _ensure_dir(args.out_dir)
    session_map.to_csv(os.path.join(args.out_dir, "USDRUBF_calendar_session_map_v1.csv"), index=False)
    sessions.to_csv(os.path.join(args.out_dir, "calendar_sessions_v1.csv"), index=False)
    summary.to_csv(os.path.join(args.out_dir, "calendar_session_summary_v1.csv"), index=False)
    _write_metadata(os.path.join(args.out_dir, "metadata.json"), args, summary)
    _write_report(os.path.join(args.out_dir, "report.md"), summary)

    print("OUT_DIR=" + args.out_dir)
    print("SESSION_RULE=date(bar_end in Europe/Moscow)")
    print("GAP_BASED_PARTITION_USED=0")
    print("TRADE_SESSION_DATE_REMAP_USED=0")
    print("CALENDAR_SESSIONS=" + str(len(sessions)))
    print("SATURDAY_SESSIONS=" + str(int(sessions["is_saturday"].sum())))
    print("DECISION=canonical_calendar_date_session_map_materialized")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        print("ERROR: " + str(e), file=sys.stderr)
        raise SystemExit(1)
