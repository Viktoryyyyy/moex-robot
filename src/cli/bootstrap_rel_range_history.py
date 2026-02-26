from __future__ import annotations

import os
import sys
import csv
import io
from datetime import date, timedelta
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr

MIN_DAYS = 60
MAX_LOOKBACK_DAYS = 365
HISTORY_PATH = Path("data/gate/rel_range_history.csv")

def _die(msg: str, code: int = 2) -> None:
    print("[CRIT] " + msg)
    raise SystemExit(code)

def _run_daily_metrics(trade_date: str) -> dict:
    from src.cli.daily_metrics_builder import main as build_daily
    os.makedirs("data/tmp", exist_ok=True)
    out_5m = f"data/tmp/5m_{trade_date}.csv"
    out_day = f"data/tmp/day_{trade_date}.csv"
    buf = io.StringIO()
    argv0 = list(sys.argv)
    try:
        sys.argv = ["daily_metrics_builder", "--key", "Si", "--date", trade_date, "--out-5m", out_5m, "--out-day", out_day]
        with redirect_stdout(buf), redirect_stderr(buf):
            build_daily()
    except SystemExit as e:
        txt = buf.getvalue()
        if "5m dataframe empty" in txt:
            return {"ok": False, "reason": "empty", "date": trade_date}
        _die("daily_metrics_builder failed: date=" + trade_date + " output=" + txt.strip()[:400])
    finally:
        sys.argv = argv0
    try:
        with open(out_day, "r", encoding="utf-8", newline="") as f:
            r = list(csv.DictReader(f))
        if len(r) != 1:
            _die("unexpected day_metrics rows: date=" + trade_date + " rows=" + str(len(r)))
        return {"ok": True, "date": r[0]["date"], "rel_range": r[0]["rel_range"]}
    except Exception as e:
        _die("failed to read day_metrics csv: date=" + trade_date + " err=" + str(e))

def main() -> None:
    asof = date.today() - timedelta(days=1)
    rows = []
    cur = asof
    tries = 0
    while len(rows) < MIN_DAYS and tries < MAX_LOOKBACK_DAYS:
        d = cur.isoformat()
        res = _run_daily_metrics(d)
        if res.get("ok"):
            rows.append((res["date"], res["rel_range"]))
        cur = cur - timedelta(days=1)
        tries += 1
    if len(rows) < MIN_DAYS:
        _die("insufficient trading days collected: got=" + str(len(rows)) + " need=" + str(MIN_DAYS))
    rows = list(reversed(rows))
    os.makedirs(HISTORY_PATH.parent, exist_ok=True)
    with open(HISTORY_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "rel_range"])
        for d, rr in rows:
            w.writerow([d, rr])
    print("[Bootstrap] history_written", len(rows), "first", rows[0][0], "last", rows[-1][0])

if __name__ == "__main__":
    main()
