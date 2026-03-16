from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple


MIN_HISTORY_DAYS = 20


@dataclass(frozen=True)
class DayMetrics:
    yday_date: date
    rel_range: float
    trend_ratio: float


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def die(msg: str, code: int = 2) -> None:
    eprint(msg)
    raise SystemExit(code)


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)


def atomic_write_text(path: str, text: str) -> None:
    ensure_dir_for_file(path)
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def atomic_write_csv(path: str, header: List[str], rows: List[Dict[str, str]]) -> None:
    ensure_dir_for_file(path)
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def parse_day_metrics(path: str) -> DayMetrics:
    if not os.path.exists(path):
        die(f"day metrics not found: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        expected = {"date", "rel_range", "trend_ratio"}
        if reader.fieldnames is None:
            die(f"day metrics empty header: {path}")
        got = set(reader.fieldnames)
        if got != expected:
            die(f"day metrics columns mismatch: got={sorted(got)} expected={sorted(expected)} path={path}")
        rows = list(reader)

    if len(rows) != 1:
        die(f"day metrics must have exactly 1 data row, got={len(rows)} path={path}")

    r = rows[0]
    try:
        yday = date.fromisoformat((r.get("date") or "").strip())
    except Exception:
        die(f"invalid yday date in day metrics: {r.get('date')} path={path}")

    try:
        rr = float((r.get("rel_range") or "").strip())
        tr = float((r.get("trend_ratio") or "").strip())
    except Exception:
        die(f"invalid floats in day metrics row: {r} path={path}")

    return DayMetrics(yday_date=yday, rel_range=rr, trend_ratio=tr)


def load_thresholds(path: str) -> Dict[str, float]:
    if not os.path.exists(path):
        die(f"config not found: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception as ex:
        die(f"failed to read json config: {path} err={ex}")

    keys = ["p10_trend_ratio", "p10_rel_range", "p10_vol_z"]
    out: Dict[str, float] = {}
    for k in keys:
        if k not in obj:
            die(f"missing threshold key in config: {k} path={path}")
        try:
            out[k] = float(obj[k])
        except Exception:
            die(f"invalid threshold value: {k}={obj.get(k)} path={path}")
    return out


def read_history(path: str) -> Tuple[bool, List[Tuple[date, float]]]:
    if not os.path.exists(path):
        return False, []

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        expected = {"date", "rel_range"}
        if reader.fieldnames is None:
            die(f"history empty header: {path}")
        got = set(reader.fieldnames)
        if got != expected:
            die(f"history columns mismatch: got={sorted(got)} expected={sorted(expected)} path={path}")

        items: List[Tuple[date, float]] = []
        for r in reader:
            try:
                d = date.fromisoformat((r.get("date") or "").strip())
                rr = float((r.get("rel_range") or "").strip())
            except Exception:
                die(f"invalid history row: {r} path={path}")
            items.append((d, rr))

    items.sort(key=lambda x: x[0])
    return True, items


def upsert_history(path: str, yday: date, rel_range_yday: float) -> Tuple[bool, List[Tuple[date, float]]]:
    existed, items = read_history(path)

    if not existed:
        atomic_write_csv(path, ["date", "rel_range"], [])
        return False, []

    if any(d == yday for d, _ in items):
        return True, items

    items.append((yday, rel_range_yday))
    items.sort(key=lambda x: x[0])

    rows = [{"date": d.isoformat(), "rel_range": f"{rr:.18g}"} for d, rr in items]
    atomic_write_csv(path, ["date", "rel_range"], rows)
    return True, items



def validate_canonical_master_for_bootstrap(master_path, yday):
    import csv
    from datetime import timedelta
    if not os.path.exists(master_path):
        die("FAIL_CLOSED: canonical master not found: " + master_path)
    with open(master_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if r.fieldnames is None:
            die("FAIL_CLOSED: canonical master empty header: " + master_path)
        req = {"end", "high", "low", "close"}
        got = set(r.fieldnames)
        if not req.issubset(got):
            die("FAIL_CLOSED: canonical master columns missing: required=" + str(sorted(req)) + " got=" + str(sorted(got)) + " path=" + master_path)
        daily = {}
        for row in r:
            try:
                end_ts = (row.get("end") or "").strip()
                d = date.fromisoformat(end_ts[:10])
                hi = float((row.get("high") or "").strip())
                lo = float((row.get("low") or "").strip())
                cl = float((row.get("close") or "").strip())
            except Exception:
                die("FAIL_CLOSED: invalid canonical master row: " + str(row) + " path=" + master_path)
            cur = daily.get(d)
            if cur is None:
                daily[d] = {"high": hi, "low": lo, "close": cl, "end": end_ts}
            else:
                cur["high"] = max(float(cur["high"]), hi)
                cur["low"] = min(float(cur["low"]), lo)
                if end_ts >= str(cur["end"]):
                    cur["close"] = cl
                    cur["end"] = end_ts
    if not daily:
        die("FAIL_CLOSED: canonical master has no rows: " + master_path)
    all_days = sorted(daily.keys())
    required_last_day = yday - timedelta(days=1)
    freshest_day = all_days[-1]
    if freshest_day < required_last_day:
        die("FAIL_CLOSED: canonical master stale for gate bootstrap: freshest=" + freshest_day.isoformat() + " required>=" + required_last_day.isoformat() + " path=" + master_path)
    if required_last_day not in daily:
        die("FAIL_CLOSED: canonical master missing required D-1 day for gate bootstrap: required=" + required_last_day.isoformat() + " path=" + master_path)
    history_days = [d for d in all_days if d < yday]
    if len(history_days) < MIN_HISTORY_DAYS:
        die("FAIL_CLOSED: canonical master coverage insufficient for gate bootstrap: have=" + str(len(history_days)) + " required>=" + str(MIN_HISTORY_DAYS) + " yday=" + yday.isoformat() + " path=" + master_path)
    return daily

def seed_history_from_canonical_master(path, daily, yday):
    rows = []
    for d in sorted(daily.keys()):
        if d >= yday:
            break
        hi = float(daily[d]["high"])
        lo = float(daily[d]["low"])
        cl = float(daily[d]["close"])
        rr = 0.0 if cl == 0.0 else (hi - lo) / cl
        rows.append({"date": d.isoformat(), "rel_range": str(rr)})
    atomic_write_csv(path, ["date", "rel_range"], rows)

def resolve_canonical_master_path():
    mp = os.getenv("MASTER_PATH", "").strip()
    if not mp:
        die("FAIL_CLOSED: history file missing and MASTER_PATH is not set")
    return mp

def ensure_history_seeded(path, yday):
    if os.path.exists(path):
        return
    mp = resolve_canonical_master_path()
    daily = validate_canonical_master_for_bootstrap(mp, yday)
    seed_history_from_canonical_master(path, daily, yday)

def compute_vol_z(yday: date, rel_range_yday: float, items: List[Tuple[date, float]]) -> float:
    hist = [rr for d, rr in items if d < yday]
    if len(hist) < MIN_HISTORY_DAYS:
        die(
            f"FAIL_CLOSED: insufficient history (<{MIN_HISTORY_DAYS}) for vol_z: "
            f"have={len(hist)} yday={yday.isoformat()}"
        )

    m = mean(hist)
    s = pstdev(hist)
    if s == 0.0:
        die(f"FAIL_CLOSED: zero std in history for vol_z: yday={yday.isoformat()}")

    return (rel_range_yday - m) / s


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-day", required=True)
    ap.add_argument("--in-history", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-history", required=True)
    args = ap.parse_args(argv)

    ensure_dir_for_file(args.out_json)
    ensure_dir_for_file(args.out_history)

    dm = parse_day_metrics(args.in_day)
    thr = load_thresholds(args.config)

    ensure_history_seeded(args.in_history, dm.yday_date)

    existed, items = upsert_history(args.in_history, dm.yday_date, dm.rel_range)
    if not existed:
        die("FAIL_CLOSED: rel_range_history.csv created, but no prior history available to compute vol_z (first day)")

    vol_z = compute_vol_z(dm.yday_date, dm.rel_range, items)

    risk = 1 if (
        dm.trend_ratio <= thr["p10_trend_ratio"]
        or dm.rel_range <= thr["p10_rel_range"]
        or vol_z <= thr["p10_vol_z"]
    ) else 0

    payload = {
        "date": date.today().isoformat(),
        "phase_transition_risk": int(risk),
        "inputs": {
            "yday_date": dm.yday_date.isoformat(),
            "rel_range_yday": dm.rel_range,
            "trend_ratio_yday": dm.trend_ratio,
            "vol_z_yday": vol_z,
        },
        "thresholds": {
            "p10_trend_ratio": thr["p10_trend_ratio"],
            "p10_rel_range": thr["p10_rel_range"],
            "p10_vol_z": thr["p10_vol_z"],
        },
        "source": "MOEX->daily_metrics_builder->day_metrics_D-1.csv",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    atomic_write_text(args.out_json, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

    print(
        "DATE={d} YDAY={y} rr={rr:.6g} tr={tr:.6g} vz={vz:.6g} thr=({ttr:.6g},{trr:.6g},{tvz:.6g}) risk={risk}".format(
            d=payload["date"],
            y=dm.yday_date.isoformat(),
            rr=dm.rel_range,
            tr=dm.trend_ratio,
            vz=vol_z,
            ttr=thr["p10_trend_ratio"],
            trr=thr["p10_rel_range"],
            tvz=thr["p10_vol_z"],
            risk=risk,
        )
    )

    if os.path.abspath(args.out_history) != os.path.abspath(args.in_history):
        _, items2 = read_history(args.in_history)
        rows2 = [{"date": d.isoformat(), "rel_range": f"{rr:.18g}"} for d, rr in items2]
        atomic_write_csv(args.out_history, ["date", "rel_range"], rows2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
