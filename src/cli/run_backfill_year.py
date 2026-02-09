#!/usr/bin/env python3
import os, sys, subprocess, pandas as pd
from datetime import date, timedelta

# экспирации: третий четверг квартала
expiry = {
    "SiH5": date(2025, 3, 20),
    "SiM5": date(2025, 6, 19),
    "SiU5": date(2025, 9, 18),
    "SiZ5": date(2025, 12, 18),
}

def get_contract(d: date) -> str:
    for k, v in sorted(expiry.items(), key=lambda x: x[1]):
        if d <= v:
            return k
    return "SiZ5"

def drange(d1, d2):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def main():
    start = date(2025,1,1)
    end   = date(2025,10,31)
    ma = 20
    smooth = 12

    produced = []
    for d in drange(start, end):
        if d.weekday() >= 5:
            continue
        futs = get_contract(d)
        cmd = [sys.executable, "run_merge_one_day.py",
               "--date", d.isoformat(),
               "--futs", futs,
               "--ma", str(ma),
               "--smooth", str(smooth)]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            sys.stderr.write(f"FAIL {d} ({futs})\n")
            continue
        print(r.stdout.strip())
        f = f"si_5m_{d.isoformat()}.csv"
        if os.path.exists(f):
            produced.append(f)

    if not produced:
        print("No daily files created", file=sys.stderr); sys.exit(2)

    frames = []
    for f in produced:
        try:
            frames.append(pd.read_csv(f))
        except Exception as e:
            print(f"WARN skip {f}: {e}", file=sys.stderr)

    if not frames:
        print("No frames to merge", file=sys.stderr); sys.exit(2)

    big = pd.concat(frames, ignore_index=True)

    # Шаг 1: сортировка и удаление дублей по datetime
    if "datetime" in big.columns:
        big["__dt"] = pd.to_datetime(big["datetime"], errors="coerce")
        before = len(big)
        big = (big.sort_values("__dt")
                  .drop_duplicates(subset=["__dt"], keep="last")
                  .drop(columns="__dt"))
        after = len(big)
        print(f"Dedup by datetime: removed {before-after} duplicates")

    out = f"si_5m_2025-01-01_2025-10-31.csv"
    big.to_csv(out, index=False)
    print(f"OK merged -> {out} rows={len(big)} days={len(frames)}")

if __name__ == "__main__":
    main()
