#!/usr/bin/env python3
import os, sys, subprocess, pandas as pd
from datetime import date, timedelta

def drange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)        # YYYY-MM-DD, напр. 2025-10-27
    ap.add_argument("--end",   required=True)        # YYYY-MM-DD, напр. 2025-10-31
    ap.add_argument("--futs",  required=True)        # напр. SiZ5
    ap.add_argument("--ma", type=int, default=20)
    ap.add_argument("--smooth", type=int, default=12)
    ap.add_argument("--out", default=None)           # итоговый CSV
    args = ap.parse_args()

    d1 = date.fromisoformat(args.start)
    d2 = date.fromisoformat(args.end)
    if d2 < d1:
        print("end < start", file=sys.stderr); sys.exit(2)

    produced = []
    for d in drange(d1, d2):
        if d.weekday() >= 5:   # пропуск выходных
            continue
        cmd = [sys.executable, "run_merge_one_day.py",
               "--date", d.isoformat(),
               "--futs", args.futs,
               "--ma", str(args.ma),
               "--smooth", str(args.smooth)]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            sys.stderr.write(f"FAIL {d} ({args.futs})\n")
            sys.stderr.write(r.stdout)
            sys.stderr.write(r.stderr)
            continue
        print(r.stdout.strip())
        f = f"si_5m_{d.isoformat()}.csv"
        if os.path.exists(f):
            produced.append(f)

    if not produced:
        print("Nothing to merge.", file=sys.stderr); sys.exit(2)

    frames = []
    for f in produced:
        try:
            frames.append(pd.read_csv(f))
        except Exception as e:
            print(f"WARN skip {f}: {e}", file=sys.stderr)

    if not frames:
        print("No frames merged.", file=sys.stderr); sys.exit(2)

    big = pd.concat(frames, ignore_index=True)
    out = args.out or f"si_5m_{args.start}_{args.end}.csv"
    big.to_csv(out, index=False)
    print(f"OK merged -> {out} rows={len(big)} days={len(frames)}")

if __name__ == "__main__":
    main()
