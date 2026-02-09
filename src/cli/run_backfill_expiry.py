#!/usr/bin/env python3
import os, sys, subprocess, pandas as pd
from datetime import date, timedelta

QMONTHS = (3, 6, 9, 12)
QCODE = {3:"H", 6:"M", 9:"U", 12:"Z"}

def third_thursday(year, month):
    d1 = date(year, month, 1)
    # Monday=0 ... Thu=3
    off = (3 - d1.weekday()) % 7
    return d1 + timedelta(days=off + 14)

def resolve_contract(d: date) -> str:
    # d ∈ (prev_expiry, this_expiry] → текущий квартал
    for y in range(d.year - 1, d.year + 2):
        for m in QMONTHS:
            ex = third_thursday(y, m)
            if d <= ex:
                return f"Si{QCODE[m]}{str(y)[-1]}"
    return f"SiZ{str(d.year)[-1]}"

def neighbor_contracts(d: date) -> list[str]:
    # [текущий, предыдущий квартал, следующий квартал] — чтобы пробовать альтернативы при пустом дне
    chain = []
    for y in range(d.year - 1, d.year + 2):
        for m in QMONTHS:
            chain.append((third_thursday(y, m), QCODE[m], y))
    chain.sort(key=lambda x: x[0])

    idx = None
    for i, (ex, code, y) in enumerate(chain):
        if d <= ex:
            idx = i; break
    if idx is None:
        idx = len(chain) - 1

    cur = f"Si{chain[idx][1]}{str(chain[idx][2])[-1]}"
    prev_c = f"Si{chain[idx-1][1]}{str(chain[idx-1][2])[-1]}" if idx - 1 >= 0 else None
    next_c = f"Si{chain[idx+1][1]}{str(chain[idx+1][2])[-1]}" if idx + 1 < len(chain) else None

    out, seen = [], set()
    for k in [cur, prev_c, next_c]:
        if k and k not in seen:
            out.append(k); seen.add(k)
    return out

def drange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)          # YYYY-MM-DD
    ap.add_argument("--end",   required=True)          # YYYY-MM-DD
    ap.add_argument("--ma", type=int, default=20)      # зарезервировано (если вернём расширенные фичи)
    ap.add_argument("--smooth", type=int, default=12)  # зарезервировано
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    d1 = date.fromisoformat(args.start)
    d2 = date.fromisoformat(args.end)
    if d2 < d1:
        print("ERR: end < start", file=sys.stderr); sys.exit(2)

    produced = []
    for d in drange(d1, d2):
        # пропуск выходных
        if d.weekday() >= 5:
            continue

        tried_contracts = neighbor_contracts(d)
        ok_day = False

        for futs in tried_contracts:
            cmd = [sys.executable, "run_merge_one_day.py", "--date", d.isoformat(), "--futs", futs]
            for attempt in range(1, 4):
                print(f">> {d} {futs} try {attempt}", flush=True)
                r = subprocess.run(cmd, capture_output=True, text=True)
                if r.returncode == 0 and os.path.exists(f"si_5m_{d}.csv"):
                    print(r.stdout.strip())
                    produced.append(f"si_5m_{d}.csv")
                    ok_day = True
                    break
                else:
                    sys.stderr.write(r.stdout)
                    sys.stderr.write(r.stderr)
            if ok_day:
                break

        if not ok_day:
            print(f"FAIL {d}: all contracts tried {tried_contracts}", file=sys.stderr)

    if not produced:
        print("No daily files produced — nothing to merge.", file=sys.stderr)
        sys.exit(2)

    # Склейка
    frames = []
    for f in produced:
        try:
            frames.append(pd.read_csv(f))
        except Exception as e:
            print(f"WARN merge skip {f}: {e}", file=sys.stderr)

    if not frames:
        print("Nothing merged.", file=sys.stderr)
        sys.exit(2)

    big = pd.concat(frames, ignore_index=True)
    out = args.out or f"si_5m_{args.start}_{args.end}.csv"
    big.to_csv(out, index=False)
    print(f"OK merged -> {out} rows={len(big)} days={len(frames)}")

if __name__ == "__main__":
    main()
