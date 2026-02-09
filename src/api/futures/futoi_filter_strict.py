#!/usr/bin/env python3
import argparse, os, pandas as pd
ap=argparse.ArgumentParser()
ap.add_argument('--ticker', required=True)
ap.add_argument('--date', required=True)
ap.add_argument('--infile', default=None)
ap.add_argument('--outfile', default=None)
a=ap.parse_args()

inf = a.infile or f"futoi_{a.ticker}_{a.date}.csv"
out = a.outfile or f"futoi_{a.ticker}_{a.date}.csv"
if not os.path.exists(inf): raise SystemExit(f"not_found {inf}")

df = pd.read_csv(inf, engine='python', sep=None, on_bad_lines='skip')
lc = {c.lower(): c for c in df.columns}
dcol = next((lc[k] for k in lc if 'date' in k or 'tradedate' in k), None)

if dcol is None:
    flt = df.tail(1).copy()
else:
    d = pd.to_datetime(df[dcol], errors='coerce').dt.date
    flt = df[d == pd.to_datetime(a.date).date()].copy()
    if flt.empty:
        flt = df.head(0).copy()

flt = flt.tail(1).copy() if len(flt) else flt
flt.to_csv(out, index=False)
print(f"{out} rows={len(flt)}")
