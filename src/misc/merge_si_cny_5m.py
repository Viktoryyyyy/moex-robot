#!/usr/bin/env python3
import sys, argparse, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--si",  required=True, help="CSV с Si 5m (chain)")
    ap.add_argument("--fx",  required=True, help="CSV с CNYRUB_TOM 5m")
    ap.add_argument("--out", required=True, help="имя выходного CSV")
    args = ap.parse_args()

    si = pd.read_csv(args.si)
    fx = pd.read_csv(args.fx)

    if "end" not in si.columns:
        sys.stderr.write("ERROR: в файле Si нет колонки 'end'\n")
        sys.exit(1)
    if "end" not in fx.columns:
        sys.stderr.write("ERROR: в файле FX нет колонки 'end'\n")
        sys.exit(1)

    si["end_dt"] = pd.to_datetime(si["end"], errors="coerce")
    fx["end_dt"] = pd.to_datetime(fx["end"], errors="coerce")

    si = si.dropna(subset=["end_dt"])
    fx = fx.dropna(subset=["end_dt"])

    si = si.sort_values("end_dt")
    fx = fx.sort_values("end_dt")

    m = si.merge(
        fx,
        on="end_dt",
        how="inner",
        suffixes=("_si", "_cny")
    )

    m = m.sort_values("end_dt").reset_index(drop=True)

    # Общий end в человекочитаемом виде
    m["end"] = m["end_dt"].astype(str)
    cols = ["end"] + [c for c in m.columns if c not in ("end_dt","end")]

    m[cols].to_csv(args.out, index=False)
    print(f"OK: {args.out} rows={len(m)}")

if __name__ == "__main__":
    main()
