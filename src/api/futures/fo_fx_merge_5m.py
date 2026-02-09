#!/usr/bin/env python3
import os, sys, argparse, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fo-base", required=True, help="базовый фьючерс, напр. Si или RI")
    ap.add_argument("--fx-ticker", required=True, help="FX тикер, напр. CNYRUB_TOM или USDRUB_TOM")
    ap.add_argument("--start", required=True, help="начало периода YYYY-MM-DD")
    ap.add_argument("--end",   required=True, help="конец периода YYYY-MM-DD")
    ap.add_argument("--out",   default=None, help="имя выходного CSV (опционально)")
    args = ap.parse_args()

    start = args.start
    end   = args.end
    fo_base = args.fo_base.strip()
    fx_ticker = args.fx_ticker.strip()

    # Имя FO-файла от fo_tradestats_chain.py
    fo_file = f"{fo_base.lower()}_5m_{start}_{end}.csv"
    # Имя FX-файла от fx_5m_period.py
    fx_file = f"fx_5m_{start}_{end}_{fx_ticker.lower()}.csv"

    if not os.path.exists(fo_file):
        sys.stderr.write(f"ERROR: FO file not found: {fo_file}\n")
        sys.exit(1)
    if not os.path.exists(fx_file):
        sys.stderr.write(f"ERROR: FX file not found: {fx_file}\n")
        sys.exit(1)

    fo = pd.read_csv(fo_file)
    fx = pd.read_csv(fx_file)

    if "end" not in fo.columns:
        sys.stderr.write("ERROR: FO file has no 'end' column\n")
        sys.exit(1)
    if "end" not in fx.columns:
        sys.stderr.write("ERROR: FX file has no 'end' column\n")
        sys.exit(1)

    fo["end_dt"] = pd.to_datetime(fo["end"], errors="coerce")
    fx["end_dt"] = pd.to_datetime(fx["end"], errors="coerce")

    fo = fo.dropna(subset=["end_dt"]).sort_values("end_dt")
    fx = fx.dropna(subset=["end_dt"]).sort_values("end_dt")

    # Мёрджим по end_dt, суффиксы _fo и _fx
    merged = fo.merge(
        fx,
        on="end_dt",
        how="inner",
        suffixes=("_fo", "_fx")
    ).sort_values("end_dt").reset_index(drop=True)

    # Человекочитаемый end
    merged["end"] = merged["end_dt"].astype(str)
    cols = ["end"] + [c for c in merged.columns if c not in ("end_dt", "end")]

    out = args.out or f"{fo_base.lower()}_{fx_ticker.lower()}_5m_{start}_{end}.csv"
    merged[cols].to_csv(out, index=False)
    print(f"OK: {out} rows={len(merged)}")

if __name__ == "__main__":
    main()
