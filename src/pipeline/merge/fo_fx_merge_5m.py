#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Merge FO (Si) 5m and FX (CNY) 5m by end timestamp."
    )
    p.add_argument(
        "--fo",
        required=True,
        help="Path to FO 5m CSV (Si chain), e.g. data/fo/si_5m_2020-01-01_2025-11-13.csv",
    )
    p.add_argument(
        "--fx",
        required=True,
        help="Path to FX 5m CSV (CNYRUB_TOM), e.g. data/fx/fx_5m_2020-01-01_2025-11-13_cnyrub_tom.csv",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Output CSV path, e.g. data/master/si_cny_5m_2020-01-01_2025-11-13.csv",
    )
    return p.parse_args()


def load_5m(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        print(f"[ERROR] {label} file not found: {path}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(path)
    if "end" not in df.columns:
        print(
            f"[ERROR] {label} file {path} has no 'end' column. Columns: {list(df.columns)}",
            file=sys.stderr,
        )
        sys.exit(1)
    df["end"] = pd.to_datetime(df["end"])
    df = df.sort_values("end").drop_duplicates("end", keep="last")
    return df


def main() -> None:
    args = parse_args()
    fo_path = Path(args.fo)
    fx_path = Path(args.fx)
    out_path = Path(args.out)

    fo = load_5m(fo_path, "FO")
    fx = load_5m(fx_path, "FX")

    # Левый merge по Si: Si-ряд — skeleton, CNY подмешиваем где есть.
    merged = pd.merge(
        fo,
        fx,
        on="end",
        how="left",
        suffixes=("_fo", "_fx"),
    )
    merged = merged.sort_values("end")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)

    print(f"FO:  {fo_path} rows={len(fo)}")
    print(f"FX:  {fx_path} rows={len(fx)}")
    print(f"OUT: {out_path} rows={len(merged)}")
    if not merged.empty:
        print("Start:", merged["end"].min())
        print("End:  ", merged["end"].max())


if __name__ == "__main__":
    main()
