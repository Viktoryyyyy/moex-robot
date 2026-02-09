import pandas as pd
import sys

PATH = "data/research/ema_pnl_day.csv"

def main():
    df = pd.read_csv(PATH)

    required = {"date", "pnl_day", "EMA_EDGE_DAY"}
    missing = required - set(df.columns)
    if missing:
        sys.exit(f"ERROR: missing columns {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["EMA_EDGE_DAY"] = pd.to_numeric(df["EMA_EDGE_DAY"], errors="coerce")

    if df["date"].isna().any():
        sys.exit("ERROR: NaN dates found")

    if df["EMA_EDGE_DAY"].isna().any():
        sys.exit("ERROR: NaN EMA_EDGE_DAY found")

    uniq = set(df["EMA_EDGE_DAY"].unique())
    if not uniq.issubset({0, 1}):
        sys.exit(f"ERROR: EMA_EDGE_DAY not binary: {uniq}")

    df = df.sort_values("date")

    n = len(df)
    n_g = int((df["EMA_EDGE_DAY"] == 1).sum())
    n_b = int((df["EMA_EDGE_DAY"] == 0).sum())

    print("=== STAGE 1 COMPLETE: EMA EDGE DAY LABELS ===")
    print(f"Period: {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"Total days: {n}")
    print(f"GOOD (EMA_EDGE_DAY=1): {n_g} ({n_g/n:.1%})")
    print(f"BAD  (EMA_EDGE_DAY=0): {n_b} ({n_b/n:.1%})")
    print(f"Source file: {PATH}")
    print("STATUS: STAGE 1 COMPLETE")

if __name__ == "__main__":
    main()
