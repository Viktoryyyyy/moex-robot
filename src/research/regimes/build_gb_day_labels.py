import argparse
import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ema_day_csv", required=True,
                   help="Daily EMA PnL CSV (one row per day)")
    p.add_argument("--date_col", default="date",
                   help="Date column name (default: date)")
    p.add_argument("--pnl_col", default="EMA_PNL_RUB_NET",
                   help="Net PnL column name in RUB")
    p.add_argument("--hurdle", type=float, default=250.0,
                   help="GOOD threshold in RUB (default: 250)")
    p.add_argument("--out_csv", default="data/research/ema_edge_day_gb.csv")
    args = p.parse_args()

    df = pd.read_csv(args.ema_day_csv)

    if args.date_col not in df.columns:
        raise SystemExit(f"Missing date column: {args.date_col}")
    if args.pnl_col not in df.columns:
        raise SystemExit(f"Missing pnl column: {args.pnl_col}")

    df[args.date_col] = pd.to_datetime(df[args.date_col], errors="coerce").dt.floor("D")
    df[args.pnl_col] = pd.to_numeric(df[args.pnl_col], errors="coerce")

    df = df.dropna(subset=[args.date_col, args.pnl_col]).copy()
    df = df.sort_values(args.date_col)

    df["GB"] = df[args.pnl_col].apply(lambda x: "G" if x >= args.hurdle else "B")

    out = df[[args.date_col, args.pnl_col, "GB"]].copy()
    out.columns = ["date", "pnl_net_rub", "GB"]

    out.to_csv(args.out_csv, index=False)

    # --- REPORT ---
    n_days = len(out)
    n_g = int((out["GB"] == "G").sum())
    n_b = int((out["GB"] == "B").sum())

    print("=== EMA EDGE DAY — G/B LABELS (STAGE 1) ===")
    print(f"Period: {out['date'].min().date()} → {out['date'].max().date()}")
    print(f"Total days: {n_days}")
    print(f"GOOD (PnL ≥ {args.hurdle} ₽): {n_g} ({n_g/n_days:.1%})")
    print(f"BAD  (PnL < {args.hurdle} ₽): {n_b} ({n_b/n_days:.1%})")
    print(f"Output file: {args.out_csv}")
    print("STATUS: STAGE 1 COMPLETE")


if __name__ == "__main__":
    main()
