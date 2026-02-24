import argparse
from pathlib import Path
import numpy as np
import pandas as pd

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", default="data/research/levels_si_d1_events.csv", help="level_events_v1.csv")
    ap.add_argument("--out", default="data/research/level_probs_v1.csv", help="output csv")
    ap.add_argument("--alpha", type=float, default=2.0, help="Bayes prior alpha")
    ap.add_argument("--beta", type=float, default=2.0, help="Bayes prior beta")
    args = ap.parse_args()

    events_path = Path(args.events)
    if not events_path.exists():
        raise RuntimeError("Events not found: " + str(events_path))

    df = pd.read_csv(events_path, parse_dates=["TRADEDATE"])
    required = ["level_id", "level_price", "direction", "outcome", "regime_day"]
    for c in required:
        if c not in df.columns:
            raise RuntimeError("Missing column: " + c)

    df = df.dropna(subset=["regime_day"])
    if df.empty:
        raise RuntimeError("No rows with regime_day (dropna removed all rows). Provide regime_day file and rebuild events.")

    df["is_break"] = (df["outcome"] == "break_confirmed").astype(int)
    df["is_false"] = (df["outcome"] == "false_break").astype(int)

    gcols = ["level_id", "level_price", "direction", "regime_day"]
    agg = df.groupby(gcols).agg(
        n=("outcome", "size"),
        n_break=("is_break", "sum"),
        n_false=("is_false", "sum"),
        first_date=("TRADEDATE", "min"),
        last_date=("TRADEDATE", "max"),
    ).reset_index()

    a = float(args.alpha)
    b = float(args.beta)

    # Posterior mean for Bernoulli with Beta(a,b)
    agg["p_break"] = (agg["n_break"] + a) / (agg["n"] + a + b)
    agg["p_false"] = (agg["n_false"] + a) / (agg["n"] + a + b)

    # sanity: p_break + p_false should be close to 1 (only two outcomes), but keep independent in case of future labels
    agg["p_sum"] = agg["p_break"] + agg["p_false"]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    agg.sort_values(["level_id", "regime_day", "direction"]).to_csv(out_path, index=False)

    print("OK")
    print("events_in:", str(events_path), "rows=" + str(len(df)))
    print("groups_out:", str(out_path), "rows=" + str(len(agg)))
    print("alpha_beta:", a, b)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
