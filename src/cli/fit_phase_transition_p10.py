import json
from datetime import date

import pandas as pd


INPUT = "data/research/day_metrics_from_master.csv"
CONFIG = "config/phase_transition_p10.json"

TRAIN_START = date(2020, 1, 1)
TRAIN_END = date(2023, 12, 31)


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def main() -> None:
    df = pd.read_csv(INPUT)
    if "date" not in df.columns:
        raise SystemExit("missing date column: date")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    for c in ["trend_ratio", "rel_range"]:
        if c not in df.columns:
            raise SystemExit(f"missing required base column: {c}")

    trend_ratio = _to_num(df["trend_ratio"])
    rel_range = _to_num(df["rel_range"])

    # Derived D-1 features
    df["trend_ratio_yday"] = trend_ratio.shift(1)
    df["rel_range_yday"] = rel_range.shift(1)

    x_yday = rel_range.shift(1)
    mu_yday = x_yday.expanding().mean()
    sigma_yday = x_yday.expanding().std(ddof=0)
    df["vol_z_yday"] = (x_yday - mu_yday) / sigma_yday

    df["date_d"] = df["date"].dt.date
    mask = (df["date_d"] >= TRAIN_START) & (df["date_d"] <= TRAIN_END)

    tr = df.loc[mask, "trend_ratio_yday"].dropna()
    rr = df.loc[mask, "rel_range_yday"].dropna()
    vz = df.loc[mask, "vol_z_yday"].dropna()

    if len(tr) == 0 or len(rr) == 0 or len(vz) == 0:
        raise SystemExit("empty train slice after filtering (check INPUT coverage)")

    p10_tr = float(tr.quantile(0.10))
    p10_rr = float(rr.quantile(0.10))
    p10_vz = float(vz.quantile(0.10))

    with open(CONFIG, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    cfg["train_window"] = "2020-01-01..2023-12-31"
    cfg["quantile"] = 0.10
    cfg["p10_trend_ratio"] = p10_tr
    cfg["p10_rel_range"] = p10_rr
    cfg["p10_vol_z"] = p10_vz

    with open(CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    print("OK: P10 fitted and frozen")
    print(f"p10_trend_ratio={p10_tr}")
    print(f"p10_rel_range={p10_rr}")
    print(f"p10_vol_z={p10_vz}")


if __name__ == "__main__":
    main()
