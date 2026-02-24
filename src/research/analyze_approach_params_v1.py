import argparse
from pathlib import Path
import numpy as np
import pandas as pd

Z_ATR = 0.25

def atr14(d1: pd.DataFrame) -> pd.Series:
    hl = d1["high"] - d1["low"]
    hc = (d1["high"] - d1["close"].shift()).abs()
    lc = (d1["low"] - d1["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(14, min_periods=5).mean()

def classify_approach(d1: pd.DataFrame, t_idx: int, L: float, direction: str, N: int, K: int, X: float):
    Z = Z_ATR * d1.loc[t_idx, "ATR14"]
    if pd.isna(Z):
        return None

    loK = max(0, t_idx - K)
    loN = max(0, t_idx - N)

    # rejection: touch in [t-K, t-1] then excursion >= X*ATR before t
    for d1_idx in range(loK, t_idx):
        low = d1.loc[d1_idx, "low"]
        high = d1.loc[d1_idx, "high"]
        close = d1.loc[d1_idx, "close"]
        atr = d1.loc[d1_idx, "ATR14"]
        if pd.isna(atr):
            continue

        touched = (low <= L <= high) or (abs(close - L) <= Z)
        if not touched:
            continue

        for d2_idx in range(d1_idx + 1, t_idx):
            atr2 = d1.loc[d2_idx, "ATR14"]
            if pd.isna(atr2):
                continue
            if direction == "up":
                excursion = L - d1.loc[d2_idx, "low"]
            else:
                excursion = d1.loc[d2_idx, "high"] - L
            if excursion >= X * atr2:
                return "rejection"

    # retest: had touch in [t-K, t-1] and no excursion >= X*ATR in that window
    had_touch = False
    for d in range(loK, t_idx):
        low = d1.loc[d, "low"]
        high = d1.loc[d, "high"]
        close = d1.loc[d, "close"]
        atr = d1.loc[d, "ATR14"]
        if pd.isna(atr):
            continue

        if (low <= L <= high) or (abs(close - L) <= Z):
            had_touch = True

        if direction == "up":
            excursion = L - low
        else:
            excursion = high - L

        if excursion >= X * atr:
            had_touch = False
            break

    if had_touch:
        return "retest"

    # direct: no touch/near in last N days
    for d in range(loN, t_idx):
        low = d1.loc[d, "low"]
        high = d1.loc[d, "high"]
        close = d1.loc[d, "close"]
        if (low <= L <= high) or (abs(close - L) <= Z):
            return None
    return "direct"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--events", default="data/research/levels_si_d1_events.csv")
    ap.add_argument("--regime", default="data/research/regime_day_r1r4.csv")
    ap.add_argument("--out", default="data/research/approach_param_grid_v1.csv")
    ap.add_argument("--alpha", type=float, default=2.0)
    ap.add_argument("--beta", type=float, default=2.0)
    ap.add_argument("--Nset", default="5,10,20")
    ap.add_argument("--Kset", default="3,5,10")
    ap.add_argument("--Xset", default="0.5,1.0,1.5")
    args = ap.parse_args()

    master_path = Path(args.master)
    if not master_path.exists():
        raise RuntimeError("Master not found: " + str(master_path))

    events_path = Path(args.events)
    if not events_path.exists():
        raise RuntimeError("Events not found: " + str(events_path))

    regime_path = Path(args.regime)
    if not regime_path.exists():
        raise RuntimeError("Regime not found: " + str(regime_path))

    # Build D1 from master (FO columns)
    df5 = pd.read_csv(master_path)
    need = ["end", "open_fo", "high_fo", "low_fo", "close_fo"]
    for c in need:
        if c not in df5.columns:
            raise RuntimeError("Master missing column: " + c)

    df5["end_dt"] = pd.to_datetime(df5["end"])
    df5 = df5.sort_values("end_dt")
    df5["TRADEDATE"] = df5["end_dt"].dt.date

    d1 = (
        df5.groupby("TRADEDATE")
           .agg(open=("open_fo", "first"),
                high=("high_fo", "max"),
                low=("low_fo", "min"),
                close=("close_fo", "last"))
           .reset_index()
    )
    d1["TRADEDATE"] = pd.to_datetime(d1["TRADEDATE"])
    d1["ATR14"] = atr14(d1)

    reg = pd.read_csv(regime_path, parse_dates=["TRADEDATE"])
    if "regime_day" not in reg.columns:
        raise RuntimeError("Regime missing column: regime_day")
    d1 = d1.merge(reg[["TRADEDATE", "regime_day"]], on="TRADEDATE", how="left")

    # Map TRADEDATE -> index
    idx_map = {d: i for i, d in enumerate(d1["TRADEDATE"].tolist())}

    ev = pd.read_csv(events_path, parse_dates=["TRADEDATE"])
    req_ev = ["TRADEDATE", "level_id", "level_price", "direction", "outcome", "regime_day"]
    for c in req_ev:
        if c not in ev.columns:
            raise RuntimeError("Events missing column: " + c)

    ev = ev.dropna(subset=["regime_day"])
    if ev.empty:
        raise RuntimeError("Events empty after dropping NA regime_day")

    # Precompute per-event core fields
    t_indices = []
    for d in ev["TRADEDATE"].tolist():
        t_indices.append(idx_map.get(d, None))
    ev = ev.assign(_t_idx=t_indices)
    ev = ev[ev["_t_idx"].notna()].copy()
    ev["_t_idx"] = ev["_t_idx"].astype(int)

    ev["is_break"] = (ev["outcome"] == "break_confirmed").astype(int)
    ev["is_false"] = (ev["outcome"] == "false_break").astype(int)

    Nset = [int(x.strip()) for x in args.Nset.split(",") if x.strip()]
    Kset = [int(x.strip()) for x in args.Kset.split(",") if x.strip()]
    Xset = [float(x.strip()) for x in args.Xset.split(",") if x.strip()]

    rows = []
    a = float(args.alpha)
    b = float(args.beta)

    for N in Nset:
        for K in Kset:
            for X in Xset:
                states = []
                for t_idx, L, direction in zip(ev["_t_idx"].tolist(), ev["level_price"].tolist(), ev["direction"].tolist()):
                    states.append(classify_approach(d1, int(t_idx), float(L), str(direction), int(N), int(K), float(X)))

                tmp = ev.copy()
                tmp["approach_state"] = states
                tmp = tmp.dropna(subset=["approach_state"])

                if tmp.empty:
                    continue

                gcols = ["level_id", "level_price", "direction", "regime_day", "approach_state"]
                agg = tmp.groupby(gcols).agg(
                    n=("outcome", "size"),
                    n_break=("is_break", "sum"),
                    n_false=("is_false", "sum"),
                    first_date=("TRADEDATE", "min"),
                    last_date=("TRADEDATE", "max"),
                ).reset_index()

                agg["N"] = N
                agg["K"] = K
                agg["X"] = X
                agg["p_break"] = (agg["n_break"] + a) / (agg["n"] + a + b)
                agg["p_false"] = (agg["n_false"] + a) / (agg["n"] + a + b)

                rows.append(agg)

    if not rows:
        raise RuntimeError("No results (check parameters / inputs)")

    out = pd.concat(rows, ignore_index=True)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print("OK")
    print("out:", str(out_path), "rows=" + str(len(out)))
    print("grid:", "N=" + str(Nset), "K=" + str(Kset), "X=" + str(Xset))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
