import argparse
from pathlib import Path
import pandas as pd

OUT_COLS = ["bucket","number_of_days","total_pnl","pnl_per_day","avg_daily_win_rate","total_trades","avg_trades_per_day","pnl_per_day_delta_vs_unconditional","win_rate_delta_vs_unconditional"]

def die(msg):
    raise SystemExit("ERROR: " + msg)

def norm_dates(df, col, ctx):
    x = pd.to_datetime(df[col], errors="coerce").dt.date.astype("string")
    if x.isna().any():
        die(ctx + ": invalid date values")
    return x

def load_baseline(path):
    if not Path(path).exists():
        die("baseline csv not found: " + path)
    df = pd.read_csv(path)
    need = ["date","pnl_day","num_trades_day"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        die("baseline: missing required columns: " + str(miss))
    if "win_rate" in df.columns:
        win_col = "win_rate"
    elif "EMA_EDGE_DAY" in df.columns:
        win_col = "EMA_EDGE_DAY"
    else:
        die("baseline: missing win_rate or EMA_EDGE_DAY")
    df = df[["date","pnl_day",win_col,"num_trades_day"]].copy()
    if win_col != "win_rate":
        df = df.rename(columns={win_col: "win_rate"})
    df["date"] = norm_dates(df, "date", "baseline")
    for c in ["pnl_day","win_rate","num_trades_day"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        if df[c].isna().any():
            die("baseline: non-numeric " + c)
    if df["date"].duplicated().any():
        die("baseline: duplicate date rows")
    return df.sort_values("date").reset_index(drop=True)

def load_gate(path):
    if not Path(path).exists():
        die("gate joined csv not found: " + path)
    df = pd.read_csv(path)
    need = ["date","gate_state"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        die("gate_joined: missing required columns: " + str(miss))
    df = df[need].copy()
    df["date"] = norm_dates(df, "date", "gate_joined")
    df["gate_state"] = pd.to_numeric(df["gate_state"], errors="coerce")
    if df["gate_state"].isna().any():
        die("gate_joined: non-numeric gate_state")
    bad = sorted(set(df.loc[~df["gate_state"].isin([0,1]), "gate_state"].tolist()))
    if bad:
        die("gate_joined: invalid gate_state values: " + str(bad))
    if df["date"].duplicated().any():
        die("gate_joined: duplicate date rows")
    df["gate_state"] = df["gate_state"].astype(int)
    return df.sort_values("date").reset_index(drop=True)

def metrics(df, bucket):
    n = len(df)
    pnl = float(df["pnl_day"].sum()) if n else 0.0
    wr = float(df["win_rate"].mean()) if n else 0.0
    tr = float(df["num_trades_day"].sum()) if n else 0.0
    ppd = float(pnl / n) if n else 0.0
    tpd = float(tr / n) if n else 0.0
    return {"bucket": bucket, "number_of_days": int(n), "total_pnl": pnl, "pnl_per_day": ppd, "avg_daily_win_rate": wr, "total_trades": tr, "avg_trades_per_day": tpd}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline-day-csv", required=True)
    ap.add_argument("--gate-joined-day-csv", required=True)
    ap.add_argument("--out-csv", required=True)
    a = ap.parse_args()
    base = load_baseline(a.baseline_day_csv)
    gate = load_gate(a.gate_joined_day_csv)
    m = base.merge(gate[["date","gate_state"]], on="date", how="inner", validate="one_to_one")
    if m.empty:
        die("joined result is empty")
    u = metrics(m, "UNCONDITIONAL")
    g0 = metrics(m[m["gate_state"] == 0].copy(), "GATE_0")
    g1 = metrics(m[m["gate_state"] == 1].copy(), "GATE_1")
    for r in [u, g0, g1]:
        r["pnl_per_day_delta_vs_unconditional"] = float(r["pnl_per_day"] - u["pnl_per_day"]) if r["bucket"] != "UNCONDITIONAL" else 0.0
        r["win_rate_delta_vs_unconditional"] = float(r["avg_daily_win_rate"] - u["avg_daily_win_rate"]) if r["bucket"] != "UNCONDITIONAL" else 0.0
    out = pd.DataFrame([u, g0, g1])[OUT_COLS]
    Path(a.out_csv).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(a.out_csv, index=False)
    print("OK " + a.out_csv)

if __name__ == "__main__":
    main()
