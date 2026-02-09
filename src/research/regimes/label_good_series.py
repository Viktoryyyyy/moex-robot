import pandas as pd
import numpy as np

IN_PATH = "data/research/ema_pnl_day.csv"
OUT_PATH = "data/research/ema_gb_series_labels.csv"

df = pd.read_csv(IN_PATH)
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

if "EMA_EDGE_DAY" not in df.columns:
    raise SystemExit("Missing column: EMA_EDGE_DAY")

s = df["EMA_EDGE_DAY"].astype(int).to_numpy()
n = len(s)

series_type = np.array(["NA"] * n, dtype=object)   # G1 / GS / NA
series_pos  = np.array(["NA"] * n, dtype=object)   # start/mid/end/NA
series_id   = np.full(n, -1, dtype=int)
run_len     = np.full(n, 0, dtype=int)

sid = 0
i = 0
while i < n:
    if s[i] != 1:
        i += 1
        continue

    j = i
    while j < n and s[j] == 1:
        j += 1
    L = j - i  # length of GOOD run

    if L == 1:
        series_type[i] = "G1"
        series_pos[i]  = "single"
        series_id[i]   = -1
        run_len[i]     = 1
    else:
        # GOOD series (length >= 2)
        series_type[i:j] = "GS"
        series_id[i:j] = sid
        run_len[i:j] = L

        series_pos[i] = "start"
        series_pos[j-1] = "end"
        if L > 2:
            series_pos[i+1:j-1] = "mid"

        sid += 1

    i = j

out = df.copy()
out["series_type"] = series_type
out["series_pos"] = series_pos
out["series_id"] = series_id
out["run_len"] = run_len

# компактный отчёт
n_days = len(out)
n_good = int((out["EMA_EDGE_DAY"] == 1).sum())
n_bad = n_days - n_good

n_g1 = int((out["series_type"] == "G1").sum())
n_gs = int((out["series_type"] == "GS").sum())
n_gs_runs = int(out["series_id"].max() + 1) if n_gs > 0 else 0

print("=== GOOD SERIES LABELING ===")
print(f"Period: {out['date'].min().date()} → {out['date'].max().date()}")
print(f"Days: {n_days} | GOOD: {n_good} | BAD: {n_bad}")
print(f"GOOD singles (G1): {n_g1}")
print(f"GOOD in series (GS): {n_gs}  | number of GS runs: {n_gs_runs}")

if n_gs_runs > 0:
    gs_lens = out.loc[out["series_type"] == "GS", "run_len"].to_numpy()
    # unique run lengths per run id
    per_run = out.loc[out["series_type"] == "GS", ["series_id", "run_len"]].drop_duplicates()["run_len"].to_numpy()
    print(f"GS run length stats (per run): mean={per_run.mean():.2f}, median={np.median(per_run):.1f}, max={per_run.max()}")
else:
    print("GS run length stats: NA")

out.to_csv(OUT_PATH, index=False)
print(f"Output: {OUT_PATH}")
print("STATUS: LABELS READY")
