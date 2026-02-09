import pandas as pd
import numpy as np

PATH = "data/research/ema_pnl_day.csv"

df = pd.read_csv(PATH)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# Binary series
s = df["EMA_EDGE_DAY"].astype(int).to_numpy()
n = len(s)

# Base rates
pG = s.mean()
pB = 1 - pG

# Transitions
GG = GB = BG = BB = 0
for i in range(1, n):
    if s[i-1] == 1 and s[i] == 1: GG += 1
    if s[i-1] == 1 and s[i] == 0: GB += 1
    if s[i-1] == 0 and s[i] == 1: BG += 1
    if s[i-1] == 0 and s[i] == 0: BB += 1

PGG = GG / (GG + GB) if (GG + GB) else np.nan
PBB = BB / (BB + BG) if (BB + BG) else np.nan

# Run lengths
runs_G = []
runs_B = []
cur = s[0]
length = 1
for i in range(1, n):
    if s[i] == cur:
        length += 1
    else:
        (runs_G if cur == 1 else runs_B).append(length)
        cur = s[i]
        length = 1
(runs_G if cur == 1 else runs_B).append(length)

# i.i.d. expectations (geometric)
E_run_G = 1 / (1 - pG) if pG < 1 else np.inf
E_run_B = 1 / (1 - pB) if pB < 1 else np.inf

print("=== STAGE 2: G/B SERIALITY ===")
print(f"Days: {n}")
print(f"Base rates: p(G)={pG:.3f}, p(B)={pB:.3f}\n")

print("Transitions:")
print(f"P(G→G)={PGG:.3f}  vs iid p(G)={pG:.3f}")
print(f"P(B→B)={PBB:.3f}  vs iid p(B)={pB:.3f}\n")

def stats(x):
    return {
        "count": len(x),
        "mean": np.mean(x) if x else np.nan,
        "median": np.median(x) if x else np.nan,
        "p90": np.percentile(x, 90) if x else np.nan,
        "max": np.max(x) if x else np.nan,
    }

g = stats(runs_G)
b = stats(runs_B)

print("Run lengths (G):", g, f"iid E={E_run_G:.2f}")
print("Run lengths (B):", b, f"iid E={E_run_B:.2f}")

# Verdict (rule-based, no tuning)
ser_G = (PGG > pG)
ser_B = (PBB > pB)

print("\nVERDICT:")
if ser_G and ser_B:
    print("SERIALITY PRESENT (both G and B cluster)")
elif ser_G or ser_B:
    print("PARTIAL SERIALITY (one side clusters)")
else:
    print("NO SERIALITY (iid-like)")
