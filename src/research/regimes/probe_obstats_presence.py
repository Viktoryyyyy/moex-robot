import glob
import pandas as pd

GLOBS = [
    "data/master/*.csv",
    "data/research/*.csv",
    "data/raw/**/*.csv",
    "data/interim/**/*.csv",
]

KEYS = ["spread", "liq", "liqu", "bid", "ask", "orderbook", "obstat", "obstats"]

def scan(path):
    try:
        df = pd.read_csv(path, nrows=5)
    except Exception:
        return None
    cols = [c for c in df.columns]
    hits = []
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in KEYS):
            hits.append(c)
    if hits:
        return hits
    return None

def main():
    files = []
    for g in GLOBS:
        files.extend(glob.glob(g, recursive=True))
    files = sorted(set(files))

    found = []
    for p in files:
        hits = scan(p)
        if hits:
            found.append((p, hits))

    print("=== OBSTATS PRESENCE PROBE ===")
    print(f"Scanned files: {len(files)}")
    print(f"Files with matching columns: {len(found)}")
    for p, hits in found[:50]:
        print(f"- {p}")
        print(f"  cols: {hits}")
    if len(found) > 50:
        print(f"... truncated, total matches: {len(found)}")
    print("STATUS: PROBE COMPLETE")

if __name__ == "__main__":
    main()
