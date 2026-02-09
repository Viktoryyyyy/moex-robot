#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

def main():
    # файлы
    skeleton = Path("data/master/si_cny_5m_2020-01-01_2025-11-13.csv")
    futoi    = Path("data/master/futoi_si_5m_2020-01-03_2025-11-13.csv")
    obstats  = Path("data/master/obstats_si_5m_2020-01-03_2025-11-13.csv")

    out      = Path("data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2025-11-13.csv")

    # Load
    df = pd.read_csv(skeleton)
    df["end"] = pd.to_datetime(df["end"])

    df_futoi = pd.read_csv(futoi)
    df_futoi["end"] = pd.to_datetime(df_futoi["end"])

    df_ob = pd.read_csv(obstats)
    df_ob["end"] = pd.to_datetime(df_ob["end"])

    # Merge 1: FO+FX skeleton + FUTOI
    df = df.merge(df_futoi, on="end", how="left", suffixes=("", ""))

    # Merge 2: + OBSTATS
    df = df.merge(df_ob, on="end", how="left", suffixes=("", ""))

    # Сортировка времени
    df = df.sort_values("end")

    # Сохранение
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    print("Merged master saved:", out)
    print("Rows:", len(df))
    print("Start:", df["end"].min())
    print("End:  ", df["end"].max())
    print("Columns:", list(df.columns))

if __name__ == "__main__":
    main()
