#!/usr/bin/env python3
# coding: utf-8
import argparse, os, re, glob, sys
import pandas as pd

def pick_file(date):
    """Предпочитаем *_hi2.csv, иначе базовый."""
    f_hi2 = f"si_5m_{date}_hi2.csv"
    f_base= f"si_5m_{date}.csv"
    if os.path.exists(f_hi2) and os.path.getsize(f_hi2)>0: 
        return f_hi2
    if os.path.exists(f_base) and os.path.getsize(f_base)>0: 
        return f_base
    return None

def ensure_key_date(df, fdate):
    """Если key не совпадает с датой файла — заменить дату, сохранив время."""
    if "key" not in df.columns or df.empty:
        return df
    if df["key"].astype(str).str[:10].eq(fdate).all():
        return df
    tm = pd.to_datetime(df["key"], errors="coerce").dt.strftime("%H:%M:%S").fillna("00:00:00")
    df["key"] = fdate + " " + tm
    # Поправим явные tradedate-колонки, если есть
    for c in df.columns:
        if "tradedate" in c.lower():
            df[c] = fdate
    return df

def main():
    ap = argparse.ArgumentParser(description="Concat daily si_5m_* into one CSV")
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    # Соберём все доступные даты из файлов в папке
    files = sorted(glob.glob("si_5m_*.csv"))
    dates_all = []
    for p in files:
        m = re.search(r"si_5m_(\d{4}-\d{2}-\d{2})(?:_hi2)?\.csv$", os.path.basename(p))
        if m:
            dates_all.append(m.group(1))
    dates_all = sorted(set(dates_all))
    # Фильтр по диапазону
    dates = [d for d in dates_all if args.start <= d <= args.end]

    print("FOUND DATES IN FOLDER:", dates_all)
    print("DATES IN RANGE:", dates)

    if not dates:
        print("Нет дневных файлов в диапазоне", file=sys.stderr)
        sys.exit(1)

    parts = []
    for d in dates:
        fp = pick_file(d)
        if not fp:
            print(f"skip (нет файла на дату) {d}")
            continue
        try:
            df = pd.read_csv(fp)
        except Exception as e:
            print(f"read error {fp}: {e}", file=sys.stderr)
            continue
        df = ensure_key_date(df, d)
        print(f"+ use {fp} | rows={len(df)}")
        parts.append(df)

    if not parts:
        print("Нечего склеивать", file=sys.stderr)
        sys.exit(1)

    all_df = pd.concat(parts, ignore_index=True)

    # Сортировка и удаление дублей по key (если есть)
    if "key" in all_df.columns:
        all_df = all_df.sort_values("key").drop_duplicates("key", keep="last")

    out = args.out or f"si_5m_{dates[0]}_{dates[-1]}.csv"
    all_df.to_csv(out, index=False)
    print(f"OK -> {out} | rows={len(all_df)} cols={all_df.shape[1]}")

if __name__ == "__main__":
    main()
