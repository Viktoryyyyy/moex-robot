#!/usr/bin/env python3
import os, re, glob, pandas as pd
from datetime import datetime

def extract_date_from_name(path):
    m = re.search(r"si_5m_(\\d{4}-\\d{2}-\\d{2})\\.csv$", os.path.basename(path), re.IGNORECASE)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d")
    except ValueError:
        return None

def pick_latest_si_csv():
    files = glob.glob("si_5m_*.csv")
    if not files:
        return None
    dated = [(p, extract_date_from_name(p)) for p in files]
    dated_valid = [t for t in dated if t[1] is not None]
    if dated_valid:
        return sorted(dated_valid, key=lambda x: x[1])[-1][0]
    return max(files, key=lambda p: os.path.getmtime(p))

def main():
    path = pick_latest_si_csv()
    if not path:
        print("⚠️ Нет файлов si_5m_*.csv")
        return
    print("📄 Файл:", path)
    df = pd.read_csv(path)
    print("🧱 Колонки:", list(df.columns))
    # Подсветим кандидатов по времени/цене
    cands = [c for c in df.columns if any(k in c.lower() for k in ["date","time","trade","end","ts","close","last","price","vol","openposition","openinterest","liq"])]
    print("🔎 Подходящие по названию:", cands)
    # Покажем 3 последние строки
    print("\n🔚 Хвост (3 строки):")
    print(df.tail(3).to_string(max_cols=200, max_rows=3))

if __name__ == "__main__":
    main()

