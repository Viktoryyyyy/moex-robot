#!/usr/bin/env python3
# coding: utf-8
import os, re, glob, sys
import pandas as pd
from datetime import datetime

TIME_CANDIDATES = ["datetime", "end", "timestamp", "TRADEDATE", "date", "time"]
CLOSE_CANDIDATES = ["close", "CLOSE", "Close"]
SIG_CANDIDATES = ["mr1_signal", "signal_mr1", "signal"]
AUX_FIELDS = ["liq_smooth", "volume", "VOL", "oi", "OPENINTEREST", "openinterest"]

def extract_date_from_name(path: str):
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
    dated = [t for t in dated if t[1] is not None]
    if dated:
        return sorted(dated, key=lambda x: x[1])[-1][0]
    return max(files, key=lambda p: os.path.getmtime(p))

def first_existing(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def normalize_time(val):
    if pd.isna(val):
        return "N/A"
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    s = str(val)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return s

def map_signal(sig_val):
    if pd.isna(sig_val):
        return "🟦 NO SIGNAL"
    s = str(sig_val).strip().upper()
    try:
        f = float(s)
        if f > 0:  return "🟢 BUY"
        if f < 0:  return "🔴 SELL"
        return "🟦 NO SIGNAL"
    except Exception:
        pass
    if s in ("BUY","LONG","+1","1"): return "🟢 BUY"
    if s in ("SELL","SHORT","-1"):  return "🔴 SELL"
    if s in ("0","NONE","NO","FLAT","HOLD"): return "🟦 NO SIGNAL"
    return f"🟦 {s}"

def main():
    path = pick_latest_si_csv()
    if not path:
        print("⚠️ Не найдено файлов si_5m_*.csv в текущей директории.")
        sys.exit(0)

    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ Файл пуст: {path}")
        sys.exit(0)

    row = df.tail(1).iloc[0]
    t_col = first_existing(df, TIME_CANDIDATES)
    c_col = first_existing(df, CLOSE_CANDIDATES)
    s_col = first_existing(df, SIG_CANDIDATES)

    ts = normalize_time(row[t_col]) if t_col else "N/A"
    close_val = row[c_col] if c_col else None
    close_str = "N/A" if pd.isna(close_val) else f"{float(close_val):.2f}"
    sig_str = map_signal(row[s_col]) if s_col else "🟦 NO SIGNAL"

    extras = []
    for f in AUX_FIELDS:
        if f in df.columns and pd.notna(row.get(f)):
            try:
                extras.append(f"{f}={float(row[f]):.3f}")
            except Exception:
                extras.append(f"{f}={row[f]}")

    extras_str = (" | " + " ".join(extras)) if extras else ""
    msg = (
        f"MOEX Bot — MR-1\\n"
        f"{sig_str}\\n"
        f"Инструмент: Si (5m)\\n"
        f"Время бара: {ts}\\n"
        f"Close: {close_str}{extras_str}"
    )

    print("-"*60)
    print(msg)
    print("Источник:", path)
    print("-"*60)

if __name__ == "__main__":
    main()

