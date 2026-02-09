#!/usr/bin/env python3
# coding: utf-8
import os, sys, glob, argparse
import pandas as pd
import numpy as np

def autodetect_input():
    # ищем самый свежий дневной файл si_5m_YYYY-MM-DD(_hi2).csv (не диапазонные)
    cand = []
    for p in glob.glob("si_5m_20??-??-??*.csv"):
        base = os.path.basename(p)
        # отфильтруем склейки диапазонов вида si_5m_YYYY-MM-DD_YYYY-MM-DD.csv
        if "_" in base.replace("si_5m_","",1)[11:]:
            continue
        cand.append(p)
    if not cand:
        raise SystemExit("Файл si_5m_YYYY-MM-DD*.csv не найден. Укажи --in.")
    cand.sort()
    return cand[-1]

def build_features(df):
    df = df.sort_values("key").reset_index(drop=True)
    # числовые
    for c in ["ts_pr_open","ts_pr_close","ob_spread_l1_rel","liq_flag_low"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # базовые фичи
    close = df["ts_pr_close"]
    df["ret1"]  = close.pct_change(1)
    df["vol20"] = df["ret1"].rolling(20, min_periods=10).std()
    if "liq_flag_low" in df.columns:
        df["liq_smooth"] = df["liq_flag_low"].rolling(5, min_periods=1).mean()
    else:
        df["liq_smooth"] = 0.0
    df.replace([np.inf,-np.inf], np.nan, inplace=True)
    return df

def main():
    ap = argparse.ArgumentParser(description="Signals: mean-reversion 1-bar, entry on next bar open")
    ap.add_argument("--in", dest="inp", default=None, help="si_5m_YYYY-MM-DD(_hi2).csv")
    ap.add_argument("--k", type=float, default=1.15, help="threshold multiplier (default 1.15)")
    ap.add_argument("--liq_thr", type=float, default=0.5, help="max liq_smooth to allow entry")
    ap.add_argument("--out", default=None, help="signals_<date>.csv (auto if not set)")
    args = ap.parse_args()

    path = args.inp or autodetect_input()
    df = pd.read_csv(path, parse_dates=["key"])
    df = build_features(df)

    # сигналы на баре t
    liq_ok = (pd.to_numeric(df["liq_smooth"], errors="coerce") < args.liq_thr)
    long_sig  = (df["ret1"] < -args.k*df["vol20"]) & liq_ok
    short_sig = (df["ret1"] >  args.k*df["vol20"]) & liq_ok
    sig = pd.Series(0, index=df.index, dtype="int8")
    sig[long_sig]  =  1
    sig[short_sig] = -1

    # позиция (вход) со следующего бара
    pos_next = sig.shift(1).fillna(0).astype("int8")

    # формируем рекомендации для ТЕКУЩЕЙ следующей свечи (последняя строка)
    last = df.iloc[-1].copy()
    # время входа — timestamp последнего бара (в реале: время следующего бара)
    entry_time = last["key"]
    entry_dir  = int(pos_next.iloc[-1])
    entry_price = float(df["ts_pr_open"].iloc[-1]) if "ts_pr_open" in df.columns else np.nan

    out = pd.DataFrame([{
        "entry_time": entry_time,
        "dir": entry_dir,           # +1 long, -1 short, 0 skip
        "k": args.k,
        "ret1": float(last["ret1"]) if pd.notna(last["ret1"]) else np.nan,
        "vol20": float(last["vol20"]) if pd.notna(last["vol20"]) else np.nan,
        "thr": float(args.k * last["vol20"]) if pd.notna(last["vol20"]) else np.nan,
        "liq_smooth": float(last["liq_smooth"]) if pd.notna(last["liq_smooth"]) else np.nan,
        "open_price_hint": entry_price,
        "source_file": os.path.basename(path)
    }])

    # имя выходного файла
    if args.out:
        out_path = args.out
    else:
        # извлечём дату из имени входного файла
        import re
        m = re.search(r"si_5m_(\d{4}-\d{2}-\d{2})", os.path.basename(path))
        d = m.group(1) if m else "today"
        out_path = f"signals_{d}.csv"

    out.to_csv(out_path, index=False)
    print(f"SIGNALS -> {out_path}")
    print(out)
    return 0

if __name__ == "__main__":
    sys.exit(main())
