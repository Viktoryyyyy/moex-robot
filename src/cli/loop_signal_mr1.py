#!/usr/bin/env python3
import os, time, glob, argparse, pandas as pd, numpy as np
from datetime import datetime

def latest_csvs(prefix: str, n: int = 3):
    files = sorted(glob.glob(f"{prefix}_*.csv"), key=os.path.getmtime)
    return files[-n:] if files else []

def read_stack(prefix: str, take_last_files: int = 3) -> pd.DataFrame:
    files = latest_csvs(prefix, take_last_files)
    frames = []
    for p in files:
        try:
            df = pd.read_csv(p, parse_dates=["end"])
            frames.append(df)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "mr1_liq_ok" not in df.columns and "liq_smooth" in df.columns:
        df["mr1_liq_ok"] = df["liq_smooth"] < 0.5
    return df.sort_values("end").reset_index(drop=True)

def compute_signal(row, k: float) -> str:
    if pd.isna(row.get("z")): return "NO SIGNAL"
    if not bool(row.get("mr1_liq_ok", False)): return "NO SIGNAL"
    z = float(row["z"])
    if z <= -k: return "LONG"
    if z >=  k: return "SHORT"
    return "NO SIGNAL"

def status_line(ts, row, k):
    now = datetime.now().strftime("%H:%M:%S")
    v = row.get("volume", np.nan); v_str = str(int(v)) if pd.notna(v) else "NaN"
    parts = dict(
        O=row.get("OPEN", np.nan), H=row.get("HIGH", np.nan), L=row.get("LOW", np.nan),
        C=row.get("CLOSE", np.nan),
        MA=row.get("MA", np.nan), STD=row.get("STD", np.nan), Z=row.get("z", np.nan),
        liq=row.get("liq_smooth", np.nan), ok=bool(row.get("mr1_liq_ok", False))
    )
    fmt = lambda x, p: (f"{x:.{p}f}" if pd.notna(x) else "NaN")
    return (f"[{now}] MR-1 end={ts}  O={parts['O']} H={parts['H']} L={parts['L']} C={parts['C']} V={v_str}  "
            f"MA={fmt(parts['MA'],2)} STD={fmt(parts['STD'],2)} Z={fmt(parts['Z'],2)}  "
            f"liq_smooth={fmt(parts['liq'],6)}  mr1_liq_ok={parts['ok']}  -> SIGNAL: {compute_signal(row, k)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full_prefix", default="si_live_5m_full")
    ap.add_argument("--span", type=int, default=48, help="span для EWM (≈ оконный размер)")
    ap.add_argument("--min_ewm", type=int, default=5, help="минимум баров для запуска EWM")
    ap.add_argument("--fallback_roll", type=int, default=12, help="короткое окно rolling в fallback")
    ap.add_argument("--k", type=float, default=1.15)
    ap.add_argument("--sleep", type=float, default=5.0)
    ap.add_argument("--print_every", type=int, default=1)
    ap.add_argument("--history_files", type=int, default=3)
    args = ap.parse_args()

    last_ts = None
    first_print_done = False
    cycle = 0

    while True:
        df = read_stack(args.full_prefix, args.history_files)
        if not df.empty and {"CLOSE","end"}.issubset(df.columns):
            if len(df) > 2000: df = df.iloc[-2000:].copy()

            close = pd.to_numeric(df["CLOSE"], errors="coerce")
            # EWM расчёт
            ma = close.ewm(span=args.span, adjust=False, min_periods=args.min_ewm).mean()
            sd = close.ewm(span=args.span, adjust=False, min_periods=args.min_ewm).std(bias=False)
            z  = (close - ma) / sd.replace(0, np.nan)

            # Fallback на короткий rolling, если EWM ещё не «разогнался»
            need_fallback = z.isna()
            if need_fallback.any():
                win = min(args.fallback_roll, max(3, len(df)))
                ma_f = close.rolling(win, min_periods=3).mean()
                sd_f = close.rolling(win, min_periods=3).std(ddof=0)
                z_f  = (close - ma_f) / sd_f.replace(0, np.nan)
                ma = ma.where(~need_fallback, ma_f)
                sd = sd.where(~need_fallback, sd_f)
                z  = z.where(~need_fallback,  z_f)

            df = df.assign(MA=ma, STD=sd, z=z)

            last_row = df.tail(1).iloc[0]
            ts = pd.to_datetime(last_row["end"])

            if not first_print_done:
                print(status_line(ts, last_row, args.k), flush=True)
                first_print_done = True
                last_ts = ts

            if ts > (last_ts or pd.Timestamp.min):
                print(status_line(ts, last_row, args.k), flush=True)
                last_ts = ts

        cycle += 1
        if cycle % max(args.print_every,1) == 0:
            files = latest_csvs(args.full_prefix, 1)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] MR-1 heartbeat (waiting new 5m bar) file={files[0] if files else 'N/A'}", flush=True)

        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
