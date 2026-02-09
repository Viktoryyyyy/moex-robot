#!/usr/bin/env python3
import os, time, glob, argparse, pandas as pd, numpy as np
from datetime import datetime

def latest_csv(prefix: str):
    files = sorted(glob.glob(f"{prefix}_*.csv"), key=os.path.getmtime)
    return files[-1] if files else None

def read_csv_safe(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path): return pd.DataFrame()
    try:
        return pd.read_csv(path, parse_dates=["end"])
    except Exception:
        return pd.DataFrame()

def dedup_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    return df.loc[:, ~df.columns.duplicated()]

def find_col(df: pd.DataFrame, names) -> str | None:
    s = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in s: return s[n.lower()]
    return None

def normalize_oi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    c_end = "end" if "end" in df.columns else find_col(df, ["end"])
    if not c_end: return pd.DataFrame()
    c_oi = None
    for cand in ["oi_total","openposition","openinterest"]:
        c_oi = find_col(df, [cand])
        if c_oi: break
    if not c_oi:
        return df[[c_end]].copy().rename(columns={c_end:"end"})
    out = df[[c_end, c_oi]].copy().rename(columns={c_end:"end", c_oi:"oi_total"})
    return dedup_cols(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full_prefix", default="si_live_5m_full")
    ap.add_argument("--oi_prefix",   default="oi_5m")
    ap.add_argument("--outfile_prefix", default="si_live_5m_full_oi")
    ap.add_argument("--sleep", type=float, default=5.0)
    ap.add_argument("--print_every", type=int, default=1)
    args = ap.parse_args()

    cycle = 0
    while True:
        f_full = latest_csv(args.full_prefix)
        f_oi   = latest_csv(args.oi_prefix)

        df_full = dedup_cols(read_csv_safe(f_full))
        df_oi   = dedup_cols(read_csv_safe(f_oi))

        if df_full.empty and df_oi.empty:
            time.sleep(args.sleep); continue

        if not df_full.empty: df_full = df_full.sort_values("end")
        if not df_oi.empty:   df_oi   = df_oi.sort_values("end")

        df_oi2 = normalize_oi(df_oi)

        merged = df_full.copy() if not df_full.empty else pd.DataFrame()

        if not df_oi2.empty:
            # 1) убираем в левой части любые колонки, которые есть в правой (кроме 'end')
            overlap_left = [c for c in df_oi2.columns if c != "end" and c in merged.columns]
            if overlap_left:
                merged = merged.drop(columns=overlap_left, errors="ignore")
            # 2) на всякий случай убираем в правой части колонки, совпадающие с левой (кроме 'end')
            overlap_right = [c for c in df_oi2.columns if c != "end" and c in merged.columns]
            df_oi2 = df_oi2.drop(columns=overlap_right, errors="ignore")
            # 3) мердж
            if merged.empty:
                merged = df_oi2.copy()
            else:
                merged = pd.merge(merged, df_oi2, on="end", how="left", copy=False)

        if "oi_total" in merged.columns:
            merged["oi_total"] = pd.to_numeric(merged["oi_total"], errors="coerce").ffill()

        if not merged.empty:
            dstr = merged["end"].iloc[-1].date().isoformat()
            out_path = f"{args.outfile_prefix}_{dstr}.csv"
            tmp = out_path + ".tmp"
            merged.to_csv(tmp, index=False)
            os.replace(tmp, out_path)

            cycle += 1
            if cycle % max(args.print_every,1) == 0:
                last = merged.tail(1).iloc[0]
                vals = {
                    "O": last.get("OPEN", np.nan), "H": last.get("HIGH", np.nan),
                    "L": last.get("LOW", np.nan),  "C": last.get("CLOSE", np.nan),
                    "V": last.get("volume", np.nan),
                    "liq": last.get("liq_smooth", np.nan),
                    "oi": last.get("oi_total", np.nan),
                }
                v_str  = str(int(vals["V"])) if pd.notna(vals["V"]) else "NaN"
                liq_str= f"{vals['liq']:.6f}" if pd.notna(vals["liq"]) else "NaN"
                oi_str = str(int(vals["oi"])) if pd.notna(vals["oi"]) else "NaN"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] MERGE+OI 5m end={last['end']}  O={vals['O']} H={vals['H']} L={vals['L']} C={vals['C']} V={v_str}  liq_smooth={liq_str}  oi_total={oi_str}  -> {out_path}", flush=True)

        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
