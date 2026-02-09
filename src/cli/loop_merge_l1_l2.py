#!/usr/bin/env python3
import os, time, glob, argparse, pandas as pd, numpy as np
from datetime import datetime

def latest_csv(prefix: str):
    files = sorted(glob.glob(f"{prefix}_*.csv"), key=os.path.getmtime)
    return files[-1] if files else None

def read_csv_safe(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, parse_dates=["end"])
    except Exception:
        return pd.DataFrame()

def dedup_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    return df.loc[:, ~df.columns.duplicated()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--l1_prefix", default="si_live_5m")   # OHLCV
    ap.add_argument("--l2_prefix", default="si_ob_5m")     # spread/liquidity
    ap.add_argument("--outfile_prefix", default="si_live_5m_full")
    ap.add_argument("--sleep", type=float, default=5.0)
    ap.add_argument("--print_every", type=int, default=1)
    args = ap.parse_args()

    cycle = 0
    while True:
        l1 = latest_csv(args.l1_prefix)
        l2 = latest_csv(args.l2_prefix)

        df1 = dedup_cols(read_csv_safe(l1))  # end, OPEN,HIGH,LOW,CLOSE,volume
        df2 = dedup_cols(read_csv_safe(l2))  # end, spread_mean, mid_mean, liq_raw, liq_smooth, ...

        if df1.empty and df2.empty:
            time.sleep(args.sleep); continue

        if not df1.empty: df1 = df1.sort_values("end")
        if not df2.empty: df2 = df2.sort_values("end")

        if not df1.empty and not df2.empty:
            # удаляем пересекающиеся колонки из L2 (кроме ключа)
            overlap = [c for c in df2.columns if c in df1.columns and c != "end"]
            if overlap:
                df2 = df2.drop(columns=overlap)
            merged = pd.merge(df1, df2, on="end", how="left", copy=False)
        elif not df1.empty:
            merged = df1.copy()
        else:
            merged = df2.copy()

        # ffill для ликвидности
        if "liq_smooth" in merged.columns:
            merged["liq_smooth"] = merged["liq_smooth"].ffill()
        if "liq_raw" in merged.columns:
            merged["liq_raw"] = merged["liq_raw"].ffill()

        # Флаг MR-1 по ликвидности
        merged["mr1_liq_ok"] = np.where(
            "liq_smooth" in merged.columns,
            merged["liq_smooth"] < 0.5,
            np.nan
        )

        dstr = merged["end"].iloc[-1].date().isoformat()
        out_path = f"{args.outfile_prefix}_{dstr}.csv"
        tmp = out_path + ".tmp"
        merged.to_csv(tmp, index=False)
        os.replace(tmp, out_path)

        cycle += 1
        if cycle % max(args.print_every,1) == 0:
            last = merged.tail(1).iloc[0]
            o = last.get("OPEN", np.nan); h = last.get("HIGH", np.nan)
            l = last.get("LOW",  np.nan); c = last.get("CLOSE", np.nan)
            v = last.get("volume", np.nan)
            liq = last.get("liq_smooth", np.nan)
            v_str = str(int(v)) if pd.notna(v) else "NaN"
            liq_str = f"{liq:.6f}" if pd.notna(liq) else "NaN"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] MERGE 5m end={last['end']}  O={o} H={h} L={l} C={c} V={v_str}  liq_smooth={liq_str}  mr1_liq_ok={last['mr1_liq_ok']}  -> {out_path}")

        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
