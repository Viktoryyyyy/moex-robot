#!/usr/bin/env python3
import os, time, argparse, pandas as pd, numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
from scripts.lib_moex_v3 import get_json

def to_df(block):
    cols = (block or {}).get("columns"); data = (block or {}).get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict): return pd.DataFrame(data)
    return pd.DataFrame(data or [], columns=cols or [])

def find_col(cols, names):
    s = {c.lower(): c for c in cols}
    for n in names:
        if n.lower() in s: return s[n.lower()]
    return None

def get_oi_snapshot(ticker: str, tz: ZoneInfo):
    # Пытаемся достать OI из marketdata, затем из securities
    raw = get_json(f"/iss/engines/futures/markets/forts/securities/{ticker}.json",
                   params={"iss.only":"marketdata,securities","iss.meta":"on"})
    md = to_df(raw.get("marketdata", {}))
    sec = to_df(raw.get("securities", {}))

    # Время
    ts = None
    for df in (md, sec):
        if df.empty: continue
        c_t = find_col(list(df.columns), ["SYSTIME","systime","TIME"])
        if c_t:
            try:
                ts = pd.to_datetime(df[c_t].iloc[0], utc=True).tz_convert(tz)
            except Exception:
                ts = pd.to_datetime(df[c_t].iloc[0]).tz_localize(tz)
            break
    if ts is None: ts = pd.Timestamp.now(tz)

    # Значение OI
    oi = np.nan
    for df in (md, sec):
        if df.empty: continue
        c_oi = find_col(list(df.columns), ["OPENPOSITION","openposition","OPENINTEREST","openinterest"])
        if c_oi:
            val = pd.to_numeric(df[c_oi].iloc[0], errors="coerce")
            if pd.notna(val): oi = float(val); break

    return ts.tz_localize(None), oi

def resample_5m(df):
    if df.empty: return df
    g = df.set_index("dt").sort_index()
    out = pd.DataFrame({
        "oi_total": g["oi_total"].resample("5min").last().ffill()
    }).dropna(how="all")
    out["end"] = out.index
    return out.reset_index(drop=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default=os.getenv("MOEX_TICKER","SiZ5"))
    ap.add_argument("--sleep", type=float, default=60.0)
    ap.add_argument("--print_every", type=int, default=1)
    ap.add_argument("--tz", default=os.getenv("MOEX_TZ","Europe/Moscow"))
    ap.add_argument("--outfile_prefix", default="oi_5m")
    args = ap.parse_args()

    tz = ZoneInfo(args.tz)
    buf = []
    cycle = 0

    while True:
        try:
            ts, oi = get_oi_snapshot(args.ticker, tz)
        except Exception as e:
            print("ERROR fetch OI:", e, flush=True)
            time.sleep(max(args.sleep,10.0)); continue

        buf.append({"dt": ts, "oi_total": oi})
        df = pd.DataFrame(buf)
        bars = resample_5m(df)
        if not bars.empty:
            dstr = bars["end"].iloc[-1].date().isoformat()
            out_path = f"{args.outfile_prefix}_{dstr}.csv"
            tmp = out_path + ".tmp"
            bars.to_csv(tmp, index=False)
            os.replace(tmp, out_path)

            cycle += 1
            if cycle % max(args.print_every,1) == 0:
                last = bars.tail(1).iloc[0]
                oi_str = str(int(last["oi_total"])) if pd.notna(last["oi_total"]) else "NaN"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] OI 5m end={last['end']}  oi_total={oi_str}  -> {out_path}", flush=True)

        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
