#!/usr/bin/env python3
import os, sys, time, argparse, pandas as pd, numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
from scripts.lib_moex_v3 import get_json

def to_df(block):
    cols = (block or {}).get("columns")
    data = (block or {}).get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return pd.DataFrame(data)
    return pd.DataFrame(data or [], columns=cols or [])

def fetch_md(ticker):
    path = f"/iss/engines/futures/markets/forts/securities/{ticker}.json"
    return get_json(path, params={"iss.only":"marketdata", "iss.meta":"on"})

def pick_cols(df):
    # ищем BID/OFFER/LAST/SYSTIME (разные регистры на всякий случай)
    def fc(name_list):
        s = {c.lower(): c for c in df.columns}
        for n in name_list:
            if n.lower() in s: return s[n.lower()]
        return None
    c_bid   = fc(["BID","bid","best_bid"])
    c_ask   = fc(["OFFER","offer","best_ask","ASK"])
    c_last  = fc(["LAST","last","LASTPRICE"])
    c_time  = fc(["SYSTIME","systime","TIME"])
    return c_bid, c_ask, c_last, c_time

def resample_5min(df):
    if df.empty: return df
    g = df.set_index("dt").sort_index()
    out = pd.DataFrame({
        "bid_mean":   g["bid"].resample("5min").mean(),
        "ask_mean":   g["ask"].resample("5min").mean(),
        "last_mean":  g["last"].resample("5min").mean(),
        "spread_mean":g["spread"].resample("5min").mean(),
        "mid_mean":   g["mid"].resample("5min").mean(),
        "liq_raw":    g["liq_raw"].resample("5min").mean(),
    }).dropna(how="all")
    if not out.empty:
        out["liq_smooth"] = out["liq_raw"].ewm(span=6, adjust=False, min_periods=1).mean()
        out["end"] = out.index
        out = out.reset_index(drop=True)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default=os.getenv("MOEX_TICKER","SiZ5"))
    ap.add_argument("--sleep", type=float, default=5.0)
    ap.add_argument("--print_every", type=int, default=1)
    ap.add_argument("--outfile_prefix", default="si_ob_5m")
    ap.add_argument("--tz", default=os.getenv("MOEX_TZ","Europe/Moscow"))
    args = ap.parse_args()

    tz = ZoneInfo(args.tz)
    buf = []
    cycle = 0
    out_path = None

    while True:
        try:
            raw = fetch_md(args.ticker)
        except Exception as e:
            print("ERROR fetch:", e, file=sys.stderr)
            time.sleep(max(args.sleep, 1.0)); continue

        md = to_df(raw.get("marketdata", {}))
        if md.empty:
            time.sleep(args.sleep); continue

        c_bid, c_ask, c_last, c_time = pick_cols(md)
        if not (c_bid and c_ask and c_time):
            # как минимум bid/ask/time нужны
            print("ERROR columns: no BID/OFFER/SYSTIME in marketdata", file=sys.stderr)
            time.sleep(args.sleep); continue

        try:
            ts = pd.to_datetime(md[c_time].iloc[0], utc=True, errors="coerce")
            if ts.tz is None:
                ts = pd.to_datetime(md[c_time].iloc[0], errors="coerce").tz_localize(tz)
            else:
                ts = ts.tz_convert(tz)
        except Exception:
            ts = pd.Timestamp.now(tz)

        bid  = pd.to_numeric(md[c_bid].iloc[0], errors="coerce")
        ask  = pd.to_numeric(md[c_ask].iloc[0], errors="coerce")
        last = pd.to_numeric(md[c_last].iloc[0], errors="coerce") if c_last else np.nan

        spread = np.nan
        mid    = np.nan
        liq    = np.nan
        if pd.notna(bid) and pd.notna(ask):
            spread = float(ask - bid)
            mid = float((ask + bid)/2) if (ask+bid) else np.nan
            if pd.notna(mid) and mid>0:
                liq = float(spread/mid)

        buf.append({
            "dt": ts.tz_convert(tz).tz_localize(None),
            "bid": bid, "ask": ask, "last": last,
            "spread": spread, "mid": mid, "liq_raw": liq
        })

        df = pd.DataFrame(buf)
        bars = resample_5min(df)
        if not bars.empty:
            dstr = bars["end"].iloc[-1].date().isoformat()
            out_path = f"{args.outfile_prefix}_{dstr}.csv"
            tmp = out_path + ".tmp"
            bars.to_csv(tmp, index=False)
            os.replace(tmp, out_path)

            cycle += 1
            if cycle % max(args.print_every,1) == 0:
                last_row = bars.tail(1).iloc[0]
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {args.ticker} MD 5m end={last_row['end']}  spread={last_row['spread_mean']:.6f}  liq_smooth={last_row['liq_smooth']:.6f}  -> {out_path}")

        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
