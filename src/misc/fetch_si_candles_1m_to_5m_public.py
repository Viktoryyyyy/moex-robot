#!/usr/bin/env python3
import requests, pandas as pd
from datetime import datetime, timedelta, date
import argparse, time

API_PUBLIC = "https://iss.moex.com"

def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)

def fetch_day_1m_public(ticker: str, d: date, max_pages=400) -> pd.DataFrame:
    url = f"{API_PUBLIC}/iss/engines/futures/markets/forts/boards/RFUD/securities/{ticker}/candles.json"
    base = {"interval": 1, "date": d.isoformat()}
    out, start, page = [], 0, 0
    while page < max_pages:
        page += 1
        p = base | {"start": start}
        r = requests.get(url, params=p, headers={"User-Agent":"moex_bot_si_1m"}, timeout=(5,25))
        j = r.json()
        blk = j.get("candles", {})
        cols = blk.get("columns", [])
        data = blk.get("data", [])
        k = len(data)
        print(f"[{d}] page={page} start={start} rows={k} route=public|{r.status_code}")
        if not data: break
        df = pd.DataFrame(data, columns=cols)
        # Наивные timestamps из ISS
        for col in ("begin","end"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        # Строгий фильтр по дате по полю 'end'
        if "end" in df.columns:
            df["d"] = df["end"].dt.date
            if (df["d"] > d).all(): break
            if (df["d"] < d).all(): start += k; continue
            df = df[df["d"] == d].copy()
            df.drop(columns=["d"], inplace=True)
        need = ["end","open","high","low","close","volume"]
        for c in need:
            if c not in df.columns: df[c] = pd.NA
        df = df.dropna(subset=["end"]).drop_duplicates(subset=["end"])
        out.append(df[need])
        start += k
        time.sleep(0.02)
    if not out:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    day = pd.concat(out, ignore_index=True).sort_values("end").drop_duplicates(subset=["end"])
    return day

def to_5m(df1m: pd.DataFrame) -> pd.DataFrame:
    if df1m.empty:
        return pd.DataFrame(columns=["end","OPEN","HIGH","LOW","CLOSE","volume"])
    tmp = df1m.set_index("end")
    o = tmp["open"].resample("5T", label="right", closed="right").first()
    h = tmp["high"].resample("5T", label="right", closed="right").max()
    l = tmp["low"].resample("5T", label="right", closed="right").min()
    c = tmp["close"].resample("5T", label="right", closed="right").last()
    v = tmp["volume"].resample("5T", label="right", closed="right").sum()
    out = pd.concat([o,h,l,c,v], axis=1)
    out.columns = ["OPEN","HIGH","LOW","CLOSE","volume"]
    out = out.dropna(subset=["OPEN","HIGH","LOW","CLOSE"]).reset_index().rename(columns={"end":"end"})
    return out[["end","OPEN","HIGH","LOW","CLOSE","volume"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d0", required=True)
    ap.add_argument("--till", dest="d1", required=True)
    ap.add_argument("--si", dest="ticker", default="SiZ5")
    ap.add_argument("--max-pages", type=int, default=400)
    args = ap.parse_args()
    d0 = datetime.strptime(args.d0, "%Y-%m-%d").date()
    d1 = datetime.strptime(args.d1, "%Y-%m-%d").date()
    all5 = []
    for D in daterange(d0, d1):
        d1m = fetch_day_1m_public(args.ticker, D, max_pages=args.max_pages)
        d5  = to_5m(d1m)
        print(f"[{D}] 1m={len(d1m)} -> 5m={len(d5)}")
        all5.append(d5)
    res = pd.concat(all5, ignore_index=True) if all5 else pd.DataFrame(columns=["end","OPEN","HIGH","LOW","CLOSE","volume"])
    res = res.sort_values("end").drop_duplicates(subset=["end"])
    fn = f"si_5m_{d0}_{d1}_from_candles_public.csv"
    res.to_csv(fn, index=False)
    print(f"Saved: {fn}; bars_5m={len(res)}")

if __name__ == "__main__":
    pd.set_option("display.width", 180)
    main()
