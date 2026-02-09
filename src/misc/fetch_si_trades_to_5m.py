#!/usr/bin/env python3
import os, time, argparse, requests
import pandas as pd
from datetime import datetime, timedelta, date

API_PUBLIC = "https://iss.moex.com"
UA = os.getenv("MOEX_UA", "moex_bot_si_trades_5m/1.1").strip()

def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)

def fetch_trades_day_public(ticker: str, d: date, max_pages=400) -> pd.DataFrame:
    """Тянем только с публичного ISS, где date фильтруется корректно."""
    path = f"/iss/engines/futures/markets/forts/boards/RFUD/securities/{ticker}/trades.json"
    url  = f"{API_PUBLIC}{path}"
    params_base = {"date": d.isoformat()}
    out = []
    start = 0
    page = 0
    while page < max_pages:
        page += 1
        p = params_base | {"start": start}
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, params=p, timeout=(5,25))
        dt = r.elapsed.total_seconds()*1000
        j  = r.json()
        blk = j.get("trades", {})
        cols = blk.get("columns", [])
        data = blk.get("data", [])
        k = len(data)
        print(f"[{d}] trades page={page} start={start} rows={k} route=public|{r.status_code}|{int(dt)}ms")
        if not data:
            break
        df = pd.DataFrame(data, columns=cols)
        if not {"TRADEDATE","TRADETIME","PRICE","QUANTITY"}.issubset(df.columns):
            start += k; continue
        # строгий фильтр по дате
        df = df[df["TRADEDATE"].astype(str) == d.isoformat()]
        if df.empty:
            start += k; continue
        # наивное время (МСК) достаточно для ресемплинга
        df["dt"] = pd.to_datetime(df["TRADEDATE"].astype(str)+" "+df["TRADETIME"].astype(str), errors="coerce")
        df = df.dropna(subset=["dt"])
        out.append(df[["dt","PRICE","QUANTITY"]])
        start += k
        time.sleep(0.02)
    if not out:
        return pd.DataFrame(columns=["dt","PRICE","QUANTITY"])
    return pd.concat(out, ignore_index=True).sort_values("dt").reset_index(drop=True)

def trades_to_5m(tr: pd.DataFrame) -> pd.DataFrame:
    if tr.empty:
        return pd.DataFrame(columns=["end","OPEN","HIGH","LOW","CLOSE","volume"])
    tmp = tr.set_index("dt")
    o = tmp["PRICE"].resample("5T", label="right", closed="right").first()
    h = tmp["PRICE"].resample("5T", label="right", closed="right").max()
    l = tmp["PRICE"].resample("5T", label="right", closed="right").min()
    c = tmp["PRICE"].resample("5T", label="right", closed="right").last()
    v = tmp["QUANTITY"].resample("5T", label="right", closed="right").sum(min_count=1)
    out = pd.concat([o,h,l,c,v], axis=1)
    out.columns = ["OPEN","HIGH","LOW","CLOSE","volume"]
    out = out.dropna(subset=["OPEN","HIGH","LOW","CLOSE"]).reset_index().rename(columns={"dt":"end"})
    return out[["end","OPEN","HIGH","LOW","CLOSE","volume"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d0", required=True)
    ap.add_argument("--till", dest="d1", required=True)
    ap.add_argument("--si", dest="ticker", default=os.getenv("SI_TICKER","SiZ5"))
    ap.add_argument("--max-pages", type=int, default=400)
    args = ap.parse_args()
    d0 = datetime.strptime(args.d0, "%Y-%m-%d").date()
    d1 = datetime.strptime(args.d1, "%Y-%m-%d").date()

    all_days = []
    total_tr = 0
    for D in daterange(d0, d1):
        tr = fetch_trades_day_public(args.ticker, D, max_pages=args.max_pages)
        total_tr += len(tr)
        d5 = trades_to_5m(tr)
        print(f"[{D}] trades rows={len(tr)} -> 5m rows={len(d5)}")
        all_days.append(d5)

    res = pd.concat(all_days, ignore_index=True) if all_days else pd.DataFrame(columns=["end","OPEN","HIGH","LOW","CLOSE","volume"])
    res = res.sort_values("end").drop_duplicates(subset=["end"])
    fn = f"si_5m_{d0}_{d1}_from_trades.csv"
    res.to_csv(fn, index=False)
    print(f"Saved: {fn}; trades_total={total_tr}, bars_5m={len(res)}")

if __name__ == "__main__":
    pd.set_option("display.width", 200)
    main()
