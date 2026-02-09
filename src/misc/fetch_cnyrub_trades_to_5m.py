#!/usr/bin/env python3
import os, time, argparse, requests
import pandas as pd
from datetime import datetime, timedelta, date

API_PRIMARY = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
API_PUBLIC  = "https://iss.moex.com"
UA  = os.getenv("MOEX_UA", "moex_bot_cny_trades_5m/1.1").strip()

def headers():
    return {"User-Agent": UA, "Accept": "application/json"}

def get_public(path, params):
    url = f"{API_PUBLIC}{path}"
    r = requests.get(url, headers=headers(), params=params, timeout=(5,25))
    r.raise_for_status()
    return r.json(), f"public|{r.status_code}"

def fetch_trades_day_cets(secid: str, d: date, max_pages=400) -> pd.DataFrame:
    """CETS trades: нет TRADEDATE; строим dt = d + TRADETIME."""
    path = f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}/trades.json"
    base = {"date": d.isoformat()}
    out, start, page = [], 0, 0
    while page < max_pages:
        page += 1
        j, route = get_public(path, base | {"start": start})
        blk = j.get("trades", {})
        cols = blk.get("columns", [])
        data = blk.get("data", [])
        k = len(data)
        print(f"[{d}] trades page={page} start={start} rows={k} route={route}")
        if not data: break
        df = pd.DataFrame(data, columns=cols)
        # требуем ключевые поля CETS
        need = {"TRADETIME","PRICE","QUANTITY","VALUE"}
        if not need.issubset(df.columns):
            start += k; continue
        # dt = <запрошенная дата> + TRADETIME
        df["dt"] = pd.to_datetime(d.isoformat() + " " + df["TRADETIME"].astype(str), errors="coerce")
        df = df.dropna(subset=["dt"])
        out.append(df[["dt","PRICE","QUANTITY","VALUE"]])
        start += k
        time.sleep(0.01)
    if not out:
        return pd.DataFrame(columns=["dt","PRICE","QUANTITY","VALUE"])
    return pd.concat(out, ignore_index=True).sort_values("dt").reset_index(drop=True)

def trades_to_5m(tr: pd.DataFrame) -> pd.DataFrame:
    if tr.empty:
        return pd.DataFrame(columns=["end","open","high","low","close","volume","turnover"])
    tmp = tr.set_index("dt")
    o = tmp["PRICE"].resample("5min", label="right", closed="right").first()
    h = tmp["PRICE"].resample("5min", label="right", closed="right").max()
    l = tmp["PRICE"].resample("5min", label="right", closed="right").min()
    c = tmp["PRICE"].resample("5min", label="right", closed="right").last()
    v = tmp["QUANTITY"].resample("5min", label="right", closed="right").sum(min_count=1)
    t = tmp["VALUE"].resample("5min", label="right", closed="right").sum(min_count=1)
    out = pd.concat([o,h,l,c,v,t], axis=1)
    out.columns = ["open","high","low","close","volume","turnover"]
    out = out.dropna(subset=["open","high","low","close"]).reset_index().rename(columns={"dt":"end"})
    return out[["end","open","high","low","close","volume","turnover"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (торговый день)")
    ap.add_argument("--secid", default="CNYRUB_TOM")
    ap.add_argument("--max-pages", type=int, default=200)
    args = ap.parse_args()
    D = datetime.strptime(args.date, "%Y-%m-%d").date()

    tr = fetch_trades_day_cets(args.secid, D, max_pages=args.max_pages)
    d5 = trades_to_5m(tr)
    fn = f"cnyrub_5m_{D}_{D}_from_trades.csv"
    d5.to_csv(fn, index=False)
    print(f"Saved: {fn}; bars_5m={len(d5)}, trades_rows={len(tr)}")
    if not d5.empty:
        top = d5.sort_values("turnover", ascending=False).head(5)
        print("\nTOP-5 5m windows by turnover (RUB):")
        print(top[["end","turnover","volume","open","close"]].to_string(index=False))

if __name__ == "__main__":
    pd.set_option("display.width", 180)
    main()
