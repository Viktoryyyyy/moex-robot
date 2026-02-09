#!/usr/bin/env python3
import os, sys, time, argparse, requests
import pandas as pd
from datetime import datetime, timedelta, date

API_PRIMARY = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
API_PUBLIC  = "https://iss.moex.com"
UA  = os.getenv("MOEX_UA", "moex_bot_cny_1m_to_5m/1.4").strip()

def token():
    t = os.getenv("MOEX_API_KEY","").strip()
    return t if t else None

def headers(use_token: bool):
    h = {"User-Agent": UA, "Accept": "application/json"}
    if use_token and token():
        h["Authorization"] = "Bearer " + token()
    return h

def try_get(url, params, use_token=True, base="primary"):
    """Одна попытка GET c явным base/токеном, возвращает (response, route_str)."""
    t0 = time.time()
    r = requests.get(url, headers=headers(use_token), params=params, timeout=(5,25))
    dt = (time.time() - t0)*1000
    route = f"{base}|{'tok' if use_token else 'no-tok'}|{r.status_code}|{int(dt)}ms"
    return r, route

def get_resilient(path, params):
    """
    Пытаемся по маршрутам:
      1) apim + токен
      2) apim без токена
      3) iss.moex.com (публичный), без токена
    Возвращает (json, last_route_for_log)
    """
    routes = []
    # 1) apim + token
    url = f"{API_PRIMARY}{path}"
    try:
        r, rt = try_get(url, params, use_token=True, base="apim")
        routes.append(rt)
        if r.status_code < 400:
            return r.json(), " -> ".join(routes)
        if r.status_code not in (401,403):
            r.raise_for_status()
    except requests.RequestException as e:
        routes.append(f"apim|tok|EXC:{e}")

    # 2) apim без токена
    try:
        r, rt = try_get(url, params, use_token=False, base="apim")
        routes.append(rt)
        if r.status_code < 400:
            return r.json(), " -> ".join(routes)
        if r.status_code not in (401,403):
            r.raise_for_status()
    except requests.RequestException as e:
        routes.append(f"apim|no-tok|EXC:{e}")

    # 3) public iss.moex.com
    url2 = f"{API_PUBLIC}{path}"
    try:
        r, rt = try_get(url2, params, use_token=False, base="public")
        routes.append(rt)
        if r.status_code < 400:
            return r.json(), " -> ".join(routes)
        r.raise_for_status()
    except requests.RequestException as e:
        routes.append(f"public|no-tok|EXC:{e}")
        raise RuntimeError("All routes failed: " + " :: ".join(routes))

def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)

def fetch_day_1m(D: date, max_pages=120) -> pd.DataFrame:
    path = f"/iss/engines/currency/markets/selt/boards/CETS/securities/CNYRUB_TOM/candles.json"
    params_base = {"interval": 1, "date": D.isoformat()}
    out = []
    start = 0
    page = 0
    while page < max_pages:
        page += 1
        params = params_base | {"start": start}
        j, rt = get_resilient(path, params)
        blk = j.get("candles", {})
        cols = blk.get("columns", [])
        data = blk.get("data", [])
        k = len(data)
        print(f"[{D}] page={page} start={start} rows={k} route={rt}")
        if not data:
            break

        df = pd.DataFrame(data, columns=cols)
        # Парсим время наивно (без TZ): валютные свечи на ISS в локальной биржевой таймзоне
        for col in ("begin","end"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if page == 1 and "end" in df.columns:
            dcounts = df["end"].dt.date.value_counts().sort_index()
            print(f"[{D}] page1 dates distribution:\n{dcounts.to_string()}")

        # Строгий фильтр по дате
        if "end" in df.columns:
            df["d"] = df["end"].dt.date
            if (df["d"] > D).all():
                break
            if (df["d"] < D).all():
                start += k
                continue
            df = df[df["d"] == D].copy()
            df.drop(columns=["d"], inplace=True)

        need = ["end","open","high","low","close","volume"]
        for c in need:
            if c not in df.columns:
                df[c] = pd.NA
        if df.empty:
            start += k
            continue

        df = df.dropna(subset=["end"]).drop_duplicates(subset=["end"])
        out.append(df[need])
        start += k
        time.sleep(0.02)

    if not out:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    day = pd.concat(out, ignore_index=True).drop_duplicates(subset=["end"]).sort_values("end")
    return day.reset_index(drop=True)

def to_5m(df1m: pd.DataFrame) -> pd.DataFrame:
    if df1m.empty:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    tmp = df1m.set_index("end")
    o = tmp["open"].resample("5T", label="right", closed="right").first()
    h = tmp["high"].resample("5T", label="right", closed="right").max()
    l = tmp["low"].resample("5T", label="right", closed="right").min()
    c = tmp["close"].resample("5T", label="right", closed="right").last()
    v = tmp["volume"].resample("5T", label="right", closed="right").sum()
    out = pd.concat([o,h,l,c,v], axis=1)
    out.columns = ["open","high","low","close","volume"]
    out = out.dropna(subset=["open","high","low","close"]).reset_index().rename(columns={"end":"end"})
    return out[["end","open","high","low","close","volume"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d0", required=True)
    ap.add_argument("--till", dest="d1", required=True)
    ap.add_argument("--max-pages", type=int, default=120)
    args = ap.parse_args()
    d0 = datetime.strptime(args.d0, "%Y-%m-%d").date()
    d1 = datetime.strptime(args.d1, "%Y-%m-%d").date()

    all_5m = []
    rows_1m = rows_5m = 0
    for D in daterange(d0, d1):
        day1m = fetch_day_1m(D, max_pages=args.max_pages)
        day5m = to_5m(day1m)
        rows_1m += len(day1m); rows_5m += len(day5m)
        all_5m.append(day5m)
        print(f"[{D}] strict 1m={len(day1m)} -> 5m={len(day5m)}")

    res = pd.concat(all_5m, ignore_index=True) if all_5m else pd.DataFrame(columns=["end","open","high","low","close","volume"])
    res = res.sort_values("end").drop_duplicates(subset=["end"])
    fn = f"cnyrub_5m_{d0}_{d1}.csv"
    res.to_csv(fn, index=False)
    print(f"Saved: {fn}; 1m_total={rows_1m}, 5m_total={rows_5m}")

if __name__ == "__main__":
    pd.set_option("display.width", 180)
    main()
