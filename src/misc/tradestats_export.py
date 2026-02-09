#!/usr/bin/env python3
import os, sys, argparse, requests, pandas as pd, datetime as dt

API = "https://apim.moex.com"
H = {"Authorization":"Bearer "+os.getenv("MOEX_API_KEY",""),
     "User-Agent":"moex_bot_tradestats_export/1.1"}

def to_df(block):
    cols = block.get("columns")
    data = block.get("data")
    meta = block.get("metadata")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    else:
        if not cols and isinstance(meta, dict): cols = list(meta.keys())
        df = pd.DataFrame(data=data, columns=cols)
    df.columns = [str(c).lower() for c in df.columns]
    return df

def fetch(ticker, d1, d2):
    urls = [
        f"{API}/iss/datashop/algopack/fo/tradestats/{ticker}.json?from={d1}&till={d2}",
        f"{API}/iss/datashop/algopack/fo/tradestats.json?ticker={ticker}&from={d1}&till={d2}",
    ]
    last = None
    for u in urls:
        r = requests.get(u, headers=H, timeout=30)
        last = (r.status_code, u, r.headers.get("Content-Type"))
        if r.ok and "application/json" in r.headers.get("Content-Type",""):
            j = r.json()
            blk = j.get("tradestats") or j.get("candles") or j.get("data") or {}
            if not isinstance(blk, dict) and "tradestats" in j and isinstance(j["tradestats"], list):
                blk = j["tradestats"][0]
            try:
                df = to_df(blk)
                return df, u
            except Exception:
                continue
    raise SystemExit(f"ERR: no data, last={last}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True)      # SiZ5
    ap.add_argument("--date", required=True)        # YYYY-MM-DD
    ap.add_argument("--ma", type=int, default=20)   # окно MA/STD (базово 20 баров)
    args = ap.parse_args()

    d = dt.date.fromisoformat(args.date).isoformat()
    df, used = fetch(args.ticker, d, d)

    # гарантируем нужные поля из пробы
    need = ["tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol","pr_vwap","disb","systime","secid","asset_code"]
    for c in need:
        if c not in df.columns: df[c] = None

    # фильтр по дате и сортировка по времени
    df = df[df["tradedate"].astype(str).str[:10] == d].copy()
    df["datetime"] = pd.to_datetime(df["tradedate"].astype(str).str[:10] + " " + df["tradetime"].astype(str), errors="coerce")
    df = df.sort_values("datetime")

    # нормализованные OHLCV
    out = pd.DataFrame({
        "datetime": df["datetime"],
        "tradedate": df["tradedate"].astype(str).str[:10],
        "tradetime": df["tradetime"],
        "open": pd.to_numeric(df["pr_open"], errors="coerce"),
        "high": pd.to_numeric(df["pr_high"], errors="coerce"),
        "low":  pd.to_numeric(df["pr_low"],  errors="coerce"),
        "close":pd.to_numeric(df["pr_close"],errors="coerce"),
        "volume": pd.to_numeric(df["vol"], errors="coerce"),
        "secid": df["secid"],
        "asset_code": df["asset_code"],
        "systime": df["systime"],
        "vwap": pd.to_numeric(df["pr_vwap"], errors="coerce"),
        "imbalance": pd.to_numeric(df["disb"], errors="coerce"),
    })

    # базовые вычисления для MR-1
    w = max(2, int(args.ma))
    out["ret1"] = out["close"].pct_change()                    # доходность t/t-1
    out["ma_close"] = out["close"].rolling(w, min_periods=1).mean()
    out["std_close"] = out["close"].rolling(w, min_periods=2).std()
    out["z_close"] = (out["close"] - out["ma_close"]) / out["std_close"]  # Z-score
    out["dev_ma"] = out["close"] / out["ma_close"]            # относительное отклонение
    # диапазон бара — полезно для фильтров волатильности
    out["hl_range"] = (out["high"] - out["low"]).abs()

    # сохранение
    fname = f"tradestats_{args.ticker}_{d}.csv"
    out.to_csv(fname, index=False)
    print(f"OK: {fname} rows={len(out)} URL={used}  (MA={w})")

if __name__ == "__main__":
    main()
