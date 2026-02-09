#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com")
UA  = os.getenv("MOEX_UA", "moex_bot_fx_fut_merge/1.1").strip()
KEY = os.getenv("MOEX_API_KEY", "")

FX_TICKER  = os.getenv("FX_TICKER", "CNYRUB_TOM").strip()
FUT_TICKER = os.getenv("FUT_TICKER", "SiZ5").strip()

MSK = ZoneInfo("Europe/Moscow")
TODAY = datetime.now(MSK).date().isoformat()

H = {"User-Agent": UA}
if KEY:
    H["Authorization"] = "Bearer " + KEY

def _to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns")
    data = block.get("data")
    meta = block.get("metadata")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return pd.DataFrame(data)
    if not cols and isinstance(meta, dict):
        cols = list(meta.keys())
    return pd.DataFrame(data=data or [], columns=cols or [])

def fetch_tradestats(kind: str, ticker: str, d: str) -> pd.DataFrame:
    url = f"{API}/iss/datashop/algopack/{kind}/tradestats/{ticker}.json"
    params = {"from": d, "till": d}
    r = requests.get(url, headers=H, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    blk = j.get("tradestats")
    if isinstance(blk, list) and len(blk) >= 2 and isinstance(blk[0], dict) and "columns" in blk[0]:
        df = pd.DataFrame(blk[1]["data"], columns=blk[0]["columns"])
    else:
        df = _to_df(blk if isinstance(blk, dict) else {})
    if df.empty:
        return pd.DataFrame(columns=["end","open","high","low","close","volume","ticker"])

    if "tradedate" in df.columns:
        tdate = pd.to_datetime(df["tradedate"].astype(str), errors="coerce").dt.date
    elif "TRADEDATE" in df.columns:
        tdate = pd.to_datetime(df["TRADEDATE"].astype(str), errors="coerce").dt.date
    else:
        tdate = pd.Series([d]*len(df), dtype="object")

    ttime_col = "tradetime" if "tradetime" in df.columns else ("TRADETIME" if "TRADETIME" in df.columns else None)
    if ttime_col:
        ttime = pd.to_datetime(df[ttime_col].astype(str), format="%H:%M:%S", errors="coerce").dt.time
    else:
        ttime = pd.Series(["00:00:00"]*len(df))

    end = pd.to_datetime(tdate.astype(str) + " " + pd.Series(ttime).astype(str))
    end = end.dt.tz_localize(MSK, nonexistent="shift_forward", ambiguous="NaT")
    df["end"] = end

    def pick(*cands):
        for c in cands:
            if c in df.columns:
                return df[c]
        return pd.Series([None]*len(df))

    out = pd.DataFrame({
        "end": df["end"],
        "open":  pd.to_numeric(pick("pr_open","OPEN","open"), errors="coerce"),
        "high":  pd.to_numeric(pick("pr_high","HIGH","high"), errors="coerce"),
        "low":   pd.to_numeric(pick("pr_low","LOW","low"), errors="coerce"),
        "close": pd.to_numeric(pick("pr_close","CLOSE","close","last"), errors="coerce"),
        "volume":pd.to_numeric(pick("vol","VOLUME","volume","VOL"), errors="coerce"),
    }).dropna(subset=["end"]).sort_values("end").reset_index(drop=True)

    out = out[~out["end"].duplicated(keep="last")]
    out["ticker"] = ticker
    return out

def safe_select(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    have = [c for c in cols if c in df.columns]
    if not have:
        return pd.DataFrame(columns=cols)
    return df[have]

def main():
    fx = fetch_tradestats("fx", FX_TICKER, TODAY)
    fut = fetch_tradestats("fo", FUT_TICKER, TODAY)

    if fx.empty:
        print(f"FX empty for {FX_TICKER} on {TODAY}", file=sys.stderr)
    if fut.empty:
        print(f"Futures empty for {FUT_TICKER} on {TODAY}", file=sys.stderr)

    fx_ren  = fx.rename(columns={c: f"{c}_fx"  for c in ["open","high","low","close","volume","ticker"] if c in fx.columns})
    fut_ren = fut.rename(columns={c: f"{c}_fut" for c in ["open","high","low","close","volume","ticker"] if c in fut.columns})

    fx_need  = ["end","open_fx","high_fx","low_fx","close_fx","volume_fx","ticker_fx"]
    fut_need = ["end","open_fut","high_fut","low_fut","close_fut","volume_fut","ticker_fut"]

    a = safe_select(fx_ren, fx_need)
    b = safe_select(fut_ren, fut_need)

    m = pd.merge(a, b, on="end", how="outer").sort_values("end").reset_index(drop=True)

    print(f"FX bars: {len(fx)} | FUT bars: {len(fut)} | MERGE rows: {len(m)}")
    if len(m) > 0:
        print("\nfirst 2 rows:")
        print(m.head(2).to_string(index=False))
        print("\nlast 2 rows:")
        print(m.tail(2).to_string(index=False))

    out_name = f"merge_5m_{FX_TICKER}_{FUT_TICKER}_{TODAY}.csv"
    m.to_csv(out_name, index=False)
    print(f"\nSaved: {out_name}")

if __name__ == "__main__":
    main()
