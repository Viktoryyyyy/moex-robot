#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_api_base/1.0").strip()
TK  = os.getenv("MOEX_API_KEY", "").strip()

TICK_FUT = os.getenv("MOEX_FUT_TICKER", "SiZ5")
BASE_OI  = os.getenv("MOEX_OI_BASE", "si")   # futoi требует 'si'
TZ_MSK   = ZoneInfo("Europe/Moscow")

H = {"Authorization": "Bearer " + TK, "User-Agent": UA}

def iss_get(path, params=None):
    url = f"{API}/{path.lstrip('/')}"
    r = requests.get(url, headers=H, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

def to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns", [])
    data = block.get("data", [])
    return pd.DataFrame(data, columns=cols)

def load_tradestats(day: str) -> pd.DataFrame:
    j = iss_get(f"/iss/datashop/algopack/fo/tradestats/{TICK_FUT}.json",
                {"from":day, "till":day})
    b = j.get("data") or {}
    if not b or "columns" not in b:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    df = to_df(b)
    need = {"tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol"}
    if not need.issubset(df.columns):
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    df["end"] = (df["tradedate"].astype(str) + " " + df["tradetime"].astype(str) + "+03:00")
    out = pd.DataFrame({
        "end": df["end"],
        "open": pd.to_numeric(df["pr_open"], errors="coerce"),
        "high": pd.to_numeric(df["pr_high"], errors="coerce"),
        "low":  pd.to_numeric(df["pr_low"],  errors="coerce"),
        "close":pd.to_numeric(df["pr_close"],errors="coerce"),
        "volume":pd.to_numeric(df["vol"], errors="coerce")
    })
    return out

def load_futoi(day: str) -> pd.DataFrame:
    j = iss_get(f"/iss/analyticalproducts/futoi/securities/{BASE_OI}.json",
                {"from":day, "till":day})
    b = j.get("futoi") or {}
    if not b or "columns" not in b:
        return pd.DataFrame(columns=["end","oi_fiz","oi_yur","oi_total"])
    df = to_df(b)
    need = {"tradedate","tradetime","clgroup","pos"}
    if not need.issubset(df.columns):
        return pd.DataFrame(columns=["end","oi_fiz","oi_yur","oi_total"])
    df["end"] = (df["tradedate"].astype(str) + " " + df["tradetime"].astype(str) + "+03:00")
    piv = df.pivot_table(index="end", columns="clgroup", values="pos", aggfunc="last")
    for c in ("FIZ","YUR"):
        if c not in piv.columns: piv[c] = pd.NA
    piv = piv.rename(columns={"FIZ":"oi_fiz","YUR":"oi_yur"}).reset_index()
    piv["oi_total"] = piv[["oi_fiz","oi_yur"]].abs().sum(axis=1, skipna=False)
    return piv[["end","oi_fiz","oi_yur","oi_total"]]

def run_for_day(day_str: str):
    ts = load_tradestats(day_str)
    oi = load_futoi(day_str)  # берем тот же день; при необходимости можно сдвигать отдельно
    df = ts.merge(oi, on="end", how="left").sort_values("end").reset_index(drop=True)
    print(f"\n===== {day_str} (MSK) — {TICK_FUT} =====")
    print(f"rows={len(df)} cols={list(df.columns)}")
    if df.empty:
        print("WARN: no data")
        return
    # head/tail по 3 строки, CSV
    print("# head(3)")
    print(df.head(3).to_csv(index=False))
    print("# tail(3)")
    print(df.tail(3).to_csv(index=False))

def main():
    if not TK or len(TK) < 10:
        print("ERROR: MOEX_API_KEY missing/too short", file=sys.stderr); sys.exit(1)
    today_msk = datetime.now(TZ_MSK).date()
    days = [(today_msk - timedelta(days=i)).isoformat() for i in range(0,3)][::-1]  # D-2, D-1, D
    print(f"TICK_FUT={TICK_FUT} BASE_OI={BASE_OI} DAYS={days}")
    for d in days:
        try:
            run_for_day(d)
        except requests.HTTPError as e:
            print(f"\n===== {d} (MSK) — {TICK_FUT} =====")
            print(f"HTTP ERROR: {e.response.status_code} {e.response.text[:160].replace(chr(10),' ')}")
        except Exception as e:
            print(f"\n===== {d} (MSK) — {TICK_FUT} =====")
            print(f"ERROR: {e}")

if __name__ == "__main__":
    main()
