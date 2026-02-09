#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_api_base/1.3").strip()
TK  = os.getenv("MOEX_API_KEY", "").strip()

# По умолчанию предполагаем актуальный контракт CNYZ5.
# Можно переопределить переменной окружения MOEX_FUT_TICKER=CNYZ5 (или нужный тикер).
TICK_FUT = os.getenv("MOEX_FUT_TICKER", "CNYZ5").strip()

TZ_MSK = ZoneInfo("Europe/Moscow")
DAY = datetime.now(TZ_MSK).date().isoformat()

H = {"Authorization": "Bearer " + TK, "User-Agent": UA}

def iss_get(path, params=None):
    url = f"{API}/{path.lstrip('/')}"
    r = requests.get(url, headers=H, params=params or {}, timeout=25)
    r.raise_for_status()
    return r.json()

def to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns", [])
    data = block.get("data", [])
    try:
        return pd.DataFrame(data, columns=cols)
    except Exception:
        return pd.DataFrame(columns=cols)

def load_tradestats(ticker: str, day: str) -> pd.DataFrame:
    j = iss_get(f"/iss/datashop/algopack/fo/tradestats/{ticker}.json",
                {"from": day, "till": day})
    b = j.get("data") or {}
    if "columns" not in b:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    raw = to_df(b)
    need = {"tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol"}
    if not need.issubset(raw.columns):
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    raw["end"] = (raw["tradedate"].astype(str) + " " + raw["tradetime"].astype(str) + "+03:00")
    df = pd.DataFrame({
        "end":    raw["end"],
        "open":   pd.to_numeric(raw["pr_open"],  errors="coerce"),
        "high":   pd.to_numeric(raw["pr_high"],  errors="coerce"),
        "low":    pd.to_numeric(raw["pr_low"],   errors="coerce"),
        "close":  pd.to_numeric(raw["pr_close"], errors="coerce"),
        "volume": pd.to_numeric(raw["vol"],      errors="coerce"),
    })
    df = df.sort_values("end").reset_index(drop=True)
    return df

def main():
    if not TK or len(TK) < 10:
        print("ERROR: MOEX_API_KEY missing/too short", file=sys.stderr); sys.exit(1)
    df = load_tradestats(TICK_FUT, DAY)
    out_name = f"{TICK_FUT.lower()}_5m_{DAY}.csv"
    if df.empty:
        print(f"WARN: no data for {TICK_FUT} {DAY}")
        return
    df.to_csv(out_name, index=False)
    print(f"Saved: {out_name} rows={len(df)}")
    # Краткий превью
    print("# head(5)")
    print(df.head(5).to_csv(index=False))
    print("# tail(5)")
    print(df.tail(5).to_csv(index=False))

if __name__ == "__main__":
    main()
