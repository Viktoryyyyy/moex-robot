#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_api_base/1.3").strip()
TK  = os.getenv("MOEX_API_KEY", "").strip()
TZ_MSK = ZoneInfo("Europe/Moscow")
DAY = datetime.now(TZ_MSK).date().isoformat()
H = {"Authorization": "Bearer " + TK, "User-Agent": UA}

def iss_get(path, params=None):
    url = f"{API}/{path.lstrip('/')}"
    r = requests.get(url, headers=H, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

def to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns", [])
    data = block.get("data", [])
    try:
        return pd.DataFrame(data, columns=cols)
    except Exception:
        return pd.DataFrame(columns=cols)

def list_cny_futures():
    # список инструментов на RFUD, отберём всё, где SECID содержит 'CNY'
    j = iss_get("/iss/engines/futures/markets/forts/boards/rfud/securities.json")
    b = j.get("securities") or {}
    df = to_df(b)
    if df.empty: return pd.DataFrame(columns=["SECID","SHORTNAME"])
    mask = df["SECID"].astype(str).str.contains("CNY", case=False, na=False)
    cols = [c for c in ["SECID","SHORTNAME","LISTLEVEL","LATNAME"] if c in df.columns]
    return df.loc[mask, cols].drop_duplicates()

def tradestats_rows(ticker: str) -> int:
    try:
        j = iss_get(f"/iss/datashop/algopack/fo/tradestats/{ticker}.json",
                    {"from":DAY, "till":DAY})
        b = j.get("data") or {}
        df = to_df(b)
        # проверим базовые поля для 5м свечей
        need = {"tradedate","tradetime","pr_open","pr_close"}
        if not need.issubset(set(df.columns)): return 0
        return len(df)
    except requests.HTTPError:
        return 0

def save_tradestats(ticker: str):
    j = iss_get(f"/iss/datashop/algopack/fo/tradestats/{ticker}.json",
                {"from":DAY, "till":DAY})
    b = j.get("data") or {}
    raw = to_df(b)
    raw["end"] = (raw["tradedate"].astype(str)+" "+raw["tradetime"].astype(str)+"+03:00")
    df = pd.DataFrame({
        "end": raw["end"],
        "open":   pd.to_numeric(raw["pr_open"],  errors="coerce"),
        "high":   pd.to_numeric(raw["pr_high"],  errors="coerce"),
        "low":    pd.to_numeric(raw["pr_low"],   errors="coerce"),
        "close":  pd.to_numeric(raw["pr_close"], errors="coerce"),
        "volume": pd.to_numeric(raw["vol"],      errors="coerce"),
    }).sort_values("end")
    out = f"{ticker.lower()}_5m_{DAY}.csv"
    df.to_csv(out, index=False)
    print(f"Saved: {out} rows={len(df)}")
    print("# head(5)"); print(df.head(5).to_csv(index=False))
    print("# tail(5)"); print(df.tail(5).to_csv(index=False))

def main():
    if not TK or len(TK) < 10:
        print("ERROR: MOEX_API_KEY missing/too short", file=sys.stderr); sys.exit(1)

    cny = list_cny_futures()
    if cny.empty:
        print("ERROR: Не нашли фьючерсы с 'CNY' на RFUD")
        sys.exit(2)

    # Отсортируем кандидатов по «похожести» на актуальный (заканчивается на текущий годовой код/месяц)
    # Но главный критерий — наличие сегодняшних 5м баров.
    best = None; best_rows = 0
    for secid in cny["SECID"].astype(str).unique():
        n = tradestats_rows(secid)
        print(f"probe {secid}: rows_today={n}")
        if n > best_rows:
            best_rows = n; best = secid

    if not best or best_rows == 0:
        print(f"WARN: На сегодня нет 5м данных по кандидатам:\n{cny.to_string(index=False)}")
        sys.exit(0)

    print(f"Use ticker: {best} (rows={best_rows})")
    save_tradestats(best)

if __name__ == "__main__":
    main()
