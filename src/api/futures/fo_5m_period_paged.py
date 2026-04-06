#!/usr/bin/env python3
import os, sys, pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from api.utils.lib_moex_api import get_json, resolve_fut_by_key

TZ_MSK = ZoneInfo("Europe/Moscow")

def load_tradestats_range_paged(ticker: str, day_from: str, day_till: str) -> pd.DataFrame:
    all_rows = []
    cols = None
    start = 0

    while True:
        j = get_json(
            "/iss/datashop/algopack/fo/tradestats/" + ticker + ".json",
            {"from": day_from, "till": day_till, "start": start},
            timeout=40.0,
        )
        b = j.get("data") or {}
        page_cols = b.get("columns", [])
        page_rows = b.get("data", [])

        if not page_cols:
            return pd.DataFrame(columns=["end", "open", "high", "low", "close", "volume"])

        if cols is None:
            cols = page_cols
        elif cols != page_cols:
            raise RuntimeError("cursor page columns mismatch")

        if not page_rows:
            break

        all_rows.extend(page_rows)

        cursor = j.get("data.cursor") or {}
        c_cols = cursor.get("columns", [])
        c_rows = cursor.get("data", [])

        if not c_cols or not c_rows:
            break

        idx = {name: i for i, name in enumerate(c_cols)}
        first = c_rows[0]
        total = first[idx["TOTAL"]] if "TOTAL" in idx else None
        page_size = first[idx["PAGESIZE"]] if "PAGESIZE" in idx else None
        index = first[idx["INDEX"]] if "INDEX" in idx else start

        if total is None or page_size is None:
            raise RuntimeError("cursor missing TOTAL/PAGESIZE")

        next_start = int(index) + int(page_size)
        if next_start >= int(total):
            break
        if next_start <= start:
            raise RuntimeError("non-increasing cursor start")

        start = next_start

    if not cols or not all_rows:
        return pd.DataFrame(columns=["end", "open", "high", "low", "close", "volume"])

    raw = pd.DataFrame(all_rows, columns=cols)
    need = {"tradedate", "tradetime", "pr_open", "pr_high", "pr_low", "pr_close", "vol"}
    if not need.issubset(raw.columns):
        return pd.DataFrame(columns=["end", "open", "high", "low", "close", "volume"])

    raw["end"] = raw["tradedate"] + " " + raw["tradetime"] + "+03:00"
    df = pd.DataFrame({
        "end": raw["end"],
        "open": pd.to_numeric(raw["pr_open"], errors="coerce"),
        "high": pd.to_numeric(raw["pr_high"], errors="coerce"),
        "low": pd.to_numeric(raw["pr_low"], errors="coerce"),
        "close": pd.to_numeric(raw["pr_close"], errors="coerce"),
        "volume": pd.to_numeric(raw["vol"], errors="coerce"),
    }).sort_values("end").reset_index(drop=True)

    return df

def main():
    key = os.getenv("FO_KEY") or (sys.argv[1] if len(sys.argv) > 1 else "")
    dfrom = os.getenv("FO_FROM") or (sys.argv[2] if len(sys.argv) > 2 else "")
    dtill = os.getenv("FO_TILL") or (sys.argv[3] if len(sys.argv) > 3 else "")

    if not key:
        print("Usage: FO_KEY=<substr> [FO_FROM=YYYY-MM-DD FO_TILL=YYYY-MM-DD] python fo_5m_period_paged.py", file=sys.stderr)
        sys.exit(2)

    if not dfrom or not dtill:
        today = datetime.now(TZ_MSK).date()
        dfrom = (today - timedelta(days=6)).isoformat()
        dtill = today.isoformat()

    secid = resolve_fut_by_key(key, board="rfud", limit_probe_day=dtill)
    if not secid:
        print("ERROR: no futures match key=" + repr(key), file=sys.stderr)
        sys.exit(3)

    df = load_tradestats_range_paged(secid, dfrom, dtill)
    if df.empty:
        print("WARN: no data for " + secid + " " + dfrom + ".." + dtill)
        sys.exit(0)

    out = secid.lower() + "_5m_" + dfrom + "_" + dtill + ".csv"
    df.to_csv(out, index=False)
    print("Ticker: " + secid)
    print("Saved: " + out + " rows=" + str(len(df)))
    print("# head(3)")
    print(df.head(3).to_csv(index=False))
    print("# tail(3)")
    print(df.tail(3).to_csv(index=False))

if __name__ == "__main__":
    main()
