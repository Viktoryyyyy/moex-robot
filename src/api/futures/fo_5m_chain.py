#!/usr/bin/env python3
import os, sys
from datetime import datetime, date, timedelta
import pandas as pd
from lib_moex_api import get_json

MONTH_CODES = {
    3:  "H",  # Mar
    6:  "M",  # Jun
    9:  "U",  # Sep
    12: "Z",  # Dec
}

def iter_days(dfrom: str, dtill: str):
    d0 = datetime.fromisoformat(dfrom).date()
    d1 = datetime.fromisoformat(dtill).date()
    step = timedelta(days=1)
    cur = d0
    while cur <= d1:
        yield cur
        cur += step

def third_wednesday(year: int, month: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != 2:  # 0=Mon, 2=Wed
        d += timedelta(days=1)
    d += timedelta(days=14)
    return d

def build_calendar_meta(key: str, dfrom: str, dtill: str) -> pd.DataFrame:
    d0 = datetime.fromisoformat(dfrom).date()
    d1 = datetime.fromisoformat(dtill).date()
    year_min = d0.year
    year_max = d1.year + 1

    rows = []
    for y in range(year_min, year_max + 1):
        for m, code in MONTH_CODES.items():
            exp = third_wednesday(y, m)
            secid = f"{key}{code}{y % 10}"
            rows.append({"SECID": secid, "exp_date": exp})

    df = pd.DataFrame(rows)
    df = df.sort_values("exp_date").reset_index(drop=True)
    return df

def pick_front_month(meta: pd.DataFrame, day: date) -> str | None:
    if meta.empty:
        return None
    cand = meta[meta["exp_date"] >= day]
    if cand.empty:
        return None
    idx = cand["exp_date"].idxmin()
    return cand.at[idx, "SECID"]

def load_tradestats(ticker: str, day: date) -> pd.DataFrame:
    ds = day.isoformat()
    j = get_json(
        f"/iss/datashop/algopack/fo/tradestats/{ticker}.json",
        {"from": ds, "till": ds},
        timeout=25.0,
    )
    b = j.get("data") or {}
    cols, data = b.get("columns", []), b.get("data", [])
    if not cols or not data:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    raw = pd.DataFrame(data, columns=cols)
    need = {"tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol"}
    if not need.issubset(raw.columns):
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    raw["end"] = raw["tradedate"] + " " + raw["tradetime"] + "+03:00"
    df = pd.DataFrame({
        "end":   raw["end"],
        "open":  pd.to_numeric(raw["pr_open"],  errors="coerce"),
        "high":  pd.to_numeric(raw["pr_high"],  errors="coerce"),
        "low":   pd.to_numeric(raw["pr_low"],   errors="coerce"),
        "close": pd.to_numeric(raw["pr_close"], errors="coerce"),
        "volume":pd.to_numeric(raw["vol"],      errors="coerce"),
    }).sort_values("end").reset_index(drop=True)
    return df

def main():
    key   = os.getenv("FO_KEY")  or (sys.argv[1] if len(sys.argv) > 1 else "")
    dfrom = os.getenv("FO_FROM") or (sys.argv[2] if len(sys.argv) > 2 else "")
    dtill = os.getenv("FO_TILL") or (sys.argv[3] if len(sys.argv) > 3 else "")
    out   = os.getenv("FO_OUT")  or (sys.argv[4] if len(sys.argv) > 4 else "")

    if not key or not dfrom or not dtill:
        print(
            "Usage: FO_KEY=<substr> FO_FROM=YYYY-MM-DD FO_TILL=YYYY-MM-DD "
            "[FO_OUT=path] python fo_5m_chain.py",
            file=sys.stderr,
        )
        sys.exit(2)

    if not out:
        out = f"{key.lower()}_chain_5m_{dfrom}_{dtill}.csv"

    meta = build_calendar_meta(key, dfrom, dtill)
    if meta.empty:
        print(f"ERROR: empty calendar meta for key='{key}'", file=sys.stderr)
        sys.exit(1)

    print("Futures calendar (by exp_date):")
    for _, r in meta.iterrows():
        print(f"  {r['SECID']} exp={r['exp_date']}")

    all_rows = []
    total_rows = 0
    last_secid = None

    for day in iter_days(dfrom, dtill):
        secid = pick_front_month(meta, day)
        if not secid:
            print(f"WARN: no front-month for {day}", file=sys.stderr)
            continue

        if secid != last_secid:
            print(f"{day}: using {secid}")
            last_secid = secid

        df = load_tradestats(secid, day)
        if df.empty:
            print(f"WARN: no tradestats for {secid} {day}", file=sys.stderr)
            continue

        df["ticker"] = secid
        all_rows.append(df)
        total_rows += len(df)

    if not all_rows:
        print("ERROR: no data collected for given period", file=sys.stderr)
        sys.exit(0)

    res = pd.concat(all_rows, ignore_index=True).sort_values("end").reset_index(drop=True)
    res.to_csv(out, index=False)

    print(f"Saved: {out} rows={len(res)} (collected={total_rows})")
    print(f"Days covered: {dfrom}..{dtill}, unique tickers: {res['ticker'].nunique()}")
    print("# head(3)")
    print(res.head(3).to_csv(index=False), end="")
    print("# tail(3)")
    print(res.tail(3).to_csv(index=False), end="")

if __name__ == "__main__":
    main()
