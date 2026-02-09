#!/usr/bin/env python3
import os, sys, argparse, requests, datetime as dt
import pandas as pd
from typing import Any, Dict

API = "https://apim.moex.com"
H = {
    "Authorization": "Bearer " + os.getenv("MOEX_API_KEY", ""),
    "User-Agent": "moex_bot_futoi_moexstyle/2.0"
}

def to_df_from_block(block: Dict[str, Any]) -> pd.DataFrame:
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

def fetch(ticker: str, d1: str, d2: str) -> pd.DataFrame:
    url = f"{API}/iss/analyticalproducts/futoi/securities/{ticker}.json?from={d1}&till={d2}"
    r = requests.get(url, headers=H, timeout=30)
    r.raise_for_status()
    j = r.json()
    blk = j.get("futoi") or j.get("securities") or {}
    return to_df_from_block(blk)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default="si", help="базовый актив FUTOI (напр. si)")
    ap.add_argument("--from", dest="dfrom", required=True, help="YYYY-MM-DD")
    ap.add_argument("--till", dest="dtill", required=True, help="YYYY-MM-DD")
    ap.add_argument("--eod", action="store_true", help="вывести только последнюю запись за день по каждой clgroup")
    args = ap.parse_args()

    try:
        d1 = dt.date.fromisoformat(args.dfrom).isoformat()
        d2 = dt.date.fromisoformat(args.dtill).isoformat()
    except Exception:
        print("Нужен формат дат YYYY-MM-DD", file=sys.stderr); sys.exit(2)

    df = fetch(args.ticker.lower(), d1, d2)

    # ожидаемые поля: tradedate, tradetime, ticker='Si', clgroup in {FIZ,YUR}, pos, pos_long, pos_short, pos_long_num, pos_short_num, systime, trade_session_date
    need = ["tradedate","tradetime","ticker","clgroup","pos","pos_long","pos_short","pos_long_num","pos_short_num","systime","trade_session_date"]
    for c in need:
        if c not in df.columns: df[c] = None

    # фильтруем по датам, оставляя только d1..d2
    if "tradedate" in df.columns:
        td = df["tradedate"].astype(str).str[:10]
        df = df[(td >= d1) & (td <= d2)]

    if args.eod:
        # берем максимум по времени в разрезе (tradedate, clgroup)
        # чтобы корректно сортировать по времени
        if "tradetime" in df.columns:
            df["_dt"] = pd.to_datetime(df["tradedate"].astype(str).str[:10] + " " + df["tradetime"].astype(str), errors="coerce")
        else:
            df["_dt"] = pd.to_datetime(df["systime"], errors="coerce")
        df = (df.sort_values("_dt")
                .groupby(["tradedate","clgroup"], as_index=False)
                .tail(1)
                .drop(columns=["_dt"]))
        df = df.sort_values(["tradedate","clgroup"])

    else:
        df = df.sort_values(["tradedate","tradetime","clgroup"])

    keep = ["tradedate","tradetime","ticker","clgroup","pos","pos_long","pos_short","pos_long_num","pos_short_num","systime","trade_session_date"]
    print("\t".join(keep))
    for _, row in df[keep].iterrows():
        print("\t".join("" if pd.isna(row[k]) else str(row[k]) for k in keep))
    print(f"\nrows={len(df)} OK for {args.ticker.upper()} {d1}..{d2} {'EOD' if args.eod else 'INTRADAY'}")

if __name__ == "__main__":
    main()
