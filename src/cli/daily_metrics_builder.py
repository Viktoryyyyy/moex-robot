#!/usr/bin/env python3
import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date
import pandas as pd

REQUIRED_5M_COLS = ["end", "open", "high", "low", "close", "volume"]

def die(msg, code=1):
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)

def ensure_dirs():
    os.makedirs("data/realtime", exist_ok=True)
    os.makedirs("data/state", exist_ok=True)

def check_api_key():
    if not os.getenv("MOEX_API_KEY"):
        die("MOEX_API_KEY missing in environment")

def import_api_layer():
    try:
        from src.api.utils.lib_moex_api import get_json, resolve_fut_by_key
        return get_json, resolve_fut_by_key
    except Exception:
        pass
    try:
        from src.misc.lib_moex import get_json, resolve_fut_by_key
        return get_json, resolve_fut_by_key
    except Exception:
        pass
    die("Cannot import MOEX API layer")

def resolve_trade_date(get_json, secid):
    """
    Resolve D-1 as the last *completed* trading day via MOEX tradestats probe.
    Rules:
      - never use today
      - accept day if last 5m bar reaches 23:50 OR number of bars >= 170
    Source of truth: MOEX only.
    """
    from datetime import date, timedelta

    today = date.today()
    for i in range(1, 21):
        d = today - timedelta(days=i)
        ds = d.isoformat()
        js = get_json(f'/iss/datashop/algopack/fo/tradestats/{secid}.json', {'from': ds, 'till': ds})

        best_cols = None
        best_data = None
        if isinstance(js, dict):
            for _, b in js.items():
                if not isinstance(b, dict):
                    continue
                cols = b.get('columns') or []
                data = b.get('data') or []
                if cols and data:
                    best_cols = cols
                    best_data = data
                    break

        if not best_cols or not best_data:
            continue

        n = len(best_data)
        # If we can find tradetime column - check last bar time
        try:
            idx = {str(c): j for j, c in enumerate(best_cols)}
            if 'tradetime' in idx:
                last_time = str(best_data[-1][idx['tradetime']])
                if last_time.startswith('23:50') or n >= 170:
                    return ds
            else:
                if n >= 170:
                    return ds
        except Exception:
            if n >= 170:
                return ds

    die('Cannot resolve D-1 as completed trading day via MOEX tradestats probe (last 20 calendar days)')


def load_5m(secid, trade_date):
    try:
        from src.api.futures.fo_5m_day import load_tradestats
        df = load_tradestats(secid, trade_date)
        if isinstance(df, pd.DataFrame):
            return df
    except Exception:
        pass
    try:
        from src.api.futures.fo_feed_intraday import load_fo_5m_day
        rows = load_fo_5m_day(secid=secid, trade_date=date.fromisoformat(trade_date))
        return pd.DataFrame(rows)
    except Exception:
        pass
    die("Cannot load 5m data via API layer")

def validate_5m(df, trade_date):
    if df is None or df.empty:
        die("5m dataframe empty")
    for c in REQUIRED_5M_COLS:
        if c not in df.columns:
            die(f"Missing column {c}")
    df = df[REQUIRED_5M_COLS].copy()
    df["end"] = df["end"].astype(str)
    df = df.sort_values("end").reset_index(drop=True)
    if not df["end"].is_monotonic_increasing:
        die("end not monotonic")
    dates = set(df["end"].str[:10])
    if dates != {trade_date}:
        die(f"5m spans multiple dates: {dates}")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--out-5m", required=True)
    ap.add_argument("--out-day", required=True)
    args = ap.parse_args()

    if args.date == "D-1":
        trade_date_override = None
    else:
        try:
            from datetime import date as _date
            _date.fromisoformat(args.date)
        except Exception:
            die("Invalid --date, expected D-1 or YYYY-MM-DD")
        trade_date_override = args.date

    ensure_dirs()
    check_api_key()

    get_json, resolve_fut_by_key = import_api_layer()

    secid = resolve_fut_by_key(args.key, board="rfud")
    if not secid:
        die(f"No futures found for key={args.key}")

    trade_date = trade_date_override or resolve_trade_date(get_json, secid)

    secid2 = resolve_fut_by_key(args.key, board="rfud", limit_probe_day=trade_date)
    if secid2:
        secid = secid2

    df = load_5m(secid, trade_date)
    df = validate_5m(df, trade_date)

    df.to_csv(args.out_5m, index=False)

    df = pd.read_csv(args.out_5m)
    df = validate_5m(df, trade_date)

    o = float(df.iloc[0]["open"])
    h = float(df["high"].max())
    l = float(df["low"].min())
    c = float(df.iloc[-1]["close"])

    if c == 0:
        rel_range = 0.0
    else:
        rel_range = (h - l) / c

    if h - l == 0:
        trend_ratio = 0.0
    else:
        trend_ratio = abs(c - o) / (h - l)

    out = pd.DataFrame(
        [{"date": trade_date, "rel_range": rel_range, "trend_ratio": trend_ratio}],
        columns=["date", "rel_range", "trend_ratio"],
    )
    out.to_csv(args.out_day, index=False)

if __name__ == "__main__":
    main()
