#!/usr/bin/env python3
import os, time, argparse, pandas as pd, numpy as np
from datetime import datetime, date
from zoneinfo import ZoneInfo
from scripts.lib_moex_v3 import get_json

def to_df(block):
    cols = (block or {}).get("columns")
    data = (block or {}).get("data")
    meta = (block or {}).get("metadata", {})
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return pd.DataFrame(data)
    if not cols and isinstance(meta, dict):
        cols = list(meta.keys())
    return pd.DataFrame(data or [], columns=cols or [])

def norm_cols(df):
    m = {c.lower(): c for c in df.columns}
    def col(name, default=None):
        return m.get(name.lower(), default)
    return col

def fetch_futoi(ticker: str, the_date: str):
    # В FUTOI тикер базового актива (si), не контракт (SiZ5)
    path = f"/iss/analyticalproducts/futoi/securities/{ticker}.json"
    return get_json(path, params={"from": the_date, "till": the_date, "iss.meta": "off"})

def build_ts(df, tz: str):
    c = norm_cols(df)
    cd = c("tradedate"); ct = c("tradetime")
    if not cd or not ct: return pd.DataFrame()
    dt = pd.to_datetime(df[cd].astype(str) + " " + df[ct].astype(str), errors="coerce")
    tzinfo = ZoneInfo(tz)
    dt = dt.dt.tz_localize(tzinfo, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(tzinfo)
    out = df.copy()
    out["dt"] = dt.dt.tz_localize(None)
    return out

def pivot_futoi(df):
    # ожидаем поля: clgroup (FIZ/YUR), pos
    if df.empty: return pd.DataFrame()
    c = norm_cols(df)
    cg = c("clgroup"); p = c("pos"); dtt = "dt"
    if not cg or not p or dtt not in df.columns: return pd.DataFrame()
    # оставим последнюю запись на момент времени внутри каждой группы
    df2 = (df[[dtt, cg, p]]
           .dropna(subset=[dtt])
           .sort_values([dtt])
           .drop_duplicates([dtt, cg], keep="last"))
    # сводная таблица: dt x {FIZ,YUR} по pos
    piv = df2.pivot(index=dtt, columns=cg, values=p).rename_axis(None, axis=1)
    for col in ["FIZ", "YUR"]:
        if col not in piv.columns:
            piv[col] = np.nan
    piv = piv[["FIZ", "YUR"]]
    piv = piv.rename(columns={"FIZ": "oi_fiz", "YUR": "oi_yur"})
    piv["oi_total"] = piv[["oi_fiz", "oi_yur"]].sum(axis=1, min_count=1)
    piv = piv.sort_index()
    piv["end"] = piv.index
    return piv.reset_index(drop=True)

def resample_5m(df):
    if df.empty: return df
    g = df.set_index("end").sort_index()
    # приведём к равномерной 5м сетке и протянем значения вперёд
    idx = pd.date_range(start=g.index.min().floor("5min"),
                        end=g.index.max().ceil("5min"),
                        freq="5min")
    g = g.reindex(idx).ffill()
    g["end"] = g.index
    return g.reset_index(drop=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default=os.getenv("FUTOI_TICKER", "si"))
    ap.add_argument("--sleep", type=float, default=300.0, help="интервал опроса, сек (по умолчанию 5 минут)")
    ap.add_argument("--print_every", type=int, default=1)
    ap.add_argument("--tz", default=os.getenv("MOEX_TZ","Europe/Moscow"))
    ap.add_argument("--outfile_prefix", default="futoi_si")
    args = ap.parse_args()

    tz = args.tz
    cycle = 0
    last_date = None
    acc = pd.DataFrame()  # аккумулируем внутри дня

    while True:
        today = date.today().isoformat()
        if last_date is None or today != last_date:
            # новый день — сбрасываем аккумулированное
            acc = pd.DataFrame()
            last_date = today

        try:
            raw = fetch_futoi(args.ticker, today)
        except Exception as e:
            print("ERROR fetch:", e, flush=True)
            time.sleep(max(args.sleep, 5.0)); continue

        futoi = to_df(raw.get("futoi", {}))
        if not futoi.empty:
            futoi = build_ts(futoi, tz)
            piv = pivot_futoi(futoi)
            if not piv.empty:
                # обновим аккумулятор: берём последние значения по dt
                if acc.empty:
                    acc = piv
                else:
                    acc = (pd.concat([acc, piv], ignore_index=True)
                           .sort_values("end")
                           .drop_duplicates("end", keep="last"))
                bars = resample_5m(acc[["end","oi_fiz","oi_yur","oi_total"]])
                if not bars.empty:
                    dstr = bars["end"].iloc[-1].date().isoformat()
                    out_path = f"{args.outfile_prefix}_{dstr}.csv"
                    tmp = out_path + ".tmp"
                    bars.to_csv(tmp, index=False)
                    os.replace(tmp, out_path)

                    cycle += 1
                    if cycle % max(args.print_every,1) == 0:
                        last = bars.tail(1).iloc[0]
                        of = last.get("oi_fiz", np.nan)
                        oy = last.get("oi_yur", np.nan)
                        ot = last.get("oi_total", np.nan)
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] FUTOI 5m end={last['end']}  oi_total={int(ot) if pd.notna(ot) else 'NaN'}  (FIZ={int(of) if pd.notna(of) else 'NaN'}, YUR={int(oy) if pd.notna(oy) else 'NaN'})  -> {out_path}", flush=True)
        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
