#!/usr/bin/env python3
import os, sys, time, argparse, pandas as pd, numpy as np
from datetime import datetime, date
from zoneinfo import ZoneInfo
from scripts.lib_moex_v3 import get_json

def to_df(block):
    cols = (block or {}).get("columns"); data = (block or {}).get("data"); meta = (block or {}).get("metadata", {})
    if isinstance(data, list) and data and isinstance(data[0], dict): return pd.DataFrame(data)
    if not cols and isinstance(meta, dict): cols = list(meta.keys())
    return pd.DataFrame(data or [], columns=cols or [])

def find_col(cols, candidates):
    s = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in s: return s[cand.lower()]
    for c in cols:
        for cand in candidates:
            if c.lower() == cand.lower(): return c
    return None

def build_dt(df, tz_name: str):
    if df.empty: return df
    tz = ZoneInfo(tz_name)
    cols = list(df.columns)
    c_systime = find_col(cols, ["systime"]); c_time = find_col(cols, ["time","tradetime"]); c_date = find_col(cols, ["date","tradedate","tradeday"])
    if c_systime:
        dt = pd.to_datetime(df[c_systime], errors="coerce", utc=True)
        if dt.dt.tz is None: dt = pd.to_datetime(df[c_systime], errors="coerce").dt.tz_localize(tz)
        else: dt = dt.dt.tz_convert(tz)
        out = df.copy(); out["dt"] = dt.dt.tz_convert(tz).dt.tz_localize(None); return out
    if not c_time: raise RuntimeError("нет time/tradetime/systime")
    t = pd.to_datetime(df[c_time].astype(str), errors="coerce", format="mixed")
    d = pd.to_datetime(df[c_date].astype(str), errors="coerce").dt.date if c_date else pd.Series([date.today()]*len(df))
    dt = [pd.Timestamp.combine(dd if pd.notna(dd) else date.today(), (tt.time() if pd.notna(tt) else datetime.now().time())) for dd,tt in zip(d,t)]
    dt = pd.to_datetime(pd.Series(dt)).dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(tz)
    out = df.copy(); out["dt"] = dt.dt.tz_localize(None); return out

def pick_numeric(df):
    cols = list(df.columns)
    c_price = find_col(cols, ["price","last","pr_close"]) or "price"
    c_qty   = find_col(cols, ["quantity","qty","volume"]) or "quantity"
    if c_price not in df.columns or c_qty not in df.columns:
        raise RuntimeError(f"нет price/quantity; cols={cols}")
    return c_price, c_qty

def id_col(df):
    cols = list(df.columns)
    for cand in ["tradenumber","tradeno","seqnum","sequencenumber","trade_id","id"]:
        c = find_col(cols, [cand])
        if c: return c
    return None

def fetch_trades_latest(ticker, limit):
    # Берём последние сделки, новейшие первыми
    path = f"/iss/engines/futures/markets/forts/boards/rfud/securities/{ticker}/trades.json"
    return get_json(path, params={"limit": limit, "reversed": 1})

def resample_5m(df, c_price, c_qty):
    if df.empty: return df
    g = df.set_index("dt").sort_index()
    o = g[c_price].resample("5min").ohlc()
    v = g[c_qty].resample("5min").sum().rename("volume")
    out = pd.concat([o, v], axis=1).dropna(how="all")
    out = out.rename(columns={"open":"OPEN","high":"HIGH","low":"LOW","close":"CLOSE"})
    out["end"] = out.index
    return out.reset_index(drop=True)[["end","OPEN","HIGH","LOW","CLOSE","volume"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default=os.getenv("MOEX_TICKER","SiZ5"))
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--sleep", type=float, default=2.0)
    ap.add_argument("--outfile_prefix", default="si_live_5m")
    ap.add_argument("--print_every", type=int, default=1)
    ap.add_argument("--tz", default=os.getenv("MOEX_TZ","Europe/Moscow"))
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    buf = pd.DataFrame()
    c_price = c_qty = None
    id_name = None
    max_id = None  # максимальный увиденный tradeno/seqnum для отсечки
    cycle = 0
    out_path = None

    while True:
        try:
            raw = fetch_trades_latest(args.ticker, args.limit)
        except Exception as e:
            print("ERROR fetch:", e, file=sys.stderr); time.sleep(max(args.sleep,1.0)); continue

        # найдём блок с данными
        blk = None
        for k, v in raw.items():
            if isinstance(v, dict):
                df_try = to_df(v)
                if not df_try.empty and any(x.lower() in map(str.lower, df_try.columns) for x in ["time","tradetime","price","quantity","tradenumber","tradeno","seqnum"]):
                    blk = df_try; break
        if blk is None or blk.empty:
            if args.debug: print("[debug] empty latest trades page")
            time.sleep(args.sleep); cycle += 1; continue

        # инициализация
        if id_name is None: id_name = id_col(blk)
        if c_price is None or c_qty is None:
            try: c_price, c_qty = pick_numeric(blk)
            except Exception as e:
                print("ERROR columns:", e, file=sys.stderr); time.sleep(args.sleep); cycle += 1; continue

        # нормализуем время
        try:
            blk = build_dt(blk, args.tz)
        except Exception as e:
            print("ERROR time:", e, file=sys.stderr); time.sleep(args.sleep); cycle += 1; continue

        # оставляем только свежие строки, с id > max_id (если id есть)
        if id_name and id_name in blk.columns:
            blk[id_name] = pd.to_numeric(blk[id_name], errors="coerce")
            if max_id is not None:
                blk = blk[blk[id_name] > max_id]
            # обновим max_id по входящему батчу
            if not blk.empty:
                new_max = blk[id_name].max(skipna=True)
                if pd.notna(new_max): max_id = int(new_max)
        else:
            # если нет явного id, будем отбрасывать по dt/price/qty на уровне ресемпла (OK)
            pass

        if blk.empty:
            if args.debug: print(f"[debug] no fresh trades (max_id={max_id})")
            time.sleep(args.sleep); cycle += 1; continue

        # в буфер только нужные колонки
        cols_keep = ["dt", c_price, c_qty] + ([id_name] if id_name else [])
        buf = pd.concat([buf, blk[cols_keep]], ignore_index=True)

        # ресемпл 5m
        bars = resample_5m(buf, c_price, c_qty)
        if not bars.empty:
            dstr = bars["end"].iloc[-1].date().isoformat()
            out_path = f"{args.outfile_prefix}_{dstr}.csv"
            tmp = out_path + ".tmp"; bars.to_csv(tmp, index=False); os.replace(tmp, out_path)

        cycle += 1
        if cycle % max(args.print_every,1) == 0 and not bars.empty:
            last = bars.tail(1).iloc[0]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {args.ticker} 5m end={last['end']}  O={last['OPEN']} H={last['HIGH']} L={last['LOW']} C={last['CLOSE']} V={int(last['volume']) if pd.notna(last['volume']) else 'NaN'}  -> {out_path}")

        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
