#!/usr/bin/env python3
import os, sys, time, argparse, pandas as pd, numpy as np
from datetime import datetime
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

def find_col(cols, candidates):
    s = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in s: return s[cand.lower()]
    for c in cols:
        for cand in candidates:
            if c.lower() == cand.lower(): return c
    return None

def map_price_qty(df):
    cols = list(df.columns)
    c_price = find_col(cols, ["price","PRICE","pr","last_price"])
    c_qty   = find_col(cols, ["quantity","qty","volume","value","NUMBER","VOLUME"])
    if not c_price or not c_qty:
        raise RuntimeError(f"Не найдено price/quantity в стакане. cols={cols}")
    return c_price, c_qty

def fetch_orderbook(ticker, depth):
    path = f"/iss/engines/futures/markets/forts/securities/{ticker}/orderbook.json"
    return get_json(path, params={"depth": depth})

def compute_snapshot(raw, tz: str, levels: int):
    tzinfo = ZoneInfo(tz)
    bids = to_df(raw.get("bids", {}))
    offers_df = to_df(raw.get("offers", {}))
    asks_df = to_df(raw.get("asks", {}))
    asks = offers_df if not offers_df.empty else asks_df
    meta = to_df(raw.get("orderbook", {}))  # иногда тут время/системные поля

    if bids.empty and asks.empty:
        return None

    ts = None
    # время пытаемся взять из служебного блока; fallback — локальное
    for blk in (raw.get("orderbook", {}), raw.get("marketdata", {}), raw.get("securities", {})):
        df = to_df(blk)
        if not df.empty:
            for cand in ["systime","SYSTIME","time","TRADETIME","datetime","timestamp"]:
                c = find_col(list(df.columns), [cand])
                if c is not None and pd.notna(df[c].iloc[0]):
                    try:
                        ts = pd.to_datetime(df[c].iloc[0], utc=True).tz_convert(tzinfo)
                    except Exception:
                        ts = pd.to_datetime(df[c].iloc[0]).tz_localize(tzinfo, nonexistent="shift_forward", ambiguous="NaT")
                    break
        if ts is not None:
            break
    if ts is None:
        ts = pd.Timestamp.now(tzinfo)

    def topn_stats(df, side: str):
        if df.empty: 
            return np.nan, 0.0
        c_price, c_qty = map_price_qty(df)
        df2 = df[[c_price, c_qty]].dropna().copy()
        # нормализуем типы
        df2[c_price] = pd.to_numeric(df2[c_price], errors="coerce")
        df2[c_qty]   = pd.to_numeric(df2[c_qty], errors="coerce")
        df2 = df2.dropna()
        # лучшие цены: для bids — max first; для asks — min first
        if side == "bid":
            df2 = df2.sort_values(c_price, ascending=False).head(levels)
            best = df2[c_price].max() if not df2.empty else np.nan
        else:
            df2 = df2.sort_values(c_price, ascending=True).head(levels)
            best = df2[c_price].min() if not df2.empty else np.nan
        depth = df2[c_qty].sum() if not df2.empty else 0.0
        return best, float(depth)

    best_bid, depth_bid = topn_stats(bids, "bid")
    best_ask, depth_ask = topn_stats(asks, "ask")
    spread = (best_ask - best_bid) if pd.notna(best_ask) and pd.notna(best_bid) else np.nan
    mid    = (best_ask + best_bid)/2 if pd.notna(best_ask) and pd.notna(best_bid) else np.nan

    # индекс "сырой" ликвидности: нормируем спред к mid; чем больше, тем хуже
    liq_raw = np.nan
    if pd.notna(spread) and pd.notna(mid) and mid > 0:
        liq_raw = float(spread / mid)

    return {
        "dt": ts.tz_convert(tzinfo).tz_localize(None),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "mid": mid,
        "depth_bid_n": depth_bid,
        "depth_ask_n": depth_ask,
        "liq_raw": liq_raw
    }

def resample_5min(df):
    if df.empty: return df
    g = df.set_index("dt").sort_index()
    out = pd.DataFrame({
        "spread_mean": g["spread"].resample("5min").mean(),
        "mid_mean":    g["mid"].resample("5min").mean(),
        "depth_bid_n": g["depth_bid_n"].resample("5min").mean(),
        "depth_ask_n": g["depth_ask_n"].resample("5min").mean(),
        "liq_raw":     g["liq_raw"].resample("5min").mean(),
        "best_bid":    g["best_bid"].resample("5min").last(),
        "best_ask":    g["best_ask"].resample("5min").last(),
    }).dropna(how="all")
    if not out.empty:
        # сглаживание EMA для liq_raw (более стабильный признак)
        out["liq_smooth"] = out["liq_raw"].ewm(span=6, adjust=False, min_periods=1).mean()
        out["end"] = out.index
        out = out.reset_index(drop=True)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default=os.getenv("MOEX_TICKER","SiZ5"))
    ap.add_argument("--depth", type=int, default=20)
    ap.add_argument("--levels", type=int, default=5, help="сколько уровней учитывать в глубине (N)")
    ap.add_argument("--sleep", type=float, default=5.0)
    ap.add_argument("--print_every", type=int, default=3)
    ap.add_argument("--outfile_prefix", default="si_ob_5m")
    ap.add_argument("--tz", default=os.getenv("MOEX_TZ","Europe/Moscow"))
    args = ap.parse_args()

    tz = args.tz
    buf = []
    cycle = 0
    out_path = None

    while True:
        try:
            raw = fetch_orderbook(args.ticker, args.depth)
        except Exception as e:
            print("ERROR fetch:", e, file=sys.stderr)
            time.sleep(max(args.sleep, 1.0)); continue

        snap = compute_snapshot(raw, tz, args.levels)
        if snap is not None:
            buf.append(snap)

        if buf:
            df = pd.DataFrame(buf)
            bars = resample_5min(df)
            if not bars.empty:
                dstr = bars["end"].iloc[-1].date().isoformat()
                out_path = f"{args.outfile_prefix}_{dstr}.csv"
                tmp = out_path + ".tmp"
                bars.to_csv(tmp, index=False)
                os.replace(tmp, out_path)

                cycle += 1
                if cycle % max(args.print_every,1) == 0:
                    last = bars.tail(1).iloc[0]
                    liq = last.get("liq_smooth", np.nan)
                    sp  = last.get("spread_mean", np.nan)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {args.ticker} OB 5m end={last['end']}  spread={sp:.6f}  liq_smooth={liq:.6f}  -> {out_path}")

        time.sleep(args.sleep)

if __name__ == "__main__":
    main()
