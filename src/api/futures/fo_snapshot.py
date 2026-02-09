#!/usr/bin/env python3
import os, sys, json, pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from lib_moex_api import get_json, resolve_fut_by_key

TZ_MSK = ZoneInfo("Europe/Moscow")

def last_trade(secid: str):
    j = get_json(f"/iss/engines/futures/markets/forts/boards/rfud/securities/{secid}/trades.json",
                 {"limit":"1"}, timeout=12.0)
    b = j.get("trades") or {}
    cols, data = b.get("columns",[]), b.get("data",[])
    if not cols or not data: return {}
    df = pd.DataFrame(data, columns=cols)
    r = df.iloc[0].to_dict()
    return {
        "last": float(r.get("PRICE")) if pd.notna(r.get("PRICE")) else None,
        "last_qty": int(r.get("QUANTITY")) if pd.notna(r.get("QUANTITY")) else None,
        "trade_dt": f"{r.get('TRADEDATE','')} {r.get('TRADETIME','')}",
        "openposition": int(r.get("OPENPOSITION")) if pd.notna(r.get("OPENPOSITION")) else None
    }

def orderbook_top(secid: str):
    j = get_json(f"/iss/engines/futures/markets/forts/boards/rfud/securities/{secid}/orderbook.json",
                 {"depth":"1"}, timeout=12.0)
    b = j.get("orderbook") or {}
    cols, data = b.get("columns",[]), b.get("data",[])
    if not cols or not data: return {}
    df = pd.DataFrame(data, columns=cols)
    bb = df[(df["BUYSELL"]=="B")]
    ba = df[(df["BUYSELL"]=="S")]
    best_bid = float(bb["PRICE"].iloc[0]) if not bb.empty else None
    best_ask = float(ba["PRICE"].iloc[0]) if not ba.empty else None
    bb_qty   = int(bb["QUANTITY"].iloc[0]) if not bb.empty else None
    ba_qty   = int(ba["QUANTITY"].iloc[0]) if not ba.empty else None
    spr = (best_ask - best_bid) if (best_ask is not None and best_bid is not None) else None
    return {"best_bid":best_bid,"best_ask":best_ask,"bid_qty":bb_qty,"ask_qty":ba_qty,"spread":spr}

def main():
    key = os.getenv("FO_KEY") or (sys.argv[1] if len(sys.argv)>1 else "")
    if not key:
        print('Usage: FO_KEY=<substr> python fo_snapshot.py', file=sys.stderr); sys.exit(2)
    secid = resolve_fut_by_key(key, board="rfud")
    if not secid:
        print(f"ERROR: no futures match key='{key}'", file=sys.stderr); sys.exit(3)
    lt = last_trade(secid)
    ob = orderbook_top(secid)
    now_msk = datetime.now(TZ_MSK).strftime("%Y-%m-%d %H:%M:%S%z")
    snap = {"secid":secid, "now_msk":now_msk}
    snap.update(lt); snap.update(ob)
    print(json.dumps(snap, ensure_ascii=False))
if __name__=="__main__":
    main()
