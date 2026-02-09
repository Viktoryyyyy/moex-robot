#!/usr/bin/env python3
import os, sys
from datetime import date
from lib_moex_api import get_json, blocks, to_rows

TICK_FUT = os.getenv("MOEX_FUT_TICKER", "SiZ5")
DAY_TS   = os.getenv("MOEX_DAY_TS", "2025-10-23")   # для tradestats/hi2
DAY_OI   = os.getenv("MOEX_DAY_OI", "2025-09-25")   # для futoi
BASE_OI  = os.getenv("MOEX_OI_BASE", "si")          # futoi требует 'si'

def head_print(title, cols, data, n=3):
    print(f"\n== {title} ==")
    print(f"cols({len(cols)}): {cols[:10]}{' ...' if len(cols)>10 else ''}")
    print(f"rows: {len(data)}")
    for i, row in enumerate(data[:n]):
        print(f"  {i}: {row[:10]}{' ...' if len(row)>10 else ''}")

def probe_trades():
    j = get_json(f"/iss/engines/futures/markets/forts/boards/rfud/securities/{TICK_FUT}/trades.json",
                 params={"limit":"5"})
    for b in blocks(j):
        cols, data = to_rows(j, b)
        if b == "trades":
            head_print("trades", cols, data)

def probe_orderbook():
    j = get_json(f"/iss/engines/futures/markets/forts/boards/rfud/securities/{TICK_FUT}/orderbook.json",
                 params={"depth":"5"})
    for b in blocks(j):
        if b == "orderbook":
            cols, data = to_rows(j, b)
            head_print("orderbook", cols, data)

def probe_tradestats():
    j = get_json(f"/iss/datashop/algopack/fo/tradestats/{TICK_FUT}.json",
                 params={"from":DAY_TS, "till":DAY_TS})
    for b in blocks(j):
        if b == "data":
            cols, data = to_rows(j, b)
            head_print("tradestats.data", cols, data)

def probe_hi2():
    j = get_json(f"/iss/datashop/algopack/fo/hi2.json",
                 params={"ticker":TICK_FUT, "from":DAY_TS, "till":DAY_TS})
    for b in blocks(j):
        if b == "data":
            cols, data = to_rows(j, b)
            head_print("hi2.data", cols, data)

def probe_futoi():
    j = get_json(f"/iss/analyticalproducts/futoi/securities/{BASE_OI}.json",
                 params={"from":DAY_OI, "till":DAY_OI})
    for b in blocks(j):
        if b == "futoi":
            cols, data = to_rows(j, b)
            head_print("futoi", cols, data)

def main():
    print(f"TICK_FUT={TICK_FUT} DAY_TS={DAY_TS} DAY_OI={DAY_OI} BASE_OI={BASE_OI}")
    probe_trades()
    probe_orderbook()
    probe_tradestats()
    probe_hi2()
    probe_futoi()

if __name__ == "__main__":
    main()
