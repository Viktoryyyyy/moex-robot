#!/usr/bin/env python3
import argparse
from mcp_moex_server import fo_5m_day, fo_marketdata


def cmd_fo_5m_day(args):
    res = fo_5m_day(args.ticker, args.date)
    bars = res["bars"]
    print(f"ticker={res['ticker']}  date={res['tradedate']}  bars={len(bars)}")
    if bars:
        print("first bar:", bars[0])
        print("last bar: ", bars[-1])


def cmd_fo_marketdata(args):
    snap = fo_marketdata(args.ticker)
    print(snap)


def main():
    ap = argparse.ArgumentParser(description="MOEX CLI tools")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # fo_5m_day
    p1 = sub.add_parser("fo_5m_day")
    p1.add_argument("--ticker", required=True)
    p1.add_argument("--date", required=True)
    p1.set_defaults(func=cmd_fo_5m_day)

    # fo_marketdata
    p2 = sub.add_parser("fo_marketdata")
    p2.add_argument("--ticker", required=True)
    p2.set_defaults(func=cmd_fo_marketdata)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
