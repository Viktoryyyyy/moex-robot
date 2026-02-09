#!/usr/bin/env python3
import os, sys, time, json, requests

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_api_base/1.0").strip()
TK  = os.getenv("MOEX_API_KEY", "").strip()

def h(): 
    return {
        "Authorization": "Bearer " + TK,
        "User-Agent": UA
    }

def get(url, params=None, name=""):
    t0 = time.time()
    try:
        r = requests.get(url, headers=h(), params=params or {}, timeout=12)
        dt = time.time() - t0
        ct = r.headers.get("Content-Type","")
        rl = {k:v for k,v in r.headers.items() if k.lower().startswith("x-rate") or k.lower().startswith("ratelimit")}
        print(f"[{name}] {r.status_code} {ct} {dt:.2f}s", end="")
        if rl:
            compact = ";".join([f"{k}={v}" for k,v in rl.items()])
            print(f" | {compact}", end="")
        print()
        # Try to parse a tiny bit of JSON to confirm shape
        if r.ok and "json" in ct.lower():
            try:
                j = r.json()
                # detect ISS blocks
                if isinstance(j, dict):
                    blocks = [k for k,v in j.items() if isinstance(v, dict) and "columns" in v and "data" in v]
                    print(f"  blocks: {','.join(blocks) if blocks else '—'}")
                    # print tiny preview for first block
                    for bk in blocks[:1]:
                        cols = j[bk].get("columns", [])
                        data = j[bk].get("data", [])
                        n = len(data) if isinstance(data, list) else 0
                        print(f"  sample[{bk}]: rows={n} cols={len(cols)}")
                else:
                    print("  json: non-dict")
            except Exception as e:
                print(f"  json parse error: {e}")
        else:
            # show small body head
            body = r.text[:120].replace("\n"," ")
            print(f"  body[:120]: {body}")
    except requests.RequestException as e:
        print(f"[{name}] ERROR: {e}")

def main():
    if not TK or len(TK) < 10:
        print("ERROR: MOEX_API_KEY is missing or too short")
        sys.exit(1)
    print(f"API={API}")
    print(f"UA={UA}")
    print(f"TOKEN_LEN={len(TK)}")

    # 1) Пинг общего ISS индекса (через APIM)
    get(f"{API}/iss/index.json", name="iss.index")

    # 2) Реальные сделки по фьючерсу (RFD board: rfud). Берём актуальный контракт-заменитель, если тикер меняется — всё равно проверим доступ.
    get(f"{API}/iss/engines/futures/markets/forts/boards/rfud/securities/SiZ5/trades.json",
        params={"limit":"1"}, name="futures.trades SiZ5 limit=1")

    # 3) Стакан по фьючерсу
    get(f"{API}/iss/engines/futures/markets/forts/boards/rfud/securities/SiZ5/orderbook.json",
        params={"depth":"5"}, name="futures.orderbook SiZ5 depth=5")

    # 4) FUTOI по базовому активу si (from=till сегодня может быть пусто; нам важен код 200 и JSON)
    get(f"{API}/iss/analyticalproducts/futoi/securities/si.json",
        params={"from":"2025-09-25","till":"2025-09-25"}, name="futoi si 2025-09-25")

    # 5) Super Candles (tradestats) — 5м. Если нет прав/данных, увидим 40x/пусто.
    get(f"{API}/iss/datashop/algopack/fo/tradestats/SiZ5.json",
        params={"from":"2025-10-23","till":"2025-10-23"}, name="tradestats SiZ5 2025-10-23")

    # 6) HI2 — концентрация
    get(f"{API}/iss/datashop/algopack/fo/hi2.json",
        params={"ticker":"SiZ5","from":"2025-10-23","till":"2025-10-23"}, name="hi2 SiZ5 2025-10-23")

if __name__ == "__main__":
    main()
