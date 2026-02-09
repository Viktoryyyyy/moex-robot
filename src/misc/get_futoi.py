#!/usr/bin/env python3
import json
import sys
from urllib.request import urlopen, Request
from urllib.parse import urlencode

# Параметры запроса — можно менять через переменные окружения
SYMBOL = "Si"
LIMIT = 100

BASE = "https://moexalgo.github.io/api/rest/futoi"
params = {"symbol": SYMBOL, "limit": LIMIT}
URL = f"{BASE}?{urlencode(params)}"

def main():
    try:
        req = Request(URL, headers={"User-Agent": "moex-robot/0.1"})
        with urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8")
        # Печатаем как есть, чтобы видеть «сырой» ответ
        print(body)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
