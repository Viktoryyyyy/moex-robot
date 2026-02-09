#!/usr/bin/env python3
import os, requests
from typing import Any, Dict, Optional

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_api_v3/0.3")
DBG = os.getenv("MOEX_DEBUG", "0") == "1"

def _token() -> str:
    t = os.getenv("MOEX_API_KEY", "")
    # убираем пробелы и случайные кавычки
    return t.strip().strip('"').strip("'")

def headers() -> Dict[str,str]:
    return {
        "Authorization": "Bearer " + _token(),
        "User-Agent": UA,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

def get_json(path: str, params: Optional[Dict[str,Any]]=None, timeout: int=20) -> Dict[str,Any]:
    url = API + "/" + path.lstrip("/")
    r = requests.get(url, headers=headers(), params=params or {}, timeout=timeout)
    if r.status_code >= 400:
        if DBG:
            print(f"[MOEX_DEBUG] {r.status_code} GET {r.url}", flush=True)
            try:
                txt = r.text
                print(f"[MOEX_DEBUG] body: {txt[:400]}", flush=True)
            except Exception:
                pass
        r.raise_for_status()
    return r.json()
