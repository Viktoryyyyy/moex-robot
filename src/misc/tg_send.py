#!/usr/bin/env python3
# coding: utf-8
import os, requests
from dotenv import load_dotenv

def main():
    load_dotenv()
    token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID", "")
    if not token:
        raise SystemExit("Нет TELEGRAM_BOT_TOKEN в .env")
    if not chat_id:
        raise SystemExit("Нет TELEGRAM_CHAT_ID в .env")

    text = "MOEX Bot — MR-1 онлайн ✅"
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=10
    )
    r.raise_for_status()
    resp = r.json()
    ok = resp.get("ok")
    mid = (resp.get("result") or {}).get("message_id")
    print(f"OK: {ok} | message_id: {mid}")

if __name__ == "__main__":
    main()

