#!/usr/bin/env python3
# coding: utf-8
import os, requests
from dotenv import load_dotenv

def send_message(text: str) -> dict:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token:
        raise SystemExit("Нет TELEGRAM_BOT_TOKEN в .env")
    if not chat_id:
        raise SystemExit("Нет TELEGRAM_CHAT_ID в .env")

    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=10
    )
    try:
        r.raise_for_status()
    except Exception as e:
        try:
            print(f"Telegram error: {r.status_code} {r.text}")
        except Exception:
            pass
        raise
    return r.json()

