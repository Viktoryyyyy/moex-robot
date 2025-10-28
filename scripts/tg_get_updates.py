#!/usr/bin/env python3
# coding: utf-8
import os, json, requests
from dotenv import load_dotenv

def main():
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise SystemExit("Нет TELEGRAM_BOT_TOKEN в .env — скопируйте .env.example в .env и заполните токен")

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    print(json.dumps(data, ensure_ascii=False, indent=2))

    chats = []
    for upd in data.get("result", []):
        msg = upd.get("message") or upd.get("channel_post") or {}
        chat = msg.get("chat") or {}
        cid = chat.get("id")
        title = chat.get("title") or chat.get("username") or chat.get("first_name")
        if cid:
            chats.append((cid, title))
    seen = set()
    out = []
    for cid, title in chats:
        if cid not in seen:
            out.append((cid, title))
            seen.add(cid)

    if out:
        print("\\nНайденные chat_id (id, название/username):")
        for cid, title in out:
            print("-", cid, title or "")
        print("\\n➡️ Скопируйте нужный chat_id в .env (TELEGRAM_CHAT_ID=...)")
    else:
        print("\\n⚠️ Апдейтов нет. Напишите боту любое сообщение и запустите снова.")

if __name__ == "__main__":
    main()

