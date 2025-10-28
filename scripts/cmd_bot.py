#!/usr/bin/env python3
# coding: utf-8
import os, time, json, requests
from pathlib import Path
from dotenv import load_dotenv

OFFSET_FILE = Path(".state/cmd_bot.offset")
STOP_FILE   = Path(".state/stop_mr1")
LOCK_FILE   = Path(".state/loop_mr1.lock")

def load_offset():
    if OFFSET_FILE.exists():
        try:
            return int(OFFSET_FILE.read_text().strip())
        except Exception:
            return None
    return None

def save_offset(offset: int):
    OFFSET_FILE.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_FILE.write_text(str(offset))

def send_message(token: str, chat_id: int, text: str):
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=10
    )
    r.raise_for_status()
    return r.json()

def main():
    load_dotenv()
    token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
    admin_id = int(os.getenv("ADMIN_USER_ID", "0"))
    if not token or not admin_id:
        raise SystemExit("Нужны TELEGRAM_BOT_TOKEN и ADMIN_USER_ID в .env")

    last_update_id = load_offset()
    print(f"cmd_bot: старт. admin_id={admin_id}, offset={last_update_id}")

    while True:
        try:
            params = {"timeout": 25}
            if last_update_id is not None:
                params["offset"] = last_update_id + 1
            resp = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for upd in data.get("result", []):
                last_update_id = upd.get("update_id", last_update_id)
                msg = upd.get("message") or upd.get("channel_post") or {}
                chat = msg.get("chat") or {}
                text = (msg.get("text") or "").strip()
                user = msg.get("from") or {}
                user_id = user.get("id")
                chat_id = chat.get("id")

                # Игнорируем всё без текста
                if not text:
                    continue

                # Разрешаем команды только от админа
                if user_id != admin_id:
                    try:
                        send_message(token, chat_id, "⛔ Только администратор может управлять ботом.")
                    except Exception:
                        pass
                    continue

                # Команды
                if text.lower() in ("/start", "start"):
                    # Разрешаем рассылку: снимаем стоп-файл
                    try:
                        if STOP_FILE.exists():
                            STOP_FILE.unlink()
                        send_message(token, chat_id, "▶️ Рассылка MR-1: ВКЛ (стоп-файл снят).")
                    except Exception as e:
                        send_message(token, chat_id, f"⚠️ Ошибка при снятии стоп-файла: {e}")

                elif text.lower() in ("/stop", "stop"):
                    # Останавливаем рассылку: создаём стоп-файл
                    try:
                        STOP_FILE.parent.mkdir(parents=True, exist_ok=True)
                        STOP_FILE.write_text("stop")
                        send_message(token, chat_id, "⏸️ Рассылка MR-1: ВЫКЛ (стоп-файл создан).")
                    except Exception as e:
                        send_message(token, chat_id, f"⚠️ Ошибка при создании стоп-файла: {e}")

                elif text.lower() in ("/status", "status"):
                    # Показываем статус
                    parts = []
                    parts.append(f"loop lock: {есть if LOCK_FILE.exists() else нет}")
                    parts.append(f"stop file: {есть if STOP_FILE.exists() else нет}")
                    send_message(token, chat_id, "ℹ️ Статус: " + "; ".join(parts))

                else:
                    send_message(token, chat_id, "Команды: /start — включить; /stop — выключить; /status — статус.")

            if last_update_id is not None:
                save_offset(last_update_id)

        except requests.exceptions.ReadTimeout:
            # нормальная ситуация на long-poll
            pass
        except KeyboardInterrupt:
            print("cmd_bot: остановка по Ctrl+C")
            break
        except Exception as e:
            print(f"cmd_bot: ошибка: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()

