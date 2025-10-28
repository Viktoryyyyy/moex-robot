#!/usr/bin/env python3
# coding: utf-8
import os, sys, time, argparse, subprocess
from pathlib import Path
from datetime import datetime, timedelta

try:
    import pytz
except ImportError:
    pytz = None

LOCK_PATH = Path(".state/loop_mr1.lock")
STOP_FILE = Path(".state/stop_mr1")

def acquire_lock(force: bool) -> None:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists() and not force:
        pid = LOCK_PATH.read_text(encoding="utf-8").strip()
        print(f"⛔ Уже запущен другой экземпляр? Найден lock: {LOCK_PATH} (pid={pid}). Если уверены — запустите с --force-lock.")
        sys.exit(1)
    LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")

def release_lock():
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except Exception:
        pass

def run_once(force: bool) -> int:
    cmd = [sys.executable, "scripts/signal_mr1.py", "--send"]
    if force:
        cmd.append("--force")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.stderr:
        print(proc.stderr.strip(), file=sys.stderr)
    return proc.returncode

def now_msk():
    if pytz:
        import pytz as _p
        return datetime.now(_p.timezone("Europe/Moscow"))
    # запасной вариант без pytz (не идеален по зоне)
    return datetime.utcnow()

def next_5m_mark(now_dt: datetime) -> datetime:
    minute = (now_dt.minute // 5) * 5
    base = now_dt.replace(minute=minute, second=1, microsecond=0)
    nxt = base + timedelta(minutes=5)
    if nxt <= now_dt:
        nxt += timedelta(minutes=5)
    return nxt

def wait_until_next_bar():
    now = now_msk()
    target = next_5m_mark(now)
    sleep_s = max(0.0, (target - now).total_seconds())
    print(f"[loop] ⏳ Ждём до {target.strftime('%H:%M:%S')} (≈{sleep_s:.1f} сек)...")
    time.sleep(sleep_s)

def main():
    ap = argparse.ArgumentParser(description="Циклическая отправка MR-1: выравнивание 5m по МСК, стоп-файл, lock.")
    ap.add_argument("--force-first", action="store_true", help="Принудительная отправка в первой итерации.")
    ap.add_argument("--force-lock", action="store_true", help="Игнорировать существующий lock и запуститься.")
    args = ap.parse_args()

    acquire_lock(force=args.force_lock)
    print("▶️ loop_signal_mr1 стартовал (5m). Стоп-файл: .state/stop_mr1")

    forced = args.force_first
    try:
        while True:
            wait_until_next_bar()
            if STOP_FILE.exists():
                print(f"🛑 Обнаружен стоп-файл: {STOP_FILE}. Останавливаюсь.")
                break
            run_once(force=forced)
            forced = False
            time.sleep(1)
    except KeyboardInterrupt:
        print("⏹ Остановка по Ctrl+C.")
    finally:
        release_lock()
        print("✅ loop_signal_mr1 завершён корректно.")

if __name__ == "__main__":
    main()
