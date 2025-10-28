#!/usr/bin/env python3
# coding: utf-8
import os, time, argparse, subprocess, sys
from pathlib import Path

LOCK_PATH = Path(".state/loop_mr1.lock")

def acquire_lock(force: bool) -> None:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists() and not force:
        pid = LOCK_PATH.read_text(encoding="utf-8").strip()
        print(f"⛔ Уже запущен другой экземпляр? Найден lock: {LOCK_PATH} (pid={pid}). "
              f"Если уверены — запустите с --force-lock.")
        sys.exit(1)
    try:
        LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")
    except Exception as e:
        print(f"Не удалось создать lock-файл: {e}")
        sys.exit(1)

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
    # Пробрасываем вывод в консоль (для логов systemd/screen/tmux)
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.stderr:
        print(proc.stderr.strip(), file=sys.stderr)
    return proc.returncode

def main():
    ap = argparse.ArgumentParser(description="Циклическая отправка сигналов MR-1 с паузой и стоп-файлом.")
    ap.add_argument("--interval", type=int, default=60, help="Интервал между итерациями (сек), по умолчанию 60.")
    ap.add_argument("--stop-file", default=".state/stop_mr1", help="Путь к стоп-файлу (наличие файла остановит цикл).")
    ap.add_argument("--force-first", action="store_true", help="Принудительная отправка в первой итерации.")
    ap.add_argument("--force-lock", action="store_true", help="Игнорировать существующий lock и запуститься.")
    args = ap.parse_args()

    acquire_lock(force=args.force_lock)
    print(f"▶️ loop_signal_mr1 стартовал: interval={args.interval}s, stop_file={args.stop_file}")

    stop_path = Path(args.stop_file)
    forced = args.force_first

    try:
        while True:
            if stop_path.exists():
                print(f"🛑 Обнаружен стоп-файл: {stop_path}. Останавливаюсь.")
                break
            rc = run_once(force=forced)
            forced = False  # только на первой итерации
            # Безопасная пауза
            for _ in range(args.interval):
                if stop_path.exists():
                    print(f"🛑 Обнаружен стоп-файл: {stop_path}. Останавливаюсь.")
                    raise KeyboardInterrupt
                time.sleep(1)
    except KeyboardInterrupt:
        print("⏹ Принудительная остановка (Ctrl+C или стоп-файл).")
    finally:
        release_lock()
        print("✅ loop_signal_mr1 завершён корректно.")

if __name__ == "__main__":
    main()

