#!/usr/bin/env python3
import argparse
import os
import shutil
from pathlib import Path
from typing import List


def find_garbage(root: Path) -> List[Path]:
    garbage: List[Path] = []

    dir_patterns = {"__pycache__", ".pytest_cache"}
    file_suffixes = {".pyc", ".pyo"}
    file_names = {".DS_Store"}
    file_suffix_ends = ("~",)

    for path in root.rglob("*"):
        try:
            if path.is_dir():
                if path.name in dir_patterns:
                    garbage.append(path)
            elif path.is_file():
                if path.suffix in file_suffixes:
                    garbage.append(path)
                elif path.name in file_names:
                    garbage.append(path)
                elif any(str(path).endswith(suf) for suf in file_suffix_ends):
                    garbage.append(path)
        except PermissionError:
            continue

    return garbage


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Очистка временных и кеш-файлов в проекте moex_bot."
    )
    ap.add_argument(
        "--root",
        type=str,
        default=".",
        help="Корневая папка для очистки (по умолчанию текущая).",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="По умолчанию только показывает, что будет удалено. "
             "С этим флагом реально удаляет.",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    garbage = find_garbage(root)

    if not garbage:
        print("Ничего не найдено для удаления.")
        return

    print(f"Найдено объектов для удаления: {len(garbage)}")
    for p in garbage:
        print(p)

    if not args.apply:
        print("\nРежим dry-run (по умолчанию). "
              "Ничего не удалено. "
              "Для удаления запусти с флагом --apply.")
        return

    removed = 0
    for p in garbage:
        try:
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
            removed += 1
        except Exception as e:
            print(f"Ошибка при удалении {p}: {e}")

    print(f"\nУдалено объектов: {removed}")


if __name__ == "__main__":
    main()
