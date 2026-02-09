#!/usr/bin/env python3
"""
Single-instance lock helper.

Used to ensure that only one robot process per key (e.g. per trading date)
is running at a time.

Lock files are stored under data/state/
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Optional


LOCK_DIR = Path("data") / "state"
LOCK_DIR.mkdir(parents=True, exist_ok=True)


def acquire_lock(lock_name: str) -> Path:
    """
    Acquire lock for given logical name.

    Creates file:
        data/state/<lock_name>.lock

    If file already exists, prints info and exits via SystemExit.
    """
    path = LOCK_DIR / f"{lock_name}.lock"

    if path.exists():
        try:
            info = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            info = ""
        print(f"[LOCK] Lock file already exists: {path}")
        if info:
            print("[LOCK] Existing lock info:")
            print(info.strip())
        raise SystemExit(
            f"Another instance appears to be running for {lock_name}. "
            f"Remove lock file manually if this is not the case."
        )

    payload = (
        f"pid={os.getpid()}\n"
        f"started={datetime.now().isoformat()}\n"
        f"lock_name={lock_name}\n"
    )
    tmp = path.with_suffix(".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)

    print(f"[LOCK] Acquired lock: {path}")
    return path


def release_lock(path: Optional[Path]) -> None:
    """
    Release lock by deleting lock file.
    """
    if path is None:
        return
    try:
        path.unlink()
        print(f"[LOCK] Released lock: {path}")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[LOCK] Failed to release lock {path}: {e}")
