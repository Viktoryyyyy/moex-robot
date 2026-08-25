from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Mapping


def fsync_file(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_generation(root: Path) -> None:
    if not root.is_dir() or root.is_symlink():
        raise ValueError("generation root must be an existing non-symlink directory")
    directories: list[Path] = []
    for current, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        directories.append(current_path)
        for dirname in dirnames:
            child = current_path / dirname
            if child.is_symlink():
                raise ValueError("generation directory must not contain symlink directories")
        for filename in filenames:
            child = current_path / filename
            if child.is_symlink() or not child.is_file():
                raise ValueError("generation directory must contain only regular files")
            fsync_file(child)
    for directory in sorted(directories, key=lambda value: len(value.parts), reverse=True):
        fsync_dir(directory)
    fsync_dir(root.parent)


def durable_replace_json(path: Path, values: Mapping[str, object]) -> str:
    import hashlib

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".stage") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        staged = Path(handle.name)
    try:
        os.replace(staged, path)
        fsync_file(path)
        fsync_dir(path.parent)
    finally:
        staged.unlink(missing_ok=True)
    return hashlib.sha256(payload).hexdigest()
