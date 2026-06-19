from __future__ import annotations

import hashlib
import os
import re
import stat
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final, Iterable

_FORBIDDEN_DYNAMIC_PARTS: Final = frozenset({"latest", "current", "autodetect"})
_FORBIDDEN_PATH_CHARACTERS: Final = frozenset("*?[]{}")
_SAFE_RUN_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_COMMIT_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-fA-F]{40}$")
_ZIP_TIMESTAMP: Final = (1980, 1, 1, 0, 0, 0)
_ZIP_FILE_MODE: Final = stat.S_IFREG | 0o644


class RunArchiveError(ValueError):
    pass


@dataclass(frozen=True)
class RunArchiveResult:
    run_id: str
    repo_commit: str
    archive_path: str
    sha256: str
    size_bytes: int
    archived_files: tuple[str, ...]
    created: bool
    idempotent: bool


def create_deterministic_run_archive(
    *,
    run_dir: Path | str,
    archive_root: Path | str,
    run_id: str,
    repo_commit: str,
    artifact_filenames: Iterable[str],
) -> RunArchiveResult:
    run_root = _validate_existing_run_dir(run_dir)
    target_root = _validate_archive_root(archive_root)
    if _is_within(target_root, run_root):
        raise RunArchiveError("archive_root must remain outside the explicit run directory")
    safe_run_id = _validate_run_id(run_id)
    safe_commit = _validate_repo_commit(repo_commit)
    declared = _normalize_declared_filenames(artifact_filenames)
    actual = _collect_run_files(run_root)

    missing = sorted(set(declared).difference(actual))
    if missing:
        raise RunArchiveError("missing declared artifact(s): " + ", ".join(missing))
    undeclared = sorted(actual.difference(declared))
    if undeclared:
        raise RunArchiveError("undeclared artifact(s) are rejected: " + ", ".join(undeclared))

    target_root.mkdir(parents=True, exist_ok=True)
    archive_name = f"{safe_run_id}__{safe_commit}.zip"
    archive_path = target_root / archive_name
    if archive_path.is_symlink():
        raise RunArchiveError("archive target must not be a symlink")

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{archive_name}.",
        suffix=".tmp",
        dir=target_root,
    )
    os.close(fd)
    temporary_path = Path(temporary_name)
    try:
        _build_zip(temporary_path, run_root, declared)
        _fsync_file(temporary_path)
        created = _install_immutable_archive(temporary_path, archive_path)
    finally:
        temporary_path.unlink(missing_ok=True)

    digest, size = _hash_and_size(archive_path)
    if size <= 0:
        raise RunArchiveError("deterministic run archive must have positive size")
    return RunArchiveResult(
        run_id=safe_run_id,
        repo_commit=safe_commit,
        archive_path=str(archive_path),
        sha256=digest,
        size_bytes=size,
        archived_files=declared,
        created=created,
        idempotent=not created,
    )


def _validate_existing_run_dir(run_dir: Path | str) -> Path:
    raw = os.fspath(run_dir)
    if not isinstance(raw, str) or not raw.strip():
        raise RunArchiveError("run_dir must be supplied explicitly")
    _reject_dynamic_path(raw, "run_dir")
    path = Path(raw).expanduser()
    if path.is_symlink():
        raise RunArchiveError("run_dir must not be a symlink")
    resolved = path.resolve(strict=False)
    if not resolved.exists() or not resolved.is_dir():
        raise RunArchiveError("run_dir must reference an existing directory")
    return resolved


def _validate_archive_root(archive_root: Path | str) -> Path:
    raw = os.fspath(archive_root)
    if not isinstance(raw, str) or not raw.strip():
        raise RunArchiveError("archive_root must be supplied explicitly")
    _reject_dynamic_path(raw, "archive_root")
    path = Path(raw).expanduser()
    if path.is_symlink():
        raise RunArchiveError("archive_root must not be a symlink")
    resolved = path.resolve(strict=False)
    if resolved.exists() and not resolved.is_dir():
        raise RunArchiveError("archive_root must reference a directory")
    return resolved


def _reject_dynamic_path(raw: str, field_name: str) -> None:
    if "\x00" in raw:
        raise RunArchiveError(f"{field_name} must not contain NUL")
    if any(character in raw for character in _FORBIDDEN_PATH_CHARACTERS):
        raise RunArchiveError(f"{field_name} must not contain glob or template syntax")
    lowered_parts = {part.casefold() for part in Path(raw).parts}
    if lowered_parts & _FORBIDDEN_DYNAMIC_PARTS:
        raise RunArchiveError(f"{field_name} must not use latest/current/autodetect aliases")


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not _SAFE_RUN_ID_RE.fullmatch(run_id):
        raise RunArchiveError("run_id is not safe for deterministic archive naming")
    return run_id


def _validate_repo_commit(repo_commit: str) -> str:
    if not isinstance(repo_commit, str) or not _COMMIT_RE.fullmatch(repo_commit):
        raise RunArchiveError("repo_commit must be an explicit 40-character hexadecimal SHA")
    return repo_commit.lower()


def _normalize_declared_filenames(filenames: Iterable[str]) -> tuple[str, ...]:
    if isinstance(filenames, (str, bytes)):
        raise RunArchiveError("artifact_filenames must be an iterable of explicit relative paths")
    normalized: list[str] = []
    for value in filenames:
        if not isinstance(value, str) or not value.strip():
            raise RunArchiveError("declared artifact filename must be a non-empty string")
        if "\\" in value or "\x00" in value:
            raise RunArchiveError("declared artifact filename must use normalized POSIX separators")
        if any(character in value for character in _FORBIDDEN_PATH_CHARACTERS):
            raise RunArchiveError("declared artifact filename must not contain glob or template syntax")
        path = PurePosixPath(value)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            raise RunArchiveError("declared artifact filename must be an explicit relative path")
        if any(part.casefold() in _FORBIDDEN_DYNAMIC_PARTS for part in path.parts):
            raise RunArchiveError("declared artifact filename must not use mutable aliases")
        normalized.append(path.as_posix())
    if not normalized:
        raise RunArchiveError("at least one declared artifact is required")
    if len(normalized) != len(set(normalized)):
        raise RunArchiveError("declared artifact filenames must be unique")
    return tuple(sorted(normalized))


def _collect_run_files(run_root: Path) -> set[str]:
    files: set[str] = set()
    for candidate in run_root.rglob("*"):
        if candidate.is_symlink():
            raise RunArchiveError("symlinks are forbidden in deterministic run archives")
        if candidate.is_dir():
            continue
        if not candidate.is_file():
            raise RunArchiveError("run_dir contains a non-regular artifact")
        resolved = candidate.resolve(strict=True)
        if not _is_within(resolved, run_root):
            raise RunArchiveError("run artifact resolves outside the explicit run directory")
        files.add(candidate.relative_to(run_root).as_posix())
    return files


def _build_zip(
    target: Path,
    run_root: Path,
    declared: tuple[str, ...],
) -> None:
    try:
        with zipfile.ZipFile(
            target,
            mode="w",
            compression=zipfile.ZIP_STORED,
            strict_timestamps=True,
        ) as archive:
            for filename in declared:
                source = run_root / PurePosixPath(filename)
                if source.is_symlink() or not source.is_file():
                    raise RunArchiveError(f"declared artifact is not a regular file: {filename}")
                resolved = source.resolve(strict=True)
                if not _is_within(resolved, run_root):
                    raise RunArchiveError("declared artifact resolves outside the explicit run directory")
                info = zipfile.ZipInfo(filename=filename, date_time=_ZIP_TIMESTAMP)
                info.create_system = 3
                info.compress_type = zipfile.ZIP_STORED
                info.external_attr = _ZIP_FILE_MODE << 16
                info.extra = b""
                info.comment = b""
                archive.writestr(info, source.read_bytes())
    except (OSError, zipfile.BadZipFile) as exc:
        raise RunArchiveError("failed to build deterministic run archive") from exc


def _install_immutable_archive(temporary_path: Path, archive_path: Path) -> bool:
    try:
        os.link(temporary_path, archive_path)
        _fsync_directory(archive_path.parent)
        return True
    except FileExistsError:
        if archive_path.is_symlink() or not archive_path.is_file():
            raise RunArchiveError("archive target collision is not a regular file")
        if not _files_equal(temporary_path, archive_path):
            raise RunArchiveError("archive name collision with different deterministic content")
        return False


def _files_equal(left: Path, right: Path) -> bool:
    if left.stat().st_size != right.stat().st_size:
        return False
    with left.open("rb") as left_handle, right.open("rb") as right_handle:
        while True:
            left_chunk = left_handle.read(1024 * 1024)
            right_chunk = right_handle.read(1024 * 1024)
            if left_chunk != right_chunk:
                return False
            if not left_chunk:
                return True


def _hash_and_size(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    flags = getattr(os, "O_DIRECTORY", 0) | os.O_RDONLY
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["RunArchiveError", "RunArchiveResult", "create_deterministic_run_archive"]
