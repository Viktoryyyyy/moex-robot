from __future__ import annotations

import argparse
from pathlib import Path

from ..publishers.research_run_registration import register_existing_research_run

_FORBIDDEN_PATH_PARTS = frozenset({"latest", "current", "autodetect"})
_FORBIDDEN_PATH_CHARACTERS = frozenset("*?[]{}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Register one explicit existing research run in the file-backed registry."
    )
    parser.add_argument("--registration-spec-path", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--registry-root", required=True)
    parser.add_argument("--archive-root", required=True)
    return parser


def _explicit_path(
    raw_value: str,
    argument_name: str,
    parser: argparse.ArgumentParser,
    *,
    must_exist: bool,
    must_be_file: bool = False,
    must_be_directory: bool = False,
) -> Path:
    raw = str(raw_value).strip()
    if not raw:
        parser.error(f"{argument_name} must be non-empty")
    if any(character in raw for character in _FORBIDDEN_PATH_CHARACTERS):
        parser.error(f"{argument_name} must not contain glob or template syntax")
    path = Path(raw).expanduser()
    if {part.casefold() for part in path.parts} & _FORBIDDEN_PATH_PARTS:
        parser.error(f"{argument_name} must not use latest/current/autodetect aliases")
    if path.is_symlink():
        parser.error(f"{argument_name} must not be a symlink")
    resolved = path.resolve(strict=False)
    if must_exist and not resolved.exists():
        parser.error(f"{argument_name} must exist")
    if must_be_file and (not resolved.exists() or not resolved.is_file()):
        parser.error(f"{argument_name} must reference a file")
    if must_be_directory and (not resolved.exists() or not resolved.is_dir()):
        parser.error(f"{argument_name} must reference a directory")
    if resolved.exists() and not (resolved.is_file() or resolved.is_dir()):
        parser.error(f"{argument_name} must reference a regular file or directory")
    return resolved


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    registration_spec_path = _explicit_path(
        args.registration_spec_path,
        "--registration-spec-path",
        parser,
        must_exist=True,
        must_be_file=True,
    )
    run_dir = _explicit_path(
        args.run_dir,
        "--run-dir",
        parser,
        must_exist=True,
        must_be_directory=True,
    )
    registry_root = _explicit_path(
        args.registry_root,
        "--registry-root",
        parser,
        must_exist=False,
    )
    archive_root = _explicit_path(
        args.archive_root,
        "--archive-root",
        parser,
        must_exist=False,
    )
    register_existing_research_run(
        registration_spec_path=registration_spec_path,
        run_dir=run_dir,
        registry_root=registry_root,
        archive_root=archive_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
