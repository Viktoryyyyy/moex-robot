from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from moex_data import step7_rub_native_d1_w1_acceptance_base as base

# Re-export the established Stage 7 acceptance surface for existing callers/tests.
for _name in dir(base):
    if _name not in globals():
        globals()[_name] = getattr(base, _name)

_BASE_REVALIDATE_FROZEN = base._revalidate_frozen


def _guard_frozen_refs_inside_run_root(manifest_path: Path) -> Path:
    manifest = Path(manifest_path).resolve(strict=True)
    try:
        run_root = manifest.parents[3]
    except IndexError as exc:
        raise base.Step7AcceptanceError("frozen raw manifest is not under declared Stage 7 run root") from exc
    if run_root.name.startswith("run_id=") is False or run_root.parent.name != "step7_rub_native_d1_w1":
        raise base.Step7AcceptanceError("frozen raw manifest is not under declared Stage 7 run root")

    values = base._load_json(manifest, "frozen raw manifest")
    records = values.get("partitions")
    if not isinstance(records, list) or not records:
        raise base.Step7AcceptanceError("frozen raw manifest partition records missing")

    expected_root = (run_root / "inputs" / "dataset_id=futures_raw_5m").resolve()
    for record in records:
        if not isinstance(record, Mapping):
            raise base.Step7AcceptanceError("frozen raw record must be object")
        path = base._expand_root_ref(record.get("frozen_ref"), run_root=run_root)
        try:
            path.relative_to(expected_root)
        except ValueError as exc:
            raise base.Step7AcceptanceError("frozen raw partition escaped immutable Stage 7 input root") from exc
    return run_root


def _revalidate_frozen(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str, validation_run_id: str) -> dict[str, object]:
    _guard_frozen_refs_inside_run_root(Path(manifest_path))
    return _BASE_REVALIDATE_FROZEN(
        repo_root=repo_root,
        data_root=data_root,
        manifest_path=manifest_path,
        instrument_id=instrument_id,
        start=start,
        end=end,
        validation_run_id=validation_run_id,
    )


def _with_run_root_guard(callable_, *args, **kwargs):
    original = base._revalidate_frozen
    base._revalidate_frozen = _revalidate_frozen
    try:
        return callable_(*args, **kwargs)
    finally:
        base._revalidate_frozen = original


def validate_pilot(values: Mapping[str, object], *, run_id: str, repo_root: str | Path = ".") -> list[dict[str, object]]:
    return _with_run_root_guard(base.validate_pilot, values, run_id=run_id, repo_root=repo_root)


def promote(*, run_id: str, repo_root: str | Path = ".") -> dict[str, object]:
    return _with_run_root_guard(base.promote, run_id=run_id, repo_root=repo_root)


def parse_args(argv: Sequence[str] | None = None):
    return base.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    base.load_env_file(args.env_file)
    try:
        result = promote(run_id=args.run_id, repo_root=args.repo_root)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 7, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
