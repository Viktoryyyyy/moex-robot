from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from moex_data import step7_rub_native_d1_w1_acceptance_base as base
from moex_data.futures.freeze_step7_accepted_raw_5m import accepted_quote_history

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


def _guard_current_content_attestation(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str) -> None:
    values = base._load_json(Path(manifest_path), "frozen raw manifest")
    if values.get("source_mode") != "stage2_content_attested_generation_snapshots_only":
        raise base.Step7AcceptanceError("frozen raw source_mode is not content-attested snapshots only")
    if values.get("legacy_pointer_consumption_used") is not False:
        raise base.Step7AcceptanceError("legacy accepted pointer consumption is forbidden")
    if values.get("network_calls_used") is not False or values.get("latest_autodetect_used") is not False:
        raise base.Step7AcceptanceError("frozen raw execution boundary mismatch")

    current = accepted_quote_history(data_root, instrument_id, start, end, repo_root=repo_root)
    exact_pairs = (
        ("content_attestation_generation_id", current.acceptance_run_id),
        ("content_attestation_marker_ref", current.pointer_ref),
        ("content_attestation_marker_sha256", current.marker_sha256),
        ("content_attested_manifest_ref", current.manifest_ref),
        ("content_attested_manifest_sha256", current.manifest_sha256),
        ("content_attested_partition_content_set_sha256", current.partition_content_set_sha256),
        ("frozen_content_sha256", current.partition_content_set_sha256),
        ("accepted_partition_dates_sha256", current.partition_dates_sha256),
    )
    for field, expected in exact_pairs:
        if values.get(field) != expected:
            raise base.Step7AcceptanceError("frozen raw current content-attestation mismatch: " + field)
    if int(values.get("partition_count") or -1) != len(current.accepted_dates):
        raise base.Step7AcceptanceError("frozen raw current content-attestation partition_count mismatch")
    if int(values.get("row_count") or -1) != current.row_count:
        raise base.Step7AcceptanceError("frozen raw current content-attestation row_count mismatch")


def _call_base_revalidate_frozen(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str, validation_run_id: str) -> dict[str, object]:
    original = base.accepted_quote_history

    def _accepted_with_explicit_repo(root, checked_instrument, checked_start, checked_end):
        return accepted_quote_history(
            root,
            checked_instrument,
            checked_start,
            checked_end,
            repo_root=repo_root,
        )

    base.accepted_quote_history = _accepted_with_explicit_repo
    try:
        return _BASE_REVALIDATE_FROZEN(
            repo_root=repo_root,
            data_root=data_root,
            manifest_path=manifest_path,
            instrument_id=instrument_id,
            start=start,
            end=end,
            validation_run_id=validation_run_id,
        )
    finally:
        base.accepted_quote_history = original


def _revalidate_frozen(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str, validation_run_id: str) -> dict[str, object]:
    _guard_frozen_refs_inside_run_root(Path(manifest_path))
    _guard_current_content_attestation(
        repo_root=repo_root,
        data_root=data_root,
        manifest_path=Path(manifest_path),
        instrument_id=instrument_id,
        start=start,
        end=end,
    )
    result = _call_base_revalidate_frozen(
        repo_root=repo_root,
        data_root=data_root,
        manifest_path=manifest_path,
        instrument_id=instrument_id,
        start=start,
        end=end,
        validation_run_id=validation_run_id,
    )
    result["current_content_attestation_match"] = True
    return result


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
