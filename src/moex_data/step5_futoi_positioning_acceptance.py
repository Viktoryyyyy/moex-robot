from __future__ import annotations

import json
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path

from moex_data import step5_futoi_positioning_acceptance_base as base
from moex_data.futures.validate_futoi_eod_from_frozen import validate_candidate_partition

# Re-export the existing acceptance surface so current tests/callers keep the same API.
for _name in dir(base):
    if _name not in globals():
        globals()[_name] = getattr(base, _name)

_BASE_VALIDATE_OUTPUT_RECORD = base._validate_output_record
_VALIDATOR_SWAP_LOCK = threading.RLock()


def _validate_output_record(row: Mapping[str, object], *, dataset_id: str, run_root: Path, expected_rows: int) -> dict[str, object]:
    checked = _BASE_VALIDATE_OUTPUT_RECORD(row, dataset_id=dataset_id, run_root=run_root, expected_rows=expected_rows)
    if dataset_id != base.EOD_DATASET:
        return checked

    manifest_values = base._load_json(checked["manifest"], "manifest")
    instrument_id = str(checked["instrument_id"])
    frozen_validation = base._validate_frozen_input(manifest_values, instrument_id, run_root, expected_rows)
    records = frozen_validation.get("records_by_date")
    if not isinstance(records, Mapping):
        base._fail("frozen validation records missing for EOD reconstruction")

    reconstruction = validate_candidate_partition(
        eod_path=checked["partition"],
        records_by_date=records,
        expand_frozen_ref=lambda ref: base._expand_root_ref(ref, "frozen_partition_ref", require_run_root=run_root),
    )
    physical = dict(checked["physical_readback"])
    physical.update(reconstruction)
    checked["physical_readback"] = physical
    return checked


def _with_wrapped_output_validator(callable_, *args, **kwargs):
    with _VALIDATOR_SWAP_LOCK:
        original = base._validate_output_record
        base._validate_output_record = _validate_output_record
        try:
            return callable_(*args, **kwargs)
        finally:
            base._validate_output_record = original


def validate_pilot(values: Mapping[str, object], *, run_id: str) -> list[dict[str, object]]:
    return _with_wrapped_output_validator(base.validate_pilot, values, run_id=run_id)


def promote(*, run_id: str) -> dict[str, object]:
    return _with_wrapped_output_validator(base.promote, run_id=run_id)


def parse_args(argv: Sequence[str] | None = None):
    return base.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    base.load_env_file(args.env_file)
    try:
        result = promote(run_id=args.run_id)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 5, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
