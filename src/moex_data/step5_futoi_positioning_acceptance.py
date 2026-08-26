from __future__ import annotations

import json
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd

from moex_data import step5_futoi_positioning_acceptance_base as base
from moex_data.futures import validate_futoi_eod_from_frozen as frozen_oracle

SNAPSHOT_POLICY = "latest_resolved_complete_balanced_FIZ_YUR_event_ts"

# Re-export the existing acceptance surface so current tests/callers keep the same API.
for _name in dir(base):
    if _name not in globals():
        globals()[_name] = getattr(base, _name)

_BASE_VALIDATE_OUTPUT_RECORD = base._validate_output_record
_VALIDATOR_SWAP_LOCK = threading.RLock()


def _reconstruct_eod_row_with_source_tail_policy(
    frame: pd.DataFrame,
    *,
    instrument_id: str,
    trade_date: str,
    frozen_partition_ref: str,
    canonical_source_ref: str,
    frozen_sha256: str,
) -> dict[str, object]:
    """Independent acceptance reconstruction for the canonical snapshot policy."""
    work = frozen_oracle._validate_raw(frame, instrument_id, trade_date)
    resolved, revisions_dropped = frozen_oracle._resolve_revisions(work)
    for candidate_ts in sorted(resolved["_ts_utc"].drop_duplicates().tolist(), reverse=True):
        candidate = resolved.loc[resolved["_ts_utc"].eq(candidate_ts)].copy()
        if len(candidate.index) != 2 or set(candidate["clgroup"].tolist()) != frozen_oracle.GROUPS:
            continue
        total_long = int(candidate["pos_long"].sum())
        total_short_abs = int(candidate["pos_short"].abs().sum())
        total_net = int(candidate["pos"].sum())
        if total_long != total_short_abs or total_net != 0:
            continue
        rebuilt = frozen_oracle.reconstruct_eod_row(
            candidate,
            instrument_id=instrument_id,
            trade_date=trade_date,
            frozen_partition_ref=frozen_partition_ref,
            canonical_source_ref=canonical_source_ref,
            frozen_sha256=frozen_sha256,
        )
        rebuilt["source_row_count"] = int(len(work.index))
        rebuilt["source_revision_rows_dropped"] = revisions_dropped
        return rebuilt
    frozen_oracle._fail("no complete balanced FIZ/YUR snapshot exists in frozen raw FUTOI")


def _validate_candidate_partition(
    *,
    eod_path: str | Path,
    records_by_date: Mapping[str, Mapping[str, object]],
    expand_frozen_ref,
) -> dict[str, object]:
    frame = pd.read_parquet(Path(eod_path))
    if len(frame.index) != len(records_by_date):
        frozen_oracle._fail("EOD/frozen raw reconstruction row count mismatch")
    rebuilt = 0
    for _, candidate in frame.iterrows():
        trade_date = str(candidate["trade_date"])
        record = records_by_date.get(trade_date)
        if not isinstance(record, Mapping):
            frozen_oracle._fail("EOD trade_date missing frozen raw reconstruction input")
        frozen_ref = str(record["frozen_partition_ref"])
        frozen_path = Path(expand_frozen_ref(frozen_ref))
        raw = pd.read_parquet(frozen_path)
        expected = _reconstruct_eod_row_with_source_tail_policy(
            raw,
            instrument_id=str(candidate["instrument_id"]),
            trade_date=trade_date,
            frozen_partition_ref=frozen_ref,
            canonical_source_ref=str(record["canonical_source_ref"]),
            frozen_sha256=str(record["frozen_sha256"]),
        )
        frozen_oracle.compare_candidate_row(candidate.to_dict(), expected)
        rebuilt += 1
    return {
        "reconstructed_eod_rows": rebuilt,
        "reconstructed_from_frozen_raw_match": True,
        "independent_from_eod_producer": True,
        "snapshot_policy": SNAPSHOT_POLICY,
    }


def _validate_output_record(row: Mapping[str, object], *, dataset_id: str, run_root: Path, expected_rows: int) -> dict[str, object]:
    checked = _BASE_VALIDATE_OUTPUT_RECORD(row, dataset_id=dataset_id, run_root=run_root, expected_rows=expected_rows)
    if dataset_id != base.EOD_DATASET:
        return checked

    manifest_values = base._load_json(checked["manifest"], "manifest")
    if manifest_values.get("snapshot_policy") != SNAPSHOT_POLICY:
        base._fail("EOD manifest snapshot policy mismatch")
    instrument_id = str(checked["instrument_id"])
    frozen_validation = base._validate_frozen_input(manifest_values, instrument_id, run_root, expected_rows)
    records = frozen_validation.get("records_by_date")
    if not isinstance(records, Mapping):
        base._fail("frozen validation records missing for EOD reconstruction")

    reconstruction = _validate_candidate_partition(
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
