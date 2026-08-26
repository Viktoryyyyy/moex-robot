from __future__ import annotations

import json
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd

from moex_data import step5_futoi_positioning_acceptance_base as base
from moex_data.futures import validate_futoi_eod_from_frozen as frozen_oracle
from moex_data.futures.step5_futoi_source_quality import expected_derived_rows, omission_records

SNAPSHOT_POLICY = "latest_resolved_complete_balanced_FIZ_YUR_event_ts"
SOURCE_QUALITY_OMISSION_POLICY = "explicit_attested_date_only_fail_closed_otherwise"
OMISSION_ORACLE_ERROR = "no complete balanced FIZ/YUR snapshot exists in frozen raw FUTOI"

# Re-export the existing acceptance surface so current tests/callers keep the same API.
for _name in dir(base):
    if _name not in globals():
        globals()[_name] = getattr(base, _name)

_BASE_VALIDATE_OUTPUT_RECORD = base._validate_output_record
_BASE_VALIDATE_FROZEN_INPUT = base._validate_frozen_input
_VALIDATOR_SWAP_LOCK = threading.RLock()


def _candidate_satisfies_position_invariants(candidate: pd.DataFrame) -> bool:
    if len(candidate.index) != 2 or set(candidate["clgroup"].tolist()) != frozen_oracle.GROUPS:
        return False
    total_long = int(candidate["pos_long"].sum())
    total_short_abs = int(candidate["pos_short"].abs().sum())
    if total_long <= 0 or total_long != total_short_abs or int(candidate["pos"].sum()) != 0:
        return False
    for _, row in candidate.iterrows():
        long_position = int(row["pos_long"])
        short_abs = abs(int(row["pos_short"]))
        if int(row["pos"]) != long_position - short_abs:
            return False
        if int(row["pos_long_num"]) == 0 and long_position != 0:
            return False
        if int(row["pos_short_num"]) == 0 and short_abs != 0:
            return False
    return True


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
        if not _candidate_satisfies_position_invariants(candidate):
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
    frozen_oracle._fail(OMISSION_ORACLE_ERROR)


def _read_frozen_record(record: Mapping[str, object], expand_frozen_ref) -> tuple[str, Path, pd.DataFrame]:
    trade_date = str(record["trade_date"])
    frozen_ref = str(record["frozen_partition_ref"])
    frozen_path = Path(expand_frozen_ref(frozen_ref))
    return trade_date, frozen_path, pd.read_parquet(frozen_path)


def _validate_candidate_partition(
    *,
    eod_path: str | Path,
    records_by_date: Mapping[str, Mapping[str, object]],
    instrument_id: str,
    omissions: Sequence[Mapping[str, object]],
    expand_frozen_ref,
) -> dict[str, object]:
    frame = pd.read_parquet(Path(eod_path))
    omitted_by_date = {str(row.get("trade_date") or ""): str(row.get("reason") or "") for row in omissions}
    if len(omitted_by_date) != len(omissions) or any(not date or not reason for date, reason in omitted_by_date.items()):
        frozen_oracle._fail("invalid source-quality omission evidence")
    candidate_dates = set(frame["trade_date"].astype(str))
    frozen_dates = set(str(value) for value in records_by_date)
    if candidate_dates & set(omitted_by_date):
        frozen_oracle._fail("source-quality omitted date present in EOD candidate")
    if candidate_dates | set(omitted_by_date) != frozen_dates:
        frozen_oracle._fail("EOD/frozen raw coverage differs beyond declared source-quality omissions")
    if len(frame.index) != len(records_by_date) - len(omitted_by_date):
        frozen_oracle._fail("EOD/frozen raw row count differs beyond declared source-quality omissions")

    rebuilt = 0
    for _, candidate in frame.iterrows():
        trade_date = str(candidate["trade_date"])
        record = records_by_date.get(trade_date)
        if not isinstance(record, Mapping):
            frozen_oracle._fail("EOD trade_date missing frozen raw reconstruction input")
        _, _, raw = _read_frozen_record(record, expand_frozen_ref)
        expected = _reconstruct_eod_row_with_source_tail_policy(
            raw,
            instrument_id=instrument_id,
            trade_date=trade_date,
            frozen_partition_ref=str(record["frozen_partition_ref"]),
            canonical_source_ref=str(record["canonical_source_ref"]),
            frozen_sha256=str(record["frozen_sha256"]),
        )
        frozen_oracle.compare_candidate_row(candidate.to_dict(), expected)
        rebuilt += 1

    verified_omissions = 0
    for trade_date, reason in sorted(omitted_by_date.items()):
        if reason != "no_complete_balanced_FIZ_YUR_snapshot":
            frozen_oracle._fail("unsupported source-quality omission reason")
        record = records_by_date.get(trade_date)
        if not isinstance(record, Mapping):
            frozen_oracle._fail("source-quality omitted date missing frozen raw input")
        _, _, raw = _read_frozen_record(record, expand_frozen_ref)
        try:
            _reconstruct_eod_row_with_source_tail_policy(
                raw,
                instrument_id=instrument_id,
                trade_date=trade_date,
                frozen_partition_ref=str(record["frozen_partition_ref"]),
                canonical_source_ref=str(record["canonical_source_ref"]),
                frozen_sha256=str(record["frozen_sha256"]),
            )
        except frozen_oracle.FrozenFutoiEodValidationError as exc:
            if str(exc) != OMISSION_ORACLE_ERROR:
                raise
        else:
            frozen_oracle._fail("declared source-quality omission now has a valid EOD snapshot")
        verified_omissions += 1

    return {
        "reconstructed_eod_rows": rebuilt,
        "reconstructed_from_frozen_raw_match": True,
        "independent_from_eod_producer": True,
        "snapshot_policy": SNAPSHOT_POLICY,
        "source_quality_omission_count": verified_omissions,
        "source_quality_omissions_independently_verified": True,
    }


def _call_base_output_validator_with_split_counts(
    row: Mapping[str, object],
    *,
    dataset_id: str,
    run_root: Path,
    raw_expected_rows: int,
    derived_expected_rows: int,
) -> dict[str, object]:
    if dataset_id != base.EOD_DATASET:
        return _BASE_VALIDATE_OUTPUT_RECORD(
            row,
            dataset_id=dataset_id,
            run_root=run_root,
            expected_rows=derived_expected_rows,
        )

    original_frozen_validator = base._validate_frozen_input

    def _raw_count_frozen_validator(manifest_values, instrument_id, inner_run_root, _ignored_expected_rows):
        return _BASE_VALIDATE_FROZEN_INPUT(manifest_values, instrument_id, inner_run_root, raw_expected_rows)

    base._validate_frozen_input = _raw_count_frozen_validator
    try:
        return _BASE_VALIDATE_OUTPUT_RECORD(
            row,
            dataset_id=dataset_id,
            run_root=run_root,
            expected_rows=derived_expected_rows,
        )
    finally:
        base._validate_frozen_input = original_frozen_validator


def _validate_output_record(row: Mapping[str, object], *, dataset_id: str, run_root: Path, expected_rows: int) -> dict[str, object]:
    instrument_id = str(row.get("instrument_id") or "")
    derived_rows = expected_derived_rows(instrument_id, expected_rows)
    checked = _call_base_output_validator_with_split_counts(
        row,
        dataset_id=dataset_id,
        run_root=run_root,
        raw_expected_rows=expected_rows,
        derived_expected_rows=derived_rows,
    )
    if dataset_id != base.EOD_DATASET:
        return checked

    manifest_values = base._load_json(checked["manifest"], "manifest")
    quality_values = base._load_json(checked["quality"], "quality")
    if manifest_values.get("snapshot_policy") != SNAPSHOT_POLICY:
        base._fail("EOD manifest snapshot policy mismatch")
    expected_omissions = omission_records(instrument_id)
    for values, name in ((manifest_values, "EOD manifest"), (quality_values, "EOD quality")):
        if values.get("source_quality_omissions") != expected_omissions:
            base._fail(name + " source-quality omission evidence mismatch")
        if int(values.get("source_quality_omission_count") or 0) != len(expected_omissions):
            base._fail(name + " source-quality omission count mismatch")
    frozen_validation = _BASE_VALIDATE_FROZEN_INPUT(manifest_values, instrument_id, run_root, expected_rows)
    records = frozen_validation.get("records_by_date")
    if not isinstance(records, Mapping):
        base._fail("frozen validation records missing for EOD reconstruction")

    reconstruction = _validate_candidate_partition(
        eod_path=checked["partition"],
        records_by_date=records,
        instrument_id=instrument_id,
        omissions=expected_omissions,
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


def _validate_pilot_omission_evidence(values: Mapping[str, object]) -> None:
    if values.get("source_quality_omission_policy") != SOURCE_QUALITY_OMISSION_POLICY:
        base._fail("pilot source-quality omission policy mismatch")
    histories = values.get("histories")
    if not isinstance(histories, Mapping):
        base._fail("pilot histories missing")
    for instrument_id, raw_expected in base.EXPECTED_ROWS.items():
        history = histories.get(instrument_id)
        if not isinstance(history, Mapping):
            base._fail("pilot history missing instrument: " + instrument_id)
        if int(history.get("expected_raw_partitions") or 0) != raw_expected:
            base._fail("pilot raw history count mismatch: " + instrument_id)
        if int(history.get("expected_eod_rows") or 0) != expected_derived_rows(instrument_id, raw_expected):
            base._fail("pilot derived history count mismatch: " + instrument_id)
        if history.get("source_quality_omissions") != omission_records(instrument_id):
            base._fail("pilot history source-quality omission mismatch: " + instrument_id)


def validate_pilot(values: Mapping[str, object], *, run_id: str) -> list[dict[str, object]]:
    _validate_pilot_omission_evidence(values)
    return _with_wrapped_output_validator(base.validate_pilot, values, run_id=run_id)


def promote(*, run_id: str) -> dict[str, object]:
    evidence_path = base._evidence_dir(run_id) / "pilot_evidence.json"
    validate_pilot(base._load_json(evidence_path, "pilot_evidence"), run_id=run_id)
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
