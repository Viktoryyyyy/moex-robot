from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd

from moex_data import step5_futoi_positioning_acceptance_base as base
from moex_data.futures import validate_futoi_eod_from_frozen as frozen_oracle
from moex_data.futures.step5_futoi_source_quality import expected_derived_rows, omission_records

SNAPSHOT_POLICY = "latest_resolved_complete_balanced_FIZ_YUR_event_ts"
OMISSION_POLICY = "explicit_attested_date_only_fail_closed_otherwise"
OMISSION_REASON = "no_complete_balanced_FIZ_YUR_snapshot"
VALIDATION_MODE = "manifest_lineage_output_semantics_targeted_omission_oracle"


def _fail(message: str) -> None:
    raise base.Step5AcceptanceError(message)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _assert_no_valid_snapshot(frame: pd.DataFrame, *, instrument_id: str, trade_date: str, frozen_partition_ref: str, canonical_source_ref: str, frozen_sha256: str) -> None:
    work = frozen_oracle._validate_raw(frame, instrument_id, trade_date)
    resolved, _ = frozen_oracle._resolve_revisions(work)
    for candidate_ts in sorted(resolved["_ts_utc"].drop_duplicates().tolist(), reverse=True):
        candidate = resolved.loc[resolved["_ts_utc"].eq(candidate_ts)].copy()
        if not _candidate_satisfies_position_invariants(candidate):
            continue
        frozen_oracle.reconstruct_eod_row(candidate, instrument_id=instrument_id, trade_date=trade_date, frozen_partition_ref=frozen_partition_ref, canonical_source_ref=canonical_source_ref, frozen_sha256=frozen_sha256)
        _fail("declared source-quality omission now has a valid EOD snapshot")


def _validate_frozen_manifest_light(*, manifest_path: Path, expected_manifest_sha256: str, instrument_id: str, run_root: Path, expected_partitions: int) -> dict[str, object]:
    if _sha256(manifest_path) != expected_manifest_sha256:
        _fail("frozen input manifest SHA-256 mismatch")
    frozen = base._load_json(manifest_path, "frozen input manifest")
    if frozen.get("schema_version") != base.FROZEN_INPUT_SCHEMA or frozen.get("producer") != base.FROZEN_INPUT_PRODUCER:
        _fail("frozen input schema/producer mismatch")
    if frozen.get("dataset_id") != base.RAW_DATASET or frozen.get("instrument_id") != instrument_id:
        _fail("frozen input dataset/instrument mismatch")
    if int(frozen.get("partition_count") or 0) != expected_partitions:
        _fail("frozen input partition count mismatch")
    if frozen.get("physical_validation") != "stage2_futoi_partition_validator_reapplied":
        _fail("frozen input physical validation marker mismatch")
    if frozen.get("freeze_mode") != "create_only_hardlink_same_validated_inode":
        _fail("frozen input freeze mode mismatch")
    if frozen.get("source_mode") != "stage2_content_attested_generation_snapshots_only":
        _fail("frozen input source_mode mismatch")
    if frozen.get("legacy_pointer_consumption_used") is not False:
        _fail("frozen input legacy pointer consumption mismatch")
    if frozen.get("network_calls_used") is not False or frozen.get("latest_autodetect_used") is not False:
        _fail("frozen input network/latest evidence mismatch")

    resolved, marker_ref, manifest_ref, report_ref = base._current_content_attestation(instrument_id)
    expected_meta = {
        "content_attestation_generation_id": str(resolved.get("generation_id") or ""),
        "content_attestation_marker_ref": marker_ref,
        "content_attestation_marker_sha256": str(resolved.get("marker_sha256") or ""),
        "content_attested_manifest_ref": manifest_ref,
        "content_attested_manifest_sha256": str(resolved.get("manifest_sha256") or ""),
        "content_attested_partition_content_set_sha256": str(resolved.get("partition_content_set_sha256") or ""),
        "accepted_raw_pointer_ref": marker_ref,
        "accepted_raw_manifest_ref": manifest_ref,
        "accepted_raw_acceptance_report_ref": report_ref,
        "accepted_raw_history_run_id": str(resolved.get("generation_id") or ""),
    }
    for field, wanted in expected_meta.items():
        if frozen.get(field) != wanted:
            _fail("frozen/current content-attestation mismatch: " + field)

    records = frozen.get("records")
    current_records = resolved.get("records")
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        _fail("frozen input records must be sequence")
    if isinstance(current_records, (str, bytes)) or not isinstance(current_records, Sequence):
        _fail("current content-attested records must be sequence")
    if len(records) != expected_partitions or len(current_records) != expected_partitions:
        _fail("frozen/current content-attested record count mismatch")

    current_by_date: dict[str, Mapping[str, object]] = {}
    for row in current_records:
        if not isinstance(row, Mapping):
            _fail("current content-attested record must be object")
        trade_date = str(row.get("trade_date") or "")
        if not trade_date or trade_date in current_by_date:
            _fail("current content-attested trade_date set invalid")
        current_by_date[trade_date] = row

    records_by_date: dict[str, Mapping[str, object]] = {}
    for row in records:
        if not isinstance(row, Mapping):
            _fail("frozen record must be object")
        trade_date = str(row.get("trade_date") or "")
        if not trade_date or trade_date in records_by_date:
            _fail("frozen trade_date set invalid")
        current = current_by_date.get(trade_date)
        if not isinstance(current, Mapping):
            _fail("frozen date absent from current content-attested generation")
        expected_sha = str(current.get("sha256") or "")
        for field in ("content_attested_sha256", "source_sha256_at_freeze", "frozen_sha256"):
            if row.get(field) != expected_sha:
                _fail("frozen record SHA lineage mismatch: " + trade_date + " " + field)
        if row.get("content_attested_snapshot_ref") != current.get("snapshot_ref"):
            _fail("frozen snapshot ref mismatch: " + trade_date)
        if row.get("canonical_source_ref") != current.get("canonical_ref"):
            _fail("frozen canonical ref mismatch: " + trade_date)
        if row.get("hardlink_same_validated_inode") is not True or row.get("physical_validation_status") != "pass":
            _fail("frozen inode/physical evidence mismatch: " + trade_date)
        frozen_path = base._expand_root_ref(row.get("frozen_partition_ref"), "frozen_partition_ref", require_run_root=run_root)
        stat = frozen_path.stat()
        if int(row.get("source_device") or -1) != int(stat.st_dev) or int(row.get("source_inode") or -1) != int(stat.st_ino):
            _fail("frozen hardlink inode identity changed: " + trade_date)
        records_by_date[trade_date] = row

    if tuple(records_by_date) != tuple(current_by_date):
        _fail("frozen date order differs from current content-attested generation")
    return {"manifest": frozen, "manifest_path": manifest_path, "records_by_date": records_by_date}


def _validate_common_output(row: Mapping[str, object], *, dataset_id: str, run_root: Path, expected_rows: int):
    instrument_id = base._safe_token(row.get("instrument_id"), "instrument_id")
    if instrument_id not in base.EXPECTED_INSTRUMENTS:
        _fail("unexpected Stage 5 instrument")
    if row.get("dataset_id") != dataset_id or row.get("quality_status") != "pass":
        _fail("output dataset/quality mismatch")
    if int(row.get("row_count") or 0) != expected_rows:
        _fail("output evidence row count mismatch")
    producer_run_id = base._safe_token(row.get("run_id"), "producer_run_id")
    partition = base._artifact_path(row.get("partition_path"), run_root, "partition_path")
    manifest = base._artifact_path(row.get("manifest_path"), run_root, "manifest_path")
    quality = base._artifact_path(row.get("quality_report_path"), run_root, "quality_report_path")
    manifest_values = base._load_json(manifest, "manifest")
    quality_values = base._load_json(quality, "quality")
    for values, name in ((manifest_values, "manifest"), (quality_values, "quality")):
        if values.get("dataset_id") != dataset_id or values.get("instrument_id") != instrument_id or values.get("run_id") != producer_run_id:
            _fail(name + " identity/run mismatch")
        if int(values.get("row_count") or 0) != expected_rows or values.get("quality_status") != "pass":
            _fail(name + " row/quality mismatch")
    if Path(str(manifest_values.get("partition_path") or "")).resolve() != partition:
        _fail("manifest partition path mismatch")
    if Path(str(manifest_values.get("quality_report_path") or "")).resolve() != quality:
        _fail("manifest quality path mismatch")
    return instrument_id, partition, manifest, quality, manifest_values, quality_values


def _validate_eod_output(row: Mapping[str, object], *, run_root: Path, raw_expected_rows: int) -> dict[str, object]:
    instrument_id = base._safe_token(row.get("instrument_id"), "instrument_id")
    manifest_path = base._artifact_path(row.get("manifest_path"), run_root, "manifest_path")
    manifest_probe = base._load_json(manifest_path, "manifest")
    start_date = str(manifest_probe.get("requested_start_date") or "")
    end_date = str(manifest_probe.get("requested_end_date") or "")
    if not start_date or not end_date or start_date > end_date:
        _fail("EOD manifest requested range invalid")
    omissions = omission_records(instrument_id, start_date=start_date, end_date=end_date)
    derived_rows = expected_derived_rows(instrument_id, raw_expected_rows, start_date=start_date, end_date=end_date)
    instrument_id, partition, manifest, quality, manifest_values, quality_values = _validate_common_output(row, dataset_id=base.EOD_DATASET, run_root=run_root, expected_rows=derived_rows)
    if manifest_values.get("snapshot_policy") != SNAPSHOT_POLICY:
        _fail("EOD snapshot policy mismatch")
    expected_coverage = "pass_with_attested_source_quality_omissions" if omissions else "pass_complete"
    for values, name in ((manifest_values, "EOD manifest"), (quality_values, "EOD quality"), (row, "EOD pilot output")):
        if values.get("source_quality_omissions") != omissions:
            _fail(name + " source-quality omission evidence mismatch")
        if int(values.get("source_quality_omission_count") or 0) != len(omissions):
            _fail(name + " source-quality omission count mismatch")
        if values.get("coverage_status") != expected_coverage:
            _fail(name + " coverage_status mismatch")
    if manifest_values.get("canonical_raw_partition_reads_used") is not False:
        _fail("EOD manifest must forbid canonical raw reads after freeze")
    base._validate_eod_raw_lineage(manifest_values, instrument_id)

    frozen_manifest = base._artifact_path(manifest_values.get("frozen_input_manifest_path"), run_root, "frozen_input_manifest_path")
    expected_frozen_sha = str(manifest_values.get("frozen_input_manifest_sha256") or "")
    if row.get("frozen_input_manifest_sha256") != expected_frozen_sha:
        _fail("EOD pilot/frozen manifest SHA mismatch")
    frozen = _validate_frozen_manifest_light(manifest_path=frozen_manifest, expected_manifest_sha256=expected_frozen_sha, instrument_id=instrument_id, run_root=run_root, expected_partitions=raw_expected_rows)
    frozen_values = frozen["manifest"]
    if (str(frozen_values.get("requested_from") or ""), str(frozen_values.get("requested_till") or "")) != (start_date, end_date):
        _fail("EOD requested range differs from frozen input requested range")

    physical = base._validate_eod(partition, instrument_id, derived_rows)
    base._validate_eod_frozen_lineage(partition, frozen)
    eod = pd.read_parquet(partition, columns=["trade_date"])
    candidate_dates = tuple(eod["trade_date"].astype(str).tolist())
    omitted_dates = tuple(str(item["trade_date"]) for item in omissions)
    frozen_dates = tuple(frozen["records_by_date"])
    if set(candidate_dates) & set(omitted_dates):
        _fail("source-quality omitted date present in EOD candidate")
    if set(candidate_dates) | set(omitted_dates) != set(frozen_dates):
        _fail("EOD/frozen coverage differs beyond declared omissions")
    if len(candidate_dates) + len(omitted_dates) != len(frozen_dates):
        _fail("EOD/frozen coverage count differs beyond declared omissions")

    for omission in omissions:
        trade_date = str(omission.get("trade_date") or "")
        if omission.get("reason") != OMISSION_REASON:
            _fail("unsupported source-quality omission reason")
        record = frozen["records_by_date"].get(trade_date)
        if not isinstance(record, Mapping):
            _fail("omitted date missing frozen input")
        frozen_path = base._expand_root_ref(record.get("frozen_partition_ref"), "frozen_partition_ref", require_run_root=run_root)
        raw = pd.read_parquet(frozen_path)
        _assert_no_valid_snapshot(raw, instrument_id=instrument_id, trade_date=trade_date, frozen_partition_ref=str(record.get("frozen_partition_ref") or ""), canonical_source_ref=str(record.get("canonical_source_ref") or ""), frozen_sha256=str(record.get("frozen_sha256") or ""))

    physical = dict(physical)
    physical.update({"acceptance_validation_mode": VALIDATION_MODE, "full_frozen_raw_rehash_at_acceptance": False, "frozen_manifest_sha256_revalidated": True, "current_content_attestation_manifest_lineage_revalidated": True, "frozen_hardlink_inode_identity_revalidated": True, "source_quality_omission_count": len(omissions), "source_quality_omissions_independently_revalidated": True})
    return {"dataset_id": base.EOD_DATASET, "instrument_id": instrument_id, "producer_run_id": str(row.get("run_id") or ""), "partition": partition, "manifest": manifest, "quality": quality, "physical_readback": physical, "source_eod_partition_path": manifest_values.get("source_eod_partition_path"), "frozen_input_manifest_path": frozen_manifest}


def _validate_feature_output(row: Mapping[str, object], *, run_root: Path, expected_rows: int) -> dict[str, object]:
    instrument_id, partition, manifest, quality, manifest_values, _ = _validate_common_output(row, dataset_id=base.FEATURE_DATASET, run_root=run_root, expected_rows=expected_rows)
    physical = dict(base._validate_features(partition, instrument_id, expected_rows))
    physical["acceptance_validation_mode"] = VALIDATION_MODE
    return {"dataset_id": base.FEATURE_DATASET, "instrument_id": instrument_id, "producer_run_id": str(row.get("run_id") or ""), "partition": partition, "manifest": manifest, "quality": quality, "physical_readback": physical, "source_eod_partition_path": manifest_values.get("source_eod_partition_path"), "frozen_input_manifest_path": manifest_values.get("frozen_input_manifest_path")}


def validate_pilot(values: Mapping[str, object], *, run_id: str) -> list[dict[str, object]]:
    checked_run = base._safe_token(run_id, "run_id")
    if values.get("project") != "MOEX_Bot" or values.get("step") != 5 or values.get("status") != "pilot_passed":
        _fail("pilot identity/status mismatch")
    if values.get("artifact_version") != checked_run or values.get("run_id") != checked_run:
        _fail("pilot run identity mismatch")
    if values.get("snapshot_policy") != SNAPSHOT_POLICY:
        _fail("pilot snapshot policy mismatch")
    if values.get("source_quality_omission_policy") != OMISSION_POLICY:
        _fail("pilot source-quality omission policy mismatch")
    for field in ("run_id_reuse_allowed", "raw_ingestion_changed", "network_calls_used", "latest_autodetect_used", "canonical_raw_partition_reads_after_freeze_used", "front_next_split_claimed", "historical_pit_research_ready_claimed"):
        if values.get(field) is not False:
            _fail(field + " must be false")
    if values.get("run_artifacts_immutable") is not True or values.get("root_aggregate_semantics") is not True:
        _fail("pilot immutable/root aggregate semantics mismatch")
    if values.get("immutable_raw_input_freeze_used") is not True:
        _fail("pilot immutable_raw_input_freeze_used must be true")
    if values.get("raw_input_freeze_mode") != "create_only_hardlink_same_validated_inode":
        _fail("pilot raw_input_freeze_mode mismatch")
    if values.get("revision_policy") != "same_analytical_key_single_sess_id_then_max_seqnum":
        _fail("pilot revision policy mismatch")

    run_root = base._run_root(checked_run)
    if not run_root.is_dir() or Path(str(values.get("run_root") or "")).resolve() != run_root.resolve():
        _fail("pilot run_root mismatch")
    histories, counts = values.get("histories"), values.get("counts")
    frozen_rows, eod_rows, feature_rows = values.get("frozen_inputs"), values.get("eod_outputs"), values.get("feature_outputs")
    if not isinstance(histories, Mapping) or not isinstance(counts, Mapping):
        _fail("pilot histories/counts missing")
    for seq, name in ((frozen_rows, "frozen_inputs"), (eod_rows, "eod_outputs"), (feature_rows, "feature_outputs")):
        if isinstance(seq, (str, bytes)) or not isinstance(seq, Sequence) or len(seq) != 2:
            _fail("pilot " + name + " must contain two records")

    total_omissions = 0
    expected_derived: dict[str, int] = {}
    for instrument_id, raw_count in base.EXPECTED_ROWS.items():
        history = histories.get(instrument_id)
        if not isinstance(history, Mapping):
            _fail("pilot history missing instrument: " + instrument_id)
        start_date, end_date = str(history.get("start_date") or ""), str(history.get("end_date") or "")
        if not start_date or not end_date or start_date > end_date:
            _fail("pilot history range invalid: " + instrument_id)
        omissions = omission_records(instrument_id, start_date=start_date, end_date=end_date)
        derived = expected_derived_rows(instrument_id, raw_count, start_date=start_date, end_date=end_date)
        expected_derived[instrument_id] = derived
        total_omissions += len(omissions)
        if int(history.get("expected_raw_partitions") or 0) != raw_count or int(history.get("expected_eod_rows") or 0) != derived or history.get("source_quality_omissions") != omissions:
            _fail("pilot history evidence mismatch: " + instrument_id)
    for field, wanted in {"mandatory_instruments": 2, "frozen_raw_inputs": 2, "eod_outputs": 2, "feature_outputs": 2, "source_quality_omission_count": total_omissions, "expected_accepted_pointers": 4}.items():
        if int(counts.get(field) or 0) != wanted:
            _fail("pilot counts mismatch: " + field)

    frozen_by_instrument: dict[str, Mapping[str, object]] = {}
    for row in frozen_rows:
        if not isinstance(row, Mapping):
            _fail("frozen input pilot record must be object")
        instrument_id = base._safe_token(row.get("instrument_id"), "instrument_id")
        if instrument_id in frozen_by_instrument or instrument_id not in base.EXPECTED_INSTRUMENTS:
            _fail("frozen input instrument set invalid")
        if row.get("status") != "succeeded" or row.get("physical_validation_status") != "pass" or int(row.get("partition_count") or 0) != base.EXPECTED_ROWS[instrument_id]:
            _fail("frozen input pilot output mismatch")
        base._artifact_path(row.get("manifest_path"), run_root, "frozen pilot manifest_path")
        frozen_by_instrument[instrument_id] = row
    if set(frozen_by_instrument) != base.EXPECTED_INSTRUMENTS:
        _fail("frozen input instrument set mismatch")

    eod_by_instrument: dict[str, dict[str, object]] = {}
    outputs: list[dict[str, object]] = []
    for row in eod_rows:
        if not isinstance(row, Mapping):
            _fail("EOD pilot record must be object")
        instrument_id = base._safe_token(row.get("instrument_id"), "instrument_id")
        if instrument_id in eod_by_instrument or instrument_id not in base.EXPECTED_INSTRUMENTS:
            _fail("EOD instrument set invalid")
        checked = _validate_eod_output(row, run_root=run_root, raw_expected_rows=base.EXPECTED_ROWS[instrument_id])
        if Path(str(checked["frozen_input_manifest_path"])).resolve() != Path(str(frozen_by_instrument[instrument_id].get("manifest_path") or "")).resolve():
            _fail("EOD frozen input does not match pilot frozen input")
        eod_by_instrument[instrument_id] = checked
        outputs.append(checked)
    if set(eod_by_instrument) != base.EXPECTED_INSTRUMENTS:
        _fail("EOD instrument set mismatch")

    feature_seen: set[str] = set()
    for row in feature_rows:
        if not isinstance(row, Mapping):
            _fail("feature pilot record must be object")
        instrument_id = base._safe_token(row.get("instrument_id"), "instrument_id")
        if instrument_id in feature_seen or instrument_id not in base.EXPECTED_INSTRUMENTS:
            _fail("feature instrument set invalid")
        checked = _validate_feature_output(row, run_root=run_root, expected_rows=expected_derived[instrument_id])
        source_path = str(checked.get("source_eod_partition_path") or "")
        if not source_path or Path(source_path).resolve() != eod_by_instrument[instrument_id]["partition"]:
            _fail("feature source EOD lineage mismatch")
        base._validate_feature_source_alignment(checked["partition"], eod_by_instrument[instrument_id]["partition"])
        physical = dict(checked["physical_readback"])
        physical["source_eod_identity_timestamp_base_match"] = True
        checked["physical_readback"] = physical
        feature_seen.add(instrument_id)
        outputs.append(checked)
    if feature_seen != base.EXPECTED_INSTRUMENTS:
        _fail("feature instrument set mismatch")
    return outputs


def promote(*, run_id: str) -> dict[str, object]:
    checked_run = base._safe_token(run_id, "run_id")
    evidence_path = base._evidence_dir(checked_run) / "pilot_evidence.json"
    outputs = validate_pilot(base._load_json(evidence_path, "pilot_evidence"), run_id=checked_run)
    records: list[tuple[Path, Mapping[str, object]]] = []
    pointer_summaries: list[dict[str, object]] = []
    for output in outputs:
        dataset_id, instrument_id = str(output["dataset_id"]), str(output["instrument_id"])
        pointer = base._pointer_path(dataset_id, instrument_id)
        pointer_values = {"dataset_id": dataset_id, "instrument_id": instrument_id, "run_id": str(output["producer_run_id"]), "acceptance_run_id": checked_run, "manifest_ref": base._rooted_ref(output["manifest"]), "manifest_sha256": hashlib.sha256(output["manifest"].read_bytes()).hexdigest(), "quality_report_ref": base._rooted_ref(output["quality"]), "quality_report_sha256": hashlib.sha256(output["quality"].read_bytes()).hexdigest(), "partition_ref": base._rooted_ref(output["partition"]), "partition_sha256": hashlib.sha256(output["partition"].read_bytes()).hexdigest(), "quality_status": "pass", "acceptance_contract_id": base.CONTRACT_ID, "immutable_frozen_raw_input_verified": True, "acceptance_validation_mode": VALIDATION_MODE, "full_frozen_raw_rehash_at_acceptance": False, "historical_pit_research_ready_claimed": False}
        records.append((pointer, pointer_values))
        pointer_summaries.append({"dataset_id": dataset_id, "instrument_id": instrument_id, "run_id": str(output["producer_run_id"]), "acceptance_run_id": checked_run, "pointer_path": pointer.as_posix(), "physical_readback": output["physical_readback"]})
    if len(pointer_summaries) != 4:
        _fail("accepted pointer count mismatch")
    marker = base._evidence_dir(checked_run) / "accepted_pointers.json"
    result: dict[str, object] = {"project": "MOEX_Bot", "step": 5, "status": "accepted", "run_id": checked_run, "acceptance_contract_id": base.CONTRACT_ID, "accepted_pointer_count": 4, "expected_pointer_count": 4, "pointers": pointer_summaries, "promotion_semantics": "transactional_with_rollback", "acceptance_validation_mode": VALIDATION_MODE, "full_frozen_raw_rehash_at_acceptance": False, "derived_output_semantic_revalidation": True, "current_content_attestation_manifest_lineage_revalidated": True, "source_quality_omissions_independently_revalidated": True, "physical_partition_readback_required": True, "immutable_frozen_raw_input_verified": True, "root_aggregate_semantics": True, "front_next_split_claimed": False, "historical_pit_research_ready_claimed": False}
    records.append((marker, result))
    base._transactional_replace(records)
    result["acceptance_evidence_path"] = marker.as_posix()
    return result


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

