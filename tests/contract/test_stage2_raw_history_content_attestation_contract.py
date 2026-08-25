from pathlib import Path


def test_content_attestation_contract_declares_exact_byte_atomic_generation() -> None:
    text = Path("contracts/datasets/futures_raw_history_content_attestation.v1.yaml").read_text(encoding="utf-8")
    for required in (
        "content_attestation_is_required_second_gate_for_stage5_stage7: true",
        "legacy_stage2_pointers_mutated: false",
        "exact_scope_count: 4",
        "explicit_prior_state_sha256_required: true",
        "concurrent_writer_lock_required: true",
        "digest_input: exact_parquet_file_bytes",
        "same_opened_inode_validation_and_hash_required: true",
        "validated_inode_hardlink_snapshot_required: true",
        "recorded_missing_dates_must_still_be_absent: true",
        "canonical_partition_inode_and_sha_recheck_before_marker_switch: true",
        "marker_switch_atomic_replace: true",
        "crash_before_marker_switch_preserves_previous_canonical_state: true",
        "stage5_stage7_must_use_resolver: true",
        "consumer_reads_generation_snapshots_not_mutable_canonical_raw: true",
        "canonical_raw_mutation_allowed: false",
    ):
        assert required in text


def test_content_attested_manifest_contract_uses_single_marker_generation() -> None:
    text = Path("contracts/datasets/futures_raw_history_content_attested_manifest.v1.yaml").read_text(encoding="utf-8")
    for required in (
        "schema_version: futures_raw_history_content_attested_manifest.v1",
        "current_marker_path:",
        "partition_content_records",
        "partition_content_set_sha256",
        "legacy_pointer_sha256",
        "legacy_manifest_sha256",
        "legacy_report_sha256",
        "schema_version: futures_raw_history_content_attested_batch_marker.v1",
        "exact_scope_count: 4",
        "single_atomic_marker_switch_required: true",
        "marker_is_only_mutable_canonical_reference: true",
        "resolve_only_through_current_marker: true",
        "verify_each_snapshot_sha256: true",
        "mutable_canonical_raw_read_required: false",
    ):
        assert required in text
