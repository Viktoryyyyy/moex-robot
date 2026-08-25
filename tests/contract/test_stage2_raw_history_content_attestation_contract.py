from pathlib import Path


def test_content_attestation_contract_declares_exact_byte_binding() -> None:
    text = Path("contracts/datasets/futures_raw_history_content_attestation.v1.yaml").read_text(encoding="utf-8")
    for required in (
        "producer_impl_ref: src/moex_data/futures/stage2_raw_history_content_reattestation.py",
        "content_attestation_is_required_second_gate_for_downstream_consumption: true",
        "expected_pointer_count: 4",
        "explicit_prior_four_pointer_state_sha256_required: true",
        "four_pointer_replacement_transactional_with_rollback: true",
        "final_batch_marker_same_transaction: true",
        "digest_input: exact_parquet_file_bytes",
        "physical_validation_before_hash_binding_required: true",
        "hash_and_validation_same_exact_bytes_required: true",
        "source_path_inode_stability_during_capture_required: true",
        "partition_content_sha256_by_trade_date_required: true",
        "partition_content_set_sha256_required: true",
        "consumer_must_verify_source_partition_sha256_against_accepted_manifest: true",
        "consumer_must_verify_batch_acceptance_marker: true",
        "current_date_set_match_without_content_match_is_insufficient: true",
        "canonical_raw_mutation_allowed: false",
    ):
        assert required in text


def test_content_attested_manifest_contract_requires_prior_state_and_batch_transaction() -> None:
    text = Path("contracts/datasets/futures_raw_history_content_attested_manifest.v1.yaml").read_text(encoding="utf-8")
    for required in (
        "schema_version: futures_raw_history_content_attested_manifest.v1",
        "partition_content_records",
        "partition_content_set_sha256",
        "prior_accepted_run_id",
        "prior_accepted_manifest_sha256",
        "prior_pointer_sha256",
        "promotion_basis: raw_history_content_attestation",
        "four_pointer_transaction_required_for_controlled_batch: true",
        "rollback_required: true",
        "final_marker_same_transaction_required: true",
        "pointer_absence_window_allowed: false",
    ):
        assert required in text
