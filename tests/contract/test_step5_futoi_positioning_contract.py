from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_stage5_scope_is_si_cr_root_aggregate_without_raw_rewrite_or_front_next_claim() -> None:
    text = _read("configs/datasets/step5_futoi_positioning.v1.yaml")
    assert "raw_ingestion_changed_by_step5: false" in text
    assert "raw_revisions_preserved: true" in text
    assert "- si_futures_family" in text
    assert "- cr_futures_family" in text
    assert "perpetual_futoi_mandatory: false" in text
    assert "root_aggregate_semantics: true" in text
    assert "front_next_split_claimed: false" in text


def test_stage5_requires_canonical_accepted_raw_history_and_immutable_byte_freeze() -> None:
    text = _read("configs/datasets/step5_futoi_positioning.v1.yaml")
    assert "raw_history_accepted_manifest_ref: contracts/datasets/futures_raw_history_accepted_manifest.v1.yaml" in text
    assert "accepted_raw_pointer_required: true" in text
    assert "accepted_raw_promotion_basis_required: raw_history_acceptance" in text
    assert "accepted_raw_manifest_date_set_digest_required: true" in text
    assert "accepted_raw_acceptance_report_sha256_required: true" in text
    assert "unaccepted_physical_partition_read_allowed: false" in text
    assert "immutable_raw_input_freeze:" in text
    assert "stage2_futoi_partition_validator_reapplied: true" in text
    assert "source_bytes_sha256_required: true" in text
    assert "frozen_bytes_sha256_required: true" in text
    assert "freeze_mode: create_only_hardlink_same_validated_inode" in text
    assert "hardlink_failure_fallback_allowed: false" in text
    assert "canonical_raw_reads_after_freeze_allowed: false" in text
    assert "frozen_partition_hash_and_physical_revalidation_at_acceptance: true" in text


def test_stage5_revision_and_final_snapshot_policy_are_fail_closed() -> None:
    text = _read("configs/datasets/step5_futoi_positioning.v1.yaml")
    assert "systime_as_revision_order_allowed: false" in text
    assert "multi_sess_id_same_analytical_key: fail_closed" in text
    assert "within_single_sess_id_revision_order: greatest_seqnum" in text
    assert "final_event_ts_rule: maximum_resolved_ts_for_trade_date" in text
    assert "incomplete_final_snapshot: fail_closed" in text
    assert "eod_clock_time_hardcoded: false" in text


def test_stage5_eod_contract_covers_frozen_lineage_position_balance_and_participant_metrics() -> None:
    text = _read("contracts/datasets/futures_futoi_eod.v1.yaml")
    for token in (
        "current_pointer_required: true",
        "promotion_basis_required: raw_history_acceptance",
        "unaccepted_physical_partition_read_allowed: false",
        "schema_version: step5_futoi_raw_frozen_input.v1",
        "source_frozen_hash_equality_required: true",
        "canonical_raw_reads_after_freeze_allowed: false",
        "source_canonical_partition_ref",
        "source_frozen_partition_sha256",
        "phys_net", "legal_net", "total_open_interest", "phys_gross", "legal_gross",
        "phys_net_share_of_oi", "phys_gross_share_of_two_sided_oi",
        "phys_avg_long_per_participant", "legal_avg_short_per_participant",
        "phys_net_plus_legal_net_equals_zero: true",
        "total_open_interest_equals_total_short_abs: true",
        "recomputed_gross_and_share_metrics",
        "recomputed_participant_averages",
    ):
        assert token in text


def test_stage5_feature_contract_uses_only_current_and_prior_eod_observations() -> None:
    text = _read("contracts/datasets/futures_futoi_positioning_features_d1.v1.yaml")
    assert "future_rows_allowed: false" in text
    assert "current_and_prior_eod_only: true" in text
    assert "d1_observation_lag: 1" in text
    assert "w1_observation_lag: 5" in text
    assert "- 252" in text
    assert "- 504" in text
    assert "historical_pit_research_ready_claimed: false" in text


def test_stage5_acceptance_requires_frozen_bytes_formula_source_and_reconstruction_revalidation_and_four_pointers() -> None:
    text = _read("contracts/datasets/step5_futoi_positioning_acceptance.v1.yaml")
    assert "accepted_pointer_count: 4" in text
    assert "immutable_raw_input_freeze_required: true" in text
    assert "raw_input_freeze_mode_required: create_only_hardlink_same_validated_inode" in text
    assert "canonical_raw_partition_reads_after_freeze_used_required: false" in text
    assert "frozen_partition_sha256_revalidated: true" in text
    assert "frozen_partition_stage2_futoi_physical_validator_reapplied_at_acceptance: true" in text
    assert "eod_source_frozen_ref_and_sha_lineage_required: true" in text
    assert "eod_reconstruction_from_physically_revalidated_frozen_raw_required: true" in text
    assert "eod_reconstruction_independent_from_eod_producer_required: true" in text
    assert "eod_reconstruction_exact_field_equality_before_promotion_required: true" in text
    assert "physical_parquet_readback_required: true" in text
    assert "eod_accepted_raw_current_pointer_required: true" in text
    assert "eod_all_derived_metrics_recomputed: true" in text
    assert "eod_participant_average_zero_count_rules_revalidated: true" in text
    assert "feature_source_eod_identity_exact_match_required: true" in text
    assert "feature_source_eod_timestamp_exact_match_required: true" in text
    assert "feature_source_eod_base_columns_exact_match_required: true" in text
    assert "feature_changes_recomputed: true" in text
    assert "feature_rolling_zscores_recomputed: true" in text
    assert "feature_rolling_percentiles_recomputed: true" in text
    assert "pointer_promotion_mode: transactional_with_rollback" in text
