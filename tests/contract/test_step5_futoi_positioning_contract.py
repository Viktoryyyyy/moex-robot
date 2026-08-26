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


def test_stage5_requires_content_attested_generation_and_immutable_byte_freeze() -> None:
    text = _read("configs/datasets/step5_futoi_positioning.v1.yaml")
    assert "raw_history_content_attestation_ref: contracts/datasets/futures_raw_history_content_attestation.v1.yaml" in text
    assert "content_attested_manifest_ref: contracts/datasets/futures_raw_history_content_attested_manifest.v1.yaml" in text
    assert "content_attestation_current_generation_marker_required: true" in text
    assert "content_attestation_resolver_required: moex_data.futures.stage2_raw_history_content_reattestation.resolve_content_attested_history" in text
    assert "legacy_accepted_raw_pointer_consumption_allowed: false" in text
    assert "per_partition_attested_sha256_required: true" in text
    assert "immutable_generation_snapshot_read_required: true" in text
    assert "canonical_raw_partition_read_allowed: false" in text
    assert "immutable_raw_input_freeze:" in text
    assert "source_mode: stage2_content_attested_generation_snapshots_only" in text
    assert "attested_snapshot_sha256_revalidated_before_freeze: true" in text
    assert "stage2_futoi_partition_validator_reapplied: true" in text
    assert "freeze_mode: create_only_hardlink_same_validated_inode" in text
    assert "hardlink_failure_fallback_allowed: false" in text
    assert "canonical_raw_reads_after_freeze_allowed: false" in text
    assert "content_attestation_generation_re_resolved_at_acceptance: true" in text
    assert "legacy_pointer_bypass_allowed: false" in text


def test_stage5_revision_snapshot_and_source_quality_omission_policy_are_fail_closed() -> None:
    text = _read("configs/datasets/step5_futoi_positioning.v1.yaml")
    assert "systime_as_revision_order_allowed: false" in text
    assert "multi_sess_id_same_analytical_key: fail_closed" in text
    assert "within_single_sess_id_revision_order: greatest_seqnum" in text
    assert "final_event_ts_rule: latest_resolved_complete_balanced_FIZ_YUR_event_ts" in text
    assert "later_incomplete_source_tail_allowed: true" in text
    assert "later_unbalanced_source_tail_allowed: true" in text
    assert "synthetic_missing_group_fill_allowed: false" in text
    assert "selected_event_must_satisfy_exact_position_balance: true" in text
    assert "no_complete_balanced_snapshot: omit_only_if_explicit_attested_source_quality_exception" in text
    assert "undeclared_no_complete_balanced_snapshot: fail_closed" in text
    assert "mode: explicit_attested_date_only_fail_closed_otherwise" in text
    assert 'trade_date: "2025-08-11"' in text
    assert "reason: no_complete_balanced_FIZ_YUR_snapshot" in text
    assert "tolerance_normalization_allowed: false" in text
    assert "eod_clock_time_hardcoded: false" in text


def test_stage5_eod_contract_covers_attested_lineage_position_balance_and_participant_metrics() -> None:
    text = _read("contracts/datasets/futures_futoi_eod.v1.yaml")
    for token in (
        "current_generation_marker_required: true",
        "resolver_required: moex_data.futures.stage2_raw_history_content_reattestation.resolve_content_attested_history",
        "legacy_accepted_pointer_read_allowed: false",
        "immutable_generation_snapshot_read_required: true",
        "per_partition_attested_sha256_required: true",
        "source_mode: stage2_content_attested_generation_snapshots_only",
        "source_frozen_attested_hash_equality_required: true",
        "canonical_raw_reads_after_freeze_allowed: false",
        "rule: latest_resolved_complete_balanced_FIZ_YUR_event_ts",
        "synthetic_missing_group_fill_allowed: false",
        "no_complete_balanced_snapshot: omit_only_if_explicit_attested_source_quality_exception",
        "undeclared_no_complete_balanced_snapshot: fail_closed",
        "acceptance_independent_frozen_raw_revalidation_required: true",
        'trade_date: "2025-08-11"',
        "source_canonical_partition_ref",
        "source_frozen_partition_sha256",
        "phys_net", "legal_net", "total_open_interest", "phys_gross", "legal_gross",
        "phys_net_share_of_oi", "phys_gross_share_of_two_sided_oi",
        "phys_avg_long_per_participant", "legal_avg_short_per_participant",
        "phys_net_plus_legal_net_equals_zero: true",
        "total_open_interest_equals_total_short_abs: true",
        "recomputed_gross_and_share_metrics",
        "recomputed_participant_averages",
        "declared_source_quality_omission_set_exact",
        "omitted_source_quality_dates_independently_revalidated",
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


def test_stage5_acceptance_requires_content_attested_frozen_bytes_and_independent_reconstruction() -> None:
    text = _read("contracts/datasets/step5_futoi_positioning_acceptance.v1.yaml")
    assert "accepted_pointer_count: 4" in text
    assert "immutable_raw_input_freeze_required: true" in text
    assert "raw_input_freeze_mode_required: create_only_hardlink_same_validated_inode" in text
    assert "content_attestation_current_generation_marker_required: true" in text
    assert "legacy_raw_pointer_consumption_forbidden: true" in text
    assert "content_attestation_generation_re_resolved_at_acceptance: true" in text
    assert "content_attestation_marker_sha256_revalidated: true" in text
    assert "content_attested_manifest_sha256_revalidated: true" in text
    assert "content_attested_content_set_sha256_revalidated: true" in text
    assert "content_attested_partition_sha256_revalidated: true" in text
    assert "frozen_partition_sha_must_equal_content_attested_sha: true" in text
    assert "canonical_raw_partition_reads_after_freeze_used_required: false" in text
    assert "frozen_partition_stage2_futoi_physical_validator_reapplied_at_acceptance: true" in text
    assert "eod_source_frozen_ref_and_sha_lineage_required: true" in text
    assert "eod_reconstruction_from_physically_revalidated_frozen_raw_required: true" in text
    assert "eod_reconstruction_independent_from_eod_producer_required: true" in text
    assert "eod_reconstruction_exact_field_equality_before_promotion_required: true" in text
    assert "source_quality_omission_policy_required: explicit_attested_date_only_fail_closed_otherwise" in text
    assert "source_quality_omitted_date_independent_frozen_raw_revalidation_required: true" in text
    assert "undeclared_derived_coverage_loss_forbidden: true" in text
    assert "historical_expected_raw_partitions:" in text
    assert "historical_expected_derived_rows:" in text
    assert "si_futures_family: 1756" in text
    assert "physical_parquet_readback_required: true" in text
    assert "eod_current_content_attestation_marker_required: true" in text
    assert "eod_all_derived_metrics_recomputed: true" in text
    assert "eod_participant_average_zero_count_rules_revalidated: true" in text
    assert "feature_source_eod_identity_exact_match_required: true" in text
    assert "feature_source_eod_timestamp_exact_match_required: true" in text
    assert "feature_source_eod_base_columns_exact_match_required: true" in text
    assert "feature_changes_recomputed: true" in text
    assert "feature_rolling_zscores_recomputed: true" in text
    assert "feature_rolling_percentiles_recomputed: true" in text
    assert "pointer_promotion_mode: transactional_with_rollback" in text
