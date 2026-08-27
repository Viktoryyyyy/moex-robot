from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_step7_production_scope_uses_only_full_history_native_perpetuals() -> None:
    text = _read("configs/datasets/step7_rub_native_d1_w1_technical.v1.yaml")
    assert "- usdrubf_futures_family" in text
    assert "- cnyrubf_futures_family" in text
    assert "continuous_series_required_for_production: false" in text
    assert "fixed_expiry_substitution_allowed: false" in text
    assert text.count("expected_d1_rows: 1100") == 2


def test_step7_requires_current_content_attested_snapshot_source() -> None:
    text = _read("configs/datasets/step7_rub_native_d1_w1_technical.v1.yaml")
    for token in (
        "current_content_attested_generation_required: true",
        "legacy_accepted_pointer_consumption_allowed: false",
        "source_mode: stage2_content_attested_generation_snapshots_only",
        "exact_full_content_attested_range_required: true",
        "current_generation_marker_binding_required: true",
        "current_generation_manifest_binding_required: true",
        "current_generation_partition_content_set_binding_required: true",
    ):
        assert token in text


def test_step7_requires_independent_validated_raw_byte_freeze() -> None:
    text = _read("configs/datasets/step7_rub_native_d1_w1_technical.v1.yaml")
    for token in (
        "physical_revalidation_before_freeze: true",
        "per_partition_sha256_required: true",
        "create_only_run_scoped_independent_inode_exact_byte_copy_required: true",
        "source_descriptor_snapshot_stability_required: true",
        "mutable_canonical_raw_read_after_freeze_allowed: false",
        "acceptance_revalidates_frozen_bytes: true",
        "acceptance_hashes_and_parses_same_opened_byte_snapshot: true",
    ):
        assert token in text


def test_step7_feature_scope_does_not_invent_unapproved_windows() -> None:
    text = _read("configs/datasets/step7_rub_native_d1_w1_technical.v1.yaml")
    assert "atr_14_wilder" in text
    assert "atr_20_wilder" in text
    assert "zero_previous_close_denominator_policy: fail_closed_before_ratio_computation" in text
    assert "emitted_numeric_feature_policy: finite_when_non_null" in text
    assert "ema_filter:" in text and "deferred_exact_window_not_approved" in text
    assert "realized_volatility:" in text and "deferred_exact_window_and_annualization_not_approved" in text
    assert "range_percentile:" in text and "deferred_exact_window_not_approved" in text
    assert "swing_high_low:" in text and "deferred_causal_confirmation_policy_not_approved" in text


def test_step7_ohlcv_contract_uses_completed_weeks_and_conservative_availability() -> None:
    text = _read("contracts/datasets/rub_native_ohlcv_htf.v1.yaml")
    assert "d1_availability_rule: trade_date_plus_1_calendar_day_at_0600_europe_moscow" in text
    assert "w1_availability_rule: following_monday_at_0600_europe_moscow" in text
    assert "completed_w1_only: true" in text
    assert "frozen_partition_sha256_required: true" in text
    assert "source_mode_required: stage2_content_attested_generation_snapshots_only" in text
    assert "legacy_accepted_pointer_consumption_allowed: false" in text


def test_step7_acceptance_requires_formula_rebuild_and_eight_pointers() -> None:
    text = _read("contracts/datasets/step7_rub_native_d1_w1_technical_acceptance.v1.yaml")
    assert "accepted_pointer_count: 8" in text
    assert "content_attested_source_mode_required: stage2_content_attested_generation_snapshots_only" in text
    assert "legacy_accepted_pointer_consumption_used_required: false" in text
    assert "current_content_attestation_marker_ref_and_sha256_must_match_frozen_upstream: true" in text
    assert "current_content_attested_partition_content_set_must_match_frozen_upstream: true" in text
    assert "frozen_raw_independent_inode_exact_byte_copy_required: true" in text
    assert "frozen_raw_single_open_byte_snapshot_hash_and_parse_required: true" in text
    assert "independent_oracle_must_consume_captured_validated_frame: true" in text
    assert "d1_ohlcv_reaggregation_required: true" in text
    assert "w1_ohlcv_reaggregation_from_d1_required: true" in text
    assert "technical_formula_recalculation_required: true" in text
    assert "technical_zero_previous_close_denominator_rejected: true" in text
    assert "atr_wilder_seed_and_recurrence_recalculation_required: true" in text
    assert "pointer_promotion_mode: serialized_transactional_with_rollback" in text
    assert "stage7_publication_lock_required: true" in text
    assert "stage2_content_attestation_lock_required_during_validation_and_commit: true" in text
