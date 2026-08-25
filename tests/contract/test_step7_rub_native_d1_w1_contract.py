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
    assert "expected_d1_rows: 1100" in text


def test_step7_requires_immutable_validated_raw_byte_freeze() -> None:
    text = _read("configs/datasets/step7_rub_native_d1_w1_technical.v1.yaml")
    for token in (
        "physical_revalidation_before_freeze: true",
        "per_partition_sha256_required: true",
        "create_only_run_scoped_hardlink_required: true",
        "validated_inode_binding_required: true",
        "mutable_canonical_raw_read_after_freeze_allowed: false",
        "acceptance_revalidates_frozen_bytes: true",
    ):
        assert token in text


def test_step7_feature_scope_does_not_invent_unapproved_windows() -> None:
    text = _read("configs/datasets/step7_rub_native_d1_w1_technical.v1.yaml")
    assert "atr_14_wilder" in text
    assert "atr_20_wilder" in text
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


def test_step7_acceptance_requires_formula_rebuild_and_eight_pointers() -> None:
    text = _read("contracts/datasets/step7_rub_native_d1_w1_technical_acceptance.v1.yaml")
    assert "accepted_pointer_count: 8" in text
    assert "frozen_raw_physical_revalidation_required: true" in text
    assert "d1_ohlcv_reaggregation_required: true" in text
    assert "w1_ohlcv_reaggregation_from_d1_required: true" in text
    assert "technical_formula_recalculation_required: true" in text
    assert "atr_wilder_seed_and_recurrence_recalculation_required: true" in text
    assert "pointer_promotion_mode: transactional_with_rollback" in text
