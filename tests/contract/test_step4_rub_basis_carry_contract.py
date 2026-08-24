from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_stage4_replaces_mandatory_continuous_with_basis_carry() -> None:
    text = _read("configs/datasets/step4_rub_basis_carry.v1.yaml")
    assert "continuous_series_required_for_step4: false" in text
    assert "scope: optional_research_backtest_only" in text
    assert "perpetual_instrument_id: usdrubf_futures_family" in text
    assert "perpetual_instrument_id: cnyrubf_futures_family" in text
    assert "front_instrument_id: si_front_contract" in text
    assert "front_instrument_id: cr_front_contract" in text
    assert "spot_instrument_id: usd_tom" in text
    assert "spot_instrument_id: cny_tom" in text


def test_stage4_normalization_causal_alignment_and_timestamp_policy_are_explicit() -> None:
    program = _read("configs/datasets/step4_rub_basis_carry.v1.yaml")
    dataset = _read("contracts/datasets/rub_basis_carry_5m.v1.yaml")
    for text in (program, dataset):
        assert "1000.0" in text
        assert "exact" in text
        assert "forward_fill_allowed: false" in text
        assert "asof_join_allowed: false" in text
    assert "CR_is_quoted_in_RUB_per_1_CNY" in program
    assert "Si_is_quoted_in_RUB_per_1000_USD_lot" in program
    assert "naive_source_timestamp_semantics: exchange_local" in dataset
    assert "naive_source_conversion: localize_Europe_Moscow_then_convert_UTC" in dataset
    assert "output_ts_timezone: UTC" in dataset


def test_stage4_expiry_day_carry_policy_is_explicit_and_enforced_by_pilot() -> None:
    program = _read("configs/datasets/step4_rub_basis_carry.v1.yaml")
    acceptance = _read("contracts/datasets/step4_rub_basis_carry_acceptance.v1.yaml")
    pilot = _read("src/moex_data/step4_basis_carry_pilot_runner.py")
    assert "front_next_selection_policy: nearest_two_contracts_with_last_trade_date_strictly_after_trade_date" in program
    assert "front_next_minimum_days_to_expiry: 1" in program
    assert "expiry_day_contract_allowed_for_annualized_carry: false" in program
    assert "front_next_minimum_days_to_expiry_required: 1" in acceptance
    assert "FRONT_NEXT_MINIMUM_DAYS_TO_EXPIRY: Final[int] = 1" in pilot
    assert "minimum_days_to_expiry=FRONT_NEXT_MINIMUM_DAYS_TO_EXPIRY" in pilot


def test_stage4_output_contract_contains_required_market_features() -> None:
    text = _read("contracts/datasets/rub_basis_carry_5m.v1.yaml")
    for token in (
        "perpetual_spot_basis_bps",
        "front_spot_basis_bps",
        "next_spot_basis_bps",
        "front_perpetual_basis_bps",
        "next_perpetual_basis_bps",
        "front_next_spread_bps",
        "front_spot_implied_carry_annualized",
        "next_spot_implied_carry_annualized",
        "front_next_term_carry_annualized",
    ):
        assert token in text
    assert "fair_value_model_claimed: false" in text
    assert "metric_semantics: market_implied_carry_proxy" in text


def test_stage4_acceptance_physically_revalidates_parquet_before_promotion() -> None:
    contract = _read("contracts/datasets/step4_rub_basis_carry_acceptance.v1.yaml")
    acceptance = _read("src/moex_data/step4_basis_carry_acceptance.py")
    validator = _read("src/moex_data/analytics/validate_rub_basis_carry_partition.py")
    for token in (
        "physical_partition_readback_required: true",
        "physical_row_count_match_required: true",
        "physical_instrument_trade_date_identity_required: true",
        "physical_timezone_aware_utc_timestamp_required: true",
        "physical_duplicate_ts_zero_required: true",
        "physical_monotonic_ts_required: true",
        "physical_market_rates_finite_positive_required: true",
        "unreadable_parquet_blocks_promotion: true",
        "physical_validation_completes_before_pointer_writes: true",
    ):
        assert token in contract
    assert "validate_rub_basis_carry_partition as physical" in acceptance
    assert "physical.validate_partition(" in acceptance
    assert "pd.read_parquet" in validator
    assert "derived partition row_count mismatch" in validator
    assert "derived partition instrument_id mismatch" in validator
    assert "derived partition trade_date mismatch" in validator
    assert "derived partition contains duplicate ts" in validator
    assert "derived partition rate must be finite and positive" in validator


def test_stage4_has_immutable_pilot_and_transactional_acceptance() -> None:
    program = _read("configs/datasets/step4_rub_basis_carry.v1.yaml")
    dataset = _read("contracts/datasets/rub_basis_carry_5m.v1.yaml")
    acceptance = _read("contracts/datasets/step4_rub_basis_carry_acceptance.v1.yaml")
    assert "run_artifacts_immutable: true" in program
    assert "expected_accepted_pointer_count: 2" in program
    assert "pointer_run_id_must_equal_referenced_manifest_run_id: true" in dataset
    assert "acceptance_run_id_stored_separately: true" in dataset
    assert "manifest_and_quality_run_id_must_equal_derived_output_run_id: true" in acceptance
    assert "timestamp_policy_required: naive_exchange_localize_europe_moscow_then_utc" in acceptance
    assert "pointer_run_id_source: referenced_manifest_run_id" in acceptance
    assert "pointer_promotion_mode: transactional_with_rollback" in acceptance
    assert "continuous_series_used_required: false" in acceptance
    assert "partial_pointer_set_without_acceptance_marker_is_not_accepted: true" in acceptance
