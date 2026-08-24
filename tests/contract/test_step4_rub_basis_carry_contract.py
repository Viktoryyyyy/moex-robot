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


def test_stage4_normalization_and_causal_alignment_are_explicit() -> None:
    program = _read("configs/datasets/step4_rub_basis_carry.v1.yaml")
    dataset = _read("contracts/datasets/rub_basis_carry_5m.v1.yaml")
    for text in (program, dataset):
        assert "1000.0" in text
        assert "exact" in text
        assert "forward_fill_allowed: false" in text
        assert "asof_join_allowed: false" in text
    assert "CR_is_quoted_in_RUB_per_1_CNY" in program
    assert "Si_is_quoted_in_RUB_per_1000_USD_lot" in program


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


def test_stage4_has_immutable_pilot_and_transactional_acceptance() -> None:
    program = _read("configs/datasets/step4_rub_basis_carry.v1.yaml")
    acceptance = _read("contracts/datasets/step4_rub_basis_carry_acceptance.v1.yaml")
    assert "run_artifacts_immutable: true" in program
    assert "expected_accepted_pointer_count: 2" in program
    assert "pointer_promotion_mode: transactional_with_rollback" in acceptance
    assert "continuous_series_used_required: false" in acceptance
    assert "partial_pointer_set_without_acceptance_marker_is_not_accepted: true" in acceptance
