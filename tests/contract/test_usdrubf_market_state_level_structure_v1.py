import json
from pathlib import Path


CONTRACT_PATH = Path("contracts/intelligence/usdrubf_market_state_level_structure_v1.json")


def _load_contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_runtime_boundary() -> None:
    contract = _load_contract()

    assert contract["project"] == "MOEX_Bot"
    assert contract["instrument"] == "USDRUBF"
    assert contract["contract_version"] == 1
    assert contract["status"] == "design_contract_only"
    assert contract["runtime_readiness_claim"] is False
    assert contract["implementation_authorized"] is False
    assert contract["trading_authorized"] is False


def test_technical_market_facts_are_deterministic_not_llm_generated() -> None:
    contract = _load_contract()
    facts = contract["source_of_facts"]

    assert facts["technical_market_facts_owner"] == "deterministic_python_engine"
    assert facts["llm_may_create_numeric_levels"] is False
    assert facts["llm_may_classify_level_events"] is False


def test_market_state_has_required_decision_fields() -> None:
    contract = _load_contract()
    market_state = contract["market_state"]

    expected = {
        "instrument",
        "as_of_timestamp",
        "price",
        "trend",
        "market_regime",
        "active_levels",
        "level_interaction",
        "ema_3_19_ai",
        "futoi",
        "news_state",
        "macro_state",
        "final_bias",
        "trade_state",
        "confidence",
        "targets",
        "invalidation",
    }
    assert expected.issubset(set(market_state["required_fields"]))
    assert market_state["trade_state_allowed"] == [
        "WAIT",
        "ENTER",
        "HOLD",
        "ADD",
        "REDUCE",
        "EXIT",
    ]
    assert market_state["previous_state_required_for_change_detection"] is True


def test_level_is_a_zone_and_forbids_lookahead() -> None:
    contract = _load_contract()
    level = contract["level_zone"]

    assert level["numeric_representation"] == "zone_not_single_magic_price"
    assert {"level_id", "center_price", "lower_bound", "upper_bound"}.issubset(
        set(level["required_fields"])
    )
    assert any("future target labels" in rule for rule in level["constraints"])


def test_level_interaction_is_bound_to_one_level_and_keeps_per_level_history() -> None:
    contract = _load_contract()
    interaction = contract["level_interaction"]

    assert interaction["record_cardinality"] == "one_interaction_record_per_level_id"
    assert {
        "level_id",
        "state",
        "direction",
        "event_timestamp",
        "previous_state",
        "structural_quality",
    }.issubset(set(interaction["required_fields"]))
    assert interaction["level_id_reference"] == (
        "must reference exactly one level_id present in MarketState.active_levels"
    )
    assert interaction["history_partition_key"] == "level_id"


def test_level_state_machine_contains_required_test_retest_paths() -> None:
    contract = _load_contract()
    interaction = contract["level_interaction"]
    transitions = interaction["transition_map"]

    assert "TEST" in transitions["APPROACH"]
    assert "BREAKOUT_ATTEMPT" in transitions["TEST"]
    assert "BREAKOUT" in transitions["BREAKOUT_ATTEMPT"]
    assert "RETEST_PENDING" in transitions["BREAKOUT"]
    assert "RETEST" in transitions["RETEST_PENDING"]
    assert "RETEST_HOLD" in transitions["RETEST"]
    assert "RETEST_FAIL" in transitions["RETEST"]
    assert "ACCEPTANCE" in transitions["RETEST_HOLD"]
    assert "FALSE_BREAKOUT" in transitions["RETEST_FAIL"]


def test_level_detection_requires_structure_not_single_tick() -> None:
    contract = _load_contract()
    interaction = contract["level_interaction"]

    assert interaction["single_tick_is_sufficient_for_test"] is False
    assert interaction["closed_bar_confirmation_required_for_breakout_or_acceptance"] is True
    assert interaction["lookahead_forbidden"] is True
    assert {
        "distance_to_zone",
        "penetration_depth",
        "time_near_zone",
        "touch_count",
        "reaction_distance",
        "closed_bar_positions",
        "volume_context",
        "volatility_context",
    } == set(interaction["event_detection_inputs"])


def test_quality_separates_structure_from_cross_factor_confirmation() -> None:
    contract = _load_contract()
    quality = contract["quality_model"]

    assert "structural_quality" in quality["composite_confirmation_inputs"]
    assert "ema_3_19_ai_confirmation" in quality["composite_confirmation_inputs"]
    assert "futoi_confirmation" in quality["composite_confirmation_inputs"]
    assert quality["news_or_macro_must_not_rewrite_technical_event"] is True
    assert quality["exact_weighting"] == "implementation_phase_only"


def test_stage_two_does_not_authorize_later_components() -> None:
    contract = _load_contract()
    non_authorizations = set(contract["explicit_non_authorizations"])

    assert "no news ingestion implementation" in non_authorizations
    assert "no LLM news classification implementation" in non_authorizations
    assert "no alert delivery implementation" in non_authorizations
    assert "no trading action" in non_authorizations
