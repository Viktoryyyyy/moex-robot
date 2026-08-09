import json
from pathlib import Path


CONTRACT_PATH = Path("contracts/intelligence/usdrubf_news_macro_intelligence_v1.json")


def _load_contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_runtime_boundary() -> None:
    contract = _load_contract()
    assert contract["project"] == "MOEX_Bot"
    assert contract["lane"] == "rub_intelligence"
    assert contract["instrument"] == "USDRUBF"
    assert contract["contract_version"] == 1
    assert contract["status"] == "design_contract_only"
    assert contract["runtime_readiness_claim"] is False
    assert contract["implementation_authorized"] is False
    assert contract["trading_authorized"] is False


def test_prior_news_restriction_is_scoped_not_mutated_globally() -> None:
    contract = _load_contract()
    scope = contract["scope_supersession"]
    assert scope["prior_contract"] == "contracts/features/usdrubf_external_feature_contract_v0.json"
    assert scope["superseded_only_for_lane"] == "rub_intelligence"
    assert scope["prior_contract_remains_unchanged"] is True
    assert scope["ema_3_19_ai_research_semantics_unchanged"] is True


def test_llm_cannot_create_factual_market_or_source_data() -> None:
    contract = _load_contract()
    ownership = contract["fact_ownership"]
    assert ownership["llm_may_invent_facts"] is False
    assert ownership["llm_may_modify_technical_level_events"] is False
    factual = set(ownership["deterministic_or_source_bound_facts"])
    assert {
        "source identity",
        "publication timestamp",
        "event existence",
        "numeric macro values",
        "technical price levels",
        "technical level events",
    }.issubset(factual)


def test_source_policy_requires_provenance_and_availability() -> None:
    contract = _load_contract()
    source = contract["source_policy"]
    assert source["unverified_source_fallback_forbidden"] is True
    assert source["scraped_social_media_as_factual_source"] is False
    assert source["source_reference_required"] is True
    assert source["publication_timestamp_required"] is True
    assert "after" in source["availability_rule"]


def test_news_pipeline_deduplicates_before_llm_classification() -> None:
    contract = _load_contract()
    pipeline = contract["news_pipeline"]
    assert pipeline.index("DEDUPLICATION_AND_CLUSTERING") < pipeline.index("LLM_CLASSIFICATION")
    assert pipeline[-1] == "PERSIST_NEWS_EVENT"


def test_news_event_has_required_provenance_and_interpretation_fields() -> None:
    contract = _load_contract()
    event = contract["news_event"]
    required = set(event["required_fields"])
    assert {
        "event_id",
        "cluster_id",
        "source_id",
        "source_reference",
        "published_at",
        "available_at",
        "content_hash",
        "rub_relevance",
        "direction",
        "importance",
        "novelty",
        "horizon",
        "confidence",
        "mechanism",
        "quality_status",
    }.issubset(required)
    assert event["direction_allowed"] == [
        "USD_BULLISH",
        "USD_BEARISH",
        "NEUTRAL",
        "MIXED",
    ]
    assert event["rub_relevance_range"] == [0.0, 1.0]
    assert event["confidence_range"] == [0.0, 1.0]


def test_duplicate_and_novelty_rules_do_not_rely_on_llm_memory() -> None:
    contract = _load_contract()
    dedupe = contract["deduplication_and_novelty"]
    assert dedupe["content_hash_required"] is True
    assert dedupe["cluster_id_required"] is True
    assert dedupe["duplicate_headline_must_not_raise_independent_event_count"] is True
    assert dedupe["novelty_must_use_supplied_cluster_history"] is True
    assert dedupe["llm_memory_alone_must_not_determine_novelty"] is True


def test_macro_observation_is_source_bound_and_point_in_time() -> None:
    contract = _load_contract()
    observation = contract["macro_observation"]
    required = set(observation["required_fields"])
    assert {
        "metric_id",
        "source_id",
        "source_reference",
        "value",
        "unit",
        "published_at",
        "available_at",
        "quality_status",
    }.issubset(required)
    assert observation["numeric_fact_owner"] == "deterministic_source_adapter"
    assert observation["llm_may_create_numeric_macro_values"] is False
    assert observation["future_observation_use_forbidden"] is True


def test_macro_state_has_pit_selection_and_explicit_missing_data() -> None:
    contract = _load_contract()
    state = contract["macro_state"]
    assert {
        "as_of_timestamp",
        "observations",
        "overall_direction",
        "confidence",
        "dominant_drivers",
    }.issubset(set(state["required_fields"]))
    assert "available_at <= as_of_timestamp" in state["observation_selection_rule"]
    assert "must not be imputed by the LLM" in state["missing_data_rule"]


def test_news_macro_cannot_rewrite_technical_structure_or_trade() -> None:
    contract = _load_contract()
    boundary = contract["decision_boundary"]
    assert boundary["news_event_may_directly_rewrite_technical_event"] is False
    assert boundary["macro_state_may_directly_rewrite_technical_event"] is False
    assert boundary["news_or_macro_may_directly_trigger_broker_action"] is False
    assert boundary["trade_recommendation_generation"] == "later_stage_only"
    assert boundary["alert_delivery"] == "later_stage_only"


def test_storage_contract_does_not_assume_content_rights() -> None:
    contract = _load_contract()
    rights = contract["rights_and_storage"]
    assert rights["raw_content_redistribution_assumed"] is False
    assert rights["raw_full_text_retention_authorized_by_this_contract"] is False
    assert "content hash" in rights["default_persisted_material"]


def test_stage_four_contract_does_not_authorize_runtime_or_trading() -> None:
    contract = _load_contract()
    blocked = set(contract["explicit_non_authorizations"])
    assert {
        "no news ingestion runtime",
        "no macro ingestion runtime",
        "no Flowise runtime classification",
        "no server apply",
        "no alert delivery",
        "no decision engine implementation",
        "no broker action",
        "no trading action",
    }.issubset(blocked)
