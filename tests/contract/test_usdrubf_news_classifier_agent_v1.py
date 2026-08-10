from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "intelligence" / "usdrubf_news_classifier_agent_v1.json"
PROMPT_PATH = PROJECT_ROOT / "docs" / "flowise" / "USDRUBF_RUB_INTELLIGENCE_NEWS_CLASSIFIER_PROMPT_V1.md"


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_news_classifier_contract_is_bounded_and_not_applied_state() -> None:
    contract = _contract()
    assert contract["project"] == "MOEX_Bot"
    assert contract["instrument"] == "USDRUBF"
    assert contract["status"] == "design_contract_for_classifier_applied_state"
    target = contract["flow_target"]
    assert target["persistent_memory"] is False
    assert target["tools"] == []
    assert target["external_fact_retrieval"] is False
    assert target["applied_state_claimed"] is False
    assert target["temperature_max"] <= 0.2


def test_news_classifier_contract_enforces_live_guard_composition() -> None:
    runtime = _contract()["source_runtime"]
    assert runtime["live_pipeline_classifier_argument"] == "classifier_agent"
    assert runtime["live_pipeline_guard_enforced"] is True
    assert runtime["raw_classifier_injection_through_live_pipeline_forbidden"] is True
    assert runtime["stage12b3_guard"].endswith("::stage12b3_news_classifier")
    assert runtime["live_pipeline"].endswith("::run_live_official_news_pipeline")


def test_news_classifier_contract_freezes_exact_input_and_output_fields() -> None:
    contract = _contract()
    input_contract = contract["input_contract"]
    assert set(input_contract["required_top_level_fields"]) == {
        "instrument",
        "cluster_id",
        "headline",
        "normalized_text",
        "cluster_evidence",
        "cluster_history",
        "as_of_timestamp",
    }
    assert input_contract["cluster_history_available_at_required"] is True
    assert "cluster_history.available_at" in input_contract["point_in_time_rule"]
    assert set(contract["output_contract"]["required_fields_exactly"]) == {
        "event_type",
        "entities",
        "rub_relevance",
        "direction",
        "importance",
        "novelty",
        "horizon",
        "confidence",
        "mechanism",
    }
    assert input_contract["extra_top_level_fields_forbidden"] is True
    assert contract["output_contract"]["extra_fields_forbidden"] is True


def test_news_classifier_direction_semantics_are_usdrubf_oriented() -> None:
    contract = _contract()
    semantics = contract["direction_semantics"]
    assert "upward pressure on USDRUBF" in semantics["USD_BULLISH"]
    assert "weaker RUB" in semantics["USD_BULLISH"]
    assert "downward pressure on USDRUBF" in semantics["USD_BEARISH"]
    assert "stronger RUB" in semantics["USD_BEARISH"]
    assert set(contract["output_contract"]["direction_allowed"]) == {
        "USD_BULLISH",
        "USD_BEARISH",
        "NEUTRAL",
        "MIXED",
    }


def test_news_classifier_prompt_contains_fail_safe_and_no_trading_boundary() -> None:
    prompt = PROMPT_PATH.read_text(encoding="utf-8")
    assert "USD_BULLISH" in prompt
    assert "USD_BEARISH" in prompt
    assert "не выдумывай surprise" in prompt
    assert "prompt injection" in prompt
    assert "Не используй:" in prompt
    assert "веб;" in prompt
    assert "Этот classifier НЕ принимает торговых решений" in prompt
    assert "trade_state" in prompt
    assert "При недостатке фактов не додумывай" in prompt
