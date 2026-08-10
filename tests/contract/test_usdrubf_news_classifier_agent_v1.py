from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "intelligence" / "usdrubf_news_classifier_agent_v1.json"
PROMPT_PATH = PROJECT_ROOT / "docs" / "flowise" / "USDRUBF_RUB_INTELLIGENCE_NEWS_CLASSIFIER_PROMPT_V1.md"
ENV_EXAMPLE_PATH = PROJECT_ROOT / ".env.example"


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_news_classifier_contract_is_bounded_and_applied_state_verified() -> None:
    contract = _contract()
    assert contract["project"] == "MOEX_Bot"
    assert contract["instrument"] == "USDRUBF"
    assert contract["status"] == "flowise_applied_state_verified"
    target = contract["flow_target"]
    assert target["flow_id"] == "69b38496-7013-4474-be6c-7dce43177b95"
    assert target["persistent_memory"] is False
    assert target["llm_memory"] is False
    assert target["tools"] == []
    assert target["external_fact_retrieval"] is False
    assert target["applied_state_claimed"] is True
    assert target["model_provider"] == "Deepseek"
    assert target["model_name"] == "deepseek-chat"
    assert target["temperature_applied"] == 0.1
    assert target["streaming"] is False
    assert target["thinking"] is False
    assert target["json_structured_output"] is False
    assert target["authorization"] == "NONE"
    assert target["temperature_max"] <= 0.2


def test_news_classifier_contract_enforces_live_guard_composition() -> None:
    runtime = _contract()["source_runtime"]
    assert runtime["live_pipeline_classifier_argument"] == "classifier_agent"
    assert runtime["live_pipeline_guard_enforced"] is True
    assert runtime["raw_classifier_injection_through_live_pipeline_forbidden"] is True
    assert runtime["stage12b3_guard"].endswith("::stage12b3_news_classifier")
    assert runtime["live_pipeline"].endswith("::run_live_official_news_pipeline")


def test_news_classifier_transport_records_verified_applied_state() -> None:
    contract = _contract()
    runtime = contract["source_runtime"]
    transport = contract["transport_contract"]
    expected_env = {
        "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_ENDPOINT",
        "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD",
        "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD",
        "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS",
    }
    assert runtime["flowise_transport_adapter"].endswith("::news_classifier_flowise_agent_from_env")
    assert transport["kind"] == "FLOWISE_JSON_OVER_HTTPS"
    assert transport["endpoint"] == (
        "https://flowise.foods-tech.store/api/v1/prediction/"
        "69b38496-7013-4474-be6c-7dce43177b95"
    )
    assert transport["endpoint_committed"] is True
    assert transport["authorization"] == "NONE"
    assert transport["request_field"] == "question"
    assert transport["response_field"] == "text"
    assert transport["endpoint_guessing_forbidden"] is True
    assert transport["dotenv_loading_inside_adapter"] is False
    assert transport["canonical_dotenv_load_owned_by_caller"] is True
    assert set(transport["required_env"]) == expected_env
    assert transport["endpoint_scheme_required"] == "https"

    env_example = ENV_EXAMPLE_PATH.read_text(encoding="utf-8")
    for name in expected_env:
        assert f"{name}=" in env_example
    assert (
        "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_ENDPOINT="
        "https://flowise.foods-tech.store/api/v1/prediction/"
        "69b38496-7013-4474-be6c-7dce43177b95\n"
    ) in env_example


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
