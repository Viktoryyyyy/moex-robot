from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts/intelligence/usdrubf_flowise_decision_agent_v1.json"
PROMPT_PATH = ROOT / "docs/flowise/USDRUBF_RUB_INTELLIGENCE_DECISION_AGENT_PROMPT_V1.md"
ENV_EXAMPLE_PATH = ROOT / ".env.example"


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_shadow_boundary() -> None:
    contract = _contract()
    assert contract["project"] == "MOEX_Bot"
    assert contract["instrument"] == "USDRUBF"
    assert contract["status"] == "flowise_applied_state_verified"

    flowise = contract["flowise"]
    assert flowise["flow_name"] == "usdrubf-rub-intelligence-decision-v1"
    assert flowise["flow_id"] == "9ad1f42c-e7f1-44a3-a574-09a7da4ddecf"
    assert flowise["applied_state_verified"] is True
    assert flowise["model_provider"] == "Deepseek"
    assert flowise["model_name"] == "deepseek-chat"
    assert flowise["persistent_memory"] is False
    assert flowise["llm_memory"] is False
    assert flowise["tools"] == []
    assert flowise["external_fact_fetch"] is False
    assert flowise["temperature_applied"] == 0.1
    assert flowise["streaming"] is False
    assert flowise["thinking"] is False
    assert flowise["json_structured_output"] is False
    assert flowise["authorization"] == "NONE"

    boundary = contract["runtime_boundary"]
    assert boundary["shadow_only"] is True
    assert boundary["stage_11_trade_state_guard_enabled"] is True
    assert boundary["alert_delivery"] is False
    assert boundary["broker_action"] is False
    assert boundary["order_placement"] is False
    assert boundary["autonomous_trading"] is False
    assert boundary["server_apply_proven_by_this_contract"] is False
    assert boundary["flowise_applied_state_proven_by_this_contract"] is True


def test_contract_matches_bounded_decision_output_shape() -> None:
    contract = _contract()
    output = contract["output_contract"]
    assert output["exact_fields"] == [
        "final_bias",
        "trade_state",
        "confidence",
        "target_references",
        "invalidation_reference",
        "scenario",
        "reason",
        "evidence_refs",
    ]
    assert set(output["final_bias_allowed"]) == {
        "BULLISH_USD",
        "NEUTRAL",
        "BEARISH_USD",
    }
    assert set(output["decision_engine_trade_state_allowed"]) == {
        "WAIT",
        "ENTER",
        "HOLD",
        "ADD",
        "REDUCE",
        "EXIT",
    }
    assert output["stage_11_shadow_trade_state_allowed"] == ["WAIT", "ENTER"]
    assert output["stage_11_runtime_enforced"] is True
    assert output["numeric_level_output_forbidden"] is True


def test_stage_11_runtime_guard_is_authoritative_for_flowise_path() -> None:
    contract = _contract()
    assert contract["source_runtime"]["stage_11_runtime_guard"] == (
        "src/moex_research/intelligence/usdrubf_flowise_decision_agent.py::"
        "stage11_shadow_decision_agent"
    )
    assert contract["input_contract"]["stage_11_runtime_trade_state_override"] == [
        "WAIT",
        "ENTER",
    ]


def test_transport_records_verified_applied_state() -> None:
    contract = _contract()
    transport = contract["flowise"]["transport"]
    assert transport["endpoint"] == (
        "https://flowise.foods-tech.store/api/v1/prediction/"
        "9ad1f42c-e7f1-44a3-a574-09a7da4ddecf"
    )
    assert transport["request_field"] == "question"
    assert transport["response_field"] == "text"
    assert transport["environment"] == {
        "endpoint": "MOEX_RUB_INTELLIGENCE_FLOWISE_ENDPOINT",
        "request_field": "MOEX_RUB_INTELLIGENCE_FLOWISE_REQUEST_FIELD",
        "response_field": "MOEX_RUB_INTELLIGENCE_FLOWISE_RESPONSE_FIELD",
        "timeout_seconds": "MOEX_RUB_INTELLIGENCE_FLOWISE_TIMEOUT_SECONDS",
    }


def test_prompt_is_exact_bounded_source_for_flowise() -> None:
    prompt = PROMPT_PATH.read_text(encoding="utf-8")
    persistent = prompt.split("---", 1)[1].lstrip()
    assert persistent.startswith("PROJECT=MOEX_Bot\n")
    assert "MODE=SHADOW_ONLY" in persistent
    assert "разрешены только `WAIT` и `ENTER`" in persistent
    assert "Не придумывай evidence refs." in persistent
    assert "Верни только один JSON object" in persistent
    assert "Числовую цену не выводить." in persistent
    assert "## CRITICAL OUTPUT ENFORCEMENT" in persistent
    assert "отсутствует reasoning" in persistent
    assert "## LANGUAGE ENFORCEMENT" in persistent
    assert "`scenario` и `reason` всегда должны быть написаны на русском языке" in persistent


def test_env_example_declares_verified_flowise_applied_state_endpoint() -> None:
    env_text = ENV_EXAMPLE_PATH.read_text(encoding="utf-8")
    assert (
        "MOEX_RUB_INTELLIGENCE_FLOWISE_ENDPOINT="
        "https://flowise.foods-tech.store/api/v1/prediction/"
        "9ad1f42c-e7f1-44a3-a574-09a7da4ddecf\n"
    ) in env_text
    assert "MOEX_RUB_INTELLIGENCE_FLOWISE_REQUEST_FIELD=question\n" in env_text
    assert "MOEX_RUB_INTELLIGENCE_FLOWISE_RESPONSE_FIELD=text\n" in env_text
    assert "MOEX_RUB_INTELLIGENCE_FLOWISE_TIMEOUT_SECONDS=20\n" in env_text
