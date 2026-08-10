from __future__ import annotations

import argparse

import pytest

from src.moex_research.intelligence.usdrubf_decision_engine import DecisionEngineError
from src.moex_research.intelligence.usdrubf_flowise_decision_agent import (
    STAGE11_SHADOW_TRADE_STATES,
    stage11_shadow_decision_agent,
)
from src.moex_research.runners import usdrubf_live_shadow_smoke as runner


def test_stage11_guard_rewrites_advertised_states_and_rejects_position_state() -> None:
    seen = {}

    def agent(payload):
        seen["trade_state_allowed"] = payload["output_contract"]["trade_state_allowed"]
        return {"trade_state": "REDUCE"}

    bounded = stage11_shadow_decision_agent(agent)

    with pytest.raises(DecisionEngineError, match="WAIT or ENTER"):
        bounded(
            {
                "output_contract": {
                    "trade_state_allowed": (
                        "WAIT",
                        "ENTER",
                        "HOLD",
                        "ADD",
                        "REDUCE",
                        "EXIT",
                    )
                }
            }
        )

    assert tuple(seen["trade_state_allowed"]) == STAGE11_SHADOW_TRADE_STATES


def test_runner_wraps_flowise_adapter_with_stage11_guard(monkeypatch) -> None:
    class FakeAdapter:
        def __init__(self, config):
            self.config = config

        def __call__(self, payload):
            assert tuple(payload["output_contract"]["trade_state_allowed"]) == (
                "WAIT",
                "ENTER",
            )
            return {"trade_state": "EXIT"}

    monkeypatch.setattr(runner, "FlowiseJsonAdapter", FakeAdapter)
    args = argparse.Namespace(
        safe_wait_agent=False,
        flowise_endpoint="https://flowise.example.invalid/prediction/test",
        flowise_request_field="question",
        flowise_response_field="text",
        flowise_timeout_seconds=20.0,
    )

    decision_agent, mode = runner._decision_agent(args)

    assert mode == "FLOWISE"
    with pytest.raises(DecisionEngineError, match="WAIT or ENTER"):
        decision_agent({"output_contract": {"trade_state_allowed": ("WAIT", "EXIT")}})
