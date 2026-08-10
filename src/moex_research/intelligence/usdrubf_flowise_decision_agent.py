from __future__ import annotations

from collections.abc import Callable, Mapping

from .usdrubf_decision_engine import DecisionEngineError


STAGE11_SHADOW_TRADE_STATES = ("WAIT", "ENTER")


def stage11_shadow_decision_agent(
    agent: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> Callable[[Mapping[str, object]], Mapping[str, object]]:
    """Wrap a Flowise decision agent with the Stage 11 shadow boundary.

    Stage 11 has no live position context. The wrapped agent therefore sees only
    WAIT/ENTER as allowed trade states, and any response outside that set is
    rejected before the generic Decision Engine can persist it.
    """

    def bounded(payload: Mapping[str, object]) -> Mapping[str, object]:
        if not isinstance(payload, Mapping):
            raise DecisionEngineError("Stage 11 decision payload must be a mapping")
        output_contract = payload.get("output_contract")
        if not isinstance(output_contract, Mapping):
            raise DecisionEngineError("Stage 11 decision payload missing output_contract")

        bounded_payload = dict(payload)
        bounded_output_contract = dict(output_contract)
        bounded_output_contract["trade_state_allowed"] = STAGE11_SHADOW_TRADE_STATES
        bounded_payload["output_contract"] = bounded_output_contract

        result = agent(bounded_payload)
        if not isinstance(result, Mapping):
            raise DecisionEngineError("Stage 11 decision agent output must be a mapping")
        if result.get("trade_state") not in STAGE11_SHADOW_TRADE_STATES:
            raise DecisionEngineError(
                "Stage 11 shadow trade_state must be WAIT or ENTER"
            )
        return dict(result)

    return bounded
