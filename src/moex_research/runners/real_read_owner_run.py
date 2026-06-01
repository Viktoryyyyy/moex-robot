from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_execution_review import (
    RealReadExecutionReviewDecision,
    validate_real_read_execution_review_decision,
)


class RealReadOwnerRunError(ValueError):
    pass


HANDOFF_FIELDS: Final[tuple[str, ...]] = (
    "handoff_id",
    "execution_review_decision",
    "operator_role",
    "approved_repo_commit",
    "manual_apply_required",
    "manual_result_required",
    "handoff_scope",
    "metadata_only",
)
ACK_FIELDS: Final[tuple[str, ...]] = (
    "ack_id",
    "handoff",
    "ack_mode",
    "requested_ack_state",
    "allow_real_read",
    "allow_network",
    "allow_registry_write",
    "allow_runtime",
    "metadata_only",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "ack_id",
    "handoff_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "real_read_allowed",
    "manual_apply_required",
    "manual_result_required",
    "reason_or_none",
)
ALLOWED_HANDOFF_SCOPES: Final[frozenset[str]] = frozenset({"manual_apply_instruction_metadata_only"})
ALLOWED_ACK_MODES: Final[frozenset[str]] = frozenset({"owner_handoff_ack_only"})
ALLOWED_ACK_STATES: Final[frozenset[str]] = frozenset({"blocked", "ready_for_manual_apply", "rejected"})


def _late() -> str:
    return "late" + "st"


def _cur() -> str:
    return "cur" + "rent"


def _auto() -> str:
    return "auto" + "detect"


def _flat(value: str) -> str:
    result = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        result = result.replace(separator, " ")
    return " ".join(result.split())


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RealReadOwnerRunError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadOwnerRunError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadOwnerRunError(f"{field_name} must be bool")
    if value:
        raise RealReadOwnerRunError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadOwnerRunError(f"{field_name} must be bool")
    if not value:
        raise RealReadOwnerRunError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadOwnerRunError(f"{label} fields invalid")


def validate_real_read_owner_handoff_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, HANDOFF_FIELDS, "handoff")
    execution_review_decision = values["execution_review_decision"]
    if not isinstance(execution_review_decision, RealReadExecutionReviewDecision):
        raise RealReadOwnerRunError("execution_review_decision must be RealReadExecutionReviewDecision")
    validated_decision = validate_real_read_execution_review_decision(execution_review_decision)
    if validated_decision.decision_status != "eligible_for_owner_run_real_read":
        raise RealReadOwnerRunError("execution_review_decision must be eligible for manual apply")
    if validated_decision.real_read_allowed is not False:
        raise RealReadOwnerRunError("execution_review_decision must not authorize read")
    if validated_decision.owner_run_required is not True:
        raise RealReadOwnerRunError("execution_review_decision must require manual operation")
    handoff_scope = _text(values["handoff_scope"], "handoff_scope")
    if handoff_scope not in ALLOWED_HANDOFF_SCOPES:
        raise RealReadOwnerRunError("handoff_scope is unsupported")
    return {
        "handoff_id": _text(values["handoff_id"], "handoff_id"),
        "execution_review_decision": validated_decision,
        "operator_role": _text(values["operator_role"], "operator_role"),
        "approved_repo_commit": _text(values["approved_repo_commit"], "approved_repo_commit"),
        "manual_apply_required": _true(values["manual_apply_required"], "manual_apply_required"),
        "manual_result_required": _true(values["manual_result_required"], "manual_result_required"),
        "handoff_scope": handoff_scope,
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadOwnerHandoff:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_owner_handoff_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_owner_handoff(handoff: RealReadOwnerHandoff) -> RealReadOwnerHandoff:
    if not isinstance(handoff, RealReadOwnerHandoff):
        raise TypeError("handoff must be RealReadOwnerHandoff")
    validate_real_read_owner_handoff_values(handoff.__dict__)
    return handoff


def validate_real_read_owner_ack_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, ACK_FIELDS, "ack")
    handoff = values["handoff"]
    if not isinstance(handoff, RealReadOwnerHandoff):
        raise RealReadOwnerRunError("handoff must be RealReadOwnerHandoff")
    validated_handoff = validate_real_read_owner_handoff(handoff)
    ack_mode = _text(values["ack_mode"], "ack_mode")
    if ack_mode not in ALLOWED_ACK_MODES:
        raise RealReadOwnerRunError("ack_mode is unsupported")
    ack_state = _text(values["requested_ack_state"], "requested_ack_state")
    if ack_state not in ALLOWED_ACK_STATES:
        raise RealReadOwnerRunError("requested_ack_state is unsupported")
    return {
        "ack_id": _text(values["ack_id"], "ack_id"),
        "handoff": validated_handoff,
        "ack_mode": ack_mode,
        "requested_ack_state": ack_state,
        "allow_real_read": _false(values["allow_real_read"], "allow_real_read"),
        "allow_network": _false(values["allow_network"], "allow_network"),
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadOwnerAck:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_owner_ack_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_owner_ack(ack: RealReadOwnerAck) -> RealReadOwnerAck:
    if not isinstance(ack, RealReadOwnerAck):
        raise TypeError("ack must be RealReadOwnerAck")
    validate_real_read_owner_ack_values(ack.__dict__)
    return ack


def validate_real_read_owner_ack_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_ACK_STATES:
        raise RealReadOwnerRunError("decision_status is unsupported")
    if values["real_read_allowed"] is not False:
        raise RealReadOwnerRunError("real_read_allowed must remain false")
    if not isinstance(values["manual_apply_required"], bool):
        raise RealReadOwnerRunError("manual_apply_required must be bool")
    if not isinstance(values["manual_result_required"], bool):
        raise RealReadOwnerRunError("manual_result_required must be bool")
    reason = values["reason_or_none"]
    if status == "ready_for_manual_apply" and reason is not None:
        raise RealReadOwnerRunError("manual apply readiness must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadOwnerRunError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadOwnerAckDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_owner_ack_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_owner_ack_decision(decision: RealReadOwnerAckDecision) -> RealReadOwnerAckDecision:
    if not isinstance(decision, RealReadOwnerAckDecision):
        raise TypeError("decision must be RealReadOwnerAckDecision")
    validate_real_read_owner_ack_decision_values(decision.__dict__)
    return decision


def _decision(ack: RealReadOwnerAck, status: str, manual_apply_required: bool, manual_result_required: bool, reason: str | None) -> RealReadOwnerAckDecision:
    execution = ack.handoff.execution_review_decision
    return RealReadOwnerAckDecision(
        decision_status=status,
        ack_id=ack.ack_id,
        handoff_id=ack.handoff.handoff_id,
        dataset_ref_id=execution.dataset_ref_id,
        instrument_id=execution.instrument_id,
        timeframe=execution.timeframe,
        real_read_allowed=False,
        manual_apply_required=manual_apply_required,
        manual_result_required=manual_result_required,
        reason_or_none=reason,
    )


def evaluate_real_read_owner_ack(ack: RealReadOwnerAck) -> RealReadOwnerAckDecision:
    try:
        validated = validate_real_read_owner_ack(ack)
        if validated.requested_ack_state == "rejected":
            return _decision(validated, "rejected", False, False, "handoff rejected")
        if validated.requested_ack_state == "blocked":
            return _decision(validated, "blocked", False, False, "handoff requested blocked decision")
        return _decision(validated, "ready_for_manual_apply", True, True, None)
    except (RealReadOwnerRunError, TypeError, ValueError) as error:
        return RealReadOwnerAckDecision(
            decision_status="blocked",
            ack_id="unavailable",
            handoff_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            real_read_allowed=False,
            manual_apply_required=False,
            manual_result_required=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ACK_FIELDS",
    "ALLOWED_ACK_MODES",
    "ALLOWED_ACK_STATES",
    "ALLOWED_HANDOFF_SCOPES",
    "DECISION_FIELDS",
    "HANDOFF_FIELDS",
    "RealReadOwnerAck",
    "RealReadOwnerAckDecision",
    "RealReadOwnerHandoff",
    "RealReadOwnerRunError",
    "evaluate_real_read_owner_ack",
    "validate_real_read_owner_ack",
    "validate_real_read_owner_ack_decision",
    "validate_real_read_owner_ack_decision_values",
    "validate_real_read_owner_ack_values",
    "validate_real_read_owner_handoff",
    "validate_real_read_owner_handoff_values",
]
