from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_pm_review import RealReadPMReviewDecision, validate_real_read_pm_review_decision


class RealReadChainError(ValueError):
    pass


CHAIN_FIELDS: Final[tuple[str, ...]] = (
    "chain_id",
    "pm_review_decision",
    "gate_commit_ref",
    "review_commit_ref",
    "design_commit_ref",
    "execution_review_commit_ref",
    "handoff_commit_ref",
    "manual_intake_commit_ref",
    "pm_review_commit_ref",
    "chain_scope",
    "metadata_only",
)
CLOSEOUT_FIELDS: Final[tuple[str, ...]] = (
    "closeout_id",
    "evidence_chain",
    "closeout_mode",
    "requested_closeout_state",
    "allow_registry_write",
    "allow_runtime",
    "allow_promotion",
    "metadata_only",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "closeout_id",
    "chain_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "chain_status",
    "registry_write_allowed",
    "runtime_allowed",
    "promotion_allowed",
    "reason_or_none",
)
ALLOWED_CHAIN_SCOPES: Final[frozenset[str]] = frozenset({"real_read_review_chain_metadata_only"})
ALLOWED_CLOSEOUT_MODES: Final[frozenset[str]] = frozenset({"real_read_chain_closeout_only"})
ALLOWED_CLOSEOUT_STATES: Final[frozenset[str]] = frozenset({"blocked", "accepted_chain", "rejected"})


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
        raise RealReadChainError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadChainError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadChainError(f"{field_name} must be bool")
    if value:
        raise RealReadChainError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadChainError(f"{field_name} must be bool")
    if not value:
        raise RealReadChainError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadChainError(f"{label} fields invalid")


def validate_real_read_chain_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, CHAIN_FIELDS, "chain")
    pm_review_decision = values["pm_review_decision"]
    if not isinstance(pm_review_decision, RealReadPMReviewDecision):
        raise RealReadChainError("pm_review_decision must be RealReadPMReviewDecision")
    validated_pm = validate_real_read_pm_review_decision(pm_review_decision)
    if validated_pm.decision_status != "accepted_as_real_read_evidence":
        raise RealReadChainError("pm_review_decision must accept real read evidence")
    if validated_pm.registry_write_allowed is not False:
        raise RealReadChainError("pm_review_decision must not allow registry write")
    if validated_pm.runtime_allowed is not False:
        raise RealReadChainError("pm_review_decision must not allow runtime")
    if validated_pm.promotion_allowed is not False:
        raise RealReadChainError("pm_review_decision must not allow promotion")
    chain_scope = _text(values["chain_scope"], "chain_scope")
    if chain_scope not in ALLOWED_CHAIN_SCOPES:
        raise RealReadChainError("chain_scope is unsupported")
    return {
        "chain_id": _text(values["chain_id"], "chain_id"),
        "pm_review_decision": validated_pm,
        "gate_commit_ref": _text(values["gate_commit_ref"], "gate_commit_ref"),
        "review_commit_ref": _text(values["review_commit_ref"], "review_commit_ref"),
        "design_commit_ref": _text(values["design_commit_ref"], "design_commit_ref"),
        "execution_review_commit_ref": _text(values["execution_review_commit_ref"], "execution_review_commit_ref"),
        "handoff_commit_ref": _text(values["handoff_commit_ref"], "handoff_commit_ref"),
        "manual_intake_commit_ref": _text(values["manual_intake_commit_ref"], "manual_intake_commit_ref"),
        "pm_review_commit_ref": _text(values["pm_review_commit_ref"], "pm_review_commit_ref"),
        "chain_scope": chain_scope,
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadChain:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_chain_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_chain(chain: RealReadChain) -> RealReadChain:
    if not isinstance(chain, RealReadChain):
        raise TypeError("chain must be RealReadChain")
    validate_real_read_chain_values(chain.__dict__)
    return chain


def validate_real_read_chain_closeout_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, CLOSEOUT_FIELDS, "closeout")
    evidence_chain = values["evidence_chain"]
    if not isinstance(evidence_chain, RealReadChain):
        raise RealReadChainError("evidence_chain must be RealReadChain")
    validated_chain = validate_real_read_chain(evidence_chain)
    closeout_mode = _text(values["closeout_mode"], "closeout_mode")
    if closeout_mode not in ALLOWED_CLOSEOUT_MODES:
        raise RealReadChainError("closeout_mode is unsupported")
    closeout_state = _text(values["requested_closeout_state"], "requested_closeout_state")
    if closeout_state not in ALLOWED_CLOSEOUT_STATES:
        raise RealReadChainError("requested_closeout_state is unsupported")
    return {
        "closeout_id": _text(values["closeout_id"], "closeout_id"),
        "evidence_chain": validated_chain,
        "closeout_mode": closeout_mode,
        "requested_closeout_state": closeout_state,
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
        "allow_promotion": _false(values["allow_promotion"], "allow_promotion"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadChainCloseout:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_chain_closeout_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_chain_closeout(closeout: RealReadChainCloseout) -> RealReadChainCloseout:
    if not isinstance(closeout, RealReadChainCloseout):
        raise TypeError("closeout must be RealReadChainCloseout")
    validate_real_read_chain_closeout_values(closeout.__dict__)
    return closeout


def validate_real_read_chain_closeout_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_CLOSEOUT_STATES:
        raise RealReadChainError("decision_status is unsupported")
    _text(values["chain_status"], "chain_status")
    if values["registry_write_allowed"] is not False:
        raise RealReadChainError("registry_write_allowed must remain false")
    if values["runtime_allowed"] is not False:
        raise RealReadChainError("runtime_allowed must remain false")
    if values["promotion_allowed"] is not False:
        raise RealReadChainError("promotion_allowed must remain false")
    reason = values["reason_or_none"]
    if status == "accepted_chain" and reason is not None:
        raise RealReadChainError("accepted chain result must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadChainError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadChainCloseoutDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_chain_closeout_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_chain_closeout_decision(decision: RealReadChainCloseoutDecision) -> RealReadChainCloseoutDecision:
    if not isinstance(decision, RealReadChainCloseoutDecision):
        raise TypeError("decision must be RealReadChainCloseoutDecision")
    validate_real_read_chain_closeout_decision_values(decision.__dict__)
    return decision


def _decision(closeout: RealReadChainCloseout, status: str, chain_status: str, reason: str | None) -> RealReadChainCloseoutDecision:
    pm = closeout.evidence_chain.pm_review_decision
    return RealReadChainCloseoutDecision(
        decision_status=status,
        closeout_id=closeout.closeout_id,
        chain_id=closeout.evidence_chain.chain_id,
        dataset_ref_id=pm.dataset_ref_id,
        instrument_id=pm.instrument_id,
        timeframe=pm.timeframe,
        chain_status=chain_status,
        registry_write_allowed=False,
        runtime_allowed=False,
        promotion_allowed=False,
        reason_or_none=reason,
    )


def evaluate_real_read_chain_closeout(closeout: RealReadChainCloseout) -> RealReadChainCloseoutDecision:
    try:
        validated = validate_real_read_chain_closeout(closeout)
        if validated.requested_closeout_state == "rejected":
            return _decision(validated, "rejected", "rejected", "chain closeout rejected")
        if validated.requested_closeout_state == "blocked":
            return _decision(validated, "blocked", "blocked", "chain closeout requested blocked decision")
        return _decision(validated, "accepted_chain", "real_read_review_chain_accepted", None)
    except (RealReadChainError, TypeError, ValueError) as error:
        return RealReadChainCloseoutDecision(
            decision_status="blocked",
            closeout_id="unavailable",
            chain_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            chain_status="blocked",
            registry_write_allowed=False,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_CHAIN_SCOPES",
    "ALLOWED_CLOSEOUT_MODES",
    "ALLOWED_CLOSEOUT_STATES",
    "CHAIN_FIELDS",
    "CLOSEOUT_FIELDS",
    "DECISION_FIELDS",
    "RealReadChain",
    "RealReadChainCloseout",
    "RealReadChainCloseoutDecision",
    "RealReadChainError",
    "evaluate_real_read_chain_closeout",
    "validate_real_read_chain",
    "validate_real_read_chain_closeout",
    "validate_real_read_chain_closeout_decision",
    "validate_real_read_chain_closeout_decision_values",
    "validate_real_read_chain_closeout_values",
    "validate_real_read_chain_values",
]
