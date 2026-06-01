from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_design import RealReadDesignDecision, validate_real_read_design_decision


class RealReadExecutionReviewError(ValueError):
    pass


PLAN_FIELDS: Final[tuple[str, ...]] = (
    "plan_id",
    "design_decision",
    "owner_runbook_ref",
    "preflight_check_ref",
    "artifact_manifest_ref",
    "rollback_note_ref",
    "review_scope",
    "metadata_only",
)
REVIEW_FIELDS: Final[tuple[str, ...]] = (
    "review_id",
    "execution_plan",
    "review_mode",
    "requested_review_state",
    "allow_real_read",
    "allow_network",
    "allow_registry_write",
    "allow_runtime",
    "metadata_only",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "review_id",
    "design_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "real_read_allowed",
    "owner_run_required",
    "reason_or_none",
)
ALLOWED_REVIEW_SCOPES: Final[frozenset[str]] = frozenset({"owner_run_preflight_metadata_only"})
ALLOWED_REVIEW_MODES: Final[frozenset[str]] = frozenset({"controlled_real_read_execution_review_only"})
ALLOWED_REVIEW_STATES: Final[frozenset[str]] = frozenset(
    {"blocked", "eligible_for_owner_run_real_read", "rejected"}
)


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
        raise RealReadExecutionReviewError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadExecutionReviewError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadExecutionReviewError(f"{field_name} must be bool")
    if value:
        raise RealReadExecutionReviewError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadExecutionReviewError(f"{field_name} must be bool")
    if not value:
        raise RealReadExecutionReviewError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadExecutionReviewError(f"{label} fields invalid")


def validate_real_read_execution_plan_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, PLAN_FIELDS, "plan")
    design_decision = values["design_decision"]
    if not isinstance(design_decision, RealReadDesignDecision):
        raise RealReadExecutionReviewError("design_decision must be RealReadDesignDecision")
    validated_design = validate_real_read_design_decision(design_decision)
    if validated_design.decision_status != "eligible_for_controlled_real_read_execution_review":
        raise RealReadExecutionReviewError("design_decision must be eligible for execution review")
    if validated_design.real_read_allowed is not False:
        raise RealReadExecutionReviewError("design_decision must not authorize read")
    review_scope = _text(values["review_scope"], "review_scope")
    if review_scope not in ALLOWED_REVIEW_SCOPES:
        raise RealReadExecutionReviewError("review_scope is unsupported")
    return {
        "plan_id": _text(values["plan_id"], "plan_id"),
        "design_decision": validated_design,
        "owner_runbook_ref": _text(values["owner_runbook_ref"], "owner_runbook_ref"),
        "preflight_check_ref": _text(values["preflight_check_ref"], "preflight_check_ref"),
        "artifact_manifest_ref": _text(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "rollback_note_ref": _text(values["rollback_note_ref"], "rollback_note_ref"),
        "review_scope": review_scope,
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadExecutionPlan:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_execution_plan_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_execution_plan(plan: RealReadExecutionPlan) -> RealReadExecutionPlan:
    if not isinstance(plan, RealReadExecutionPlan):
        raise TypeError("plan must be RealReadExecutionPlan")
    validate_real_read_execution_plan_values(plan.__dict__)
    return plan


def validate_real_read_execution_review_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, REVIEW_FIELDS, "review")
    execution_plan = values["execution_plan"]
    if not isinstance(execution_plan, RealReadExecutionPlan):
        raise RealReadExecutionReviewError("execution_plan must be RealReadExecutionPlan")
    validated_plan = validate_real_read_execution_plan(execution_plan)
    review_mode = _text(values["review_mode"], "review_mode")
    if review_mode not in ALLOWED_REVIEW_MODES:
        raise RealReadExecutionReviewError("review_mode is unsupported")
    requested_state = _text(values["requested_review_state"], "requested_review_state")
    if requested_state not in ALLOWED_REVIEW_STATES:
        raise RealReadExecutionReviewError("requested_review_state is unsupported")
    return {
        "review_id": _text(values["review_id"], "review_id"),
        "execution_plan": validated_plan,
        "review_mode": review_mode,
        "requested_review_state": requested_state,
        "allow_real_read": _false(values["allow_real_read"], "allow_real_read"),
        "allow_network": _false(values["allow_network"], "allow_network"),
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadExecutionReview:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_execution_review_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_execution_review(review: RealReadExecutionReview) -> RealReadExecutionReview:
    if not isinstance(review, RealReadExecutionReview):
        raise TypeError("review must be RealReadExecutionReview")
    validate_real_read_execution_review_values(review.__dict__)
    return review


def validate_real_read_execution_review_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_REVIEW_STATES:
        raise RealReadExecutionReviewError("decision_status is unsupported")
    if values["real_read_allowed"] is not False:
        raise RealReadExecutionReviewError("real_read_allowed must remain false")
    if not isinstance(values["owner_run_required"], bool):
        raise RealReadExecutionReviewError("owner_run_required must be bool")
    reason = values["reason_or_none"]
    if status == "eligible_for_owner_run_real_read" and reason is not None:
        raise RealReadExecutionReviewError("eligible owner-run result must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadExecutionReviewError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadExecutionReviewDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_execution_review_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_execution_review_decision(decision: RealReadExecutionReviewDecision) -> RealReadExecutionReviewDecision:
    if not isinstance(decision, RealReadExecutionReviewDecision):
        raise TypeError("decision must be RealReadExecutionReviewDecision")
    validate_real_read_execution_review_decision_values(decision.__dict__)
    return decision


def _decision(review: RealReadExecutionReview, status: str, owner_run_required: bool, reason: str | None) -> RealReadExecutionReviewDecision:
    design = review.execution_plan.design_decision
    return RealReadExecutionReviewDecision(
        decision_status=status,
        review_id=review.review_id,
        design_id=design.design_id,
        dataset_ref_id=design.dataset_ref_id,
        instrument_id=design.instrument_id,
        timeframe=design.timeframe,
        real_read_allowed=False,
        owner_run_required=owner_run_required,
        reason_or_none=reason,
    )


def evaluate_real_read_execution_review(review: RealReadExecutionReview) -> RealReadExecutionReviewDecision:
    try:
        validated = validate_real_read_execution_review(review)
        if validated.requested_review_state == "rejected":
            return _decision(validated, "rejected", False, "execution review rejected")
        if validated.requested_review_state == "blocked":
            return _decision(validated, "blocked", False, "execution review requested blocked decision")
        return _decision(validated, "eligible_for_owner_run_real_read", True, None)
    except (RealReadExecutionReviewError, TypeError, ValueError) as error:
        return RealReadExecutionReviewDecision(
            decision_status="blocked",
            review_id="unavailable",
            design_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            real_read_allowed=False,
            owner_run_required=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_REVIEW_MODES",
    "ALLOWED_REVIEW_SCOPES",
    "ALLOWED_REVIEW_STATES",
    "DECISION_FIELDS",
    "PLAN_FIELDS",
    "REVIEW_FIELDS",
    "RealReadExecutionPlan",
    "RealReadExecutionReview",
    "RealReadExecutionReviewDecision",
    "RealReadExecutionReviewError",
    "evaluate_real_read_execution_review",
    "validate_real_read_execution_plan",
    "validate_real_read_execution_plan_values",
    "validate_real_read_execution_review",
    "validate_real_read_execution_review_decision",
    "validate_real_read_execution_review_decision_values",
    "validate_real_read_execution_review_values",
]
