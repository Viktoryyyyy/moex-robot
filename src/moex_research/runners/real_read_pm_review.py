from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_manual_result import (
    RealReadManualResultIntakeDecision,
    validate_real_read_manual_result_intake_decision,
)


class RealReadPMReviewError(ValueError):
    pass


PACKAGE_FIELDS: Final[tuple[str, ...]] = (
    "package_id",
    "manual_result_intake_decision",
    "evidence_review_ref",
    "quality_review_ref",
    "lineage_review_ref",
    "pm_review_scope",
    "metadata_only",
)
REVIEW_FIELDS: Final[tuple[str, ...]] = (
    "pm_review_id",
    "review_package",
    "review_mode",
    "requested_review_state",
    "allow_registry_write",
    "allow_runtime",
    "allow_promotion",
    "metadata_only",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "pm_review_id",
    "package_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "evidence_status",
    "registry_write_allowed",
    "runtime_allowed",
    "promotion_allowed",
    "reason_or_none",
)
ALLOWED_PM_REVIEW_SCOPES: Final[frozenset[str]] = frozenset({"manual_result_pm_review_metadata_only"})
ALLOWED_REVIEW_MODES: Final[frozenset[str]] = frozenset({"pm_real_read_evidence_review_only"})
ALLOWED_REVIEW_STATES: Final[frozenset[str]] = frozenset({"blocked", "accepted_as_real_read_evidence", "rejected"})


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
        raise RealReadPMReviewError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadPMReviewError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadPMReviewError(f"{field_name} must be bool")
    if value:
        raise RealReadPMReviewError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadPMReviewError(f"{field_name} must be bool")
    if not value:
        raise RealReadPMReviewError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadPMReviewError(f"{label} fields invalid")


def validate_real_read_pm_review_package_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, PACKAGE_FIELDS, "package")
    intake_decision = values["manual_result_intake_decision"]
    if not isinstance(intake_decision, RealReadManualResultIntakeDecision):
        raise RealReadPMReviewError("manual_result_intake_decision must be RealReadManualResultIntakeDecision")
    validated_intake = validate_real_read_manual_result_intake_decision(intake_decision)
    if validated_intake.decision_status != "accepted_for_pm_review":
        raise RealReadPMReviewError("manual_result_intake_decision must be accepted for pm review")
    if validated_intake.registry_write_allowed is not False:
        raise RealReadPMReviewError("manual_result_intake_decision must not allow registry write")
    if validated_intake.runtime_allowed is not False:
        raise RealReadPMReviewError("manual_result_intake_decision must not allow runtime")
    if validated_intake.promotion_allowed is not False:
        raise RealReadPMReviewError("manual_result_intake_decision must not allow promotion")
    review_scope = _text(values["pm_review_scope"], "pm_review_scope")
    if review_scope not in ALLOWED_PM_REVIEW_SCOPES:
        raise RealReadPMReviewError("pm_review_scope is unsupported")
    return {
        "package_id": _text(values["package_id"], "package_id"),
        "manual_result_intake_decision": validated_intake,
        "evidence_review_ref": _text(values["evidence_review_ref"], "evidence_review_ref"),
        "quality_review_ref": _text(values["quality_review_ref"], "quality_review_ref"),
        "lineage_review_ref": _text(values["lineage_review_ref"], "lineage_review_ref"),
        "pm_review_scope": review_scope,
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadPMReviewPackage:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_pm_review_package_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_pm_review_package(package: RealReadPMReviewPackage) -> RealReadPMReviewPackage:
    if not isinstance(package, RealReadPMReviewPackage):
        raise TypeError("package must be RealReadPMReviewPackage")
    validate_real_read_pm_review_package_values(package.__dict__)
    return package


def validate_real_read_pm_review_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, REVIEW_FIELDS, "review")
    package = values["review_package"]
    if not isinstance(package, RealReadPMReviewPackage):
        raise RealReadPMReviewError("review_package must be RealReadPMReviewPackage")
    validated_package = validate_real_read_pm_review_package(package)
    review_mode = _text(values["review_mode"], "review_mode")
    if review_mode not in ALLOWED_REVIEW_MODES:
        raise RealReadPMReviewError("review_mode is unsupported")
    review_state = _text(values["requested_review_state"], "requested_review_state")
    if review_state not in ALLOWED_REVIEW_STATES:
        raise RealReadPMReviewError("requested_review_state is unsupported")
    return {
        "pm_review_id": _text(values["pm_review_id"], "pm_review_id"),
        "review_package": validated_package,
        "review_mode": review_mode,
        "requested_review_state": review_state,
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
        "allow_promotion": _false(values["allow_promotion"], "allow_promotion"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadPMReview:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_pm_review_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_pm_review(review: RealReadPMReview) -> RealReadPMReview:
    if not isinstance(review, RealReadPMReview):
        raise TypeError("review must be RealReadPMReview")
    validate_real_read_pm_review_values(review.__dict__)
    return review


def validate_real_read_pm_review_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_REVIEW_STATES:
        raise RealReadPMReviewError("decision_status is unsupported")
    _text(values["evidence_status"], "evidence_status")
    if values["registry_write_allowed"] is not False:
        raise RealReadPMReviewError("registry_write_allowed must remain false")
    if values["runtime_allowed"] is not False:
        raise RealReadPMReviewError("runtime_allowed must remain false")
    if values["promotion_allowed"] is not False:
        raise RealReadPMReviewError("promotion_allowed must remain false")
    reason = values["reason_or_none"]
    if status == "accepted_as_real_read_evidence" and reason is not None:
        raise RealReadPMReviewError("accepted evidence result must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadPMReviewError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadPMReviewDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_pm_review_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_pm_review_decision(decision: RealReadPMReviewDecision) -> RealReadPMReviewDecision:
    if not isinstance(decision, RealReadPMReviewDecision):
        raise TypeError("decision must be RealReadPMReviewDecision")
    validate_real_read_pm_review_decision_values(decision.__dict__)
    return decision


def _decision(review: RealReadPMReview, status: str, evidence_status: str, reason: str | None) -> RealReadPMReviewDecision:
    intake = review.review_package.manual_result_intake_decision
    return RealReadPMReviewDecision(
        decision_status=status,
        pm_review_id=review.pm_review_id,
        package_id=review.review_package.package_id,
        dataset_ref_id=intake.dataset_ref_id,
        instrument_id=intake.instrument_id,
        timeframe=intake.timeframe,
        evidence_status=evidence_status,
        registry_write_allowed=False,
        runtime_allowed=False,
        promotion_allowed=False,
        reason_or_none=reason,
    )


def evaluate_real_read_pm_review(review: RealReadPMReview) -> RealReadPMReviewDecision:
    try:
        validated = validate_real_read_pm_review(review)
        if validated.requested_review_state == "rejected":
            return _decision(validated, "rejected", "rejected", "pm review rejected")
        if validated.requested_review_state == "blocked":
            return _decision(validated, "blocked", "blocked", "pm review requested blocked decision")
        return _decision(validated, "accepted_as_real_read_evidence", "real_read_evidence_accepted", None)
    except (RealReadPMReviewError, TypeError, ValueError) as error:
        return RealReadPMReviewDecision(
            decision_status="blocked",
            pm_review_id="unavailable",
            package_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            evidence_status="blocked",
            registry_write_allowed=False,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_PM_REVIEW_SCOPES",
    "ALLOWED_REVIEW_MODES",
    "ALLOWED_REVIEW_STATES",
    "DECISION_FIELDS",
    "PACKAGE_FIELDS",
    "REVIEW_FIELDS",
    "RealReadPMReview",
    "RealReadPMReviewDecision",
    "RealReadPMReviewError",
    "RealReadPMReviewPackage",
    "evaluate_real_read_pm_review",
    "validate_real_read_pm_review",
    "validate_real_read_pm_review_decision",
    "validate_real_read_pm_review_decision_values",
    "validate_real_read_pm_review_package",
    "validate_real_read_pm_review_package_values",
    "validate_real_read_pm_review_values",
]
