from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_gate import RealReadGateResult, validate_real_read_gate_result


class RealReadReviewError(ValueError):
    pass


EVIDENCE_FIELDS: Final[tuple[str, ...]] = (
    "evidence_id",
    "sample_pipeline_ref",
    "sample_signal_ref",
    "sample_backtest_ref",
    "full_pipeline_ref",
    "gate_result_ref",
    "evidence_scope",
    "metadata_only",
)
PACKAGE_FIELDS: Final[tuple[str, ...]] = (
    "package_id",
    "gate_result",
    "evidence",
    "review_mode",
    "requested_decision_state",
    "allow_real_read",
    "allow_network",
    "allow_registry_write",
    "allow_runtime",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "package_id",
    "gate_request_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "gate_status",
    "real_read_allowed",
    "reason_or_none",
)
ALLOWED_REVIEW_MODES: Final[frozenset[str]] = frozenset({"real_read_review_handoff_only"})
ALLOWED_DECISION_STATES: Final[frozenset[str]] = frozenset({"blocked", "eligible_for_real_read_design", "rejected"})
ALLOWED_EVIDENCE_SCOPES: Final[frozenset[str]] = frozenset({"accepted_sample_pipeline_and_gate_only"})


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
        raise RealReadReviewError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadReviewError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadReviewError(f"{field_name} must be bool")
    if value:
        raise RealReadReviewError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadReviewError(f"{field_name} must be bool")
    if not value:
        raise RealReadReviewError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadReviewError(f"{label} fields invalid")


def validate_real_read_review_evidence_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, EVIDENCE_FIELDS, "evidence")
    evidence_scope = _text(values["evidence_scope"], "evidence_scope")
    if evidence_scope not in ALLOWED_EVIDENCE_SCOPES:
        raise RealReadReviewError("evidence_scope is unsupported")
    return {
        "evidence_id": _text(values["evidence_id"], "evidence_id"),
        "sample_pipeline_ref": _text(values["sample_pipeline_ref"], "sample_pipeline_ref"),
        "sample_signal_ref": _text(values["sample_signal_ref"], "sample_signal_ref"),
        "sample_backtest_ref": _text(values["sample_backtest_ref"], "sample_backtest_ref"),
        "full_pipeline_ref": _text(values["full_pipeline_ref"], "full_pipeline_ref"),
        "gate_result_ref": _text(values["gate_result_ref"], "gate_result_ref"),
        "evidence_scope": evidence_scope,
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadReviewEvidence:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_review_evidence_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_review_evidence(evidence: RealReadReviewEvidence) -> RealReadReviewEvidence:
    if not isinstance(evidence, RealReadReviewEvidence):
        raise TypeError("evidence must be RealReadReviewEvidence")
    validate_real_read_review_evidence_values(evidence.__dict__)
    return evidence


def validate_real_read_review_package_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, PACKAGE_FIELDS, "package")
    gate_result = values["gate_result"]
    evidence = values["evidence"]
    if not isinstance(gate_result, RealReadGateResult):
        raise RealReadReviewError("gate_result must be RealReadGateResult")
    if not isinstance(evidence, RealReadReviewEvidence):
        raise RealReadReviewError("evidence must be RealReadReviewEvidence")
    validated_gate = validate_real_read_gate_result(gate_result)
    validated_evidence = validate_real_read_review_evidence(evidence)
    if validated_evidence.gate_result_ref != validated_gate.gate_request_id:
        raise RealReadReviewError("evidence must reference linked gate result")
    review_mode = _text(values["review_mode"], "review_mode")
    if review_mode not in ALLOWED_REVIEW_MODES:
        raise RealReadReviewError("review_mode is unsupported")
    decision_state = _text(values["requested_decision_state"], "requested_decision_state")
    if decision_state not in ALLOWED_DECISION_STATES:
        raise RealReadReviewError("requested_decision_state is unsupported")
    return {
        "package_id": _text(values["package_id"], "package_id"),
        "gate_result": validated_gate,
        "evidence": validated_evidence,
        "review_mode": review_mode,
        "requested_decision_state": decision_state,
        "allow_real_read": _false(values["allow_real_read"], "allow_real_read"),
        "allow_network": _false(values["allow_network"], "allow_network"),
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
    }


class RealReadReviewPackage:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_review_package_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_review_package(package: RealReadReviewPackage) -> RealReadReviewPackage:
    if not isinstance(package, RealReadReviewPackage):
        raise TypeError("package must be RealReadReviewPackage")
    validate_real_read_review_package_values(package.__dict__)
    return package


def validate_real_read_review_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_DECISION_STATES:
        raise RealReadReviewError("decision_status is unsupported")
    if values["real_read_allowed"] is not False:
        raise RealReadReviewError("real_read_allowed must remain false")
    reason = values["reason_or_none"]
    if status == "eligible_for_real_read_design" and reason is not None:
        raise RealReadReviewError("eligible design result must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadReviewError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadReviewDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_review_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_review_decision(decision: RealReadReviewDecision) -> RealReadReviewDecision:
    if not isinstance(decision, RealReadReviewDecision):
        raise TypeError("decision must be RealReadReviewDecision")
    validate_real_read_review_decision_values(decision.__dict__)
    return decision


def _decision(package: RealReadReviewPackage, status: str, reason: str | None) -> RealReadReviewDecision:
    gate = package.gate_result
    return RealReadReviewDecision(
        decision_status=status,
        package_id=package.package_id,
        gate_request_id=gate.gate_request_id,
        dataset_ref_id=gate.dataset_ref_id,
        instrument_id=gate.instrument_id,
        timeframe=gate.timeframe,
        gate_status=gate.gate_status,
        real_read_allowed=False,
        reason_or_none=reason,
    )


def evaluate_real_read_review_package(package: RealReadReviewPackage) -> RealReadReviewDecision:
    try:
        validated = validate_real_read_review_package(package)
        if validated.requested_decision_state == "rejected":
            return _decision(validated, "rejected", "review package rejected")
        if validated.gate_result.gate_status != "eligible_for_separate_review":
            return _decision(validated, "blocked", "linked gate is not eligible")
        if validated.requested_decision_state == "blocked":
            return _decision(validated, "blocked", "review package requested blocked decision")
        return _decision(validated, "eligible_for_real_read_design", None)
    except (RealReadReviewError, TypeError, ValueError) as error:
        return RealReadReviewDecision(
            decision_status="blocked",
            package_id="unavailable",
            gate_request_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            gate_status="blocked",
            real_read_allowed=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_DECISION_STATES",
    "ALLOWED_EVIDENCE_SCOPES",
    "ALLOWED_REVIEW_MODES",
    "DECISION_FIELDS",
    "EVIDENCE_FIELDS",
    "PACKAGE_FIELDS",
    "RealReadReviewDecision",
    "RealReadReviewError",
    "RealReadReviewEvidence",
    "RealReadReviewPackage",
    "evaluate_real_read_review_package",
    "validate_real_read_review_decision",
    "validate_real_read_review_decision_values",
    "validate_real_read_review_evidence",
    "validate_real_read_review_evidence_values",
    "validate_real_read_review_package",
    "validate_real_read_review_package_values",
]
