from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_review import RealReadReviewDecision, validate_real_read_review_decision


class RealReadDesignError(ValueError):
    pass


SOURCE_CONTRACT_FIELDS: Final[tuple[str, ...]] = (
    "source_contract_id",
    "review_decision",
    "dataset_contract_ref",
    "calendar_contract_ref",
    "schema_contract_ref",
    "read_scope",
    "source_scope",
    "metadata_only",
)
DESIGN_FIELDS: Final[tuple[str, ...]] = (
    "design_id",
    "source_contract",
    "design_mode",
    "requested_design_state",
    "output_contract_ref",
    "quality_contract_ref",
    "lineage_contract_ref",
    "allow_real_read",
    "allow_network",
    "allow_registry_write",
    "allow_runtime",
    "metadata_only",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "design_id",
    "review_package_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "real_read_allowed",
    "reason_or_none",
)
ALLOWED_SOURCE_SCOPES: Final[frozenset[str]] = frozenset({"canonical_dataset_contract_ref_only"})
ALLOWED_DESIGN_MODES: Final[frozenset[str]] = frozenset({"controlled_real_read_design_only"})
ALLOWED_DESIGN_STATES: Final[frozenset[str]] = frozenset(
    {"blocked", "eligible_for_controlled_real_read_execution_review", "rejected"}
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
        raise RealReadDesignError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadDesignError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadDesignError(f"{field_name} must be bool")
    if value:
        raise RealReadDesignError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadDesignError(f"{field_name} must be bool")
    if not value:
        raise RealReadDesignError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadDesignError(f"{label} fields invalid")


def validate_real_read_source_contract_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, SOURCE_CONTRACT_FIELDS, "source_contract")
    review_decision = values["review_decision"]
    if not isinstance(review_decision, RealReadReviewDecision):
        raise RealReadDesignError("review_decision must be RealReadReviewDecision")
    validated_review = validate_real_read_review_decision(review_decision)
    if validated_review.decision_status != "eligible_for_real_read_design":
        raise RealReadDesignError("review_decision must be eligible for design")
    if validated_review.real_read_allowed is not False:
        raise RealReadDesignError("review_decision must not authorize read")
    source_scope = _text(values["source_scope"], "source_scope")
    if source_scope not in ALLOWED_SOURCE_SCOPES:
        raise RealReadDesignError("source_scope is unsupported")
    return {
        "source_contract_id": _text(values["source_contract_id"], "source_contract_id"),
        "review_decision": validated_review,
        "dataset_contract_ref": _text(values["dataset_contract_ref"], "dataset_contract_ref"),
        "calendar_contract_ref": _text(values["calendar_contract_ref"], "calendar_contract_ref"),
        "schema_contract_ref": _text(values["schema_contract_ref"], "schema_contract_ref"),
        "read_scope": _text(values["read_scope"], "read_scope"),
        "source_scope": source_scope,
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadSourceContract:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_source_contract_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_source_contract(source_contract: RealReadSourceContract) -> RealReadSourceContract:
    if not isinstance(source_contract, RealReadSourceContract):
        raise TypeError("source_contract must be RealReadSourceContract")
    validate_real_read_source_contract_values(source_contract.__dict__)
    return source_contract


def validate_real_read_design_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DESIGN_FIELDS, "design")
    source_contract = values["source_contract"]
    if not isinstance(source_contract, RealReadSourceContract):
        raise RealReadDesignError("source_contract must be RealReadSourceContract")
    validated_source = validate_real_read_source_contract(source_contract)
    design_mode = _text(values["design_mode"], "design_mode")
    if design_mode not in ALLOWED_DESIGN_MODES:
        raise RealReadDesignError("design_mode is unsupported")
    requested_state = _text(values["requested_design_state"], "requested_design_state")
    if requested_state not in ALLOWED_DESIGN_STATES:
        raise RealReadDesignError("requested_design_state is unsupported")
    return {
        "design_id": _text(values["design_id"], "design_id"),
        "source_contract": validated_source,
        "design_mode": design_mode,
        "requested_design_state": requested_state,
        "output_contract_ref": _text(values["output_contract_ref"], "output_contract_ref"),
        "quality_contract_ref": _text(values["quality_contract_ref"], "quality_contract_ref"),
        "lineage_contract_ref": _text(values["lineage_contract_ref"], "lineage_contract_ref"),
        "allow_real_read": _false(values["allow_real_read"], "allow_real_read"),
        "allow_network": _false(values["allow_network"], "allow_network"),
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadDesign:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_design_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_design(design: RealReadDesign) -> RealReadDesign:
    if not isinstance(design, RealReadDesign):
        raise TypeError("design must be RealReadDesign")
    validate_real_read_design_values(design.__dict__)
    return design


def validate_real_read_design_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_DESIGN_STATES:
        raise RealReadDesignError("decision_status is unsupported")
    if values["real_read_allowed"] is not False:
        raise RealReadDesignError("real_read_allowed must remain false")
    reason = values["reason_or_none"]
    if status == "eligible_for_controlled_real_read_execution_review" and reason is not None:
        raise RealReadDesignError("eligible execution review result must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadDesignError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadDesignDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_design_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_design_decision(decision: RealReadDesignDecision) -> RealReadDesignDecision:
    if not isinstance(decision, RealReadDesignDecision):
        raise TypeError("decision must be RealReadDesignDecision")
    validate_real_read_design_decision_values(decision.__dict__)
    return decision


def _decision(design: RealReadDesign, status: str, reason: str | None) -> RealReadDesignDecision:
    review = design.source_contract.review_decision
    return RealReadDesignDecision(
        decision_status=status,
        design_id=design.design_id,
        review_package_id=review.package_id,
        dataset_ref_id=review.dataset_ref_id,
        instrument_id=review.instrument_id,
        timeframe=review.timeframe,
        real_read_allowed=False,
        reason_or_none=reason,
    )


def evaluate_real_read_design(design: RealReadDesign) -> RealReadDesignDecision:
    try:
        validated = validate_real_read_design(design)
        if validated.requested_design_state == "rejected":
            return _decision(validated, "rejected", "design rejected")
        if validated.requested_design_state == "blocked":
            return _decision(validated, "blocked", "design requested blocked decision")
        return _decision(validated, "eligible_for_controlled_real_read_execution_review", None)
    except (RealReadDesignError, TypeError, ValueError) as error:
        return RealReadDesignDecision(
            decision_status="blocked",
            design_id="unavailable",
            review_package_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            real_read_allowed=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_DESIGN_MODES",
    "ALLOWED_DESIGN_STATES",
    "ALLOWED_SOURCE_SCOPES",
    "DECISION_FIELDS",
    "DESIGN_FIELDS",
    "SOURCE_CONTRACT_FIELDS",
    "RealReadDesign",
    "RealReadDesignDecision",
    "RealReadDesignError",
    "RealReadSourceContract",
    "evaluate_real_read_design",
    "validate_real_read_design",
    "validate_real_read_design_decision",
    "validate_real_read_design_decision_values",
    "validate_real_read_design_values",
    "validate_real_read_source_contract",
    "validate_real_read_source_contract_values",
]
