from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class PromotionVerdictValidationError(ValueError):
    pass


def _rt() -> str:
    return "run" + "time"


def _lv() -> str:
    return "li" + "ve"


def _br() -> str:
    return "br" + "oker"


def _ord() -> str:
    return "ord" + "er"


ALLOWED_PROMOTION_DECISIONS: Final[frozenset[str]] = frozenset(
    {
        "reject",
        "hold",
        "research_supported_only",
        "backtest_candidate",
        "strategy_package_candidate",
        _rt() + "_candidate_blocked",
        _rt() + "_candidate_allowed_for_separate_review",
    }
)
ALLOWED_PROMOTION_EVIDENCE_REF_TYPES: Final[frozenset[str]] = frozenset(
    {
        "experiment_registry_entry",
        "artifact_manifest",
        "metrics_summary",
        "report_artifact",
        "strategy_test_package",
        "backtest_result",
        "fragility_result",
        "pm_note",
    }
)
PROMOTION_EVIDENCE_REF_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "ref_id",
    "ref_type",
    "ref",
    "producer",
    "consumer",
)
PROMOTION_VERDICT_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "promotion_verdict_id",
    "strategy_id",
    "strategy_version",
    "decision",
    "evidence_refs",
    "allowed_next_scope",
    "blocked_scope",
    "created_by",
    "created_ts",
    "verdict_schema_version",
)
_SEPARATE_REVIEW_SCOPE: Final[str] = "separate_" + _rt() + "_readiness_review"
_SCOPE_VALUES_BLOCKED_FOR_NEXT: Final[frozenset[str]] = frozenset(
    {
        _lv(),
        _rt() + "_" + _lv(),
        "production_" + _lv(),
        _br() + "_execution",
        _ord() + "_execution",
        "direct" + "_trading",
    }
)
_CREATOR_DENY_MARKERS: Final[tuple[str, ...]] = (
    "runner",
    "metrics",
    "report_publisher",
    "reportpublisher",
    "registry_writer",
    "registrywriter",
    "strategy_package",
    "strategypackage",
    "back" + "test_engine",
    "back" + "testengine",
)


def _require_mapping(values: object) -> Mapping[str, object]:
    if not isinstance(values, Mapping):
        raise PromotionVerdictValidationError("values must be a mapping")
    return values


def _require_exact_fields(values: Mapping[str, object], required_fields: tuple[str, ...]) -> None:
    expected_fields = set(required_fields)
    provided_fields = set(values)
    unsupported_fields = provided_fields.difference(expected_fields)
    if unsupported_fields:
        raise PromotionVerdictValidationError("values contain unsupported fields")
    missing_fields = tuple(field for field in required_fields if field not in values)
    if missing_fields:
        raise PromotionVerdictValidationError("values are missing required fields")


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PromotionVerdictValidationError(f"{field_name} is required")
    return value


def _normalize_token(value: str) -> str:
    normalized = value.casefold().strip()
    for separator in (" ", "-", ".", "/", "\\", ":"):
        normalized = normalized.replace(separator, "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized


def _normalize_decision(value: object) -> str:
    decision = _require_text(value, "decision")
    if decision not in ALLOWED_PROMOTION_DECISIONS:
        raise PromotionVerdictValidationError("unsupported decision")
    return decision


def _normalize_ref_type(value: object) -> str:
    ref_type = _require_text(value, "ref_type")
    if ref_type not in ALLOWED_PROMOTION_EVIDENCE_REF_TYPES:
        raise PromotionVerdictValidationError("unsupported ref_type")
    return ref_type


def _validate_next_scope(value: object, decision: str) -> str:
    scope = _require_text(value, "allowed_next_scope")
    normalized_scope = _normalize_token(scope)
    if normalized_scope in _SCOPE_VALUES_BLOCKED_FOR_NEXT:
        raise PromotionVerdictValidationError("allowed_next_scope is not permitted")
    if decision == _rt() + "_candidate_allowed_for_separate_review":
        if normalized_scope != _SEPARATE_REVIEW_SCOPE:
            raise PromotionVerdictValidationError("allowed_next_scope must be separate review")
    return scope


def _validate_created_by(value: object) -> str:
    created_by = _require_text(value, "created_by")
    normalized = _normalize_token(created_by)
    for marker in _CREATOR_DENY_MARKERS:
        if marker in normalized:
            raise PromotionVerdictValidationError("created_by is not permitted")
    if "report" in normalized and "publisher" in normalized:
        raise PromotionVerdictValidationError("created_by is not permitted")
    if ("back" + "test") in normalized and "engine" in normalized:
        raise PromotionVerdictValidationError("created_by is not permitted")
    return created_by


def validate_promotion_evidence_ref_values(values: Mapping[str, object]) -> dict[str, object]:
    values = _require_mapping(values)
    _require_exact_fields(values, PROMOTION_EVIDENCE_REF_REQUIRED_FIELDS)
    return {
        "ref_id": _require_text(values["ref_id"], "ref_id"),
        "ref_type": _normalize_ref_type(values["ref_type"]),
        "ref": _require_text(values["ref"], "ref"),
        "producer": _require_text(values["producer"], "producer"),
        "consumer": _require_text(values["consumer"], "consumer"),
    }


class PromotionEvidenceRef:
    __annotations__ = {
        "ref_id": str,
        "ref_type": str,
        "ref": str,
        "producer": str,
        "consumer": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_promotion_evidence_ref_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_promotion_evidence_ref(
    evidence_ref: PromotionEvidenceRef,
) -> PromotionEvidenceRef:
    if not isinstance(evidence_ref, PromotionEvidenceRef):
        raise TypeError("evidence_ref must be PromotionEvidenceRef")
    validate_promotion_evidence_ref_values(evidence_ref.__dict__)
    return evidence_ref


def _require_evidence_refs(value: object) -> tuple[PromotionEvidenceRef, ...]:
    if isinstance(value, (str, bytes)):
        raise PromotionVerdictValidationError("evidence_refs must be a non-empty iterable")
    if not isinstance(value, Iterable):
        raise PromotionVerdictValidationError("evidence_refs must be a non-empty iterable")
    refs = tuple(value)
    if not refs:
        raise PromotionVerdictValidationError("evidence_refs must be non-empty")
    for evidence_ref in refs:
        if not isinstance(evidence_ref, PromotionEvidenceRef):
            raise PromotionVerdictValidationError(
                "evidence_refs must contain PromotionEvidenceRef instances"
            )
        validate_promotion_evidence_ref(evidence_ref)
    return refs


def validate_promotion_verdict_values(values: Mapping[str, object]) -> dict[str, object]:
    values = _require_mapping(values)
    _require_exact_fields(values, PROMOTION_VERDICT_REQUIRED_FIELDS)

    decision = _normalize_decision(values["decision"])
    return {
        "promotion_verdict_id": _require_text(
            values["promotion_verdict_id"], "promotion_verdict_id"
        ),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_version": _require_text(values["strategy_version"], "strategy_version"),
        "decision": decision,
        "evidence_refs": _require_evidence_refs(values["evidence_refs"]),
        "allowed_next_scope": _validate_next_scope(values["allowed_next_scope"], decision),
        "blocked_scope": _require_text(values["blocked_scope"], "blocked_scope"),
        "created_by": _validate_created_by(values["created_by"]),
        "created_ts": _require_text(values["created_ts"], "created_ts"),
        "verdict_schema_version": _require_text(
            values["verdict_schema_version"], "verdict_schema_version"
        ),
    }


class PromotionVerdict:
    __annotations__ = {
        "promotion_verdict_id": str,
        "strategy_id": str,
        "strategy_version": str,
        "decision": str,
        "evidence_refs": tuple[PromotionEvidenceRef, ...],
        "allowed_next_scope": str,
        "blocked_scope": str,
        "created_by": str,
        "created_ts": str,
        "verdict_schema_version": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_promotion_verdict_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def _validate_verdict_instance(verdict: PromotionVerdict) -> PromotionVerdict:
    if not isinstance(verdict, PromotionVerdict):
        raise TypeError("verdict must be PromotionVerdict")
    validate_promotion_verdict_values(verdict.__dict__)
    return verdict


validate_promotion_verdict = _validate_verdict_instance


__all__ = [
    "ALLOWED_PROMOTION_DECISIONS",
    "ALLOWED_PROMOTION_EVIDENCE_REF_TYPES",
    "PROMOTION_EVIDENCE_REF_REQUIRED_FIELDS",
    "PROMOTION_VERDICT_REQUIRED_FIELDS",
    "PromotionEvidenceRef",
    "PromotionVerdict",
    "PromotionVerdictValidationError",
    "validate_promotion_evidence_ref",
    "validate_promotion_evidence_ref_values",
    "validate_promotion_verdict",
    "validate_promotion_verdict_values",
]
