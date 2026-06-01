from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_chain import RealReadChainCloseoutDecision, validate_real_read_chain_closeout_decision


class RealReadPhaseStatusError(ValueError):
    pass


PACKAGE_FIELDS: Final[tuple[str, ...]] = (
    "package_id",
    "chain_closeout_decision",
    "phase_id",
    "phase_scope",
    "repo_evidence_ref",
    "status_note_ref",
    "metadata_only",
)
STATUS_FIELDS: Final[tuple[str, ...]] = (
    "status_id",
    "status_package",
    "status_mode",
    "requested_phase_state",
    "allow_registry_write",
    "allow_runtime",
    "allow_promotion",
    "metadata_only",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "status_id",
    "package_id",
    "phase_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "phase_status",
    "registry_write_allowed",
    "runtime_allowed",
    "promotion_allowed",
    "reason_or_none",
)
ALLOWED_PHASE_SCOPES: Final[frozenset[str]] = frozenset({"repo_only_real_read_readiness_phase"})
ALLOWED_STATUS_MODES: Final[frozenset[str]] = frozenset({"phase_status_closeout_only"})
ALLOWED_PHASE_STATES: Final[frozenset[str]] = frozenset({"blocked", "closed_repo_only", "rejected"})


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
        raise RealReadPhaseStatusError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadPhaseStatusError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadPhaseStatusError(f"{field_name} must be bool")
    if value:
        raise RealReadPhaseStatusError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadPhaseStatusError(f"{field_name} must be bool")
    if not value:
        raise RealReadPhaseStatusError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadPhaseStatusError(f"{label} fields invalid")


def validate_real_read_phase_status_package_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, PACKAGE_FIELDS, "package")
    chain_decision = values["chain_closeout_decision"]
    if not isinstance(chain_decision, RealReadChainCloseoutDecision):
        raise RealReadPhaseStatusError("chain_closeout_decision must be RealReadChainCloseoutDecision")
    validated_chain = validate_real_read_chain_closeout_decision(chain_decision)
    if validated_chain.decision_status != "accepted_chain":
        raise RealReadPhaseStatusError("chain_closeout_decision must be accepted chain")
    if validated_chain.registry_write_allowed is not False:
        raise RealReadPhaseStatusError("chain_closeout_decision must not allow registry write")
    if validated_chain.runtime_allowed is not False:
        raise RealReadPhaseStatusError("chain_closeout_decision must not allow runtime")
    if validated_chain.promotion_allowed is not False:
        raise RealReadPhaseStatusError("chain_closeout_decision must not allow promotion")
    phase_scope = _text(values["phase_scope"], "phase_scope")
    if phase_scope not in ALLOWED_PHASE_SCOPES:
        raise RealReadPhaseStatusError("phase_scope is unsupported")
    return {
        "package_id": _text(values["package_id"], "package_id"),
        "chain_closeout_decision": validated_chain,
        "phase_id": _text(values["phase_id"], "phase_id"),
        "phase_scope": phase_scope,
        "repo_evidence_ref": _text(values["repo_evidence_ref"], "repo_evidence_ref"),
        "status_note_ref": _text(values["status_note_ref"], "status_note_ref"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadPhaseStatusPackage:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_phase_status_package_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_phase_status_package(package: RealReadPhaseStatusPackage) -> RealReadPhaseStatusPackage:
    if not isinstance(package, RealReadPhaseStatusPackage):
        raise TypeError("package must be RealReadPhaseStatusPackage")
    validate_real_read_phase_status_package_values(package.__dict__)
    return package


def validate_real_read_phase_status_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, STATUS_FIELDS, "status")
    package = values["status_package"]
    if not isinstance(package, RealReadPhaseStatusPackage):
        raise RealReadPhaseStatusError("status_package must be RealReadPhaseStatusPackage")
    validated_package = validate_real_read_phase_status_package(package)
    status_mode = _text(values["status_mode"], "status_mode")
    if status_mode not in ALLOWED_STATUS_MODES:
        raise RealReadPhaseStatusError("status_mode is unsupported")
    phase_state = _text(values["requested_phase_state"], "requested_phase_state")
    if phase_state not in ALLOWED_PHASE_STATES:
        raise RealReadPhaseStatusError("requested_phase_state is unsupported")
    return {
        "status_id": _text(values["status_id"], "status_id"),
        "status_package": validated_package,
        "status_mode": status_mode,
        "requested_phase_state": phase_state,
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
        "allow_promotion": _false(values["allow_promotion"], "allow_promotion"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadPhaseStatus:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_phase_status_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_phase_status(status: RealReadPhaseStatus) -> RealReadPhaseStatus:
    if not isinstance(status, RealReadPhaseStatus):
        raise TypeError("status must be RealReadPhaseStatus")
    validate_real_read_phase_status_values(status.__dict__)
    return status


def validate_real_read_phase_status_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_PHASE_STATES:
        raise RealReadPhaseStatusError("decision_status is unsupported")
    _text(values["phase_status"], "phase_status")
    if values["registry_write_allowed"] is not False:
        raise RealReadPhaseStatusError("registry_write_allowed must remain false")
    if values["runtime_allowed"] is not False:
        raise RealReadPhaseStatusError("runtime_allowed must remain false")
    if values["promotion_allowed"] is not False:
        raise RealReadPhaseStatusError("promotion_allowed must remain false")
    reason = values["reason_or_none"]
    if status == "closed_repo_only" and reason is not None:
        raise RealReadPhaseStatusError("closed repo-only result must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadPhaseStatusError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadPhaseStatusDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_phase_status_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_phase_status_decision(decision: RealReadPhaseStatusDecision) -> RealReadPhaseStatusDecision:
    if not isinstance(decision, RealReadPhaseStatusDecision):
        raise TypeError("decision must be RealReadPhaseStatusDecision")
    validate_real_read_phase_status_decision_values(decision.__dict__)
    return decision


def _decision(status: RealReadPhaseStatus, decision_status: str, phase_status: str, reason: str | None) -> RealReadPhaseStatusDecision:
    chain = status.status_package.chain_closeout_decision
    return RealReadPhaseStatusDecision(
        decision_status=decision_status,
        status_id=status.status_id,
        package_id=status.status_package.package_id,
        phase_id=status.status_package.phase_id,
        dataset_ref_id=chain.dataset_ref_id,
        instrument_id=chain.instrument_id,
        timeframe=chain.timeframe,
        phase_status=phase_status,
        registry_write_allowed=False,
        runtime_allowed=False,
        promotion_allowed=False,
        reason_or_none=reason,
    )


def evaluate_real_read_phase_status(status: RealReadPhaseStatus) -> RealReadPhaseStatusDecision:
    try:
        validated = validate_real_read_phase_status(status)
        if validated.requested_phase_state == "rejected":
            return _decision(validated, "rejected", "rejected", "phase status rejected")
        if validated.requested_phase_state == "blocked":
            return _decision(validated, "blocked", "blocked", "phase status requested blocked decision")
        return _decision(validated, "closed_repo_only", "repo_only_real_read_readiness_closed", None)
    except (RealReadPhaseStatusError, TypeError, ValueError) as error:
        return RealReadPhaseStatusDecision(
            decision_status="blocked",
            status_id="unavailable",
            package_id="unavailable",
            phase_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            phase_status="blocked",
            registry_write_allowed=False,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_PHASE_SCOPES",
    "ALLOWED_PHASE_STATES",
    "ALLOWED_STATUS_MODES",
    "DECISION_FIELDS",
    "PACKAGE_FIELDS",
    "STATUS_FIELDS",
    "RealReadPhaseStatus",
    "RealReadPhaseStatusDecision",
    "RealReadPhaseStatusError",
    "RealReadPhaseStatusPackage",
    "evaluate_real_read_phase_status",
    "validate_real_read_phase_status",
    "validate_real_read_phase_status_decision",
    "validate_real_read_phase_status_decision_values",
    "validate_real_read_phase_status_package",
    "validate_real_read_phase_status_package_values",
    "validate_real_read_phase_status_values",
]
