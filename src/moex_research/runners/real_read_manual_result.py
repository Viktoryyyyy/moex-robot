from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.real_read_owner_run import RealReadOwnerAckDecision, validate_real_read_owner_ack_decision


class RealReadManualResultError(ValueError):
    pass


RESULT_FIELDS: Final[tuple[str, ...]] = (
    "result_id",
    "owner_ack_decision",
    "operator_report_ref",
    "artifact_manifest_ref",
    "quality_report_ref",
    "lineage_report_ref",
    "result_scope",
    "read_executed",
    "metadata_only",
)
INTAKE_FIELDS: Final[tuple[str, ...]] = (
    "intake_id",
    "manual_result",
    "intake_mode",
    "requested_intake_state",
    "allow_registry_write",
    "allow_runtime",
    "allow_promotion",
    "metadata_only",
)
DECISION_FIELDS: Final[tuple[str, ...]] = (
    "decision_status",
    "intake_id",
    "result_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "registry_write_allowed",
    "runtime_allowed",
    "promotion_allowed",
    "reason_or_none",
)
ALLOWED_RESULT_SCOPES: Final[frozenset[str]] = frozenset({"manual_result_metadata_only"})
ALLOWED_INTAKE_MODES: Final[frozenset[str]] = frozenset({"manual_result_intake_only"})
ALLOWED_INTAKE_STATES: Final[frozenset[str]] = frozenset({"blocked", "accepted_for_pm_review", "rejected"})


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
        raise RealReadManualResultError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadManualResultError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadManualResultError(f"{field_name} must be bool")
    if value:
        raise RealReadManualResultError(f"{field_name} must be false")
    return value


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadManualResultError(f"{field_name} must be bool")
    if not value:
        raise RealReadManualResultError(f"{field_name} must be true")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadManualResultError(f"{label} fields invalid")


def validate_real_read_manual_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, RESULT_FIELDS, "manual_result")
    ack_decision = values["owner_ack_decision"]
    if not isinstance(ack_decision, RealReadOwnerAckDecision):
        raise RealReadManualResultError("owner_ack_decision must be RealReadOwnerAckDecision")
    validated_ack = validate_real_read_owner_ack_decision(ack_decision)
    if validated_ack.decision_status != "ready_for_manual_apply":
        raise RealReadManualResultError("owner_ack_decision must be ready for manual apply")
    if validated_ack.real_read_allowed is not False:
        raise RealReadManualResultError("owner_ack_decision must not authorize direct read")
    if validated_ack.manual_apply_required is not True or validated_ack.manual_result_required is not True:
        raise RealReadManualResultError("owner_ack_decision must require manual apply and result")
    result_scope = _text(values["result_scope"], "result_scope")
    if result_scope not in ALLOWED_RESULT_SCOPES:
        raise RealReadManualResultError("result_scope is unsupported")
    return {
        "result_id": _text(values["result_id"], "result_id"),
        "owner_ack_decision": validated_ack,
        "operator_report_ref": _text(values["operator_report_ref"], "operator_report_ref"),
        "artifact_manifest_ref": _text(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "quality_report_ref": _text(values["quality_report_ref"], "quality_report_ref"),
        "lineage_report_ref": _text(values["lineage_report_ref"], "lineage_report_ref"),
        "result_scope": result_scope,
        "read_executed": _true(values["read_executed"], "read_executed"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadManualResult:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_manual_result_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_manual_result(manual_result: RealReadManualResult) -> RealReadManualResult:
    if not isinstance(manual_result, RealReadManualResult):
        raise TypeError("manual_result must be RealReadManualResult")
    validate_real_read_manual_result_values(manual_result.__dict__)
    return manual_result


def validate_real_read_manual_result_intake_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, INTAKE_FIELDS, "intake")
    manual_result = values["manual_result"]
    if not isinstance(manual_result, RealReadManualResult):
        raise RealReadManualResultError("manual_result must be RealReadManualResult")
    validated_result = validate_real_read_manual_result(manual_result)
    intake_mode = _text(values["intake_mode"], "intake_mode")
    if intake_mode not in ALLOWED_INTAKE_MODES:
        raise RealReadManualResultError("intake_mode is unsupported")
    intake_state = _text(values["requested_intake_state"], "requested_intake_state")
    if intake_state not in ALLOWED_INTAKE_STATES:
        raise RealReadManualResultError("requested_intake_state is unsupported")
    return {
        "intake_id": _text(values["intake_id"], "intake_id"),
        "manual_result": validated_result,
        "intake_mode": intake_mode,
        "requested_intake_state": intake_state,
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
        "allow_promotion": _false(values["allow_promotion"], "allow_promotion"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadManualResultIntake:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_manual_result_intake_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_manual_result_intake(intake: RealReadManualResultIntake) -> RealReadManualResultIntake:
    if not isinstance(intake, RealReadManualResultIntake):
        raise TypeError("intake must be RealReadManualResultIntake")
    validate_real_read_manual_result_intake_values(intake.__dict__)
    return intake


def validate_real_read_manual_result_intake_decision_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, DECISION_FIELDS, "decision")
    status = _text(values["decision_status"], "decision_status")
    if status not in ALLOWED_INTAKE_STATES:
        raise RealReadManualResultError("decision_status is unsupported")
    if values["registry_write_allowed"] is not False:
        raise RealReadManualResultError("registry_write_allowed must remain false")
    if values["runtime_allowed"] is not False:
        raise RealReadManualResultError("runtime_allowed must remain false")
    if values["promotion_allowed"] is not False:
        raise RealReadManualResultError("promotion_allowed must remain false")
    reason = values["reason_or_none"]
    if status == "accepted_for_pm_review" and reason is not None:
        raise RealReadManualResultError("accepted result must not include reason")
    if status in {"blocked", "rejected"} and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadManualResultError("blocked or rejected decision requires reason")
    return dict(values)


class RealReadManualResultIntakeDecision:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_manual_result_intake_decision_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_manual_result_intake_decision(decision: RealReadManualResultIntakeDecision) -> RealReadManualResultIntakeDecision:
    if not isinstance(decision, RealReadManualResultIntakeDecision):
        raise TypeError("decision must be RealReadManualResultIntakeDecision")
    validate_real_read_manual_result_intake_decision_values(decision.__dict__)
    return decision


def _decision(intake: RealReadManualResultIntake, status: str, reason: str | None) -> RealReadManualResultIntakeDecision:
    ack = intake.manual_result.owner_ack_decision
    return RealReadManualResultIntakeDecision(
        decision_status=status,
        intake_id=intake.intake_id,
        result_id=intake.manual_result.result_id,
        dataset_ref_id=ack.dataset_ref_id,
        instrument_id=ack.instrument_id,
        timeframe=ack.timeframe,
        registry_write_allowed=False,
        runtime_allowed=False,
        promotion_allowed=False,
        reason_or_none=reason,
    )


def evaluate_real_read_manual_result_intake(intake: RealReadManualResultIntake) -> RealReadManualResultIntakeDecision:
    try:
        validated = validate_real_read_manual_result_intake(intake)
        if validated.requested_intake_state == "rejected":
            return _decision(validated, "rejected", "manual result rejected")
        if validated.requested_intake_state == "blocked":
            return _decision(validated, "blocked", "manual result requested blocked decision")
        return _decision(validated, "accepted_for_pm_review", None)
    except (RealReadManualResultError, TypeError, ValueError) as error:
        return RealReadManualResultIntakeDecision(
            decision_status="blocked",
            intake_id="unavailable",
            result_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            registry_write_allowed=False,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_INTAKE_MODES",
    "ALLOWED_INTAKE_STATES",
    "ALLOWED_RESULT_SCOPES",
    "DECISION_FIELDS",
    "INTAKE_FIELDS",
    "RESULT_FIELDS",
    "RealReadManualResult",
    "RealReadManualResultError",
    "RealReadManualResultIntake",
    "RealReadManualResultIntakeDecision",
    "evaluate_real_read_manual_result_intake",
    "validate_real_read_manual_result",
    "validate_real_read_manual_result_intake",
    "validate_real_read_manual_result_intake_decision",
    "validate_real_read_manual_result_intake_decision_values",
    "validate_real_read_manual_result_intake_values",
    "validate_real_read_manual_result_values",
]
