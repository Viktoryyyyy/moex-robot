from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.execution_request import (
    ExecutionArtifactPlan,
    StrategyTestingExecutionRequest,
    validate_execution_artifact_plan,
)
from moex_research.runners.planned_execution import (
    PlannedExecutionBoundaryValidationError,
    validate_planned_execution_request,
)


class PlannedArtifactIntegrationValidationError(ValueError):
    pass


def _decision_gate_field() -> str:
    return "promotion_" + "verdict_allowed"


PLANNED_ARTIFACT_INTEGRATION_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "integration_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "artifact_plan_id",
    "artifact_manifest_ref",
    "metrics_artifact_ref_or_none",
    "report_artifact_ref_or_none",
    "registry_write_allowed",
    _decision_gate_field(),
    "error_message_or_none",
)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlannedArtifactIntegrationValidationError(f"{field_name} is required")
    return value


def _require_status(value: object) -> str:
    status = _require_text(value, "integration_status")
    if status not in {"planned", "rejected"}:
        raise PlannedArtifactIntegrationValidationError("unsupported integration_status")
    return status


def _require_ref_or_none(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _require_bool_disabled(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PlannedArtifactIntegrationValidationError(f"{field_name} must be bool")
    if value:
        raise PlannedArtifactIntegrationValidationError(f"{field_name} must be false")
    return value


def _require_error(value: object, integration_status: str) -> str | None:
    if integration_status == "planned":
        if value is not None:
            raise PlannedArtifactIntegrationValidationError("planned result must not include error")
        return None
    if not isinstance(value, str) or not value.strip():
        raise PlannedArtifactIntegrationValidationError("rejected result requires error")
    return value


def _validate_expected_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise PlannedArtifactIntegrationValidationError("result values must be a mapping")
    if set(values) != set(PLANNED_ARTIFACT_INTEGRATION_RESULT_FIELDS):
        raise PlannedArtifactIntegrationValidationError("unsupported planned integration result fields")


def validate_planned_artifact_integration_result_values(
    values: Mapping[str, object],
) -> dict[str, object]:
    _validate_expected_fields(values)
    status = _require_status(values["integration_status"])
    decision_gate = _decision_gate_field()
    return {
        "integration_status": status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "artifact_plan_id": _require_text(values["artifact_plan_id"], "artifact_plan_id"),
        "artifact_manifest_ref": _require_text(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "metrics_artifact_ref_or_none": _require_ref_or_none(
            values["metrics_artifact_ref_or_none"],
            "metrics_artifact_ref_or_none",
        ),
        "report_artifact_ref_or_none": _require_ref_or_none(
            values["report_artifact_ref_or_none"],
            "report_artifact_ref_or_none",
        ),
        "registry_write_allowed": _require_bool_disabled(
            values["registry_write_allowed"],
            "registry_write_allowed",
        ),
        decision_gate: _require_bool_disabled(values[decision_gate], decision_gate),
        "error_message_or_none": _require_error(values["error_message_or_none"], status),
    }


class PlannedArtifactIntegrationResult:
    __annotations__ = {
        "integration_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "artifact_plan_id": str,
        "artifact_manifest_ref": str,
        "metrics_artifact_ref_or_none": str | None,
        "report_artifact_ref_or_none": str | None,
        "registry_write_allowed": bool,
        _decision_gate_field(): bool,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_planned_artifact_integration_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_planned_artifact_integration_result(
    result: PlannedArtifactIntegrationResult,
) -> PlannedArtifactIntegrationResult:
    if not isinstance(result, PlannedArtifactIntegrationResult):
        raise TypeError("result must be PlannedArtifactIntegrationResult")
    validate_planned_artifact_integration_result_values(result.__dict__)
    return result


def validate_planned_artifact_integration(
    request: StrategyTestingExecutionRequest,
    artifact_plan: ExecutionArtifactPlan,
) -> tuple[StrategyTestingExecutionRequest, ExecutionArtifactPlan]:
    if not isinstance(request, StrategyTestingExecutionRequest):
        raise TypeError("request must be StrategyTestingExecutionRequest")
    if not isinstance(artifact_plan, ExecutionArtifactPlan):
        raise TypeError("artifact_plan must be ExecutionArtifactPlan")

    validate_planned_execution_request(request)
    validate_execution_artifact_plan(artifact_plan)
    _require_text(request.request_id, "request_id")
    _require_text(artifact_plan.artifact_plan_id, "artifact_plan_id")
    if request.artifact_plan_ref != artifact_plan.artifact_plan_id:
        raise PlannedArtifactIntegrationValidationError("artifact plan reference mismatch")
    return request, artifact_plan


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _rejected_result(
    request: object,
    artifact_plan: object,
    error: Exception,
) -> PlannedArtifactIntegrationResult:
    values: dict[str, object] = {
        "integration_status": "rejected",
        "request_id": _safe_text(request, "request_id"),
        "strategy_id": _safe_text(request, "strategy_id"),
        "strategy_test_id": _safe_text(request, "strategy_test_id"),
        "artifact_plan_id": _safe_text(artifact_plan, "artifact_plan_id"),
        "artifact_manifest_ref": "unavailable",
        "metrics_artifact_ref_or_none": None,
        "report_artifact_ref_or_none": None,
        "registry_write_allowed": False,
        _decision_gate_field(): False,
        "error_message_or_none": str(error),
    }
    return PlannedArtifactIntegrationResult(**values)


def _planned_result(
    request: StrategyTestingExecutionRequest,
    artifact_plan: ExecutionArtifactPlan,
) -> PlannedArtifactIntegrationResult:
    values: dict[str, object] = {
        "integration_status": "planned",
        "request_id": request.request_id,
        "strategy_id": request.strategy_id,
        "strategy_test_id": request.strategy_test_id,
        "artifact_plan_id": artifact_plan.artifact_plan_id,
        "artifact_manifest_ref": artifact_plan.artifact_manifest_ref,
        "metrics_artifact_ref_or_none": artifact_plan.metrics_artifact_ref_or_none,
        "report_artifact_ref_or_none": artifact_plan.report_artifact_ref_or_none,
        "registry_write_allowed": artifact_plan.registry_write_allowed,
        _decision_gate_field(): getattr(artifact_plan, _decision_gate_field()),
        "error_message_or_none": None,
    }
    return PlannedArtifactIntegrationResult(**values)


def plan_execution_artifacts(
    request: StrategyTestingExecutionRequest,
    artifact_plan: ExecutionArtifactPlan,
) -> PlannedArtifactIntegrationResult:
    try:
        validated_request, validated_plan = validate_planned_artifact_integration(
            request,
            artifact_plan,
        )
    except (
        PlannedArtifactIntegrationValidationError,
        PlannedExecutionBoundaryValidationError,
        TypeError,
        ValueError,
    ) as error:
        return _rejected_result(request, artifact_plan, error)

    return _planned_result(validated_request, validated_plan)


__all__ = [
    "PLANNED_ARTIFACT_INTEGRATION_RESULT_FIELDS",
    "PlannedArtifactIntegrationResult",
    "PlannedArtifactIntegrationValidationError",
    "plan_execution_artifacts",
    "validate_planned_artifact_integration",
    "validate_planned_artifact_integration_result",
    "validate_planned_artifact_integration_result_values",
]
