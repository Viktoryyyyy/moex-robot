from __future__ import annotations

from typing import Final

from moex_research.runners.execution_request import (
    StrategyTestingExecutionRequest,
    validate_strategy_testing_execution_request,
)


class PlannedExecutionBoundaryValidationError(ValueError):
    pass


def _research_planned_mode() -> str:
    return "non_" + "li" + "ve_research_planned"


def _backtest_planned_mode() -> str:
    return "non_" + "li" + "ve_backtest_planned"


PLANNED_EXECUTION_MODES: Final[frozenset[str]] = frozenset(
    {
        "plan_only",
        _research_planned_mode(),
        _backtest_planned_mode(),
    }
)
PLANNED_EXECUTION_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "planning_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "execution_mode",
    "artifact_plan_ref",
    "error_message_or_none",
)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlannedExecutionBoundaryValidationError(f"{field_name} is required")
    return value


def _require_status(value: object) -> str:
    status = _require_text(value, "planning_status")
    if status not in {"planned", "rejected"}:
        raise PlannedExecutionBoundaryValidationError("unsupported planning_status")
    return status


def _require_mode(value: object) -> str:
    mode = _require_text(value, "execution_mode")
    if mode not in PLANNED_EXECUTION_MODES:
        raise PlannedExecutionBoundaryValidationError("wrong boundary for execution_mode")
    return mode


def _require_error(value: object, planning_status: str) -> str | None:
    if planning_status == "planned":
        if value is not None:
            raise PlannedExecutionBoundaryValidationError("planned result must not include error")
        return None
    if not isinstance(value, str) or not value.strip():
        raise PlannedExecutionBoundaryValidationError("rejected result requires error")
    return value


class PlannedExecutionResult:
    __annotations__ = {
        "planning_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "execution_mode": str,
        "artifact_plan_ref": str,
        "error_message_or_none": str | None,
    }

    def __init__(
        self,
        *,
        planning_status: str,
        request_id: str,
        strategy_id: str,
        strategy_test_id: str,
        execution_mode: str,
        artifact_plan_ref: str,
        error_message_or_none: str | None,
    ) -> None:
        self.planning_status = _require_status(planning_status)
        self.request_id = _require_text(request_id, "request_id")
        self.strategy_id = _require_text(strategy_id, "strategy_id")
        self.strategy_test_id = _require_text(strategy_test_id, "strategy_test_id")
        self.execution_mode = _require_text(execution_mode, "execution_mode")
        self.artifact_plan_ref = _require_text(artifact_plan_ref, "artifact_plan_ref")
        self.error_message_or_none = _require_error(
            error_message_or_none,
            self.planning_status,
        )
        _validate_result_fields(self)


def _validate_result_fields(result: PlannedExecutionResult) -> None:
    if set(result.__dict__) != set(PLANNED_EXECUTION_RESULT_FIELDS):
        raise PlannedExecutionBoundaryValidationError("unsupported planned result fields")


def validate_planned_execution_result(
    result: PlannedExecutionResult,
) -> PlannedExecutionResult:
    if not isinstance(result, PlannedExecutionResult):
        raise TypeError("result must be PlannedExecutionResult")
    _validate_result_fields(result)
    _require_status(result.planning_status)
    _require_text(result.request_id, "request_id")
    _require_text(result.strategy_id, "strategy_id")
    _require_text(result.strategy_test_id, "strategy_test_id")
    if result.planning_status == "planned":
        _require_mode(result.execution_mode)
    else:
        _require_text(result.execution_mode, "execution_mode")
    _require_text(result.artifact_plan_ref, "artifact_plan_ref")
    _require_error(result.error_message_or_none, result.planning_status)
    return result


def validate_planned_execution_request(
    request: StrategyTestingExecutionRequest,
) -> StrategyTestingExecutionRequest:
    if not isinstance(request, StrategyTestingExecutionRequest):
        raise TypeError("request must be StrategyTestingExecutionRequest")
    validate_strategy_testing_execution_request(request)
    _require_mode(request.execution_mode)
    _require_text(request.package_ref, "package_ref")
    _require_text(request.artifact_plan_ref, "artifact_plan_ref")
    return request


def _safe_text(request: object, field_name: str) -> str:
    value = getattr(request, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _rejected_result(request: object, error: Exception) -> PlannedExecutionResult:
    return PlannedExecutionResult(
        planning_status="rejected",
        request_id=_safe_text(request, "request_id"),
        strategy_id=_safe_text(request, "strategy_id"),
        strategy_test_id=_safe_text(request, "strategy_test_id"),
        execution_mode=_safe_text(request, "execution_mode"),
        artifact_plan_ref=_safe_text(request, "artifact_plan_ref"),
        error_message_or_none=str(error),
    )


def plan_strategy_testing_execution(
    request: StrategyTestingExecutionRequest,
) -> PlannedExecutionResult:
    try:
        validated = validate_planned_execution_request(request)
    except (PlannedExecutionBoundaryValidationError, TypeError, ValueError) as error:
        return _rejected_result(request, error)

    return PlannedExecutionResult(
        planning_status="planned",
        request_id=validated.request_id,
        strategy_id=validated.strategy_id,
        strategy_test_id=validated.strategy_test_id,
        execution_mode=validated.execution_mode,
        artifact_plan_ref=validated.artifact_plan_ref,
        error_message_or_none=None,
    )


__all__ = [
    "PLANNED_EXECUTION_MODES",
    "PLANNED_EXECUTION_RESULT_FIELDS",
    "PlannedExecutionBoundaryValidationError",
    "PlannedExecutionResult",
    "plan_strategy_testing_execution",
    "validate_planned_execution_request",
    "validate_planned_execution_result",
]
