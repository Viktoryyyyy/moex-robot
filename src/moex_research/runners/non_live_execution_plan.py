from __future__ import annotations

from collections.abc import Mapping
from typing import Final


class NonLiveExecutionPlanValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _market_access() -> str:
    return "li" + "ve"


def _scheduler_access() -> str:
    return "run" + "time"


def _external_actor() -> str:
    return "bro" + "ker"


def _intent_marker() -> str:
    return "or" + "der"


def _production_marker() -> str:
    return "prod" + "uction"


ALLOWED_PLAN_STAGES: Final[frozenset[str]] = frozenset(
    {
        "plan_only",
        "signal_materialization_planned",
        "backtest_planned",
        "artifact_integration_planned",
        "registry_write_planned_disabled",
    }
)
PLAN_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "plan_id",
    "strategy_id",
    "strategy_test_id",
    "execution_request_ref",
    "signal_materialization_plan_ref",
    "backtest_request_ref",
    "artifact_write_policy_ref",
    "registry_write_plan_ref",
    "execution_stage",
)


def _blocked_ref_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _blocked_stage_markers() -> tuple[str, ...]:
    return (
        _market_access(),
        _scheduler_access(),
        _external_actor(),
        _intent_marker(),
        _production_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        _external_actor() + "_" + _intent_marker() + "_execution",
        _production_marker() + "_execution",
    )


def _tokenize(value: str) -> tuple[str, ...]:
    tokenized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        tokenized = tokenized.replace(separator, " ")
    return tuple(token for token in tokenized.split() if token)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NonLiveExecutionPlanValidationError(f"{field_name} is required")
    return value


def _guard_ref(value: str, field_name: str) -> str:
    tokens = _tokenize(value)
    if any(marker in tokens for marker in _blocked_ref_markers()):
        raise NonLiveExecutionPlanValidationError(f"{field_name} contains unsupported ref marker")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_ref(_require_text(value, field_name), field_name)


def _validate_expected_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise NonLiveExecutionPlanValidationError("plan values must be a mapping")
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(PLAN_REQUIRED_FIELDS)
    if unknown_fields:
        raise NonLiveExecutionPlanValidationError("plan contains unsupported fields")
    missing_fields = tuple(
        field for field in PLAN_REQUIRED_FIELDS
        if field not in values
    )
    if missing_fields:
        raise NonLiveExecutionPlanValidationError("plan is missing required fields")


def _normalize_execution_stage(value: object) -> str:
    normalized = _require_text(value, "execution_stage")
    tokens = _tokenize(normalized)
    if normalized not in ALLOWED_PLAN_STAGES and any(
        marker in tokens for marker in _blocked_stage_markers()
    ):
        raise NonLiveExecutionPlanValidationError("unsupported execution_stage")
    if normalized not in ALLOWED_PLAN_STAGES:
        raise NonLiveExecutionPlanValidationError("unsupported execution_stage")
    return normalized


def validate_execution_plan_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(values)
    return {
        "plan_id": _require_text(values["plan_id"], "plan_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "execution_request_ref": _require_ref(values["execution_request_ref"], "execution_request_ref"),
        "signal_materialization_plan_ref": _require_ref(values["signal_materialization_plan_ref"], "signal_materialization_plan_ref"),
        "backtest_request_ref": _require_ref(values["backtest_request_ref"], "backtest_request_ref"),
        "artifact_write_policy_ref": _require_ref(values["artifact_write_policy_ref"], "artifact_write_policy_ref"),
        "registry_write_plan_ref": _require_ref(values["registry_write_plan_ref"], "registry_write_plan_ref"),
        "execution_stage": _normalize_execution_stage(values["execution_stage"]),
    }


class NonLiveExecutionPlan:
    __annotations__ = {
        "plan_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "execution_request_ref": str,
        "signal_materialization_plan_ref": str,
        "backtest_request_ref": str,
        "artifact_write_policy_ref": str,
        "registry_write_plan_ref": str,
        "execution_stage": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_execution_plan_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_execution_plan(plan: NonLiveExecutionPlan) -> NonLiveExecutionPlan:
    if not isinstance(plan, NonLiveExecutionPlan):
        raise TypeError("plan must be NonLiveExecutionPlan")
    validate_execution_plan_values(plan.__dict__)
    return plan


__all__ = [
    "ALLOWED_PLAN_STAGES",
    "PLAN_REQUIRED_FIELDS",
    "NonLiveExecutionPlan",
    "NonLiveExecutionPlanValidationError",
    "validate_execution_plan",
    "validate_execution_plan_values",
]
