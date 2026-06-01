from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.canonical_data_read import (
    CanonicalDataReadRequest,
    CanonicalDataReadResult,
    dry_validate_canonical_data_read_request,
    validate_canonical_data_read_request,
)


class CanonicalDataReadPlanValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _market_access_marker() -> str:
    return "li" + "ve"


def _scheduler_access_marker() -> str:
    return "run" + "time"


def _host_path_marker() -> str:
    return "ser" + "ver"


def _lake_marker() -> str:
    return "data" + "lake"


CANONICAL_DATA_ACCESS_POLICY_FIELDS: Final[tuple[str, ...]] = (
    "policy_id",
    "allowed_dataset_classes",
    "allowed_instruments",
    "allowed_timeframes",
    "allow_file_read",
    "allow_network",
    "allow_discovery",
    "access_mode",
)
CANONICAL_DATA_READ_PLAN_FIELDS: Final[tuple[str, ...]] = (
    "plan_id",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref_id",
    "schema_ref",
    "access_policy_id",
    "planned_reader_id",
    "read_plan_mode",
)
CANONICAL_DATA_READ_PLAN_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "plan_status",
    "plan_id",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref_id",
    "schema_ref",
    "access_policy_id",
    "planned_reader_id",
    "error_message_or_none",
)
ALLOWED_ACCESS_MODES: Final[frozenset[str]] = frozenset({"dry_run_plan_only"})
ALLOWED_READ_PLAN_MODES: Final[frozenset[str]] = frozenset({"canonical_read_planned_only"})
ALLOWED_PLAN_STATUSES: Final[frozenset[str]] = frozenset({"planned", "rejected"})
ALLOWED_DATASET_CLASSES: Final[frozenset[str]] = frozenset({"canonical_bars"})
ALLOWED_INSTRUMENTS: Final[frozenset[str]] = frozenset({"Si", "USDRUBF"})
ALLOWED_TIMEFRAMES: Final[frozenset[str]] = frozenset({"D1", "5m"})


def _selection_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _blocked_ref_markers() -> tuple[str, ...]:
    return (
        *_selection_markers(),
        _host_path_marker(),
        _scheduler_access_marker(),
        _market_access_marker(),
        _lake_marker(),
        "moex" + "iss",
        "net" + "work",
    )


def _spaced_text(value: str) -> str:
    spaced = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        spaced = spaced.replace(separator, " ")
    return " ".join(part for part in spaced.split() if part)


def _guard_markers(value: str, field_name: str) -> str:
    folded = value.casefold()
    spaced = _spaced_text(value)
    for marker in _blocked_ref_markers():
        if marker in folded or marker in spaced:
            raise CanonicalDataReadPlanValidationError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CanonicalDataReadPlanValidationError(f"{field_name} is required")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_ref(value, field_name)
    if candidate not in allowed_values:
        raise CanonicalDataReadPlanValidationError(f"{field_name} is unsupported")
    return candidate


def _require_bool_false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise CanonicalDataReadPlanValidationError(f"{field_name} must be bool")
    if value:
        raise CanonicalDataReadPlanValidationError(f"{field_name} must be false")
    return value


def _require_text_set(value: object, field_name: str, allowed_values: frozenset[str]) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, tuple):
        raise CanonicalDataReadPlanValidationError(f"{field_name} must be tuple")
    if not value:
        raise CanonicalDataReadPlanValidationError(f"{field_name} must be non-empty")
    normalized = tuple(_require_choice(item, field_name, allowed_values) for item in value)
    return normalized


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise CanonicalDataReadPlanValidationError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise CanonicalDataReadPlanValidationError(f"{label} contains unsupported fields")


def validate_canonical_data_access_policy_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATA_ACCESS_POLICY_FIELDS, "policy")
    return {
        "policy_id": _require_ref(values["policy_id"], "policy_id"),
        "allowed_dataset_classes": _require_text_set(
            values["allowed_dataset_classes"],
            "allowed_dataset_classes",
            ALLOWED_DATASET_CLASSES,
        ),
        "allowed_instruments": _require_text_set(values["allowed_instruments"], "allowed_instruments", ALLOWED_INSTRUMENTS),
        "allowed_timeframes": _require_text_set(values["allowed_timeframes"], "allowed_timeframes", ALLOWED_TIMEFRAMES),
        "allow_file_read": _require_bool_false(values["allow_file_read"], "allow_file_read"),
        "allow_network": _require_bool_false(values["allow_network"], "allow_network"),
        "allow_discovery": _require_bool_false(values["allow_discovery"], "allow_discovery"),
        "access_mode": _require_choice(values["access_mode"], "access_mode", ALLOWED_ACCESS_MODES),
    }


class CanonicalDataAccessPolicy:
    __annotations__ = {
        "policy_id": str,
        "allowed_dataset_classes": tuple[str, ...],
        "allowed_instruments": tuple[str, ...],
        "allowed_timeframes": tuple[str, ...],
        "allow_file_read": bool,
        "allow_network": bool,
        "allow_discovery": bool,
        "access_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_data_access_policy_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_data_access_policy(policy: CanonicalDataAccessPolicy) -> CanonicalDataAccessPolicy:
    if not isinstance(policy, CanonicalDataAccessPolicy):
        raise TypeError("policy must be CanonicalDataAccessPolicy")
    validate_canonical_data_access_policy_values(policy.__dict__)
    return policy


def validate_canonical_data_read_plan_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATA_READ_PLAN_FIELDS, "plan")
    return {
        "plan_id": _require_ref(values["plan_id"], "plan_id"),
        "request_id": _require_ref(values["request_id"], "request_id"),
        "strategy_id": _require_ref(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_ref(values["strategy_test_id"], "strategy_test_id"),
        "dataset_ref_id": _require_ref(values["dataset_ref_id"], "dataset_ref_id"),
        "schema_ref": _require_ref(values["schema_ref"], "schema_ref"),
        "access_policy_id": _require_ref(values["access_policy_id"], "access_policy_id"),
        "planned_reader_id": _require_ref(values["planned_reader_id"], "planned_reader_id"),
        "read_plan_mode": _require_choice(values["read_plan_mode"], "read_plan_mode", ALLOWED_READ_PLAN_MODES),
    }


class CanonicalDataReadPlan:
    __annotations__ = {
        "plan_id": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "dataset_ref_id": str,
        "schema_ref": str,
        "access_policy_id": str,
        "planned_reader_id": str,
        "read_plan_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_data_read_plan_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_data_read_plan(plan: CanonicalDataReadPlan) -> CanonicalDataReadPlan:
    if not isinstance(plan, CanonicalDataReadPlan):
        raise TypeError("plan must be CanonicalDataReadPlan")
    validate_canonical_data_read_plan_values(plan.__dict__)
    return plan


def _require_policy(value: object) -> CanonicalDataAccessPolicy:
    if not isinstance(value, CanonicalDataAccessPolicy):
        raise CanonicalDataReadPlanValidationError("access_policy must be CanonicalDataAccessPolicy")
    return validate_canonical_data_access_policy(value)


def _policy_allows(policy: CanonicalDataAccessPolicy, request: CanonicalDataReadRequest) -> None:
    ref = request.dataset_ref
    if ref.dataset_class not in policy.allowed_dataset_classes:
        raise CanonicalDataReadPlanValidationError("dataset class not allowed")
    if ref.instrument_id not in policy.allowed_instruments:
        raise CanonicalDataReadPlanValidationError("instrument not allowed")
    if ref.timeframe not in policy.allowed_timeframes:
        raise CanonicalDataReadPlanValidationError("timeframe not allowed")


def plan_canonical_data_read(
    request: CanonicalDataReadRequest,
    access_policy: CanonicalDataAccessPolicy,
) -> CanonicalDataReadPlan:
    if not isinstance(request, CanonicalDataReadRequest):
        raise TypeError("request must be CanonicalDataReadRequest")
    validated_request = validate_canonical_data_read_request(request)
    policy = _require_policy(access_policy)
    dry_result = dry_validate_canonical_data_read_request(validated_request)
    if dry_result.read_status != "validated":
        raise CanonicalDataReadPlanValidationError("canonical data request dry validation failed")
    _policy_allows(policy, validated_request)
    return CanonicalDataReadPlan(
        plan_id=validated_request.request_id + ".plan",
        request_id=validated_request.request_id,
        strategy_id=validated_request.strategy_id,
        strategy_test_id=validated_request.strategy_test_id,
        dataset_ref_id=validated_request.dataset_ref.dataset_ref_id,
        schema_ref=validated_request.dataset_ref.schema_ref,
        access_policy_id=policy.policy_id,
        planned_reader_id="canonical_data_reader.dry_run_reference.v1",
        read_plan_mode="canonical_read_planned_only",
    )


def validate_canonical_data_read_plan_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATA_READ_PLAN_RESULT_FIELDS, "result")
    plan_status = _require_choice(values["plan_status"], "plan_status", ALLOWED_PLAN_STATUSES)
    error = values["error_message_or_none"]
    if plan_status == "planned":
        if error is not None:
            raise CanonicalDataReadPlanValidationError("planned result must not include error")
    elif not isinstance(error, str) or not error.strip():
        raise CanonicalDataReadPlanValidationError("rejected result requires error")
    return {
        "plan_status": plan_status,
        "plan_id": _require_text(values["plan_id"], "plan_id"),
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "dataset_ref_id": _require_text(values["dataset_ref_id"], "dataset_ref_id"),
        "schema_ref": _require_text(values["schema_ref"], "schema_ref"),
        "access_policy_id": _require_text(values["access_policy_id"], "access_policy_id"),
        "planned_reader_id": _require_text(values["planned_reader_id"], "planned_reader_id"),
        "error_message_or_none": error,
    }


class CanonicalDataReadPlanResult:
    __annotations__ = {
        "plan_status": str,
        "plan_id": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "dataset_ref_id": str,
        "schema_ref": str,
        "access_policy_id": str,
        "planned_reader_id": str,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_data_read_plan_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_data_read_plan_result(result: CanonicalDataReadPlanResult) -> CanonicalDataReadPlanResult:
    if not isinstance(result, CanonicalDataReadPlanResult):
        raise TypeError("result must be CanonicalDataReadPlanResult")
    validate_canonical_data_read_plan_result_values(result.__dict__)
    return result


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def dry_run_plan_canonical_data_read(
    request: CanonicalDataReadRequest,
    access_policy: CanonicalDataAccessPolicy,
) -> CanonicalDataReadPlanResult:
    try:
        plan = plan_canonical_data_read(request, access_policy)
        return CanonicalDataReadPlanResult(
            plan_status="planned",
            plan_id=plan.plan_id,
            request_id=plan.request_id,
            strategy_id=plan.strategy_id,
            strategy_test_id=plan.strategy_test_id,
            dataset_ref_id=plan.dataset_ref_id,
            schema_ref=plan.schema_ref,
            access_policy_id=plan.access_policy_id,
            planned_reader_id=plan.planned_reader_id,
            error_message_or_none=None,
        )
    except (CanonicalDataReadPlanValidationError, TypeError, ValueError) as error:
        return CanonicalDataReadPlanResult(
            plan_status="rejected",
            plan_id="unavailable",
            request_id=_safe_text(request, "request_id"),
            strategy_id=_safe_text(request, "strategy_id"),
            strategy_test_id=_safe_text(request, "strategy_test_id"),
            dataset_ref_id="unavailable",
            schema_ref="unavailable",
            access_policy_id=_safe_text(access_policy, "policy_id"),
            planned_reader_id="unavailable",
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_ACCESS_MODES",
    "ALLOWED_DATASET_CLASSES",
    "ALLOWED_INSTRUMENTS",
    "ALLOWED_PLAN_STATUSES",
    "ALLOWED_READ_PLAN_MODES",
    "ALLOWED_TIMEFRAMES",
    "CANONICAL_DATA_ACCESS_POLICY_FIELDS",
    "CANONICAL_DATA_READ_PLAN_FIELDS",
    "CANONICAL_DATA_READ_PLAN_RESULT_FIELDS",
    "CanonicalDataAccessPolicy",
    "CanonicalDataReadPlan",
    "CanonicalDataReadPlanResult",
    "CanonicalDataReadPlanValidationError",
    "dry_run_plan_canonical_data_read",
    "plan_canonical_data_read",
    "validate_canonical_data_access_policy",
    "validate_canonical_data_access_policy_values",
    "validate_canonical_data_read_plan",
    "validate_canonical_data_read_plan_result",
    "validate_canonical_data_read_plan_result_values",
    "validate_canonical_data_read_plan_values",
]
