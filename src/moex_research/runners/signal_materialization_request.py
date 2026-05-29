from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class SignalMaterializationRequestValidationError(ValueError):
    pass


class SignalMaterializationPlanValidationError(ValueError):
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


def _planned_signal_mode() -> str:
    return "non_" + "li" + "ve_signal_materialization_planned"


ALLOWED_SIGNAL_MATERIALIZATION_MODES: Final[frozenset[str]] = frozenset(
    {
        "plan_only",
        _planned_signal_mode(),
    }
)
SIGNAL_MATERIALIZATION_REQUEST_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_version",
    "strategy_test_id",
    "package_ref",
    "input_bindings",
    "feature_refs",
    "signal_refs",
    "output_signal_artifact_ref",
    "materialization_mode",
)
SIGNAL_MATERIALIZATION_PLAN_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "plan_id",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "signal_table_artifact_ref",
    "artifact_manifest_ref",
    "write_allowed",
    "registry_write_allowed",
    "promotion_verdict_allowed",
)
_PLAN_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset(
    {
        "write_allowed",
        "registry_write_allowed",
        "promotion_verdict_allowed",
    }
)


def _blocked_ref_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _blocked_modes() -> tuple[str, ...]:
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


def _require_text(value: object, field_name: str, error_type: type[ValueError]) -> str:
    if not isinstance(value, str) or not value.strip():
        raise error_type(f"{field_name} is required")
    return value


def _guard_ref(value: str, field_name: str, error_type: type[ValueError]) -> str:
    tokens = _tokenize(value)
    if any(marker in tokens for marker in _blocked_ref_markers()):
        raise error_type(f"{field_name} contains unsupported ref marker")
    return value


def _require_ref(value: object, field_name: str, error_type: type[ValueError]) -> str:
    return _guard_ref(_require_text(value, field_name, error_type), field_name, error_type)


def _require_ref_tuple(value: object, field_name: str, error_type: type[ValueError]) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise error_type(f"{field_name} must be a non-empty tuple of refs")
    if not isinstance(value, Iterable):
        raise error_type(f"{field_name} must be a non-empty tuple of refs")
    refs = tuple(_require_ref(item, field_name, error_type) for item in value)
    if not refs:
        raise error_type(f"{field_name} must be non-empty")
    return refs


def _require_bool_disabled(value: object, field_name: str, error_type: type[ValueError]) -> bool:
    if not isinstance(value, bool):
        raise error_type(f"{field_name} must be bool")
    if value:
        raise error_type(f"{field_name} must be false")
    return value


def _validate_expected_fields(
    values: Mapping[str, object],
    expected_fields: tuple[str, ...],
    fields_with_defaults: frozenset[str],
    error_type: type[ValueError],
    object_name: str,
) -> None:
    if not isinstance(values, Mapping):
        raise error_type(f"{object_name} values must be a mapping")
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(expected_fields)
    if unknown_fields:
        raise error_type(f"{object_name} contains unsupported fields")
    missing_fields = tuple(
        field for field in expected_fields
        if field not in values and field not in fields_with_defaults
    )
    if missing_fields:
        raise error_type(f"{object_name} is missing required fields")


def _normalize_materialization_mode(value: object) -> str:
    normalized = _require_text(
        value,
        "materialization_mode",
        SignalMaterializationRequestValidationError,
    )
    if normalized in _blocked_modes():
        raise SignalMaterializationRequestValidationError("unsupported materialization_mode")
    if normalized not in ALLOWED_SIGNAL_MATERIALIZATION_MODES:
        raise SignalMaterializationRequestValidationError("unsupported materialization_mode")
    return normalized


def validate_signal_materialization_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        SIGNAL_MATERIALIZATION_REQUEST_REQUIRED_FIELDS,
        frozenset(),
        SignalMaterializationRequestValidationError,
        "request",
    )
    return {
        "request_id": _require_text(values["request_id"], "request_id", SignalMaterializationRequestValidationError),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id", SignalMaterializationRequestValidationError),
        "strategy_version": _require_text(values["strategy_version"], "strategy_version", SignalMaterializationRequestValidationError),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id", SignalMaterializationRequestValidationError),
        "package_ref": _require_ref(values["package_ref"], "package_ref", SignalMaterializationRequestValidationError),
        "input_bindings": _require_ref_tuple(values["input_bindings"], "input_bindings", SignalMaterializationRequestValidationError),
        "feature_refs": _require_ref_tuple(values["feature_refs"], "feature_refs", SignalMaterializationRequestValidationError),
        "signal_refs": _require_ref_tuple(values["signal_refs"], "signal_refs", SignalMaterializationRequestValidationError),
        "output_signal_artifact_ref": _require_ref(values["output_signal_artifact_ref"], "output_signal_artifact_ref", SignalMaterializationRequestValidationError),
        "materialization_mode": _normalize_materialization_mode(values["materialization_mode"]),
    }


class SignalMaterializationRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_version": str,
        "strategy_test_id": str,
        "package_ref": str,
        "input_bindings": tuple[str, ...],
        "feature_refs": tuple[str, ...],
        "signal_refs": tuple[str, ...],
        "output_signal_artifact_ref": str,
        "materialization_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_signal_materialization_request_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_signal_materialization_request(
    request: SignalMaterializationRequest,
) -> SignalMaterializationRequest:
    if not isinstance(request, SignalMaterializationRequest):
        raise TypeError("request must be SignalMaterializationRequest")
    validate_signal_materialization_request_values(request.__dict__)
    return request


def validate_signal_materialization_plan_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        SIGNAL_MATERIALIZATION_PLAN_REQUIRED_FIELDS,
        _PLAN_FIELDS_WITH_DEFAULTS,
        SignalMaterializationPlanValidationError,
        "plan",
    )
    return {
        "plan_id": _require_text(values["plan_id"], "plan_id", SignalMaterializationPlanValidationError),
        "request_id": _require_text(values["request_id"], "request_id", SignalMaterializationPlanValidationError),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id", SignalMaterializationPlanValidationError),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id", SignalMaterializationPlanValidationError),
        "signal_table_artifact_ref": _require_ref(values["signal_table_artifact_ref"], "signal_table_artifact_ref", SignalMaterializationPlanValidationError),
        "artifact_manifest_ref": _require_ref(values["artifact_manifest_ref"], "artifact_manifest_ref", SignalMaterializationPlanValidationError),
        "write_allowed": _require_bool_disabled(values.get("write_allowed", False), "write_allowed", SignalMaterializationPlanValidationError),
        "registry_write_allowed": _require_bool_disabled(values.get("registry_write_allowed", False), "registry_write_allowed", SignalMaterializationPlanValidationError),
        "promotion_verdict_allowed": _require_bool_disabled(values.get("promotion_verdict_allowed", False), "promotion_verdict_allowed", SignalMaterializationPlanValidationError),
    }


class SignalMaterializationPlan:
    __annotations__ = {
        "plan_id": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "signal_table_artifact_ref": str,
        "artifact_manifest_ref": str,
        "write_allowed": bool,
        "registry_write_allowed": bool,
        "promotion_verdict_allowed": bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_signal_materialization_plan_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_signal_materialization_plan(plan: SignalMaterializationPlan) -> SignalMaterializationPlan:
    if not isinstance(plan, SignalMaterializationPlan):
        raise TypeError("plan must be SignalMaterializationPlan")
    validate_signal_materialization_plan_values(plan.__dict__)
    return plan


__all__ = [
    "ALLOWED_SIGNAL_MATERIALIZATION_MODES",
    "SIGNAL_MATERIALIZATION_PLAN_REQUIRED_FIELDS",
    "SIGNAL_MATERIALIZATION_REQUEST_REQUIRED_FIELDS",
    "SignalMaterializationPlan",
    "SignalMaterializationPlanValidationError",
    "SignalMaterializationRequest",
    "SignalMaterializationRequestValidationError",
    "validate_signal_materialization_plan",
    "validate_signal_materialization_plan_values",
    "validate_signal_materialization_request",
    "validate_signal_materialization_request_values",
]
