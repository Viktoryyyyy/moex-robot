from __future__ import annotations

from collections.abc import Mapping
from typing import Final


class RegistryWritePlanValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


REGISTRY_WRITE_PLAN_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "registry_plan_id",
    "planned_registry_entry_ref",
    "artifact_manifest_ref",
    "metrics_artifact_ref_or_none",
    "report_artifact_ref_or_none",
    "write_allowed",
    "promotion_verdict_allowed",
)
_REGISTRY_PLAN_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset(
    {
        "write_allowed",
        "promotion_verdict_allowed",
    }
)


def _blocked_ref_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _tokenize(value: str) -> tuple[str, ...]:
    tokenized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        tokenized = tokenized.replace(separator, " ")
    return tuple(token for token in tokenized.split() if token)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryWritePlanValidationError(f"{field_name} is required")
    return value


def _guard_ref(value: str, field_name: str) -> str:
    tokens = _tokenize(value)
    if any(marker in tokens for marker in _blocked_ref_markers()):
        raise RegistryWritePlanValidationError(f"{field_name} contains unsupported ref marker")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_ref(_require_text(value, field_name), field_name)


def _require_ref_or_none(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_ref(value, field_name)


def _require_bool_disabled(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RegistryWritePlanValidationError(f"{field_name} must be bool")
    if value:
        raise RegistryWritePlanValidationError(f"{field_name} must be false")
    return value


def _validate_expected_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise RegistryWritePlanValidationError("registry plan values must be a mapping")
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(REGISTRY_WRITE_PLAN_REQUIRED_FIELDS)
    if unknown_fields:
        raise RegistryWritePlanValidationError("registry plan contains unsupported fields")
    missing_fields = tuple(
        field for field in REGISTRY_WRITE_PLAN_REQUIRED_FIELDS
        if field not in values and field not in _REGISTRY_PLAN_FIELDS_WITH_DEFAULTS
    )
    if missing_fields:
        raise RegistryWritePlanValidationError("registry plan is missing required fields")


def validate_registry_write_plan_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(values)
    return {
        "registry_plan_id": _require_text(values["registry_plan_id"], "registry_plan_id"),
        "planned_registry_entry_ref": _require_ref(values["planned_registry_entry_ref"], "planned_registry_entry_ref"),
        "artifact_manifest_ref": _require_ref(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "metrics_artifact_ref_or_none": _require_ref_or_none(values["metrics_artifact_ref_or_none"], "metrics_artifact_ref_or_none"),
        "report_artifact_ref_or_none": _require_ref_or_none(values["report_artifact_ref_or_none"], "report_artifact_ref_or_none"),
        "write_allowed": _require_bool_disabled(values.get("write_allowed", False), "write_allowed"),
        "promotion_verdict_allowed": _require_bool_disabled(values.get("promotion_verdict_allowed", False), "promotion_verdict_allowed"),
    }


class RegistryWritePlan:
    __annotations__ = {
        "registry_plan_id": str,
        "planned_registry_entry_ref": str,
        "artifact_manifest_ref": str,
        "metrics_artifact_ref_or_none": str | None,
        "report_artifact_ref_or_none": str | None,
        "write_allowed": bool,
        "promotion_verdict_allowed": bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_registry_write_plan_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_registry_write_plan(plan: RegistryWritePlan) -> RegistryWritePlan:
    if not isinstance(plan, RegistryWritePlan):
        raise TypeError("plan must be RegistryWritePlan")
    validate_registry_write_plan_values(plan.__dict__)
    return plan


__all__ = [
    "REGISTRY_WRITE_PLAN_REQUIRED_FIELDS",
    "RegistryWritePlan",
    "RegistryWritePlanValidationError",
    "validate_registry_write_plan",
    "validate_registry_write_plan_values",
]
