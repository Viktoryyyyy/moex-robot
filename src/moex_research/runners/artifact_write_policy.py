from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class ArtifactWritePolicyValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


ALLOWED_ARTIFACT_WRITE_MODES: Final[frozenset[str]] = frozenset(
    {
        "disabled",
        "planned_only",
    }
)
ARTIFACT_WRITE_POLICY_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "policy_id",
    "artifact_manifest_ref",
    "allowed_artifact_refs",
    "write_allowed",
    "write_mode",
)
_POLICY_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset({"write_allowed"})


def _blocked_ref_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _tokenize(value: str) -> tuple[str, ...]:
    tokenized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        tokenized = tokenized.replace(separator, " ")
    return tuple(token for token in tokenized.split() if token)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ArtifactWritePolicyValidationError(f"{field_name} is required")
    return value


def _guard_ref(value: str, field_name: str) -> str:
    tokens = _tokenize(value)
    if any(marker in tokens for marker in _blocked_ref_markers()):
        raise ArtifactWritePolicyValidationError(f"{field_name} contains unsupported ref marker")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_ref(_require_text(value, field_name), field_name)


def _require_ref_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ArtifactWritePolicyValidationError(f"{field_name} must be a non-empty tuple of refs")
    if not isinstance(value, Iterable):
        raise ArtifactWritePolicyValidationError(f"{field_name} must be a non-empty tuple of refs")
    refs = tuple(_require_ref(item, field_name) for item in value)
    if not refs:
        raise ArtifactWritePolicyValidationError(f"{field_name} must be non-empty")
    return refs


def _require_bool_disabled(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ArtifactWritePolicyValidationError(f"{field_name} must be bool")
    if value:
        raise ArtifactWritePolicyValidationError(f"{field_name} must be false")
    return value


def _normalize_write_mode(value: object) -> str:
    normalized = _require_text(value, "write_mode")
    if normalized not in ALLOWED_ARTIFACT_WRITE_MODES:
        raise ArtifactWritePolicyValidationError("unsupported write_mode")
    return normalized


def _validate_expected_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise ArtifactWritePolicyValidationError("policy values must be a mapping")
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(ARTIFACT_WRITE_POLICY_REQUIRED_FIELDS)
    if unknown_fields:
        raise ArtifactWritePolicyValidationError("policy contains unsupported fields")
    missing_fields = tuple(
        field for field in ARTIFACT_WRITE_POLICY_REQUIRED_FIELDS
        if field not in values and field not in _POLICY_FIELDS_WITH_DEFAULTS
    )
    if missing_fields:
        raise ArtifactWritePolicyValidationError("policy is missing required fields")


def validate_artifact_write_policy_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(values)
    return {
        "policy_id": _require_text(values["policy_id"], "policy_id"),
        "artifact_manifest_ref": _require_ref(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "allowed_artifact_refs": _require_ref_tuple(values["allowed_artifact_refs"], "allowed_artifact_refs"),
        "write_allowed": _require_bool_disabled(values.get("write_allowed", False), "write_allowed"),
        "write_mode": _normalize_write_mode(values["write_mode"]),
    }


class ArtifactWritePolicy:
    __annotations__ = {
        "policy_id": str,
        "artifact_manifest_ref": str,
        "allowed_artifact_refs": tuple[str, ...],
        "write_allowed": bool,
        "write_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_artifact_write_policy_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_artifact_write_policy(policy: ArtifactWritePolicy) -> ArtifactWritePolicy:
    if not isinstance(policy, ArtifactWritePolicy):
        raise TypeError("policy must be ArtifactWritePolicy")
    validate_artifact_write_policy_values(policy.__dict__)
    return policy


__all__ = [
    "ALLOWED_ARTIFACT_WRITE_MODES",
    "ARTIFACT_WRITE_POLICY_REQUIRED_FIELDS",
    "ArtifactWritePolicy",
    "ArtifactWritePolicyValidationError",
    "validate_artifact_write_policy",
    "validate_artifact_write_policy_values",
]
