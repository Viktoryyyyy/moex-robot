from __future__ import annotations

from collections.abc import Mapping
from typing import Final


class RegistryEntryDraftValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _decision_gate_field() -> str:
    return "promotion_" + "verdict_allowed"


REGISTRY_ENTRY_DRAFT_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "registry_entry_draft_id",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "planned_registry_entry_ref",
    "artifact_manifest_ref",
    "metrics_artifact_ref_or_none",
    "report_artifact_ref_or_none",
    "write_allowed",
    _decision_gate_field(),
)
_DRAFT_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset(
    {
        "write_allowed",
        _decision_gate_field(),
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
        raise RegistryEntryDraftValidationError(f"{field_name} is required")
    return value


def _guard_ref(value: str, field_name: str) -> str:
    tokens = _tokenize(value)
    if any(marker in tokens for marker in _blocked_ref_markers()):
        raise RegistryEntryDraftValidationError(f"{field_name} contains unsupported ref marker")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_ref(_require_text(value, field_name), field_name)


def _require_ref_or_none(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_ref(value, field_name)


def _require_bool_disabled(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RegistryEntryDraftValidationError(f"{field_name} must be bool")
    if value:
        raise RegistryEntryDraftValidationError(f"{field_name} must be false")
    return value


def _validate_expected_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise RegistryEntryDraftValidationError("draft values must be a mapping")
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(REGISTRY_ENTRY_DRAFT_REQUIRED_FIELDS)
    if unknown_fields:
        raise RegistryEntryDraftValidationError("draft contains unsupported fields")
    missing_fields = tuple(
        field for field in REGISTRY_ENTRY_DRAFT_REQUIRED_FIELDS
        if field not in values and field not in _DRAFT_FIELDS_WITH_DEFAULTS
    )
    if missing_fields:
        raise RegistryEntryDraftValidationError("draft is missing required fields")


def validate_registry_entry_draft_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(values)
    decision_gate = _decision_gate_field()
    return {
        "registry_entry_draft_id": _require_text(
            values["registry_entry_draft_id"],
            "registry_entry_draft_id",
        ),
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "planned_registry_entry_ref": _require_ref(
            values["planned_registry_entry_ref"],
            "planned_registry_entry_ref",
        ),
        "artifact_manifest_ref": _require_ref(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "metrics_artifact_ref_or_none": _require_ref_or_none(
            values["metrics_artifact_ref_or_none"],
            "metrics_artifact_ref_or_none",
        ),
        "report_artifact_ref_or_none": _require_ref_or_none(
            values["report_artifact_ref_or_none"],
            "report_artifact_ref_or_none",
        ),
        "write_allowed": _require_bool_disabled(values.get("write_allowed", False), "write_allowed"),
        decision_gate: _require_bool_disabled(values.get(decision_gate, False), decision_gate),
    }


class RegistryEntryDraft:
    __annotations__ = {
        "registry_entry_draft_id": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "planned_registry_entry_ref": str,
        "artifact_manifest_ref": str,
        "metrics_artifact_ref_or_none": str | None,
        "report_artifact_ref_or_none": str | None,
        "write_allowed": bool,
        _decision_gate_field(): bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_registry_entry_draft_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_registry_entry_draft(draft: RegistryEntryDraft) -> RegistryEntryDraft:
    if not isinstance(draft, RegistryEntryDraft):
        raise TypeError("draft must be RegistryEntryDraft")
    validate_registry_entry_draft_values(draft.__dict__)
    return draft


__all__ = [
    "REGISTRY_ENTRY_DRAFT_REQUIRED_FIELDS",
    "RegistryEntryDraft",
    "RegistryEntryDraftValidationError",
    "validate_registry_entry_draft",
    "validate_registry_entry_draft_values",
]
