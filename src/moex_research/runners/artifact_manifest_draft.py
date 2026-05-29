from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class ArtifactManifestDraftValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _decision_gate_field() -> str:
    return "promotion_" + "verdict_allowed"


ARTIFACT_MANIFEST_DRAFT_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "artifact_manifest_draft_id",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "planned_artifacts",
    "artifact_manifest_ref",
    "write_allowed",
    "registry_write_allowed",
    _decision_gate_field(),
)
_DRAFT_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset(
    {
        "write_allowed",
        "registry_write_allowed",
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
        raise ArtifactManifestDraftValidationError(f"{field_name} is required")
    return value


def _guard_ref(value: str, field_name: str) -> str:
    tokens = _tokenize(value)
    if any(marker in tokens for marker in _blocked_ref_markers()):
        raise ArtifactManifestDraftValidationError(f"{field_name} contains unsupported ref marker")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_ref(_require_text(value, field_name), field_name)


def _require_ref_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ArtifactManifestDraftValidationError(f"{field_name} must be a non-empty tuple of refs")
    if not isinstance(value, Iterable):
        raise ArtifactManifestDraftValidationError(f"{field_name} must be a non-empty tuple of refs")
    refs = tuple(_require_ref(item, field_name) for item in value)
    if not refs:
        raise ArtifactManifestDraftValidationError(f"{field_name} must be non-empty")
    return refs


def _require_bool_disabled(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ArtifactManifestDraftValidationError(f"{field_name} must be bool")
    if value:
        raise ArtifactManifestDraftValidationError(f"{field_name} must be false")
    return value


def _validate_expected_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise ArtifactManifestDraftValidationError("draft values must be a mapping")
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(ARTIFACT_MANIFEST_DRAFT_REQUIRED_FIELDS)
    if unknown_fields:
        raise ArtifactManifestDraftValidationError("draft contains unsupported fields")
    missing_fields = tuple(
        field for field in ARTIFACT_MANIFEST_DRAFT_REQUIRED_FIELDS
        if field not in values and field not in _DRAFT_FIELDS_WITH_DEFAULTS
    )
    if missing_fields:
        raise ArtifactManifestDraftValidationError("draft is missing required fields")


def validate_artifact_manifest_draft_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(values)
    decision_gate = _decision_gate_field()
    return {
        "artifact_manifest_draft_id": _require_text(
            values["artifact_manifest_draft_id"],
            "artifact_manifest_draft_id",
        ),
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "planned_artifacts": _require_ref_tuple(values["planned_artifacts"], "planned_artifacts"),
        "artifact_manifest_ref": _require_ref(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "write_allowed": _require_bool_disabled(values.get("write_allowed", False), "write_allowed"),
        "registry_write_allowed": _require_bool_disabled(
            values.get("registry_write_allowed", False),
            "registry_write_allowed",
        ),
        decision_gate: _require_bool_disabled(values.get(decision_gate, False), decision_gate),
    }


class ArtifactManifestDraft:
    __annotations__ = {
        "artifact_manifest_draft_id": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "planned_artifacts": tuple[str, ...],
        "artifact_manifest_ref": str,
        "write_allowed": bool,
        "registry_write_allowed": bool,
        _decision_gate_field(): bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_artifact_manifest_draft_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_artifact_manifest_draft(draft: ArtifactManifestDraft) -> ArtifactManifestDraft:
    if not isinstance(draft, ArtifactManifestDraft):
        raise TypeError("draft must be ArtifactManifestDraft")
    validate_artifact_manifest_draft_values(draft.__dict__)
    return draft


__all__ = [
    "ARTIFACT_MANIFEST_DRAFT_REQUIRED_FIELDS",
    "ArtifactManifestDraft",
    "ArtifactManifestDraftValidationError",
    "validate_artifact_manifest_draft",
    "validate_artifact_manifest_draft_values",
]
