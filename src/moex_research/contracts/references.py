from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import Enum
from typing import Final


class ReferenceValidationError(ValueError):
    pass


class ArtifactClass(str, Enum):
    REPO_RELATIVE = "repo_relative"
    EXTERNAL_PATTERN = "external_pattern"
    CLI_ARGUMENT = "cli_argument"
    ENV_CONTRACT = "env_contract"


class LabelClass(str, Enum):
    PRIMARY_RESEARCH = "primary_research"
    SECONDARY_EXECUTION_COMPATIBLE = "secondary_execution_compatible"


ALLOWED_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset(item.value for item in ArtifactClass)
ALLOWED_LABEL_CLASSES: Final[frozenset[str]] = frozenset(item.value for item in LabelClass)

DATASET_REF_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "ref_id",
    "dataset_id",
    "schema_version",
    "artifact_class",
    "producer",
    "consumer",
    "known_by_when",
    "quality_status",
)
FEATURE_REF_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "ref_id",
    "feature_id",
    "feature_version",
    "input_dataset_refs",
    "known_by_when",
    "anti_leakage_rule",
    "producer",
    "consumer",
)
LABEL_REF_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "ref_id",
    "label_id",
    "label_version",
    "label_class",
    "anchor",
    "outcome_window",
    "known_by_when",
    "producer",
    "consumer",
)
SIGNAL_REF_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "ref_id",
    "signal_id",
    "strategy_id",
    "signal_version",
    "input_feature_refs",
    "known_by_when",
    "signal_timestamp_rule",
    "producer",
    "consumer",
)


_IDENTIFIER_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "ref_id",
        "dataset_id",
        "feature_id",
        "feature_version",
        "label_id",
        "label_version",
        "signal_id",
        "strategy_id",
        "signal_version",
        "schema_version",
    }
)
_REF_LIST_FIELDS: Final[frozenset[str]] = frozenset({"input_dataset_refs", "input_feature_refs"})


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReferenceValidationError(f"{field_name} is required")
    return value


def _require_text_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ReferenceValidationError(f"{field_name} must be a non-empty tuple of strings")
    if not isinstance(value, Iterable):
        raise ReferenceValidationError(f"{field_name} must be a non-empty tuple of strings")
    items = tuple(value)
    if not items:
        raise ReferenceValidationError(f"{field_name} must be non-empty")
    if any(not isinstance(item, str) or not item.strip() for item in items):
        raise ReferenceValidationError(f"{field_name} must contain non-empty strings")
    return items


def _normalize_artifact_class(value: object) -> str:
    if isinstance(value, ArtifactClass):
        value = value.value
    value = _require_text(value, "artifact_class")
    if value not in ALLOWED_ARTIFACT_CLASSES:
        raise ReferenceValidationError("unsupported artifact_class")
    return value


def _normalize_label_class(value: object) -> str:
    if isinstance(value, LabelClass):
        value = value.value
    value = _require_text(value, "label_class")
    if value not in ALLOWED_LABEL_CLASSES:
        raise ReferenceValidationError("unsupported label_class")
    return value


def _validate_values(values: Mapping[str, object], required_fields: tuple[str, ...]) -> dict[str, object]:
    if not isinstance(values, Mapping):
        raise ReferenceValidationError("reference values must be a mapping")

    expected_fields = set(required_fields)
    provided_fields = set(values)
    if provided_fields.difference(expected_fields):
        raise ReferenceValidationError("reference contains unsupported fields")

    missing_fields = tuple(field for field in required_fields if field not in values)
    if missing_fields:
        raise ReferenceValidationError("reference is missing required fields")

    normalized: dict[str, object] = {}
    for field in required_fields:
        value = values[field]
        if field in _IDENTIFIER_FIELDS:
            normalized[field] = _require_text(value, field)
        elif field in _REF_LIST_FIELDS:
            normalized[field] = _require_text_tuple(value, field)
        elif field == "artifact_class":
            normalized[field] = _normalize_artifact_class(value)
        elif field == "label_class":
            normalized[field] = _normalize_label_class(value)
        else:
            normalized[field] = _require_text(value, field)
    return normalized


def validate_dataset_ref_values(values: Mapping[str, object]) -> dict[str, object]:
    return _validate_values(values, DATASET_REF_REQUIRED_FIELDS)


def validate_feature_ref_values(values: Mapping[str, object]) -> dict[str, object]:
    return _validate_values(values, FEATURE_REF_REQUIRED_FIELDS)


def validate_label_ref_values(values: Mapping[str, object]) -> dict[str, object]:
    return _validate_values(values, LABEL_REF_REQUIRED_FIELDS)


def validate_signal_ref_values(values: Mapping[str, object]) -> dict[str, object]:
    return _validate_values(values, SIGNAL_REF_REQUIRED_FIELDS)


class DatasetRef:
    __annotations__ = {
        "ref_id": str,
        "dataset_id": str,
        "schema_version": str,
        "artifact_class": str,
        "producer": str,
        "consumer": str,
        "known_by_when": str,
        "quality_status": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_dataset_ref_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


class FeatureRef:
    __annotations__ = {
        "ref_id": str,
        "feature_id": str,
        "feature_version": str,
        "input_dataset_refs": tuple[str, ...],
        "known_by_when": str,
        "anti_leakage_rule": str,
        "producer": str,
        "consumer": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_feature_ref_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


class LabelRef:
    __annotations__ = {
        "ref_id": str,
        "label_id": str,
        "label_version": str,
        "label_class": str,
        "anchor": str,
        "outcome_window": str,
        "known_by_when": str,
        "producer": str,
        "consumer": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_label_ref_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


class SignalRef:
    __annotations__ = {
        "ref_id": str,
        "signal_id": str,
        "strategy_id": str,
        "signal_version": str,
        "input_feature_refs": tuple[str, ...],
        "known_by_when": str,
        "signal_timestamp_rule": str,
        "producer": str,
        "consumer": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_signal_ref_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_dataset_ref(reference: DatasetRef) -> DatasetRef:
    if not isinstance(reference, DatasetRef):
        raise TypeError("reference must be DatasetRef")
    validate_dataset_ref_values(reference.__dict__)
    return reference


def validate_feature_ref(reference: FeatureRef) -> FeatureRef:
    if not isinstance(reference, FeatureRef):
        raise TypeError("reference must be FeatureRef")
    validate_feature_ref_values(reference.__dict__)
    return reference


def validate_label_ref(reference: LabelRef) -> LabelRef:
    if not isinstance(reference, LabelRef):
        raise TypeError("reference must be LabelRef")
    validate_label_ref_values(reference.__dict__)
    return reference


def validate_signal_ref(reference: SignalRef) -> SignalRef:
    if not isinstance(reference, SignalRef):
        raise TypeError("reference must be SignalRef")
    validate_signal_ref_values(reference.__dict__)
    return reference


__all__ = [
    "ALLOWED_ARTIFACT_CLASSES",
    "ALLOWED_LABEL_CLASSES",
    "DATASET_REF_REQUIRED_FIELDS",
    "FEATURE_REF_REQUIRED_FIELDS",
    "LABEL_REF_REQUIRED_FIELDS",
    "SIGNAL_REF_REQUIRED_FIELDS",
    "ArtifactClass",
    "DatasetRef",
    "FeatureRef",
    "LabelClass",
    "LabelRef",
    "ReferenceValidationError",
    "SignalRef",
    "validate_dataset_ref",
    "validate_dataset_ref_values",
    "validate_feature_ref",
    "validate_feature_ref_values",
    "validate_label_ref",
    "validate_label_ref_values",
    "validate_signal_ref",
    "validate_signal_ref_values",
]
