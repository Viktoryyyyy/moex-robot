from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class ReportArtifactValidationError(ValueError):
    pass


ALLOWED_REPORT_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset(
    {"repo_relative", "external_pattern", "cli_argument", "env_contract"}
)
ALLOWED_REPORT_FORMATS: Final[frozenset[str]] = frozenset({"markdown", "json"})
REPORT_SECTION_SPEC_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "section_id",
    "title",
    "required",
)
REPORT_ARTIFACT_SPEC_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "report_id",
    "run_id",
    "strategy_id",
    "report_schema_version",
    "artifact_class",
    "producer",
    "consumer",
    "format",
    "required_sections",
)


def _require_mapping(values: object) -> Mapping[str, object]:
    if not isinstance(values, Mapping):
        raise ReportArtifactValidationError("values must be a mapping")
    return values


def _require_exact_fields(values: Mapping[str, object], required_fields: tuple[str, ...]) -> None:
    expected_fields = set(required_fields)
    provided_fields = set(values)
    if provided_fields.difference(expected_fields):
        raise ReportArtifactValidationError("values contain unsupported fields")
    missing_fields = tuple(field for field in required_fields if field not in values)
    if missing_fields:
        raise ReportArtifactValidationError("values are missing required fields")


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReportArtifactValidationError(f"{field_name} is required")
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ReportArtifactValidationError(f"{field_name} must be bool")
    return value


def _normalize_artifact_class(value: object) -> str:
    value = _require_text(value, "artifact_class")
    if value not in ALLOWED_REPORT_ARTIFACT_CLASSES:
        raise ReportArtifactValidationError("unsupported artifact_class")
    return value


def _normalize_format(value: object) -> str:
    value = _require_text(value, "format")
    if value not in ALLOWED_REPORT_FORMATS:
        raise ReportArtifactValidationError("unsupported format")
    return value


def _require_sections(value: object) -> tuple["ReportSectionSpec", ...]:
    if isinstance(value, (str, bytes)):
        raise ReportArtifactValidationError("required_sections must be a non-empty iterable")
    if not isinstance(value, Iterable):
        raise ReportArtifactValidationError("required_sections must be a non-empty iterable")
    sections = tuple(value)
    if not sections:
        raise ReportArtifactValidationError("required_sections must be non-empty")
    for section in sections:
        if not isinstance(section, ReportSectionSpec):
            raise ReportArtifactValidationError(
                "required_sections must contain ReportSectionSpec instances"
            )
        validate_report_section_spec(section)
    return sections


def validate_report_section_spec_values(values: Mapping[str, object]) -> dict[str, object]:
    values = _require_mapping(values)
    _require_exact_fields(values, REPORT_SECTION_SPEC_REQUIRED_FIELDS)
    return {
        "section_id": _require_text(values["section_id"], "section_id"),
        "title": _require_text(values["title"], "title"),
        "required": _require_bool(values["required"], "required"),
    }


def validate_report_artifact_spec_values(values: Mapping[str, object]) -> dict[str, object]:
    values = _require_mapping(values)
    _require_exact_fields(values, REPORT_ARTIFACT_SPEC_REQUIRED_FIELDS)
    return {
        "report_id": _require_text(values["report_id"], "report_id"),
        "run_id": _require_text(values["run_id"], "run_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "report_schema_version": _require_text(
            values["report_schema_version"], "report_schema_version"
        ),
        "artifact_class": _normalize_artifact_class(values["artifact_class"]),
        "producer": _require_text(values["producer"], "producer"),
        "consumer": _require_text(values["consumer"], "consumer"),
        "format": _normalize_format(values["format"]),
        "required_sections": _require_sections(values["required_sections"]),
    }


class ReportSectionSpec:
    __annotations__ = {
        "section_id": str,
        "title": str,
        "required": bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_report_section_spec_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


class ReportArtifactSpec:
    __annotations__ = {
        "report_id": str,
        "run_id": str,
        "strategy_id": str,
        "report_schema_version": str,
        "artifact_class": str,
        "producer": str,
        "consumer": str,
        "format": str,
        "required_sections": tuple[ReportSectionSpec, ...],
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_report_artifact_spec_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_report_section_spec(section: ReportSectionSpec) -> ReportSectionSpec:
    if not isinstance(section, ReportSectionSpec):
        raise TypeError("section must be ReportSectionSpec")
    validate_report_section_spec_values(section.__dict__)
    return section


def validate_report_artifact_spec(spec: ReportArtifactSpec) -> ReportArtifactSpec:
    if not isinstance(spec, ReportArtifactSpec):
        raise TypeError("spec must be ReportArtifactSpec")
    validate_report_artifact_spec_values(spec.__dict__)
    return spec


__all__ = [
    "ALLOWED_REPORT_ARTIFACT_CLASSES",
    "ALLOWED_REPORT_FORMATS",
    "REPORT_ARTIFACT_SPEC_REQUIRED_FIELDS",
    "REPORT_SECTION_SPEC_REQUIRED_FIELDS",
    "ReportArtifactSpec",
    "ReportArtifactValidationError",
    "ReportSectionSpec",
    "validate_report_artifact_spec",
    "validate_report_artifact_spec_values",
    "validate_report_section_spec",
    "validate_report_section_spec_values",
]
