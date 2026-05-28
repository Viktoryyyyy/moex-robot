from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class MetricsValidationError(ValueError):
    pass


ALLOWED_GROSS_OR_NET: Final[frozenset[str]] = frozenset(
    {"gross", "net", "not_applicable"}
)
METRIC_RECORD_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "metric_id",
    "metric_name",
    "metric_value",
    "metric_unit",
    "scope",
    "gross_or_net",
    "producer",
    "consumer",
)
METRICS_SUMMARY_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "run_id",
    "strategy_id",
    "test_type",
    "scope_level",
    "result_status",
    "canonicality_status",
    "metric_schema_version",
    "metric_records",
    "artifact_ref",
)
METRICS_ARTIFACT_SPEC_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "artifact_ref",
    "artifact_class",
    "format",
    "metric_schema_version",
    "producer",
    "consumer",
)


def _require_mapping(values: object) -> Mapping[str, object]:
    if not isinstance(values, Mapping):
        raise MetricsValidationError("values must be a mapping")
    return values


def _require_exact_fields(values: Mapping[str, object], required_fields: tuple[str, ...]) -> None:
    expected_fields = set(required_fields)
    provided_fields = set(values)
    if provided_fields.difference(expected_fields):
        raise MetricsValidationError("values contain unsupported fields")
    missing_fields = tuple(field for field in required_fields if field not in values)
    if missing_fields:
        raise MetricsValidationError("values are missing required fields")


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MetricsValidationError(f"{field_name} is required")
    return value


def _require_metric_value(value: object) -> object:
    if value is None:
        raise MetricsValidationError("metric_value is required")
    return value


def _normalize_gross_or_net(value: object) -> str:
    value = _require_text(value, "gross_or_net")
    if value not in ALLOWED_GROSS_OR_NET:
        raise MetricsValidationError("unsupported gross_or_net")
    return value


def _require_metric_records(value: object) -> tuple["MetricRecord", ...]:
    if isinstance(value, (str, bytes)):
        raise MetricsValidationError("metric_records must be a non-empty iterable")
    if not isinstance(value, Iterable):
        raise MetricsValidationError("metric_records must be a non-empty iterable")
    records = tuple(value)
    if not records:
        raise MetricsValidationError("metric_records must be non-empty")
    for record in records:
        if not isinstance(record, MetricRecord):
            raise MetricsValidationError("metric_records must contain MetricRecord instances")
        validate_metric_record(record)
    return records


def validate_metric_record_values(values: Mapping[str, object]) -> dict[str, object]:
    values = _require_mapping(values)
    _require_exact_fields(values, METRIC_RECORD_REQUIRED_FIELDS)
    return {
        "metric_id": _require_text(values["metric_id"], "metric_id"),
        "metric_name": _require_text(values["metric_name"], "metric_name"),
        "metric_value": _require_metric_value(values["metric_value"]),
        "metric_unit": _require_text(values["metric_unit"], "metric_unit"),
        "scope": _require_text(values["scope"], "scope"),
        "gross_or_net": _normalize_gross_or_net(values["gross_or_net"]),
        "producer": _require_text(values["producer"], "producer"),
        "consumer": _require_text(values["consumer"], "consumer"),
    }


def validate_metrics_summary_values(values: Mapping[str, object]) -> dict[str, object]:
    values = _require_mapping(values)
    _require_exact_fields(values, METRICS_SUMMARY_REQUIRED_FIELDS)
    return {
        "run_id": _require_text(values["run_id"], "run_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "test_type": _require_text(values["test_type"], "test_type"),
        "scope_level": _require_text(values["scope_level"], "scope_level"),
        "result_status": _require_text(values["result_status"], "result_status"),
        "canonicality_status": _require_text(
            values["canonicality_status"], "canonicality_status"
        ),
        "metric_schema_version": _require_text(
            values["metric_schema_version"], "metric_schema_version"
        ),
        "metric_records": _require_metric_records(values["metric_records"]),
        "artifact_ref": _require_text(values["artifact_ref"], "artifact_ref"),
    }


def validate_metrics_artifact_spec_values(values: Mapping[str, object]) -> dict[str, object]:
    values = _require_mapping(values)
    _require_exact_fields(values, METRICS_ARTIFACT_SPEC_REQUIRED_FIELDS)
    return {
        "artifact_ref": _require_text(values["artifact_ref"], "artifact_ref"),
        "artifact_class": _require_text(values["artifact_class"], "artifact_class"),
        "format": _require_text(values["format"], "format"),
        "metric_schema_version": _require_text(
            values["metric_schema_version"], "metric_schema_version"
        ),
        "producer": _require_text(values["producer"], "producer"),
        "consumer": _require_text(values["consumer"], "consumer"),
    }


class MetricRecord:
    __annotations__ = {
        "metric_id": str,
        "metric_name": str,
        "metric_value": object,
        "metric_unit": str,
        "scope": str,
        "gross_or_net": str,
        "producer": str,
        "consumer": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_metric_record_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


class MetricsSummary:
    __annotations__ = {
        "run_id": str,
        "strategy_id": str,
        "test_type": str,
        "scope_level": str,
        "result_status": str,
        "canonicality_status": str,
        "metric_schema_version": str,
        "metric_records": tuple[MetricRecord, ...],
        "artifact_ref": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_metrics_summary_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


class MetricsArtifactSpec:
    __annotations__ = {
        "artifact_ref": str,
        "artifact_class": str,
        "format": str,
        "metric_schema_version": str,
        "producer": str,
        "consumer": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_metrics_artifact_spec_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_metric_record(record: MetricRecord) -> MetricRecord:
    if not isinstance(record, MetricRecord):
        raise TypeError("record must be MetricRecord")
    validate_metric_record_values(record.__dict__)
    return record


def validate_metrics_summary(summary: MetricsSummary) -> MetricsSummary:
    if not isinstance(summary, MetricsSummary):
        raise TypeError("summary must be MetricsSummary")
    validate_metrics_summary_values(summary.__dict__)
    return summary


def validate_metrics_artifact_spec(spec: MetricsArtifactSpec) -> MetricsArtifactSpec:
    if not isinstance(spec, MetricsArtifactSpec):
        raise TypeError("spec must be MetricsArtifactSpec")
    validate_metrics_artifact_spec_values(spec.__dict__)
    return spec


__all__ = [
    "ALLOWED_GROSS_OR_NET",
    "METRICS_ARTIFACT_SPEC_REQUIRED_FIELDS",
    "METRICS_SUMMARY_REQUIRED_FIELDS",
    "METRIC_RECORD_REQUIRED_FIELDS",
    "MetricRecord",
    "MetricsArtifactSpec",
    "MetricsSummary",
    "MetricsValidationError",
    "validate_metric_record",
    "validate_metric_record_values",
    "validate_metrics_artifact_spec",
    "validate_metrics_artifact_spec_values",
    "validate_metrics_summary",
    "validate_metrics_summary_values",
]
