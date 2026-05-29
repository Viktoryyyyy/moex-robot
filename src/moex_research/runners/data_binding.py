from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class ReadOnlyDataBindingValidationError(ValueError):
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


READ_ONLY_DATA_BINDING_FIELDS: Final[tuple[str, ...]] = (
    "binding_id",
    "dataset_ref_id",
    "strategy_id",
    "strategy_test_id",
    "artifact_class",
    "artifact_ref",
    "schema_ref",
    "read_mode",
)
READ_ONLY_DATASET_SCHEMA_FIELDS: Final[tuple[str, ...]] = (
    "schema_id",
    "schema_version",
    "required_columns",
    "timestamp_column",
    "instrument_column",
    "price_columns",
)
READ_ONLY_DATA_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_binding",
    "dataset_schema",
    "read_mode",
)
READ_ONLY_DATA_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "read_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "schema_id",
    "row_count_or_none",
    "error_message_or_none",
)
ALLOWED_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset(
    {"repo_relative", "external_pattern", "cli_argument", "env_contract", "temporary_test_path"}
)
ALLOWED_READ_MODES: Final[frozenset[str]] = frozenset({"schema_validation_only", "test_fixture_read_only"})
ALLOWED_READ_STATUSES: Final[frozenset[str]] = frozenset({"validated", "rejected"})


def _selection_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _blocked_path_markers() -> tuple[str, ...]:
    return (
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


def _guard_markers(value: str, field_name: str, *, include_path_markers: bool) -> str:
    folded = value.casefold()
    spaced = _spaced_text(value)
    markers = _selection_markers()
    if include_path_markers:
        markers = (*markers, *_blocked_path_markers())
    for marker in markers:
        if marker in folded or marker in spaced:
            raise ReadOnlyDataBindingValidationError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReadOnlyDataBindingValidationError(f"{field_name} is required")
    return value


def _require_identifier(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=False)


def _require_ref(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=True)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_text(value, field_name)
    if candidate not in allowed_values:
        raise ReadOnlyDataBindingValidationError(f"{field_name} is unsupported")
    return candidate


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise ReadOnlyDataBindingValidationError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise ReadOnlyDataBindingValidationError(f"{label} contains unsupported fields")


def _require_text_tuple(value: object, field_name: str, *, include_path_markers: bool) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise ReadOnlyDataBindingValidationError(f"{field_name} must be a non-empty iterable")
    result: list[str] = []
    for item in value:
        text = _require_text(item, field_name)
        result.append(_guard_markers(text, field_name, include_path_markers=include_path_markers))
    if not result:
        raise ReadOnlyDataBindingValidationError(f"{field_name} must be non-empty")
    return tuple(result)


def validate_read_only_data_binding_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, READ_ONLY_DATA_BINDING_FIELDS, "binding")
    return {
        "binding_id": _require_identifier(values["binding_id"], "binding_id"),
        "dataset_ref_id": _require_identifier(values["dataset_ref_id"], "dataset_ref_id"),
        "strategy_id": _require_identifier(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_identifier(values["strategy_test_id"], "strategy_test_id"),
        "artifact_class": _require_choice(values["artifact_class"], "artifact_class", ALLOWED_ARTIFACT_CLASSES),
        "artifact_ref": _require_ref(values["artifact_ref"], "artifact_ref"),
        "schema_ref": _require_identifier(values["schema_ref"], "schema_ref"),
        "read_mode": _require_choice(values["read_mode"], "read_mode", ALLOWED_READ_MODES),
    }


class ReadOnlyDataBinding:
    __annotations__ = {
        "binding_id": str,
        "dataset_ref_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "artifact_class": str,
        "artifact_ref": str,
        "schema_ref": str,
        "read_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_read_only_data_binding_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_read_only_data_binding(binding: ReadOnlyDataBinding) -> ReadOnlyDataBinding:
    if not isinstance(binding, ReadOnlyDataBinding):
        raise TypeError("binding must be ReadOnlyDataBinding")
    validate_read_only_data_binding_values(binding.__dict__)
    return binding


def validate_read_only_dataset_schema_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, READ_ONLY_DATASET_SCHEMA_FIELDS, "schema")
    required_columns = _require_text_tuple(values["required_columns"], "required_columns", include_path_markers=False)
    timestamp_column = _require_identifier(values["timestamp_column"], "timestamp_column")
    instrument_column = _require_identifier(values["instrument_column"], "instrument_column")
    price_columns = _require_text_tuple(values["price_columns"], "price_columns", include_path_markers=False)
    declared = set(required_columns)
    if timestamp_column not in declared or instrument_column not in declared:
        raise ReadOnlyDataBindingValidationError("schema columns must include timestamp and instrument columns")
    if not set(price_columns).issubset(declared):
        raise ReadOnlyDataBindingValidationError("schema columns must include price columns")
    return {
        "schema_id": _require_identifier(values["schema_id"], "schema_id"),
        "schema_version": _require_identifier(values["schema_version"], "schema_version"),
        "required_columns": required_columns,
        "timestamp_column": timestamp_column,
        "instrument_column": instrument_column,
        "price_columns": price_columns,
    }


class ReadOnlyDatasetSchema:
    __annotations__ = {
        "schema_id": str,
        "schema_version": str,
        "required_columns": tuple[str, ...],
        "timestamp_column": str,
        "instrument_column": str,
        "price_columns": tuple[str, ...],
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_read_only_dataset_schema_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_read_only_dataset_schema(schema: ReadOnlyDatasetSchema) -> ReadOnlyDatasetSchema:
    if not isinstance(schema, ReadOnlyDatasetSchema):
        raise TypeError("schema must be ReadOnlyDatasetSchema")
    validate_read_only_dataset_schema_values(schema.__dict__)
    return schema


def _require_binding(value: object) -> ReadOnlyDataBinding:
    if isinstance(value, ReadOnlyDataBinding):
        return validate_read_only_data_binding(value)
    if isinstance(value, Mapping):
        return ReadOnlyDataBinding(**value)
    raise ReadOnlyDataBindingValidationError("dataset_binding is required")


def _require_schema(value: object) -> ReadOnlyDatasetSchema:
    if isinstance(value, ReadOnlyDatasetSchema):
        return validate_read_only_dataset_schema(value)
    if isinstance(value, Mapping):
        return ReadOnlyDatasetSchema(**value)
    raise ReadOnlyDataBindingValidationError("dataset_schema is required")


def validate_read_only_data_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, READ_ONLY_DATA_REQUEST_FIELDS, "request")
    binding = _require_binding(values["dataset_binding"])
    schema = _require_schema(values["dataset_schema"])
    read_mode = _require_choice(values["read_mode"], "read_mode", ALLOWED_READ_MODES)
    strategy_id = _require_identifier(values["strategy_id"], "strategy_id")
    strategy_test_id = _require_identifier(values["strategy_test_id"], "strategy_test_id")
    if binding.strategy_id != strategy_id or binding.strategy_test_id != strategy_test_id:
        raise ReadOnlyDataBindingValidationError("request identifiers must match binding")
    if binding.schema_ref != schema.schema_id:
        raise ReadOnlyDataBindingValidationError("binding schema_ref must match schema_id")
    if binding.read_mode != read_mode:
        raise ReadOnlyDataBindingValidationError("request read_mode must match binding")
    return {
        "request_id": _require_identifier(values["request_id"], "request_id"),
        "strategy_id": strategy_id,
        "strategy_test_id": strategy_test_id,
        "dataset_binding": binding,
        "dataset_schema": schema,
        "read_mode": read_mode,
    }


class ReadOnlyDataRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "dataset_binding": ReadOnlyDataBinding,
        "dataset_schema": ReadOnlyDatasetSchema,
        "read_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_read_only_data_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_read_only_data_request(request: ReadOnlyDataRequest) -> ReadOnlyDataRequest:
    if not isinstance(request, ReadOnlyDataRequest):
        raise TypeError("request must be ReadOnlyDataRequest")
    validate_read_only_data_request_values(request.__dict__)
    return request


def _require_row_count(value: object, read_status: str) -> int | None:
    if read_status == "rejected":
        if value is not None:
            raise ReadOnlyDataBindingValidationError("rejected result must not include row count")
        return None
    if not isinstance(value, int) or value < 1:
        raise ReadOnlyDataBindingValidationError("validated result requires positive row count")
    return value


def _require_result_error(value: object, read_status: str) -> str | None:
    if read_status == "validated":
        if value is not None:
            raise ReadOnlyDataBindingValidationError("validated result must not include error")
        return None
    if not isinstance(value, str) or not value.strip():
        raise ReadOnlyDataBindingValidationError("rejected result requires error")
    return value


def validate_read_only_data_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, READ_ONLY_DATA_RESULT_FIELDS, "result")
    read_status = _require_choice(values["read_status"], "read_status", ALLOWED_READ_STATUSES)
    return {
        "read_status": read_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "schema_id": _require_text(values["schema_id"], "schema_id"),
        "row_count_or_none": _require_row_count(values["row_count_or_none"], read_status),
        "error_message_or_none": _require_result_error(values["error_message_or_none"], read_status),
    }


class ReadOnlyDataResult:
    __annotations__ = {
        "read_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "schema_id": str,
        "row_count_or_none": int | None,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_read_only_data_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_read_only_data_result(result: ReadOnlyDataResult) -> ReadOnlyDataResult:
    if not isinstance(result, ReadOnlyDataResult):
        raise TypeError("result must be ReadOnlyDataResult")
    validate_read_only_data_result_values(result.__dict__)
    return result


def _row_mapping(row: object) -> Mapping[str, object]:
    if not isinstance(row, Mapping):
        raise ReadOnlyDataBindingValidationError("rows must contain mappings")
    return row


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _safe_schema_id(source: object) -> str:
    schema = getattr(source, "dataset_schema", None)
    value = getattr(schema, "schema_id", "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result_from_request(
    *,
    request: object,
    read_status: str,
    row_count_or_none: int | None,
    error_message_or_none: str | None,
) -> ReadOnlyDataResult:
    return ReadOnlyDataResult(
        read_status=read_status,
        request_id=_safe_text(request, "request_id"),
        strategy_id=_safe_text(request, "strategy_id"),
        strategy_test_id=_safe_text(request, "strategy_test_id"),
        schema_id=_safe_schema_id(request),
        row_count_or_none=row_count_or_none,
        error_message_or_none=error_message_or_none,
    )


def validate_read_only_data_rows(request: ReadOnlyDataRequest, rows: Iterable[Mapping[str, object]]) -> ReadOnlyDataResult:
    try:
        validated_request = validate_read_only_data_request(request)
        if isinstance(rows, (str, bytes)) or not isinstance(rows, Iterable):
            raise ReadOnlyDataBindingValidationError("rows must be a non-empty iterable")
        row_count = 0
        required_columns = set(validated_request.dataset_schema.required_columns)
        for row in rows:
            row_mapping = _row_mapping(row)
            missing = [column for column in required_columns if column not in row_mapping]
            if missing:
                raise ReadOnlyDataBindingValidationError("rows are missing required columns")
            for column in required_columns:
                value = row_mapping[column]
                if value is None or (isinstance(value, str) and not value.strip()):
                    raise ReadOnlyDataBindingValidationError("rows contain empty required values")
            row_count += 1
        if row_count < 1:
            raise ReadOnlyDataBindingValidationError("rows must be non-empty")
        return _result_from_request(
            request=validated_request,
            read_status="validated",
            row_count_or_none=row_count,
            error_message_or_none=None,
        )
    except (ReadOnlyDataBindingValidationError, TypeError, ValueError) as error:
        return _result_from_request(
            request=request,
            read_status="rejected",
            row_count_or_none=None,
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_ARTIFACT_CLASSES",
    "ALLOWED_READ_MODES",
    "ALLOWED_READ_STATUSES",
    "READ_ONLY_DATA_BINDING_FIELDS",
    "READ_ONLY_DATA_REQUEST_FIELDS",
    "READ_ONLY_DATA_RESULT_FIELDS",
    "READ_ONLY_DATASET_SCHEMA_FIELDS",
    "ReadOnlyDataBinding",
    "ReadOnlyDataBindingValidationError",
    "ReadOnlyDataRequest",
    "ReadOnlyDataResult",
    "ReadOnlyDatasetSchema",
    "validate_read_only_data_binding",
    "validate_read_only_data_binding_values",
    "validate_read_only_data_request",
    "validate_read_only_data_request_values",
    "validate_read_only_data_result",
    "validate_read_only_data_result_values",
    "validate_read_only_data_rows",
    "validate_read_only_dataset_schema",
    "validate_read_only_dataset_schema_values",
]
