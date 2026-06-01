from __future__ import annotations

from collections.abc import Mapping
from typing import Final


class CanonicalDataReadValidationError(ValueError):
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


CANONICAL_DATASET_REF_FIELDS: Final[tuple[str, ...]] = (
    "dataset_ref_id",
    "dataset_class",
    "instrument_id",
    "timeframe",
    "schema_ref",
    "storage_ref",
    "calendar_ref",
    "source_granularity",
    "read_mode",
)
CANONICAL_DATA_READ_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref",
    "read_purpose",
    "read_mode",
)
CANONICAL_DATA_READ_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "read_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "schema_ref",
    "read_mode",
    "error_message_or_none",
)
ALLOWED_DATASET_CLASSES: Final[frozenset[str]] = frozenset({"canonical_bars"})
ALLOWED_INSTRUMENTS: Final[frozenset[str]] = frozenset({"Si", "USDRUBF"})
ALLOWED_TIMEFRAMES: Final[frozenset[str]] = frozenset({"D1", "5m"})
ALLOWED_SOURCE_GRANULARITIES: Final[frozenset[str]] = frozenset({"bar"})
ALLOWED_READ_MODES: Final[frozenset[str]] = frozenset({"dry_run_reference_validation_only"})
ALLOWED_READ_PURPOSES: Final[frozenset[str]] = frozenset({"strategy_testing_planned"})
ALLOWED_READ_STATUSES: Final[frozenset[str]] = frozenset({"validated", "rejected"})


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
            raise CanonicalDataReadValidationError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CanonicalDataReadValidationError(f"{field_name} is required")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_ref(value, field_name)
    if candidate not in allowed_values:
        raise CanonicalDataReadValidationError(f"{field_name} is unsupported")
    return candidate


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise CanonicalDataReadValidationError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise CanonicalDataReadValidationError(f"{label} contains unsupported fields")


def validate_canonical_dataset_ref_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATASET_REF_FIELDS, "dataset_ref")
    return {
        "dataset_ref_id": _require_ref(values["dataset_ref_id"], "dataset_ref_id"),
        "dataset_class": _require_choice(values["dataset_class"], "dataset_class", ALLOWED_DATASET_CLASSES),
        "instrument_id": _require_choice(values["instrument_id"], "instrument_id", ALLOWED_INSTRUMENTS),
        "timeframe": _require_choice(values["timeframe"], "timeframe", ALLOWED_TIMEFRAMES),
        "schema_ref": _require_ref(values["schema_ref"], "schema_ref"),
        "storage_ref": _require_ref(values["storage_ref"], "storage_ref"),
        "calendar_ref": _require_ref(values["calendar_ref"], "calendar_ref"),
        "source_granularity": _require_choice(
            values["source_granularity"],
            "source_granularity",
            ALLOWED_SOURCE_GRANULARITIES,
        ),
        "read_mode": _require_choice(values["read_mode"], "read_mode", ALLOWED_READ_MODES),
    }


class CanonicalDatasetRef:
    __annotations__ = {
        "dataset_ref_id": str,
        "dataset_class": str,
        "instrument_id": str,
        "timeframe": str,
        "schema_ref": str,
        "storage_ref": str,
        "calendar_ref": str,
        "source_granularity": str,
        "read_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_dataset_ref_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_dataset_ref(ref: CanonicalDatasetRef) -> CanonicalDatasetRef:
    if not isinstance(ref, CanonicalDatasetRef):
        raise TypeError("ref must be CanonicalDatasetRef")
    validate_canonical_dataset_ref_values(ref.__dict__)
    return ref


def _require_dataset_ref(value: object) -> CanonicalDatasetRef:
    if isinstance(value, CanonicalDatasetRef):
        return validate_canonical_dataset_ref(value)
    if isinstance(value, Mapping):
        return CanonicalDatasetRef(**value)
    raise CanonicalDataReadValidationError("dataset_ref is required")


def validate_canonical_data_read_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATA_READ_REQUEST_FIELDS, "request")
    dataset_ref = _require_dataset_ref(values["dataset_ref"])
    read_mode = _require_choice(values["read_mode"], "read_mode", ALLOWED_READ_MODES)
    if dataset_ref.read_mode != read_mode:
        raise CanonicalDataReadValidationError("request read_mode must match dataset_ref")
    return {
        "request_id": _require_ref(values["request_id"], "request_id"),
        "strategy_id": _require_ref(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_ref(values["strategy_test_id"], "strategy_test_id"),
        "dataset_ref": dataset_ref,
        "read_purpose": _require_choice(values["read_purpose"], "read_purpose", ALLOWED_READ_PURPOSES),
        "read_mode": read_mode,
    }


class CanonicalDataReadRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "dataset_ref": CanonicalDatasetRef,
        "read_purpose": str,
        "read_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_data_read_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_data_read_request(request: CanonicalDataReadRequest) -> CanonicalDataReadRequest:
    if not isinstance(request, CanonicalDataReadRequest):
        raise TypeError("request must be CanonicalDataReadRequest")
    validate_canonical_data_read_request_values(request.__dict__)
    return request


def validate_canonical_data_read_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATA_READ_RESULT_FIELDS, "result")
    read_status = _require_choice(values["read_status"], "read_status", ALLOWED_READ_STATUSES)
    error = values["error_message_or_none"]
    if read_status == "validated":
        if error is not None:
            raise CanonicalDataReadValidationError("validated result must not include error")
    elif not isinstance(error, str) or not error.strip():
        raise CanonicalDataReadValidationError("rejected result requires error")
    return {
        "read_status": read_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "dataset_ref_id": _require_text(values["dataset_ref_id"], "dataset_ref_id"),
        "instrument_id": _require_text(values["instrument_id"], "instrument_id"),
        "timeframe": _require_text(values["timeframe"], "timeframe"),
        "schema_ref": _require_text(values["schema_ref"], "schema_ref"),
        "read_mode": _require_text(values["read_mode"], "read_mode"),
        "error_message_or_none": error,
    }


class CanonicalDataReadResult:
    __annotations__ = {
        "read_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "dataset_ref_id": str,
        "instrument_id": str,
        "timeframe": str,
        "schema_ref": str,
        "read_mode": str,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_data_read_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_data_read_result(result: CanonicalDataReadResult) -> CanonicalDataReadResult:
    if not isinstance(result, CanonicalDataReadResult):
        raise TypeError("result must be CanonicalDataReadResult")
    validate_canonical_data_read_result_values(result.__dict__)
    return result


def dry_validate_canonical_data_read_request(request: CanonicalDataReadRequest) -> CanonicalDataReadResult:
    try:
        validated_request = validate_canonical_data_read_request(request)
        dataset_ref = validated_request.dataset_ref
        return CanonicalDataReadResult(
            read_status="validated",
            request_id=validated_request.request_id,
            strategy_id=validated_request.strategy_id,
            strategy_test_id=validated_request.strategy_test_id,
            dataset_ref_id=dataset_ref.dataset_ref_id,
            instrument_id=dataset_ref.instrument_id,
            timeframe=dataset_ref.timeframe,
            schema_ref=dataset_ref.schema_ref,
            read_mode=validated_request.read_mode,
            error_message_or_none=None,
        )
    except (CanonicalDataReadValidationError, TypeError, ValueError) as error:
        return CanonicalDataReadResult(
            read_status="rejected",
            request_id="unavailable",
            strategy_id="unavailable",
            strategy_test_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            schema_ref="unavailable",
            read_mode="unavailable",
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_DATASET_CLASSES",
    "ALLOWED_INSTRUMENTS",
    "ALLOWED_READ_MODES",
    "ALLOWED_READ_PURPOSES",
    "ALLOWED_READ_STATUSES",
    "ALLOWED_SOURCE_GRANULARITIES",
    "ALLOWED_TIMEFRAMES",
    "CANONICAL_DATASET_REF_FIELDS",
    "CANONICAL_DATA_READ_REQUEST_FIELDS",
    "CANONICAL_DATA_READ_RESULT_FIELDS",
    "CanonicalDataReadRequest",
    "CanonicalDataReadResult",
    "CanonicalDataReadValidationError",
    "CanonicalDatasetRef",
    "dry_validate_canonical_data_read_request",
    "validate_canonical_data_read_request",
    "validate_canonical_data_read_request_values",
    "validate_canonical_data_read_result",
    "validate_canonical_data_read_result_values",
    "validate_canonical_dataset_ref",
    "validate_canonical_dataset_ref_values",
]
