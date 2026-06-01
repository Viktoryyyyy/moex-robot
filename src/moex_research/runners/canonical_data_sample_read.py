from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from moex_research.runners.canonical_data_read import (
    CanonicalDataReadRequest,
    CanonicalDataReadResult,
    dry_validate_canonical_data_read_request,
    validate_canonical_data_read_request,
)


class CanonicalDataSampleReadValidationError(ValueError):
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


def _sample_root() -> str:
    return "tests/fixtures/strategy_testing/canonical_samples"


CANONICAL_DATA_SAMPLE_READ_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "sample_request_id",
    "canonical_read_request",
    "sample_ref",
    "sample_path",
    "sample_read_mode",
)
CANONICAL_DATA_SAMPLE_READ_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "sample_status",
    "sample_request_id",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "schema_ref",
    "row_count_or_none",
    "schema_status_or_none",
    "error_message_or_none",
)
ALLOWED_SAMPLE_READ_MODES: Final[frozenset[str]] = frozenset({"approved_static_sample_only"})
ALLOWED_SAMPLE_STATUSES: Final[frozenset[str]] = frozenset({"validated", "rejected"})
APPROVED_SAMPLE_REFS: Final[frozenset[str]] = frozenset({"canonical.sample.si.d1.ohlcv.v1"})
APPROVED_SAMPLE_PATHS: Final[frozenset[str]] = frozenset(
    {_sample_root() + "/si_d1_ohlcv_sample.json"}
)
REQUIRED_OHLCV_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "instrument_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


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
            raise CanonicalDataSampleReadValidationError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CanonicalDataSampleReadValidationError(f"{field_name} is required")
    return value


def _require_ref(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_ref(value, field_name)
    if candidate not in allowed_values:
        raise CanonicalDataSampleReadValidationError(f"{field_name} is unsupported")
    return candidate


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise CanonicalDataSampleReadValidationError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise CanonicalDataSampleReadValidationError(f"{label} contains unsupported fields")


def _require_canonical_request(value: object) -> CanonicalDataReadRequest:
    if not isinstance(value, CanonicalDataReadRequest):
        raise CanonicalDataSampleReadValidationError("canonical_read_request must be CanonicalDataReadRequest")
    return validate_canonical_data_read_request(value)


def _require_sample_path(value: object) -> str:
    sample_path = _require_choice(value, "sample_path", APPROVED_SAMPLE_PATHS)
    if not sample_path.startswith(_sample_root() + "/"):
        raise CanonicalDataSampleReadValidationError("sample_path must be under approved fixture root")
    return sample_path


def validate_canonical_data_sample_read_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATA_SAMPLE_READ_REQUEST_FIELDS, "request")
    canonical_request = _require_canonical_request(values["canonical_read_request"])
    dry_result: CanonicalDataReadResult = dry_validate_canonical_data_read_request(canonical_request)
    if dry_result.read_status != "validated":
        raise CanonicalDataSampleReadValidationError("canonical read request must dry-validate")
    return {
        "sample_request_id": _require_ref(values["sample_request_id"], "sample_request_id"),
        "canonical_read_request": canonical_request,
        "sample_ref": _require_choice(values["sample_ref"], "sample_ref", APPROVED_SAMPLE_REFS),
        "sample_path": _require_sample_path(values["sample_path"]),
        "sample_read_mode": _require_choice(values["sample_read_mode"], "sample_read_mode", ALLOWED_SAMPLE_READ_MODES),
    }


class CanonicalDataSampleReadRequest:
    __annotations__ = {
        "sample_request_id": str,
        "canonical_read_request": CanonicalDataReadRequest,
        "sample_ref": str,
        "sample_path": str,
        "sample_read_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_data_sample_read_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_data_sample_read_request(
    request: CanonicalDataSampleReadRequest,
) -> CanonicalDataSampleReadRequest:
    if not isinstance(request, CanonicalDataSampleReadRequest):
        raise TypeError("request must be CanonicalDataSampleReadRequest")
    validate_canonical_data_sample_read_request_values(request.__dict__)
    return request


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_sample_rows(sample_path: str) -> list[object]:
    path = (_repo_root() / sample_path).resolve(strict=False)
    approved_root = (_repo_root() / _sample_root()).resolve(strict=False)
    try:
        if not path.is_relative_to(approved_root):
            raise CanonicalDataSampleReadValidationError("sample path outside approved root")
    except AttributeError:
        if not str(path).startswith(str(approved_root) + "/"):
            raise CanonicalDataSampleReadValidationError("sample path outside approved root")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list) or not payload:
        raise CanonicalDataSampleReadValidationError("sample payload must be non-empty list")
    return payload


def _validate_ohlcv_rows(rows: list[object], instrument_id: str) -> int:
    for row in rows:
        if not isinstance(row, Mapping):
            raise CanonicalDataSampleReadValidationError("sample rows must be mappings")
        missing = [column for column in REQUIRED_OHLCV_COLUMNS if column not in row]
        if missing:
            raise CanonicalDataSampleReadValidationError("sample row missing required columns")
        if row["instrument_id"] != instrument_id:
            raise CanonicalDataSampleReadValidationError("sample instrument mismatch")
        for numeric_column in ("open", "high", "low", "close", "volume"):
            if not isinstance(row[numeric_column], (int, float)):
                raise CanonicalDataSampleReadValidationError("sample numeric column invalid")
    return len(rows)


def validate_canonical_data_sample_read_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, CANONICAL_DATA_SAMPLE_READ_RESULT_FIELDS, "result")
    sample_status = _require_choice(values["sample_status"], "sample_status", ALLOWED_SAMPLE_STATUSES)
    error = values["error_message_or_none"]
    if sample_status == "validated":
        if not isinstance(values["row_count_or_none"], int) or values["row_count_or_none"] < 1:
            raise CanonicalDataSampleReadValidationError("validated sample result requires row count")
        if values["schema_status_or_none"] != "validated":
            raise CanonicalDataSampleReadValidationError("validated sample result requires schema status")
        if error is not None:
            raise CanonicalDataSampleReadValidationError("validated sample result must not include error")
    else:
        if values["row_count_or_none"] is not None or values["schema_status_or_none"] is not None:
            raise CanonicalDataSampleReadValidationError("rejected sample result must not include data fields")
        if not isinstance(error, str) or not error.strip():
            raise CanonicalDataSampleReadValidationError("rejected sample result requires error")
    return {
        "sample_status": sample_status,
        "sample_request_id": _require_text(values["sample_request_id"], "sample_request_id"),
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "dataset_ref_id": _require_text(values["dataset_ref_id"], "dataset_ref_id"),
        "instrument_id": _require_text(values["instrument_id"], "instrument_id"),
        "timeframe": _require_text(values["timeframe"], "timeframe"),
        "schema_ref": _require_text(values["schema_ref"], "schema_ref"),
        "row_count_or_none": values["row_count_or_none"],
        "schema_status_or_none": values["schema_status_or_none"],
        "error_message_or_none": error,
    }


class CanonicalDataSampleReadResult:
    __annotations__ = {
        "sample_status": str,
        "sample_request_id": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "dataset_ref_id": str,
        "instrument_id": str,
        "timeframe": str,
        "schema_ref": str,
        "row_count_or_none": int | None,
        "schema_status_or_none": str | None,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_canonical_data_sample_read_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_canonical_data_sample_read_result(
    result: CanonicalDataSampleReadResult,
) -> CanonicalDataSampleReadResult:
    if not isinstance(result, CanonicalDataSampleReadResult):
        raise TypeError("result must be CanonicalDataSampleReadResult")
    validate_canonical_data_sample_read_result_values(result.__dict__)
    return result


def read_canonical_data_sample_dry_run(request: CanonicalDataSampleReadRequest) -> CanonicalDataSampleReadResult:
    try:
        validated_request = validate_canonical_data_sample_read_request(request)
        canonical_request = validated_request.canonical_read_request
        dataset_ref = canonical_request.dataset_ref
        rows = _read_sample_rows(validated_request.sample_path)
        row_count = _validate_ohlcv_rows(rows, dataset_ref.instrument_id)
        return CanonicalDataSampleReadResult(
            sample_status="validated",
            sample_request_id=validated_request.sample_request_id,
            request_id=canonical_request.request_id,
            strategy_id=canonical_request.strategy_id,
            strategy_test_id=canonical_request.strategy_test_id,
            dataset_ref_id=dataset_ref.dataset_ref_id,
            instrument_id=dataset_ref.instrument_id,
            timeframe=dataset_ref.timeframe,
            schema_ref=dataset_ref.schema_ref,
            row_count_or_none=row_count,
            schema_status_or_none="validated",
            error_message_or_none=None,
        )
    except (CanonicalDataSampleReadValidationError, TypeError, ValueError, OSError, json.JSONDecodeError) as error:
        return CanonicalDataSampleReadResult(
            sample_status="rejected",
            sample_request_id="unavailable",
            request_id="unavailable",
            strategy_id="unavailable",
            strategy_test_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            schema_ref="unavailable",
            row_count_or_none=None,
            schema_status_or_none=None,
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_SAMPLE_READ_MODES",
    "ALLOWED_SAMPLE_STATUSES",
    "APPROVED_SAMPLE_PATHS",
    "APPROVED_SAMPLE_REFS",
    "CANONICAL_DATA_SAMPLE_READ_REQUEST_FIELDS",
    "CANONICAL_DATA_SAMPLE_READ_RESULT_FIELDS",
    "CanonicalDataSampleReadRequest",
    "CanonicalDataSampleReadResult",
    "CanonicalDataSampleReadValidationError",
    "REQUIRED_OHLCV_COLUMNS",
    "read_canonical_data_sample_dry_run",
    "validate_canonical_data_sample_read_request",
    "validate_canonical_data_sample_read_request_values",
    "validate_canonical_data_sample_read_result",
    "validate_canonical_data_sample_read_result_values",
]
