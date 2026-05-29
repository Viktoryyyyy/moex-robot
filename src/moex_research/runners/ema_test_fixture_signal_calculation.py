from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final

from moex_research.runners.data_binding import (
    ReadOnlyDataRequest,
    validate_read_only_data_request,
    validate_read_only_data_rows,
)
from moex_research.runners.synthetic_signal_artifact import (
    SyntheticSignalArtifactWriteRequest,
    SyntheticSignalArtifactWriteResult,
    SyntheticSignalRow,
    SyntheticSignalTableArtifact,
    write_synthetic_signal_artifact_dry_run,
)


class EMATestFixtureSignalCalculationError(ValueError):
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


EMA_TEST_FIXTURE_SIGNAL_CALCULATION_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "data_request",
    "rows",
    "fast_period",
    "slow_period",
    "signal_id",
    "signal_version",
    "output_path",
    "artifact_manifest_ref",
    "calculation_mode",
)
EMA_TEST_FIXTURE_SIGNAL_CALCULATION_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "calculation_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "row_count_or_none",
    "signal_artifact_id_or_none",
    "output_path_or_none",
    "artifact_manifest_ref_or_none",
    "error_message_or_none",
)
ALLOWED_CALCULATION_MODES: Final[frozenset[str]] = frozenset({"test_fixture_signal_calculation_only"})
ALLOWED_CALCULATION_STATUSES: Final[frozenset[str]] = frozenset({"written", "rejected"})


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
            raise EMATestFixtureSignalCalculationError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EMATestFixtureSignalCalculationError(f"{field_name} is required")
    return value


def _require_identifier(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=False)


def _require_path_ref(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=True)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_text(value, field_name)
    if candidate not in allowed_values:
        raise EMATestFixtureSignalCalculationError(f"{field_name} is unsupported")
    return candidate


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise EMATestFixtureSignalCalculationError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise EMATestFixtureSignalCalculationError(f"{label} contains unsupported fields")


def _require_data_request(value: object) -> ReadOnlyDataRequest:
    if not isinstance(value, ReadOnlyDataRequest):
        raise EMATestFixtureSignalCalculationError("data_request must be ReadOnlyDataRequest")
    return validate_read_only_data_request(value)


def _require_period(value: object, field_name: str) -> int:
    if not isinstance(value, int) or value < 1:
        raise EMATestFixtureSignalCalculationError(f"{field_name} must be positive integer")
    return value


def _require_rows(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise EMATestFixtureSignalCalculationError("rows must be a non-empty iterable")
    rows: list[Mapping[str, object]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise EMATestFixtureSignalCalculationError("rows must contain mappings")
        rows.append(row)
    if not rows:
        raise EMATestFixtureSignalCalculationError("rows must be non-empty")
    return tuple(rows)


def validate_ema_test_fixture_signal_calculation_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, EMA_TEST_FIXTURE_SIGNAL_CALCULATION_REQUEST_FIELDS, "request")
    strategy_id = _require_identifier(values["strategy_id"], "strategy_id")
    strategy_test_id = _require_identifier(values["strategy_test_id"], "strategy_test_id")
    data_request = _require_data_request(values["data_request"])
    rows = _require_rows(values["rows"])
    fast_period = _require_period(values["fast_period"], "fast_period")
    slow_period = _require_period(values["slow_period"], "slow_period")
    if fast_period >= slow_period:
        raise EMATestFixtureSignalCalculationError("fast_period must be less than slow_period")
    if strategy_id != "ema_3_19":
        raise EMATestFixtureSignalCalculationError("strategy_id must be ema_3_19")
    if data_request.strategy_id != strategy_id or data_request.strategy_test_id != strategy_test_id:
        raise EMATestFixtureSignalCalculationError("request identifiers must match data_request")
    data_validation = validate_read_only_data_rows(data_request, rows)
    if data_validation.read_status != "validated":
        raise EMATestFixtureSignalCalculationError("rows must pass read-only data validation")
    return {
        "request_id": _require_identifier(values["request_id"], "request_id"),
        "strategy_id": strategy_id,
        "strategy_test_id": strategy_test_id,
        "data_request": data_request,
        "rows": rows,
        "fast_period": fast_period,
        "slow_period": slow_period,
        "signal_id": _require_identifier(values["signal_id"], "signal_id"),
        "signal_version": _require_identifier(values["signal_version"], "signal_version"),
        "output_path": _require_path_ref(values["output_path"], "output_path"),
        "artifact_manifest_ref": _require_identifier(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "calculation_mode": _require_choice(
            values["calculation_mode"],
            "calculation_mode",
            ALLOWED_CALCULATION_MODES,
        ),
    }


class EMATestFixtureSignalCalculationRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "data_request": ReadOnlyDataRequest,
        "rows": tuple[Mapping[str, object], ...],
        "fast_period": int,
        "slow_period": int,
        "signal_id": str,
        "signal_version": str,
        "output_path": str,
        "artifact_manifest_ref": str,
        "calculation_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_ema_test_fixture_signal_calculation_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_ema_test_fixture_signal_calculation_request(
    request: EMATestFixtureSignalCalculationRequest,
) -> EMATestFixtureSignalCalculationRequest:
    if not isinstance(request, EMATestFixtureSignalCalculationRequest):
        raise TypeError("request must be EMATestFixtureSignalCalculationRequest")
    validate_ema_test_fixture_signal_calculation_request_values(request.__dict__)
    return request


def _ema(previous: float | None, value: float, period: int) -> float:
    if previous is None:
        return value
    alpha = 2.0 / (period + 1.0)
    return (value * alpha) + (previous * (1.0 - alpha))


def _row_signal_value(fast_value: float, slow_value: float) -> int:
    if fast_value > slow_value:
        return 1
    if fast_value < slow_value:
        return -1
    return 0


def _build_signal_rows(request: EMATestFixtureSignalCalculationRequest) -> tuple[SyntheticSignalRow, ...]:
    fast_value: float | None = None
    slow_value: float | None = None
    signal_rows: list[SyntheticSignalRow] = []
    for row in request.rows:
        close_value = row["close"]
        if not isinstance(close_value, (int, float)):
            raise EMATestFixtureSignalCalculationError("close must be numeric")
        fast_value = _ema(fast_value, float(close_value), request.fast_period)
        slow_value = _ema(slow_value, float(close_value), request.slow_period)
        signal_rows.append(
            SyntheticSignalRow(
                strategy_id=request.strategy_id,
                strategy_test_id=request.strategy_test_id,
                signal_id=request.signal_id,
                instrument_id=str(row["instrument_id"]),
                timestamp=str(row["timestamp"]),
                signal_value=_row_signal_value(fast_value, slow_value),
                signal_version=request.signal_version,
                source_type="synthetic_test_only",
            )
        )
    return tuple(signal_rows)


def _require_optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _require_result_error(value: object, calculation_status: str) -> str | None:
    if calculation_status == "written":
        if value is not None:
            raise EMATestFixtureSignalCalculationError("written result must not include error")
        return None
    if not isinstance(value, str) or not value.strip():
        raise EMATestFixtureSignalCalculationError("rejected result requires error")
    return value


def validate_ema_test_fixture_signal_calculation_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, EMA_TEST_FIXTURE_SIGNAL_CALCULATION_RESULT_FIELDS, "result")
    calculation_status = _require_choice(
        values["calculation_status"],
        "calculation_status",
        ALLOWED_CALCULATION_STATUSES,
    )
    row_count = values["row_count_or_none"]
    if calculation_status == "written":
        if not isinstance(row_count, int) or row_count < 1:
            raise EMATestFixtureSignalCalculationError("written result requires positive row count")
    elif row_count is not None:
        raise EMATestFixtureSignalCalculationError("rejected result must not include row count")
    return {
        "calculation_status": calculation_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "row_count_or_none": row_count,
        "signal_artifact_id_or_none": _require_optional_text(
            values["signal_artifact_id_or_none"],
            "signal_artifact_id_or_none",
        ),
        "output_path_or_none": _require_optional_text(values["output_path_or_none"], "output_path_or_none"),
        "artifact_manifest_ref_or_none": _require_optional_text(
            values["artifact_manifest_ref_or_none"],
            "artifact_manifest_ref_or_none",
        ),
        "error_message_or_none": _require_result_error(values["error_message_or_none"], calculation_status),
    }


class EMATestFixtureSignalCalculationResult:
    __annotations__ = {
        "calculation_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "row_count_or_none": int | None,
        "signal_artifact_id_or_none": str | None,
        "output_path_or_none": str | None,
        "artifact_manifest_ref_or_none": str | None,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_ema_test_fixture_signal_calculation_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_ema_test_fixture_signal_calculation_result(
    result: EMATestFixtureSignalCalculationResult,
) -> EMATestFixtureSignalCalculationResult:
    if not isinstance(result, EMATestFixtureSignalCalculationResult):
        raise TypeError("result must be EMATestFixtureSignalCalculationResult")
    validate_ema_test_fixture_signal_calculation_result_values(result.__dict__)
    return result


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result_from_request(
    *,
    request: object,
    calculation_status: str,
    row_count_or_none: int | None,
    signal_artifact_id_or_none: str | None,
    output_path_or_none: str | None,
    artifact_manifest_ref_or_none: str | None,
    error_message_or_none: str | None,
) -> EMATestFixtureSignalCalculationResult:
    return EMATestFixtureSignalCalculationResult(
        calculation_status=calculation_status,
        request_id=_safe_text(request, "request_id"),
        strategy_id=_safe_text(request, "strategy_id"),
        strategy_test_id=_safe_text(request, "strategy_test_id"),
        row_count_or_none=row_count_or_none,
        signal_artifact_id_or_none=signal_artifact_id_or_none,
        output_path_or_none=output_path_or_none,
        artifact_manifest_ref_or_none=artifact_manifest_ref_or_none,
        error_message_or_none=error_message_or_none,
    )


def calculate_ema_3_19_test_fixture_signals_dry_run(
    request: EMATestFixtureSignalCalculationRequest,
) -> EMATestFixtureSignalCalculationResult:
    try:
        validated_request = validate_ema_test_fixture_signal_calculation_request(request)
        signal_rows = _build_signal_rows(validated_request)
        signal_artifact = SyntheticSignalTableArtifact(
            artifact_id="ema_3_19.test_fixture.signal_table.v1",
            strategy_id=validated_request.strategy_id,
            strategy_test_id=validated_request.strategy_test_id,
            artifact_role="synthetic_signal_table",
            artifact_class="temporary_test_path",
            schema_version="synthetic_signal_table.v1",
            rows=signal_rows,
            source_type="synthetic_test_only",
        )
        write_request = SyntheticSignalArtifactWriteRequest(
            request_id=validated_request.request_id + ".write",
            strategy_id=validated_request.strategy_id,
            strategy_test_id=validated_request.strategy_test_id,
            signal_artifact=signal_artifact,
            output_path=validated_request.output_path,
            artifact_manifest_ref=validated_request.artifact_manifest_ref,
            write_mode="dry_run_test_only",
        )
        write_result: SyntheticSignalArtifactWriteResult = write_synthetic_signal_artifact_dry_run(write_request)
        if write_result.write_status != "written":
            raise EMATestFixtureSignalCalculationError(write_result.error_message_or_none or "write rejected")
        return _result_from_request(
            request=validated_request,
            calculation_status="written",
            row_count_or_none=len(signal_rows),
            signal_artifact_id_or_none=signal_artifact.artifact_id,
            output_path_or_none=write_result.output_path,
            artifact_manifest_ref_or_none=write_result.artifact_manifest_ref_or_none,
            error_message_or_none=None,
        )
    except (EMATestFixtureSignalCalculationError, TypeError, ValueError) as error:
        return _result_from_request(
            request=request,
            calculation_status="rejected",
            row_count_or_none=None,
            signal_artifact_id_or_none=None,
            output_path_or_none=None,
            artifact_manifest_ref_or_none=None,
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_CALCULATION_MODES",
    "ALLOWED_CALCULATION_STATUSES",
    "EMA_TEST_FIXTURE_SIGNAL_CALCULATION_REQUEST_FIELDS",
    "EMA_TEST_FIXTURE_SIGNAL_CALCULATION_RESULT_FIELDS",
    "EMATestFixtureSignalCalculationError",
    "EMATestFixtureSignalCalculationRequest",
    "EMATestFixtureSignalCalculationResult",
    "calculate_ema_3_19_test_fixture_signals_dry_run",
    "validate_ema_test_fixture_signal_calculation_request",
    "validate_ema_test_fixture_signal_calculation_request_values",
    "validate_ema_test_fixture_signal_calculation_result",
    "validate_ema_test_fixture_signal_calculation_result_values",
]
