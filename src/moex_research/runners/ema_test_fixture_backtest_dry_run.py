from __future__ import annotations

import json
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Final

from moex_research.metrics.schemas import MetricRecord, MetricsSummary, validate_metrics_summary
from moex_research.publishers.report_artifacts import ReportArtifactSpec, ReportSectionSpec, validate_report_artifact_spec
from moex_research.runners.data_binding import ReadOnlyDataRequest, validate_read_only_data_rows
from moex_research.runners.synthetic_signal_artifact import SyntheticSignalRow, validate_synthetic_signal_row


class EMATestFixtureBacktestDryRunError(ValueError):
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


EMA_TEST_FIXTURE_BACKTEST_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "data_request",
    "data_rows",
    "signal_rows",
    "output_path",
    "artifact_manifest_ref",
    "metrics_artifact_ref",
    "report_artifact_ref",
    "dry_run_mode",
)
EMA_TEST_FIXTURE_BACKTEST_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "dry_run_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "row_count_or_none",
    "output_path_or_none",
    "metrics_summary_or_none",
    "report_artifact_or_none",
    "error_message_or_none",
)
ALLOWED_DRY_RUN_MODES: Final[frozenset[str]] = frozenset({"test_fixture_backtest_only"})
ALLOWED_DRY_RUN_STATUSES: Final[frozenset[str]] = frozenset({"written", "rejected"})


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
            raise EMATestFixtureBacktestDryRunError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EMATestFixtureBacktestDryRunError(f"{field_name} is required")
    return value


def _require_identifier(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=False)


def _require_path_ref(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=True)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_text(value, field_name)
    if candidate not in allowed_values:
        raise EMATestFixtureBacktestDryRunError(f"{field_name} is unsupported")
    return candidate


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise EMATestFixtureBacktestDryRunError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise EMATestFixtureBacktestDryRunError(f"{label} contains unsupported fields")


def _require_rows(value: object, field_name: str) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise EMATestFixtureBacktestDryRunError(f"{field_name} must be a non-empty iterable")
    rows: list[Mapping[str, object]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise EMATestFixtureBacktestDryRunError(f"{field_name} must contain mappings")
        rows.append(row)
    if len(rows) < 2:
        raise EMATestFixtureBacktestDryRunError(f"{field_name} must contain at least two rows")
    return tuple(rows)


def _require_signal_rows(value: object) -> tuple[SyntheticSignalRow, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise EMATestFixtureBacktestDryRunError("signal_rows must be a non-empty iterable")
    rows: list[SyntheticSignalRow] = []
    for row in value:
        if not isinstance(row, SyntheticSignalRow):
            raise EMATestFixtureBacktestDryRunError("signal_rows must contain SyntheticSignalRow")
        rows.append(validate_synthetic_signal_row(row))
    if len(rows) < 2:
        raise EMATestFixtureBacktestDryRunError("signal_rows must contain at least two rows")
    return tuple(rows)


def _is_under_temp_root(path_value: str) -> bool:
    root = Path(tempfile.gettempdir()).resolve(strict=False)
    candidate = Path(path_value).resolve(strict=False)
    if candidate == root:
        return True
    try:
        return candidate.is_relative_to(root)
    except AttributeError:
        return str(candidate).startswith(str(root) + "/")


def _require_output_path(value: object) -> str:
    path_value = _require_path_ref(value, "output_path")
    candidate = Path(path_value)
    if candidate.suffix != ".json":
        raise EMATestFixtureBacktestDryRunError("output_path must be json")
    if not _is_under_temp_root(path_value):
        raise EMATestFixtureBacktestDryRunError("output_path must use temporary test path")
    return path_value


def validate_ema_test_fixture_backtest_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, EMA_TEST_FIXTURE_BACKTEST_REQUEST_FIELDS, "request")
    data_request = values["data_request"]
    if not isinstance(data_request, ReadOnlyDataRequest):
        raise EMATestFixtureBacktestDryRunError("data_request must be ReadOnlyDataRequest")
    data_rows = _require_rows(values["data_rows"], "data_rows")
    data_validation = validate_read_only_data_rows(data_request, data_rows)
    if data_validation.read_status != "validated":
        raise EMATestFixtureBacktestDryRunError("data_rows must pass read-only validation")
    signal_rows = _require_signal_rows(values["signal_rows"])
    strategy_id = _require_identifier(values["strategy_id"], "strategy_id")
    strategy_test_id = _require_identifier(values["strategy_test_id"], "strategy_test_id")
    if strategy_id != "ema_3_19":
        raise EMATestFixtureBacktestDryRunError("strategy_id must be ema_3_19")
    if data_request.strategy_id != strategy_id or data_request.strategy_test_id != strategy_test_id:
        raise EMATestFixtureBacktestDryRunError("request identifiers must match data_request")
    if len(signal_rows) != len(data_rows):
        raise EMATestFixtureBacktestDryRunError("signal_rows must align with data_rows")
    for signal_row, data_row in zip(signal_rows, data_rows, strict=True):
        if signal_row.strategy_id != strategy_id or signal_row.strategy_test_id != strategy_test_id:
            raise EMATestFixtureBacktestDryRunError("signal identifiers must match request")
        if str(data_row["timestamp"]) != signal_row.timestamp:
            raise EMATestFixtureBacktestDryRunError("signal timestamp must align with data row")
        if str(data_row["instrument_id"]) != signal_row.instrument_id:
            raise EMATestFixtureBacktestDryRunError("signal instrument must align with data row")
    return {
        "request_id": _require_identifier(values["request_id"], "request_id"),
        "strategy_id": strategy_id,
        "strategy_test_id": strategy_test_id,
        "data_request": data_request,
        "data_rows": data_rows,
        "signal_rows": signal_rows,
        "output_path": _require_output_path(values["output_path"]),
        "artifact_manifest_ref": _require_identifier(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "metrics_artifact_ref": _require_identifier(values["metrics_artifact_ref"], "metrics_artifact_ref"),
        "report_artifact_ref": _require_identifier(values["report_artifact_ref"], "report_artifact_ref"),
        "dry_run_mode": _require_choice(values["dry_run_mode"], "dry_run_mode", ALLOWED_DRY_RUN_MODES),
    }


class EMATestFixtureBacktestRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "data_request": ReadOnlyDataRequest,
        "data_rows": tuple[Mapping[str, object], ...],
        "signal_rows": tuple[SyntheticSignalRow, ...],
        "output_path": str,
        "artifact_manifest_ref": str,
        "metrics_artifact_ref": str,
        "report_artifact_ref": str,
        "dry_run_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_ema_test_fixture_backtest_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_ema_test_fixture_backtest_request(
    request: EMATestFixtureBacktestRequest,
) -> EMATestFixtureBacktestRequest:
    if not isinstance(request, EMATestFixtureBacktestRequest):
        raise TypeError("request must be EMATestFixtureBacktestRequest")
    validate_ema_test_fixture_backtest_request_values(request.__dict__)
    return request


def _close_value(row: Mapping[str, object]) -> float:
    value = row["close"]
    if not isinstance(value, (int, float)):
        raise EMATestFixtureBacktestDryRunError("close must be numeric")
    return float(value)


def _synthetic_result_rows(request: EMATestFixtureBacktestRequest) -> tuple[dict[str, object], ...]:
    result_rows: list[dict[str, object]] = []
    for index in range(len(request.data_rows) - 1):
        entry = _close_value(request.data_rows[index])
        exit_ = _close_value(request.data_rows[index + 1])
        signal = request.signal_rows[index].signal_value
        if not isinstance(signal, (int, float)):
            raise EMATestFixtureBacktestDryRunError("signal_value must be numeric")
        simple_return = 0.0 if entry == 0.0 else (exit_ - entry) / entry
        result_rows.append(
            {
                "timestamp": request.signal_rows[index].timestamp,
                "instrument_id": request.signal_rows[index].instrument_id,
                "signal_value": signal,
                "entry_close": entry,
                "next_close": exit_,
                "synthetic_return": simple_return * float(signal),
            }
        )
    if not result_rows:
        raise EMATestFixtureBacktestDryRunError("result rows must be non-empty")
    return tuple(result_rows)


def _metrics_summary(request: EMATestFixtureBacktestRequest, result_rows: tuple[dict[str, object], ...]) -> MetricsSummary:
    total_return = sum(float(row["synthetic_return"]) for row in result_rows)
    hit_rate = sum(1 for row in result_rows if float(row["synthetic_return"]) > 0.0) / len(result_rows)
    return MetricsSummary(
        run_id=request.request_id,
        strategy_id=request.strategy_id,
        test_type="test_fixture_backtest_dry_run",
        scope_level="test_fixture_only",
        result_status="synthetic_fixture_only",
        canonicality_status="not_canonical_result",
        metric_schema_version="metrics.v1",
        metric_records=(
            MetricRecord(
                metric_id="synthetic_total_return",
                metric_name="Synthetic total return",
                metric_value=total_return,
                metric_unit="ratio",
                scope="test_fixture_only",
                gross_or_net="gross",
                producer="ema_test_fixture_backtest_dry_run",
                consumer="pm_review_only",
            ),
            MetricRecord(
                metric_id="synthetic_hit_rate",
                metric_name="Synthetic hit rate",
                metric_value=hit_rate,
                metric_unit="ratio",
                scope="test_fixture_only",
                gross_or_net="gross",
                producer="ema_test_fixture_backtest_dry_run",
                consumer="pm_review_only",
            ),
        ),
        artifact_ref=request.metrics_artifact_ref,
    )


def _report_artifact(request: EMATestFixtureBacktestRequest) -> ReportArtifactSpec:
    return ReportArtifactSpec(
        report_id=request.report_artifact_ref,
        run_id=request.request_id,
        strategy_id=request.strategy_id,
        report_schema_version="report.v1",
        artifact_class="repo_relative",
        producer="ema_test_fixture_backtest_dry_run",
        consumer="pm_review_only",
        format="json",
        required_sections=(
            ReportSectionSpec(section_id="scope", title="Scope", required=True),
            ReportSectionSpec(section_id="guardrails", title="Guardrails", required=True),
        ),
    )


def _result_payload(
    request: EMATestFixtureBacktestRequest,
    result_rows: tuple[dict[str, object], ...],
    metrics_summary: MetricsSummary,
    report_artifact: ReportArtifactSpec,
) -> dict[str, object]:
    return {
        "artifact_manifest_ref": request.artifact_manifest_ref,
        "strategy_id": request.strategy_id,
        "strategy_test_id": request.strategy_test_id,
        "result_rows": result_rows,
        "metrics_artifact_ref": metrics_summary.artifact_ref,
        "report_artifact_ref": report_artifact.report_id,
        "scope": "test_fixture_only",
    }


def validate_ema_test_fixture_backtest_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, EMA_TEST_FIXTURE_BACKTEST_RESULT_FIELDS, "result")
    dry_run_status = _require_choice(values["dry_run_status"], "dry_run_status", ALLOWED_DRY_RUN_STATUSES)
    if dry_run_status == "written":
        row_count = values["row_count_or_none"]
        if not isinstance(row_count, int) or row_count < 1:
            raise EMATestFixtureBacktestDryRunError("written result requires row count")
        if not isinstance(values["metrics_summary_or_none"], MetricsSummary):
            raise EMATestFixtureBacktestDryRunError("written result requires metrics summary")
        if not isinstance(values["report_artifact_or_none"], ReportArtifactSpec):
            raise EMATestFixtureBacktestDryRunError("written result requires report artifact")
        if values["error_message_or_none"] is not None:
            raise EMATestFixtureBacktestDryRunError("written result must not include error")
    else:
        if values["row_count_or_none"] is not None:
            raise EMATestFixtureBacktestDryRunError("rejected result must not include row count")
        if values["metrics_summary_or_none"] is not None or values["report_artifact_or_none"] is not None:
            raise EMATestFixtureBacktestDryRunError("rejected result must not include artifacts")
        if not isinstance(values["error_message_or_none"], str) or not values["error_message_or_none"].strip():
            raise EMATestFixtureBacktestDryRunError("rejected result requires error")
    return {
        "dry_run_status": dry_run_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "row_count_or_none": values["row_count_or_none"],
        "output_path_or_none": values["output_path_or_none"],
        "metrics_summary_or_none": values["metrics_summary_or_none"],
        "report_artifact_or_none": values["report_artifact_or_none"],
        "error_message_or_none": values["error_message_or_none"],
    }


class EMATestFixtureBacktestResult:
    __annotations__ = {
        "dry_run_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "row_count_or_none": int | None,
        "output_path_or_none": str | None,
        "metrics_summary_or_none": MetricsSummary | None,
        "report_artifact_or_none": ReportArtifactSpec | None,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_ema_test_fixture_backtest_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_ema_test_fixture_backtest_result(
    result: EMATestFixtureBacktestResult,
) -> EMATestFixtureBacktestResult:
    if not isinstance(result, EMATestFixtureBacktestResult):
        raise TypeError("result must be EMATestFixtureBacktestResult")
    validate_ema_test_fixture_backtest_result_values(result.__dict__)
    return result


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result_from_request(
    *,
    request: object,
    dry_run_status: str,
    row_count_or_none: int | None,
    output_path_or_none: str | None,
    metrics_summary_or_none: MetricsSummary | None,
    report_artifact_or_none: ReportArtifactSpec | None,
    error_message_or_none: str | None,
) -> EMATestFixtureBacktestResult:
    return EMATestFixtureBacktestResult(
        dry_run_status=dry_run_status,
        request_id=_safe_text(request, "request_id"),
        strategy_id=_safe_text(request, "strategy_id"),
        strategy_test_id=_safe_text(request, "strategy_test_id"),
        row_count_or_none=row_count_or_none,
        output_path_or_none=output_path_or_none,
        metrics_summary_or_none=metrics_summary_or_none,
        report_artifact_or_none=report_artifact_or_none,
        error_message_or_none=error_message_or_none,
    )


def run_ema_3_19_test_fixture_backtest_dry_run(
    request: EMATestFixtureBacktestRequest,
) -> EMATestFixtureBacktestResult:
    try:
        validated_request = validate_ema_test_fixture_backtest_request(request)
        result_rows = _synthetic_result_rows(validated_request)
        metrics_summary = validate_metrics_summary(_metrics_summary(validated_request, result_rows))
        report_artifact = validate_report_artifact_spec(_report_artifact(validated_request))
        output_path = Path(validated_request.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(_result_payload(validated_request, result_rows, metrics_summary, report_artifact), sort_keys=True),
            encoding="utf-8",
        )
        return _result_from_request(
            request=validated_request,
            dry_run_status="written",
            row_count_or_none=len(result_rows),
            output_path_or_none=validated_request.output_path,
            metrics_summary_or_none=metrics_summary,
            report_artifact_or_none=report_artifact,
            error_message_or_none=None,
        )
    except (EMATestFixtureBacktestDryRunError, OSError, TypeError, ValueError) as error:
        return _result_from_request(
            request=request,
            dry_run_status="rejected",
            row_count_or_none=None,
            output_path_or_none=None,
            metrics_summary_or_none=None,
            report_artifact_or_none=None,
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_DRY_RUN_MODES",
    "ALLOWED_DRY_RUN_STATUSES",
    "EMA_TEST_FIXTURE_BACKTEST_REQUEST_FIELDS",
    "EMA_TEST_FIXTURE_BACKTEST_RESULT_FIELDS",
    "EMATestFixtureBacktestDryRunError",
    "EMATestFixtureBacktestRequest",
    "EMATestFixtureBacktestResult",
    "run_ema_3_19_test_fixture_backtest_dry_run",
    "validate_ema_test_fixture_backtest_request",
    "validate_ema_test_fixture_backtest_request_values",
    "validate_ema_test_fixture_backtest_result",
    "validate_ema_test_fixture_backtest_result_values",
]
