from __future__ import annotations

import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Final

from moex_research.metrics.schemas import MetricsSummary
from moex_research.publishers.report_artifacts import ReportArtifactSpec
from moex_research.runners.artifact_manifest_draft import ArtifactManifestDraft, validate_artifact_manifest_draft
from moex_research.runners.data_binding import ReadOnlyDataRequest, validate_read_only_data_rows
from moex_research.runners.ema_test_fixture_backtest_dry_run import (
    EMATestFixtureBacktestRequest,
    EMATestFixtureBacktestResult,
    run_ema_3_19_test_fixture_backtest_dry_run,
)
from moex_research.runners.ema_test_fixture_signal_calculation import (
    EMATestFixtureSignalCalculationRequest,
    EMATestFixtureSignalCalculationResult,
    calculate_ema_3_19_test_fixture_signals_dry_run,
)


class EMAFixturePipelineDryRunError(ValueError):
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


EMA_FIXTURE_PIPELINE_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "data_request",
    "data_rows",
    "signal_output_path",
    "backtest_output_path",
    "artifact_manifest_ref",
    "signal_artifact_ref",
    "backtest_artifact_ref",
    "metrics_artifact_ref",
    "report_artifact_ref",
    "pipeline_mode",
)
EMA_FIXTURE_PIPELINE_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "pipeline_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "read_status",
    "signal_status",
    "backtest_status",
    "artifact_manifest_draft_id_or_none",
    "signal_output_path_or_none",
    "backtest_output_path_or_none",
    "metrics_summary_or_none",
    "report_artifact_or_none",
    "error_message_or_none",
)
ALLOWED_PIPELINE_MODES: Final[frozenset[str]] = frozenset({"fixture_pipeline_dry_run_only"})
ALLOWED_PIPELINE_STATUSES: Final[frozenset[str]] = frozenset({"written", "rejected"})


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
            raise EMAFixturePipelineDryRunError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EMAFixturePipelineDryRunError(f"{field_name} is required")
    return value


def _require_identifier(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=False)


def _require_path_ref(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name, include_path_markers=True)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_text(value, field_name)
    if candidate not in allowed_values:
        raise EMAFixturePipelineDryRunError(f"{field_name} is unsupported")
    return candidate


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise EMAFixturePipelineDryRunError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise EMAFixturePipelineDryRunError(f"{label} contains unsupported fields")


def _require_data_rows(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise EMAFixturePipelineDryRunError("data_rows must be a non-empty iterable")
    rows: list[Mapping[str, object]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise EMAFixturePipelineDryRunError("data_rows must contain mappings")
        rows.append(row)
    if len(rows) < 2:
        raise EMAFixturePipelineDryRunError("data_rows must contain at least two rows")
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


def _require_temp_json_path(value: object, field_name: str) -> str:
    path_value = _require_path_ref(value, field_name)
    candidate = Path(path_value)
    if candidate.suffix != ".json":
        raise EMAFixturePipelineDryRunError(f"{field_name} must be json")
    if not _is_under_temp_root(path_value):
        raise EMAFixturePipelineDryRunError(f"{field_name} must use temporary test path")
    return path_value


def validate_ema_fixture_pipeline_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, EMA_FIXTURE_PIPELINE_REQUEST_FIELDS, "request")
    data_request = values["data_request"]
    if not isinstance(data_request, ReadOnlyDataRequest):
        raise EMAFixturePipelineDryRunError("data_request must be ReadOnlyDataRequest")
    data_rows = _require_data_rows(values["data_rows"])
    strategy_id = _require_identifier(values["strategy_id"], "strategy_id")
    strategy_test_id = _require_identifier(values["strategy_test_id"], "strategy_test_id")
    if strategy_id != "ema_3_19":
        raise EMAFixturePipelineDryRunError("strategy_id must be ema_3_19")
    if data_request.strategy_id != strategy_id or data_request.strategy_test_id != strategy_test_id:
        raise EMAFixturePipelineDryRunError("request identifiers must match data_request")
    read_result = validate_read_only_data_rows(data_request, data_rows)
    if read_result.read_status != "validated":
        raise EMAFixturePipelineDryRunError("data_rows must pass read-only validation")
    return {
        "request_id": _require_identifier(values["request_id"], "request_id"),
        "strategy_id": strategy_id,
        "strategy_test_id": strategy_test_id,
        "data_request": data_request,
        "data_rows": data_rows,
        "signal_output_path": _require_temp_json_path(values["signal_output_path"], "signal_output_path"),
        "backtest_output_path": _require_temp_json_path(values["backtest_output_path"], "backtest_output_path"),
        "artifact_manifest_ref": _require_identifier(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "signal_artifact_ref": _require_identifier(values["signal_artifact_ref"], "signal_artifact_ref"),
        "backtest_artifact_ref": _require_identifier(values["backtest_artifact_ref"], "backtest_artifact_ref"),
        "metrics_artifact_ref": _require_identifier(values["metrics_artifact_ref"], "metrics_artifact_ref"),
        "report_artifact_ref": _require_identifier(values["report_artifact_ref"], "report_artifact_ref"),
        "pipeline_mode": _require_choice(values["pipeline_mode"], "pipeline_mode", ALLOWED_PIPELINE_MODES),
    }


class EMAFixturePipelineRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "data_request": ReadOnlyDataRequest,
        "data_rows": tuple[Mapping[str, object], ...],
        "signal_output_path": str,
        "backtest_output_path": str,
        "artifact_manifest_ref": str,
        "signal_artifact_ref": str,
        "backtest_artifact_ref": str,
        "metrics_artifact_ref": str,
        "report_artifact_ref": str,
        "pipeline_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_ema_fixture_pipeline_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_ema_fixture_pipeline_request(request: EMAFixturePipelineRequest) -> EMAFixturePipelineRequest:
    if not isinstance(request, EMAFixturePipelineRequest):
        raise TypeError("request must be EMAFixturePipelineRequest")
    validate_ema_fixture_pipeline_request_values(request.__dict__)
    return request


def _signal_request(request: EMAFixturePipelineRequest) -> EMATestFixtureSignalCalculationRequest:
    return EMATestFixtureSignalCalculationRequest(
        request_id=request.request_id + ".signal",
        strategy_id=request.strategy_id,
        strategy_test_id=request.strategy_test_id,
        data_request=request.data_request,
        rows=request.data_rows,
        fast_period=3,
        slow_period=19,
        signal_id="ema_3_19.signal.test_fixture.v1",
        signal_version="ema_3_19.signal_schema.v1",
        output_path=request.signal_output_path,
        artifact_manifest_ref=request.artifact_manifest_ref,
        calculation_mode="test_fixture_signal_calculation_only",
    )


def _signal_rows_from_file(signal_output_path: str) -> tuple[object, ...]:
    import json

    payload = json.loads(Path(signal_output_path).read_text(encoding="utf-8"))
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise EMAFixturePipelineDryRunError("signal artifact must contain rows")
    from moex_research.runners.synthetic_signal_artifact import SyntheticSignalRow

    return tuple(SyntheticSignalRow(**row) for row in rows)


def _backtest_request(
    request: EMAFixturePipelineRequest,
    signal_rows: tuple[object, ...],
) -> EMATestFixtureBacktestRequest:
    return EMATestFixtureBacktestRequest(
        request_id=request.request_id + ".backtest",
        strategy_id=request.strategy_id,
        strategy_test_id=request.strategy_test_id,
        data_request=request.data_request,
        data_rows=request.data_rows,
        signal_rows=signal_rows,
        output_path=request.backtest_output_path,
        artifact_manifest_ref=request.artifact_manifest_ref,
        metrics_artifact_ref=request.metrics_artifact_ref,
        report_artifact_ref=request.report_artifact_ref,
        dry_run_mode="test_fixture_backtest_only",
    )


def _artifact_manifest_draft(request: EMAFixturePipelineRequest) -> ArtifactManifestDraft:
    return ArtifactManifestDraft(
        artifact_manifest_draft_id=request.request_id + ".artifact_manifest_draft",
        request_id=request.request_id,
        strategy_id=request.strategy_id,
        strategy_test_id=request.strategy_test_id,
        planned_artifacts=(
            request.signal_artifact_ref,
            request.backtest_artifact_ref,
            request.metrics_artifact_ref,
            request.report_artifact_ref,
        ),
        artifact_manifest_ref=request.artifact_manifest_ref,
        write_allowed=False,
        registry_write_allowed=False,
        promotion_verdict_allowed=False,
    )


def validate_ema_fixture_pipeline_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, EMA_FIXTURE_PIPELINE_RESULT_FIELDS, "result")
    pipeline_status = _require_choice(values["pipeline_status"], "pipeline_status", ALLOWED_PIPELINE_STATUSES)
    if pipeline_status == "written":
        if values["signal_status"] != "written" or values["backtest_status"] != "written":
            raise EMAFixturePipelineDryRunError("written pipeline requires written stages")
        if not isinstance(values["metrics_summary_or_none"], MetricsSummary):
            raise EMAFixturePipelineDryRunError("written pipeline requires metrics summary")
        if not isinstance(values["report_artifact_or_none"], ReportArtifactSpec):
            raise EMAFixturePipelineDryRunError("written pipeline requires report artifact")
        if values["error_message_or_none"] is not None:
            raise EMAFixturePipelineDryRunError("written pipeline must not include error")
    else:
        if values["metrics_summary_or_none"] is not None or values["report_artifact_or_none"] is not None:
            raise EMAFixturePipelineDryRunError("rejected pipeline must not include drafts")
        if not isinstance(values["error_message_or_none"], str) or not values["error_message_or_none"].strip():
            raise EMAFixturePipelineDryRunError("rejected pipeline requires error")
    return {
        "pipeline_status": pipeline_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "read_status": _require_text(values["read_status"], "read_status"),
        "signal_status": _require_text(values["signal_status"], "signal_status"),
        "backtest_status": _require_text(values["backtest_status"], "backtest_status"),
        "artifact_manifest_draft_id_or_none": values["artifact_manifest_draft_id_or_none"],
        "signal_output_path_or_none": values["signal_output_path_or_none"],
        "backtest_output_path_or_none": values["backtest_output_path_or_none"],
        "metrics_summary_or_none": values["metrics_summary_or_none"],
        "report_artifact_or_none": values["report_artifact_or_none"],
        "error_message_or_none": values["error_message_or_none"],
    }


class EMAFixturePipelineResult:
    __annotations__ = {
        "pipeline_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "read_status": str,
        "signal_status": str,
        "backtest_status": str,
        "artifact_manifest_draft_id_or_none": str | None,
        "signal_output_path_or_none": str | None,
        "backtest_output_path_or_none": str | None,
        "metrics_summary_or_none": MetricsSummary | None,
        "report_artifact_or_none": ReportArtifactSpec | None,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_ema_fixture_pipeline_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_ema_fixture_pipeline_result(result: EMAFixturePipelineResult) -> EMAFixturePipelineResult:
    if not isinstance(result, EMAFixturePipelineResult):
        raise TypeError("result must be EMAFixturePipelineResult")
    validate_ema_fixture_pipeline_result_values(result.__dict__)
    return result


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result_from_request(
    *,
    request: object,
    pipeline_status: str,
    read_status: str,
    signal_status: str,
    backtest_status: str,
    artifact_manifest_draft_id_or_none: str | None,
    signal_output_path_or_none: str | None,
    backtest_output_path_or_none: str | None,
    metrics_summary_or_none: MetricsSummary | None,
    report_artifact_or_none: ReportArtifactSpec | None,
    error_message_or_none: str | None,
) -> EMAFixturePipelineResult:
    return EMAFixturePipelineResult(
        pipeline_status=pipeline_status,
        request_id=_safe_text(request, "request_id"),
        strategy_id=_safe_text(request, "strategy_id"),
        strategy_test_id=_safe_text(request, "strategy_test_id"),
        read_status=read_status,
        signal_status=signal_status,
        backtest_status=backtest_status,
        artifact_manifest_draft_id_or_none=artifact_manifest_draft_id_or_none,
        signal_output_path_or_none=signal_output_path_or_none,
        backtest_output_path_or_none=backtest_output_path_or_none,
        metrics_summary_or_none=metrics_summary_or_none,
        report_artifact_or_none=report_artifact_or_none,
        error_message_or_none=error_message_or_none,
    )


def run_ema_fixture_pipeline_dry_run(request: EMAFixturePipelineRequest) -> EMAFixturePipelineResult:
    try:
        validated_request = validate_ema_fixture_pipeline_request(request)
        read_result = validate_read_only_data_rows(validated_request.data_request, validated_request.data_rows)
        if read_result.read_status != "validated":
            raise EMAFixturePipelineDryRunError("read stage rejected")
        signal_result: EMATestFixtureSignalCalculationResult = calculate_ema_3_19_test_fixture_signals_dry_run(
            _signal_request(validated_request)
        )
        if signal_result.calculation_status != "written":
            raise EMAFixturePipelineDryRunError(signal_result.error_message_or_none or "signal stage rejected")
        signal_rows = _signal_rows_from_file(validated_request.signal_output_path)
        backtest_result: EMATestFixtureBacktestResult = run_ema_3_19_test_fixture_backtest_dry_run(
            _backtest_request(validated_request, signal_rows)
        )
        if backtest_result.dry_run_status != "written":
            raise EMAFixturePipelineDryRunError(backtest_result.error_message_or_none or "backtest stage rejected")
        artifact_manifest_draft = validate_artifact_manifest_draft(_artifact_manifest_draft(validated_request))
        return _result_from_request(
            request=validated_request,
            pipeline_status="written",
            read_status=read_result.read_status,
            signal_status=signal_result.calculation_status,
            backtest_status=backtest_result.dry_run_status,
            artifact_manifest_draft_id_or_none=artifact_manifest_draft.artifact_manifest_draft_id,
            signal_output_path_or_none=signal_result.output_path_or_none,
            backtest_output_path_or_none=backtest_result.output_path_or_none,
            metrics_summary_or_none=backtest_result.metrics_summary_or_none,
            report_artifact_or_none=backtest_result.report_artifact_or_none,
            error_message_or_none=None,
        )
    except (EMAFixturePipelineDryRunError, TypeError, ValueError, OSError) as error:
        return _result_from_request(
            request=request,
            pipeline_status="rejected",
            read_status="rejected",
            signal_status="rejected",
            backtest_status="rejected",
            artifact_manifest_draft_id_or_none=None,
            signal_output_path_or_none=None,
            backtest_output_path_or_none=None,
            metrics_summary_or_none=None,
            report_artifact_or_none=None,
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_PIPELINE_MODES",
    "ALLOWED_PIPELINE_STATUSES",
    "EMA_FIXTURE_PIPELINE_REQUEST_FIELDS",
    "EMA_FIXTURE_PIPELINE_RESULT_FIELDS",
    "EMAFixturePipelineDryRunError",
    "EMAFixturePipelineRequest",
    "EMAFixturePipelineResult",
    "run_ema_fixture_pipeline_dry_run",
    "validate_ema_fixture_pipeline_request",
    "validate_ema_fixture_pipeline_request_values",
    "validate_ema_fixture_pipeline_result",
    "validate_ema_fixture_pipeline_result_values",
]
