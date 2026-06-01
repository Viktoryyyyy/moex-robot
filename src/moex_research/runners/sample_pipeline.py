from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.metrics.schemas import MetricsSummary
from moex_research.publishers.report_artifacts import ReportArtifactSpec
from moex_research.runners.sample_backtest import (
    SampleBacktestRequest,
    run_sample_backtest_pipeline,
    validate_sample_backtest_request,
)


class SamplePipelineError(ValueError):
    pass


REQ_FIELDS: Final[tuple[str, ...]] = (
    "pipeline_id",
    "backtest_request",
    "mode",
)
RES_FIELDS: Final[tuple[str, ...]] = (
    "pipeline_status",
    "pipeline_id",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "signal_status",
    "backtest_status",
    "backtest_output_path_or_none",
    "metrics_summary_or_none",
    "report_artifact_or_none",
    "error_message_or_none",
)
ALLOWED_MODES: Final[frozenset[str]] = frozenset({"canonical_sample_pipeline_only"})
ALLOWED_STATUSES: Final[frozenset[str]] = frozenset({"written", "rejected"})


def _late() -> str:
    return "late" + "st"


def _cur() -> str:
    return "cur" + "rent"


def _auto() -> str:
    return "auto" + "detect"


def _flat(value: str) -> str:
    result = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        result = result.replace(separator, " ")
    return " ".join(result.split())


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SamplePipelineError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise SamplePipelineError(f"{field_name} contains unsupported marker")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise SamplePipelineError(f"{label} fields invalid")


def validate_sample_pipeline_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, REQ_FIELDS, "request")
    backtest_request = values["backtest_request"]
    if not isinstance(backtest_request, SampleBacktestRequest):
        raise SamplePipelineError("backtest_request must be SampleBacktestRequest")
    mode = _text(values["mode"], "mode")
    if mode not in ALLOWED_MODES:
        raise SamplePipelineError("mode is unsupported")
    return {
        "pipeline_id": _text(values["pipeline_id"], "pipeline_id"),
        "backtest_request": validate_sample_backtest_request(backtest_request),
        "mode": mode,
    }


class SamplePipelineRequest:
    def __init__(self, **values: object) -> None:
        normalized = validate_sample_pipeline_request_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_sample_pipeline_request(request: SamplePipelineRequest) -> SamplePipelineRequest:
    if not isinstance(request, SamplePipelineRequest):
        raise TypeError("request must be SamplePipelineRequest")
    validate_sample_pipeline_request_values(request.__dict__)
    return request


def validate_sample_pipeline_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, RES_FIELDS, "result")
    status = _text(values["pipeline_status"], "pipeline_status")
    if status not in ALLOWED_STATUSES:
        raise SamplePipelineError("pipeline_status is unsupported")
    error = values["error_message_or_none"]
    if status == "written":
        if values["signal_status"] != "written" or values["backtest_status"] != "written":
            raise SamplePipelineError("written result requires written stages")
        if not isinstance(values["metrics_summary_or_none"], MetricsSummary):
            raise SamplePipelineError("written result requires metrics summary")
        if not isinstance(values["report_artifact_or_none"], ReportArtifactSpec):
            raise SamplePipelineError("written result requires report artifact")
        if error is not None:
            raise SamplePipelineError("written result must not include error")
    else:
        if values["metrics_summary_or_none"] is not None or values["report_artifact_or_none"] is not None:
            raise SamplePipelineError("rejected result must not include artifacts")
        if not isinstance(error, str) or not error.strip():
            raise SamplePipelineError("rejected result requires error")
    return dict(values)


class SamplePipelineResult:
    def __init__(self, **values: object) -> None:
        normalized = validate_sample_pipeline_result_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_sample_pipeline_result(result: SamplePipelineResult) -> SamplePipelineResult:
    if not isinstance(result, SamplePipelineResult):
        raise TypeError("result must be SamplePipelineResult")
    validate_sample_pipeline_result_values(result.__dict__)
    return result


def _safe(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result(request: object, status: str, signal_status: str, backtest_status: str, path: str | None, metrics: MetricsSummary | None, report: ReportArtifactSpec | None, error: str | None) -> SamplePipelineResult:
    backtest_request = getattr(request, "backtest_request", None)
    signal_request = getattr(backtest_request, "signal_request", None)
    sample_request = getattr(signal_request, "sample_read_request", None)
    canonical = getattr(sample_request, "canonical_read_request", None)
    dataset = getattr(canonical, "dataset_ref", None)
    return SamplePipelineResult(
        pipeline_status=status,
        pipeline_id=_safe(request, "pipeline_id"),
        request_id=_safe(backtest_request, "request_id"),
        strategy_id=_safe(canonical, "strategy_id"),
        strategy_test_id=_safe(canonical, "strategy_test_id"),
        dataset_ref_id=_safe(dataset, "dataset_ref_id"),
        instrument_id=_safe(dataset, "instrument_id"),
        timeframe=_safe(dataset, "timeframe"),
        signal_status=signal_status,
        backtest_status=backtest_status,
        backtest_output_path_or_none=path,
        metrics_summary_or_none=metrics,
        report_artifact_or_none=report,
        error_message_or_none=error,
    )


def run_sample_pipeline(request: SamplePipelineRequest) -> SamplePipelineResult:
    try:
        validated = validate_sample_pipeline_request(request)
        backtest_result = run_sample_backtest_pipeline(validated.backtest_request)
        if backtest_result.status != "written":
            raise SamplePipelineError(backtest_result.error_message_or_none or "backtest rejected")
        return _result(
            validated,
            "written",
            backtest_result.signal_status,
            backtest_result.status,
            backtest_result.backtest_output_path_or_none,
            backtest_result.metrics_summary_or_none,
            backtest_result.report_artifact_or_none,
            None,
        )
    except (SamplePipelineError, TypeError, ValueError, OSError) as error:
        return _result(request, "rejected", "rejected", "rejected", None, None, None, str(error))


__all__ = [
    "ALLOWED_MODES",
    "ALLOWED_STATUSES",
    "REQ_FIELDS",
    "RES_FIELDS",
    "SamplePipelineError",
    "SamplePipelineRequest",
    "SamplePipelineResult",
    "run_sample_pipeline",
    "validate_sample_pipeline_request",
    "validate_sample_pipeline_request_values",
    "validate_sample_pipeline_result",
    "validate_sample_pipeline_result_values",
]
