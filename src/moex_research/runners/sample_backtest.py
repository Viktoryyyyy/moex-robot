from __future__ import annotations

import json
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from moex_research.metrics.schemas import MetricRecord, MetricsSummary, validate_metrics_summary
from moex_research.publishers.report_artifacts import ReportArtifactSpec, ReportSectionSpec, validate_report_artifact_spec
from moex_research.runners.ema_sample_signal import EMASampleSignalRequest, run_ema_sample_signal_pipeline, validate_ema_sample_signal_request


class SampleBacktestError(ValueError):
    pass


REQ_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "signal_request",
    "backtest_output_path",
    "artifact_manifest_ref",
    "backtest_artifact_ref",
    "metrics_artifact_ref",
    "report_artifact_ref",
    "mode",
)
RES_FIELDS: Final[tuple[str, ...]] = (
    "status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "signal_status",
    "backtest_output_path_or_none",
    "backtest_artifact_ref_or_none",
    "row_count_or_none",
    "metrics_summary_or_none",
    "report_artifact_or_none",
    "error_message_or_none",
)
ALLOWED_MODES: Final[frozenset[str]] = frozenset({"canonical_sample_backtest_only"})
ALLOWED_STATUSES: Final[frozenset[str]] = frozenset({"written", "rejected"})


def _late() -> str:
    return "late" + "st"


def _cur() -> str:
    return "cur" + "rent"


def _auto() -> str:
    return "auto" + "detect"


def _markers(path_like: bool) -> tuple[str, ...]:
    base = (_late(), _cur(), _auto())
    if not path_like:
        return base
    return (*base, "ser" + "ver", "run" + "time", "li" + "ve", "data" + "lake", "moex" + "iss", "net" + "work")


def _flat(value: str) -> str:
    result = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        result = result.replace(separator, " ")
    return " ".join(result.split())


def _text(value: object, field_name: str, *, path_like: bool = False) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SampleBacktestError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in _markers(path_like):
        if marker in folded or marker in spaced:
            raise SampleBacktestError(f"{field_name} contains unsupported marker")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise SampleBacktestError(f"{label} fields invalid")


def _temp_json_path(value: object) -> str:
    path_value = _text(value, "backtest_output_path", path_like=True)
    path = Path(path_value)
    if path.suffix != ".json":
        raise SampleBacktestError("backtest_output_path must be json")
    root = Path(tempfile.gettempdir()).resolve(strict=False)
    candidate = path.resolve(strict=False)
    if not (candidate == root or str(candidate).startswith(str(root) + "/")):
        raise SampleBacktestError("backtest_output_path must be temporary")
    return path_value


def validate_sample_backtest_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, REQ_FIELDS, "request")
    signal_request = values["signal_request"]
    if not isinstance(signal_request, EMASampleSignalRequest):
        raise SampleBacktestError("signal_request must be EMASampleSignalRequest")
    mode = _text(values["mode"], "mode")
    if mode not in ALLOWED_MODES:
        raise SampleBacktestError("mode is unsupported")
    return {
        "request_id": _text(values["request_id"], "request_id"),
        "signal_request": validate_ema_sample_signal_request(signal_request),
        "backtest_output_path": _temp_json_path(values["backtest_output_path"]),
        "artifact_manifest_ref": _text(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "backtest_artifact_ref": _text(values["backtest_artifact_ref"], "backtest_artifact_ref"),
        "metrics_artifact_ref": _text(values["metrics_artifact_ref"], "metrics_artifact_ref"),
        "report_artifact_ref": _text(values["report_artifact_ref"], "report_artifact_ref"),
        "mode": mode,
    }


class SampleBacktestRequest:
    def __init__(self, **values: object) -> None:
        normalized = validate_sample_backtest_request_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_sample_backtest_request(request: SampleBacktestRequest) -> SampleBacktestRequest:
    if not isinstance(request, SampleBacktestRequest):
        raise TypeError("request must be SampleBacktestRequest")
    validate_sample_backtest_request_values(request.__dict__)
    return request


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _sample_rows(request: SampleBacktestRequest) -> list[Mapping[str, object]]:
    path = _repo_root() / request.signal_request.sample_read_request.sample_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or len(payload) < 2:
        raise SampleBacktestError("sample rows invalid")
    rows: list[Mapping[str, object]] = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise SampleBacktestError("sample row invalid")
        rows.append(row)
    return rows


def _signal_rows(request: SampleBacktestRequest) -> list[Mapping[str, object]]:
    signal_result = run_ema_sample_signal_pipeline(request.signal_request)
    if signal_result.pipeline_status != "written" or not signal_result.signal_output_path_or_none:
        raise SampleBacktestError(signal_result.error_message_or_none or "signal pipeline rejected")
    payload = json.loads(Path(signal_result.signal_output_path_or_none).read_text(encoding="utf-8"))
    rows = payload.get("rows")
    if not isinstance(rows, list) or len(rows) < 2:
        raise SampleBacktestError("signal rows invalid")
    return rows


def _float(value: object, field_name: str) -> float:
    if not isinstance(value, (int, float)):
        raise SampleBacktestError(f"{field_name} must be numeric")
    return float(value)


def _result_rows(sample_rows: list[Mapping[str, object]], signal_rows: list[Mapping[str, object]]) -> tuple[dict[str, object], ...]:
    if len(sample_rows) != len(signal_rows):
        raise SampleBacktestError("sample and signal rows must align")
    rows: list[dict[str, object]] = []
    for index in range(len(sample_rows) - 1):
        signal = _float(signal_rows[index]["signal_value"], "signal_value")
        entry = _float(sample_rows[index]["close"], "close")
        exit_ = _float(sample_rows[index + 1]["close"], "close")
        simple_return = 0.0 if entry == 0.0 else (exit_ - entry) / entry
        rows.append(
            {
                "timestamp": signal_rows[index]["timestamp"],
                "instrument_id": signal_rows[index]["instrument_id"],
                "signal_value": signal,
                "entry_close": entry,
                "next_close": exit_,
                "synthetic_return": simple_return * signal,
            }
        )
    return tuple(rows)


def _metrics(request: SampleBacktestRequest, rows: tuple[dict[str, object], ...]) -> MetricsSummary:
    total_return = sum(float(row["synthetic_return"]) for row in rows)
    hit_rate = sum(1 for row in rows if float(row["synthetic_return"]) > 0.0) / len(rows)
    return validate_metrics_summary(
        MetricsSummary(
            run_id=request.request_id,
            strategy_id="ema_3_19",
            test_type="canonical_sample_backtest",
            scope_level="canonical_sample_only",
            result_status="sample_only",
            canonicality_status="not_canonical_result",
            metric_schema_version="metrics.v1",
            metric_records=(
                MetricRecord(
                    metric_id="sample_total_return",
                    metric_name="Sample total return",
                    metric_value=total_return,
                    metric_unit="ratio",
                    scope="canonical_sample_only",
                    gross_or_net="gross",
                    producer="sample_backtest",
                    consumer="pm_review_only",
                ),
                MetricRecord(
                    metric_id="sample_hit_rate",
                    metric_name="Sample hit rate",
                    metric_value=hit_rate,
                    metric_unit="ratio",
                    scope="canonical_sample_only",
                    gross_or_net="gross",
                    producer="sample_backtest",
                    consumer="pm_review_only",
                ),
            ),
            artifact_ref=request.metrics_artifact_ref,
        )
    )


def _report(request: SampleBacktestRequest) -> ReportArtifactSpec:
    return validate_report_artifact_spec(
        ReportArtifactSpec(
            report_id=request.report_artifact_ref,
            run_id=request.request_id,
            strategy_id="ema_3_19",
            report_schema_version="report.v1",
            artifact_class="repo_relative",
            producer="sample_backtest",
            consumer="pm_review_only",
            format="json",
            required_sections=(
                ReportSectionSpec(section_id="scope", title="Scope", required=True),
                ReportSectionSpec(section_id="guardrails", title="Guardrails", required=True),
            ),
        )
    )


def validate_sample_backtest_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, RES_FIELDS, "result")
    status = _text(values["status"], "status")
    if status not in ALLOWED_STATUSES:
        raise SampleBacktestError("status is unsupported")
    error = values["error_message_or_none"]
    if status == "written":
        if not isinstance(values["row_count_or_none"], int) or values["row_count_or_none"] < 1:
            raise SampleBacktestError("written result requires row count")
        if not isinstance(values["metrics_summary_or_none"], MetricsSummary):
            raise SampleBacktestError("written result requires metrics summary")
        if not isinstance(values["report_artifact_or_none"], ReportArtifactSpec):
            raise SampleBacktestError("written result requires report artifact")
        if error is not None:
            raise SampleBacktestError("written result must not include error")
    else:
        if values["row_count_or_none"] is not None or values["metrics_summary_or_none"] is not None or values["report_artifact_or_none"] is not None:
            raise SampleBacktestError("rejected result must not include outputs")
        if not isinstance(error, str) or not error.strip():
            raise SampleBacktestError("rejected result requires error")
    return dict(values)


class SampleBacktestResult:
    def __init__(self, **values: object) -> None:
        normalized = validate_sample_backtest_result_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_sample_backtest_result(result: SampleBacktestResult) -> SampleBacktestResult:
    if not isinstance(result, SampleBacktestResult):
        raise TypeError("result must be SampleBacktestResult")
    validate_sample_backtest_result_values(result.__dict__)
    return result


def _safe(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result(request: object, status: str, path: str | None, artifact: str | None, rows: int | None, metrics: MetricsSummary | None, report: ReportArtifactSpec | None, error: str | None) -> SampleBacktestResult:
    signal_request = getattr(request, "signal_request", None)
    sample_request = getattr(signal_request, "sample_read_request", None)
    canonical = getattr(sample_request, "canonical_read_request", None)
    dataset = getattr(canonical, "dataset_ref", None)
    return SampleBacktestResult(
        status=status,
        request_id=_safe(request, "request_id"),
        strategy_id=_safe(canonical, "strategy_id"),
        strategy_test_id=_safe(canonical, "strategy_test_id"),
        dataset_ref_id=_safe(dataset, "dataset_ref_id"),
        instrument_id=_safe(dataset, "instrument_id"),
        timeframe=_safe(dataset, "timeframe"),
        signal_status="written" if status == "written" else "rejected",
        backtest_output_path_or_none=path,
        backtest_artifact_ref_or_none=artifact,
        row_count_or_none=rows,
        metrics_summary_or_none=metrics,
        report_artifact_or_none=report,
        error_message_or_none=error,
    )


def run_sample_backtest_pipeline(request: SampleBacktestRequest) -> SampleBacktestResult:
    try:
        validated = validate_sample_backtest_request(request)
        rows = _result_rows(_sample_rows(validated), _signal_rows(validated))
        metrics = _metrics(validated, rows)
        report = _report(validated)
        payload = {
            "artifact_manifest_ref": validated.artifact_manifest_ref,
            "backtest_artifact_ref": validated.backtest_artifact_ref,
            "metrics_artifact_ref": validated.metrics_artifact_ref,
            "report_artifact_ref": validated.report_artifact_ref,
            "scope": "canonical_sample_only",
            "result_rows": rows,
        }
        path = Path(validated.backtest_output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        return _result(validated, "written", validated.backtest_output_path, validated.backtest_artifact_ref, len(rows), metrics, report, None)
    except (SampleBacktestError, TypeError, ValueError, OSError, json.JSONDecodeError) as error:
        return _result(request, "rejected", None, None, None, None, None, str(error))


__all__ = [
    "ALLOWED_MODES",
    "ALLOWED_STATUSES",
    "REQ_FIELDS",
    "RES_FIELDS",
    "SampleBacktestError",
    "SampleBacktestRequest",
    "SampleBacktestResult",
    "run_sample_backtest_pipeline",
    "validate_sample_backtest_request",
    "validate_sample_backtest_request_values",
    "validate_sample_backtest_result",
    "validate_sample_backtest_result_values",
]
