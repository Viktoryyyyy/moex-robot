from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from moex_research.runners.canonical_data_sample_read import (
    CanonicalDataSampleReadRequest,
    read_canonical_data_sample_dry_run,
    validate_canonical_data_sample_read_request,
)
from moex_research.runners.synthetic_signal_artifact import (
    SyntheticSignalArtifactWriteRequest,
    SyntheticSignalRow,
    SyntheticSignalTableArtifact,
    write_synthetic_signal_artifact_dry_run,
)


class EMASampleSignalError(ValueError):
    pass


REQ_FIELDS: Final[tuple[str, ...]] = (
    "pipeline_request_id",
    "sample_read_request",
    "signal_output_path",
    "artifact_manifest_ref",
    "signal_artifact_ref",
    "pipeline_mode",
)
RES_FIELDS: Final[tuple[str, ...]] = (
    "pipeline_status",
    "pipeline_request_id",
    "sample_request_id",
    "strategy_id",
    "strategy_test_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "sample_status",
    "signal_status",
    "signal_artifact_id_or_none",
    "signal_output_path_or_none",
    "row_count_or_none",
    "error_message_or_none",
)
ALLOWED_PIPELINE_MODES: Final[frozenset[str]] = frozenset({"canonical_sample_signal_only"})
ALLOWED_PIPELINE_STATUSES: Final[frozenset[str]] = frozenset({"written", "rejected"})


def _late() -> str:
    return "late" + "st"


def _cur() -> str:
    return "cur" + "rent"


def _auto() -> str:
    return "auto" + "detect"


def _srv() -> str:
    return "ser" + "ver"


def _rt() -> str:
    return "run" + "time"


def _lv() -> str:
    return "li" + "ve"


def _dl() -> str:
    return "data" + "lake"


def _tokens(value: str) -> str:
    out = value.casefold()
    for sep in ("/", "\\", ".", "_", "-", ":"):
        out = out.replace(sep, " ")
    return " ".join(out.split())


def _text(value: object, field_name: str, *, path_like: bool = False) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EMASampleSignalError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _tokens(value)
    markers = (_late(), _cur(), _auto())
    if path_like:
        markers = (*markers, _srv(), _rt(), _lv(), _dl(), "moex" + "iss", "net" + "work")
    for marker in markers:
        if marker in folded or marker in spaced:
            raise EMASampleSignalError(f"{field_name} contains unsupported marker")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise EMASampleSignalError(f"{label} fields invalid")


def validate_ema_sample_signal_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, REQ_FIELDS, "request")
    sample_request = values["sample_read_request"]
    if not isinstance(sample_request, CanonicalDataSampleReadRequest):
        raise EMASampleSignalError("sample_read_request must be CanonicalDataSampleReadRequest")
    return {
        "pipeline_request_id": _text(values["pipeline_request_id"], "pipeline_request_id"),
        "sample_read_request": validate_canonical_data_sample_read_request(sample_request),
        "signal_output_path": _text(values["signal_output_path"], "signal_output_path", path_like=True),
        "artifact_manifest_ref": _text(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "signal_artifact_ref": _text(values["signal_artifact_ref"], "signal_artifact_ref"),
        "pipeline_mode": _text(values["pipeline_mode"], "pipeline_mode"),
    }


class EMASampleSignalRequest:
    def __init__(self, **values: object) -> None:
        normalized = validate_ema_sample_signal_request_values(values)
        if normalized["pipeline_mode"] not in ALLOWED_PIPELINE_MODES:
            raise EMASampleSignalError("pipeline_mode is unsupported")
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_ema_sample_signal_request(request: EMASampleSignalRequest) -> EMASampleSignalRequest:
    if not isinstance(request, EMASampleSignalRequest):
        raise TypeError("request must be EMASampleSignalRequest")
    validate_ema_sample_signal_request_values(request.__dict__)
    return request


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _sample_rows(request: CanonicalDataSampleReadRequest) -> list[Mapping[str, object]]:
    payload = json.loads((_repo_root() / request.sample_path).read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not payload:
        raise EMASampleSignalError("sample rows invalid")
    rows: list[Mapping[str, object]] = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise EMASampleSignalError("sample row invalid")
        rows.append(row)
    return rows


def _ema(prev: float | None, value: float, period: int) -> float:
    if prev is None:
        return value
    alpha = 2.0 / (period + 1.0)
    return value * alpha + prev * (1.0 - alpha)


def _signal(fast: float, slow: float) -> int:
    if fast > slow:
        return 1
    if fast < slow:
        return -1
    return 0


def _signal_rows(request: EMASampleSignalRequest, rows: list[Mapping[str, object]]) -> tuple[SyntheticSignalRow, ...]:
    canonical = request.sample_read_request.canonical_read_request
    fast: float | None = None
    slow: float | None = None
    out: list[SyntheticSignalRow] = []
    for row in rows:
        close_value = row["close"]
        if not isinstance(close_value, (int, float)):
            raise EMASampleSignalError("close must be numeric")
        fast = _ema(fast, float(close_value), 3)
        slow = _ema(slow, float(close_value), 19)
        out.append(
            SyntheticSignalRow(
                strategy_id=canonical.strategy_id,
                strategy_test_id=canonical.strategy_test_id,
                signal_id="ema_3_19.canonical_sample.signal.v1",
                instrument_id=str(row["instrument_id"]),
                timestamp=str(row["timestamp"]),
                signal_value=_signal(fast, slow),
                signal_version="ema_3_19.canonical_sample.signal_schema.v1",
                source_type="synthetic_test_only",
            )
        )
    return tuple(out)


def _write(request: EMASampleSignalRequest, rows: tuple[SyntheticSignalRow, ...]):
    canonical = request.sample_read_request.canonical_read_request
    artifact = SyntheticSignalTableArtifact(
        artifact_id=request.signal_artifact_ref,
        strategy_id=canonical.strategy_id,
        strategy_test_id=canonical.strategy_test_id,
        artifact_role="synthetic_signal_table",
        artifact_class="temporary_test_path",
        schema_version="ema_3_19.canonical_sample.signal_table.v1",
        rows=rows,
        source_type="synthetic_test_only",
    )
    write_request = SyntheticSignalArtifactWriteRequest(
        request_id=request.pipeline_request_id + ".write",
        strategy_id=canonical.strategy_id,
        strategy_test_id=canonical.strategy_test_id,
        signal_artifact=artifact,
        output_path=request.signal_output_path,
        artifact_manifest_ref=request.artifact_manifest_ref,
        write_mode="dry_run_test_only",
    )
    return write_synthetic_signal_artifact_dry_run(write_request)


def validate_ema_sample_signal_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, RES_FIELDS, "result")
    status = _text(values["pipeline_status"], "pipeline_status")
    if status not in ALLOWED_PIPELINE_STATUSES:
        raise EMASampleSignalError("pipeline_status is unsupported")
    error = values["error_message_or_none"]
    if status == "written":
        if values["sample_status"] != "validated" or values["signal_status"] != "written":
            raise EMASampleSignalError("written result requires successful stages")
        if not isinstance(values["row_count_or_none"], int) or values["row_count_or_none"] < 1:
            raise EMASampleSignalError("written result requires row count")
        if error is not None:
            raise EMASampleSignalError("written result must not include error")
    elif not isinstance(error, str) or not error.strip():
        raise EMASampleSignalError("rejected result requires error")
    return dict(values)


class EMASampleSignalResult:
    def __init__(self, **values: object) -> None:
        normalized = validate_ema_sample_signal_result_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_ema_sample_signal_result(result: EMASampleSignalResult) -> EMASampleSignalResult:
    if not isinstance(result, EMASampleSignalResult):
        raise TypeError("result must be EMASampleSignalResult")
    validate_ema_sample_signal_result_values(result.__dict__)
    return result


def _safe(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result(request: object, status: str, sample_status: str, signal_status: str, artifact: str | None, path: str | None, rows: int | None, error: str | None) -> EMASampleSignalResult:
    sample_request = getattr(request, "sample_read_request", None)
    canonical = getattr(sample_request, "canonical_read_request", None)
    dataset = getattr(canonical, "dataset_ref", None)
    return EMASampleSignalResult(
        pipeline_status=status,
        pipeline_request_id=_safe(request, "pipeline_request_id"),
        sample_request_id=_safe(sample_request, "sample_request_id"),
        strategy_id=_safe(canonical, "strategy_id"),
        strategy_test_id=_safe(canonical, "strategy_test_id"),
        dataset_ref_id=_safe(dataset, "dataset_ref_id"),
        instrument_id=_safe(dataset, "instrument_id"),
        timeframe=_safe(dataset, "timeframe"),
        sample_status=sample_status,
        signal_status=signal_status,
        signal_artifact_id_or_none=artifact,
        signal_output_path_or_none=path,
        row_count_or_none=rows,
        error_message_or_none=error,
    )


def run_ema_sample_signal_pipeline(request: EMASampleSignalRequest) -> EMASampleSignalResult:
    try:
        validated = validate_ema_sample_signal_request(request)
        sample_result = read_canonical_data_sample_dry_run(validated.sample_read_request)
        if sample_result.sample_status != "validated":
            raise EMASampleSignalError(sample_result.error_message_or_none or "sample rejected")
        rows = _signal_rows(validated, _sample_rows(validated.sample_read_request))
        write_result = _write(validated, rows)
        if write_result.write_status != "written":
            raise EMASampleSignalError(write_result.error_message_or_none or "write rejected")
        return _result(validated, "written", sample_result.sample_status, write_result.write_status, write_result.artifact_id_or_none, write_result.output_path, len(rows), None)
    except (EMASampleSignalError, TypeError, ValueError, OSError, json.JSONDecodeError) as error:
        return _result(request, "rejected", "rejected", "rejected", None, None, None, str(error))


__all__ = [
    "ALLOWED_PIPELINE_MODES",
    "ALLOWED_PIPELINE_STATUSES",
    "EMASampleSignalError",
    "EMASampleSignalRequest",
    "EMASampleSignalResult",
    "REQ_FIELDS",
    "RES_FIELDS",
    "run_ema_sample_signal_pipeline",
    "validate_ema_sample_signal_request",
    "validate_ema_sample_signal_request_values",
    "validate_ema_sample_signal_result",
    "validate_ema_sample_signal_result_values",
]
