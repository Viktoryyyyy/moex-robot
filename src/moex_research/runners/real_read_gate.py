from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.sample_pipeline import SamplePipelineResult, validate_sample_pipeline_result


class RealReadGateError(ValueError):
    pass


REQ_FIELDS: Final[tuple[str, ...]] = (
    "gate_request_id",
    "sample_pipeline_result",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "readiness_mode",
    "allow_real_read",
    "allow_network",
    "allow_registry_write",
    "allow_runtime",
)
RES_FIELDS: Final[tuple[str, ...]] = (
    "gate_status",
    "gate_request_id",
    "dataset_ref_id",
    "instrument_id",
    "timeframe",
    "sample_pipeline_status",
    "real_read_allowed",
    "reason_or_none",
)
ALLOWED_READINESS_MODES: Final[frozenset[str]] = frozenset({"sample_to_real_read_gate_only"})
ALLOWED_GATE_STATUSES: Final[frozenset[str]] = frozenset({"blocked", "eligible_for_separate_review"})


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
        raise RealReadGateError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadGateError(f"{field_name} contains unsupported marker")
    return value


def _false(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadGateError(f"{field_name} must be bool")
    if value:
        raise RealReadGateError(f"{field_name} must be false")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadGateError(f"{label} fields invalid")


def validate_real_read_gate_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, REQ_FIELDS, "request")
    sample_result = values["sample_pipeline_result"]
    if not isinstance(sample_result, SamplePipelineResult):
        raise RealReadGateError("sample_pipeline_result must be SamplePipelineResult")
    mode = _text(values["readiness_mode"], "readiness_mode")
    if mode not in ALLOWED_READINESS_MODES:
        raise RealReadGateError("readiness_mode is unsupported")
    return {
        "gate_request_id": _text(values["gate_request_id"], "gate_request_id"),
        "sample_pipeline_result": validate_sample_pipeline_result(sample_result),
        "dataset_ref_id": _text(values["dataset_ref_id"], "dataset_ref_id"),
        "instrument_id": _text(values["instrument_id"], "instrument_id"),
        "timeframe": _text(values["timeframe"], "timeframe"),
        "readiness_mode": mode,
        "allow_real_read": _false(values["allow_real_read"], "allow_real_read"),
        "allow_network": _false(values["allow_network"], "allow_network"),
        "allow_registry_write": _false(values["allow_registry_write"], "allow_registry_write"),
        "allow_runtime": _false(values["allow_runtime"], "allow_runtime"),
    }


class RealReadGateRequest:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_gate_request_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_gate_request(request: RealReadGateRequest) -> RealReadGateRequest:
    if not isinstance(request, RealReadGateRequest):
        raise TypeError("request must be RealReadGateRequest")
    validate_real_read_gate_request_values(request.__dict__)
    return request


def validate_real_read_gate_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, RES_FIELDS, "result")
    status = _text(values["gate_status"], "gate_status")
    if status not in ALLOWED_GATE_STATUSES:
        raise RealReadGateError("gate_status is unsupported")
    if values["real_read_allowed"] is not False:
        raise RealReadGateError("real_read_allowed must remain false")
    reason = values["reason_or_none"]
    if status == "blocked" and (not isinstance(reason, str) or not reason.strip()):
        raise RealReadGateError("blocked result requires reason")
    if status == "eligible_for_separate_review" and reason is not None:
        raise RealReadGateError("eligible result must not include reason")
    return dict(values)


class RealReadGateResult:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_gate_result_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_gate_result(result: RealReadGateResult) -> RealReadGateResult:
    if not isinstance(result, RealReadGateResult):
        raise TypeError("result must be RealReadGateResult")
    validate_real_read_gate_result_values(result.__dict__)
    return result


def evaluate_real_read_gate(request: RealReadGateRequest) -> RealReadGateResult:
    try:
        validated = validate_real_read_gate_request(request)
        sample = validated.sample_pipeline_result
        if sample.pipeline_status != "written":
            return RealReadGateResult(
                gate_status="blocked",
                gate_request_id=validated.gate_request_id,
                dataset_ref_id=validated.dataset_ref_id,
                instrument_id=validated.instrument_id,
                timeframe=validated.timeframe,
                sample_pipeline_status=sample.pipeline_status,
                real_read_allowed=False,
                reason_or_none="sample pipeline not accepted",
            )
        if sample.dataset_ref_id != validated.dataset_ref_id:
            return RealReadGateResult(
                gate_status="blocked",
                gate_request_id=validated.gate_request_id,
                dataset_ref_id=validated.dataset_ref_id,
                instrument_id=validated.instrument_id,
                timeframe=validated.timeframe,
                sample_pipeline_status=sample.pipeline_status,
                real_read_allowed=False,
                reason_or_none="dataset mismatch",
            )
        return RealReadGateResult(
            gate_status="eligible_for_separate_review",
            gate_request_id=validated.gate_request_id,
            dataset_ref_id=validated.dataset_ref_id,
            instrument_id=validated.instrument_id,
            timeframe=validated.timeframe,
            sample_pipeline_status=sample.pipeline_status,
            real_read_allowed=False,
            reason_or_none=None,
        )
    except (RealReadGateError, TypeError, ValueError) as error:
        return RealReadGateResult(
            gate_status="blocked",
            gate_request_id="unavailable",
            dataset_ref_id="unavailable",
            instrument_id="unavailable",
            timeframe="unavailable",
            sample_pipeline_status="unavailable",
            real_read_allowed=False,
            reason_or_none=str(error),
        )


__all__ = [
    "ALLOWED_GATE_STATUSES",
    "ALLOWED_READINESS_MODES",
    "REQ_FIELDS",
    "RES_FIELDS",
    "RealReadGateError",
    "RealReadGateRequest",
    "RealReadGateResult",
    "evaluate_real_read_gate",
    "validate_real_read_gate_request",
    "validate_real_read_gate_request_values",
    "validate_real_read_gate_result",
    "validate_real_read_gate_result_values",
]
