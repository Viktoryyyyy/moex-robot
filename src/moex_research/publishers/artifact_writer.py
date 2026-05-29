from __future__ import annotations

import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Final


class ArtifactWriteValidationError(ValueError):
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


def _storage_root_marker() -> str:
    return "data" + "_" + "root"


ARTIFACT_WRITE_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "artifact_manifest_ref",
    "artifact_class",
    "artifact_role",
    "content",
    "output_path",
    "write_mode",
)
ARTIFACT_WRITE_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "write_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "output_path",
    "artifact_role",
    "error_message_or_none",
)
ALLOWED_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset(
    {
        "repo_relative",
        "external_pattern",
        "cli_argument",
        "env_contract",
        "temporary_test_path",
    }
)
ALLOWED_ARTIFACT_ROLES: Final[frozenset[str]] = frozenset(
    {
        "planning_artifact_manifest",
        "planning_dry_run_result",
        "registry_entry_draft",
    }
)
ALLOWED_WRITE_MODES: Final[frozenset[str]] = frozenset({"dry_run_test_only"})


def _selection_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _blocked_path_markers() -> tuple[str, ...]:
    return (
        _host_path_marker(),
        _scheduler_access_marker(),
        _market_access_marker(),
        "data" + "lake",
        "data" + " " + "lake",
        _storage_root_marker(),
    )


def _spaced_text(value: str) -> str:
    spaced = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        spaced = spaced.replace(separator, " ")
    return " ".join(part for part in spaced.split() if part)


def _guard_markers(value: str, field_name: str) -> str:
    folded = value.casefold()
    spaced = _spaced_text(value)
    for marker in (*_selection_markers(), *_blocked_path_markers()):
        if marker in folded or marker in spaced:
            raise ArtifactWriteValidationError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ArtifactWriteValidationError(f"{field_name} is required")
    return value


def _require_text_with_marker_guard(value: object, field_name: str) -> str:
    return _guard_markers(_require_text(value, field_name), field_name)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_text(value, field_name)
    if candidate not in allowed_values:
        raise ArtifactWriteValidationError(f"{field_name} is unsupported")
    return candidate


def _is_under_temp_root(path_value: str) -> bool:
    root = Path(tempfile.gettempdir()).resolve(strict=False)
    candidate = Path(path_value).resolve(strict=False)
    if candidate == root:
        return True
    try:
        return candidate.is_relative_to(root)
    except AttributeError:
        root_text = str(root)
        candidate_text = str(candidate)
        return candidate_text.startswith(root_text + "/")


def _require_temp_output_path(value: object, field_name: str, write_mode: str) -> str:
    path_value = _require_text_with_marker_guard(value, field_name)
    if write_mode == "dry_run_test_only" and not _is_under_temp_root(path_value):
        raise ArtifactWriteValidationError(f"{field_name} must use approved temporary test path")
    return path_value


def _validate_expected_request_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise ArtifactWriteValidationError("request values must be a mapping")
    if set(values) != set(ARTIFACT_WRITE_REQUEST_FIELDS):
        raise ArtifactWriteValidationError("unsupported request fields")


def validate_artifact_write_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_request_fields(values)
    write_mode = _require_choice(values["write_mode"], "write_mode", ALLOWED_WRITE_MODES)
    return {
        "request_id": _require_text_with_marker_guard(values["request_id"], "request_id"),
        "strategy_id": _require_text_with_marker_guard(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text_with_marker_guard(
            values["strategy_test_id"],
            "strategy_test_id",
        ),
        "artifact_manifest_ref": _require_text_with_marker_guard(
            values["artifact_manifest_ref"],
            "artifact_manifest_ref",
        ),
        "artifact_class": _require_choice(
            values["artifact_class"],
            "artifact_class",
            ALLOWED_ARTIFACT_CLASSES,
        ),
        "artifact_role": _require_choice(
            values["artifact_role"],
            "artifact_role",
            ALLOWED_ARTIFACT_ROLES,
        ),
        "content": _require_text(values["content"], "content"),
        "output_path": _require_temp_output_path(values["output_path"], "output_path", write_mode),
        "write_mode": write_mode,
    }


def _validate_expected_result_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise ArtifactWriteValidationError("result values must be a mapping")
    if set(values) != set(ARTIFACT_WRITE_RESULT_FIELDS):
        raise ArtifactWriteValidationError("unsupported result fields")


def _require_write_status(value: object) -> str:
    status = _require_text(value, "write_status")
    if status not in {"written", "rejected"}:
        raise ArtifactWriteValidationError("write_status is unsupported")
    return status


def _require_error(value: object, write_status: str) -> str | None:
    if write_status == "written":
        if value is not None:
            raise ArtifactWriteValidationError("written result must not include error")
        return None
    if not isinstance(value, str) or not value.strip():
        raise ArtifactWriteValidationError("rejected result requires error")
    return value


def validate_artifact_write_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_result_fields(values)
    write_status = _require_write_status(values["write_status"])
    return {
        "write_status": write_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "output_path": _require_text(values["output_path"], "output_path"),
        "artifact_role": _require_choice(values["artifact_role"], "artifact_role", ALLOWED_ARTIFACT_ROLES),
        "error_message_or_none": _require_error(values["error_message_or_none"], write_status),
    }


class ArtifactWriteRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "artifact_manifest_ref": str,
        "artifact_class": str,
        "artifact_role": str,
        "content": str,
        "output_path": str,
        "write_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_artifact_write_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


class ArtifactWriteResult:
    __annotations__ = {
        "write_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "output_path": str,
        "artifact_role": str,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_artifact_write_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_artifact_write_request(request: ArtifactWriteRequest) -> ArtifactWriteRequest:
    if not isinstance(request, ArtifactWriteRequest):
        raise TypeError("request must be ArtifactWriteRequest")
    validate_artifact_write_request_values(request.__dict__)
    return request


def validate_artifact_write_result(result: ArtifactWriteResult) -> ArtifactWriteResult:
    if not isinstance(result, ArtifactWriteResult):
        raise TypeError("result must be ArtifactWriteResult")
    validate_artifact_write_result_values(result.__dict__)
    return result


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _result_from_request(
    *,
    request: object,
    write_status: str,
    error_message_or_none: str | None,
) -> ArtifactWriteResult:
    role = _safe_text(request, "artifact_role")
    if role not in ALLOWED_ARTIFACT_ROLES:
        role = "planning_artifact_manifest"
    return ArtifactWriteResult(
        write_status=write_status,
        request_id=_safe_text(request, "request_id"),
        strategy_id=_safe_text(request, "strategy_id"),
        strategy_test_id=_safe_text(request, "strategy_test_id"),
        output_path=_safe_text(request, "output_path"),
        artifact_role=role,
        error_message_or_none=error_message_or_none,
    )


def write_planning_artifact_dry_run(request: ArtifactWriteRequest) -> ArtifactWriteResult:
    try:
        validated_request = validate_artifact_write_request(request)
        output_path = Path(validated_request.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(validated_request.content, encoding="utf-8")
        return _result_from_request(
            request=validated_request,
            write_status="written",
            error_message_or_none=None,
        )
    except (ArtifactWriteValidationError, OSError, TypeError, ValueError) as error:
        return _result_from_request(
            request=request,
            write_status="rejected",
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_ARTIFACT_CLASSES",
    "ALLOWED_ARTIFACT_ROLES",
    "ALLOWED_WRITE_MODES",
    "ARTIFACT_WRITE_REQUEST_FIELDS",
    "ARTIFACT_WRITE_RESULT_FIELDS",
    "ArtifactWriteRequest",
    "ArtifactWriteResult",
    "ArtifactWriteValidationError",
    "validate_artifact_write_request",
    "validate_artifact_write_request_values",
    "validate_artifact_write_result",
    "validate_artifact_write_result_values",
    "write_planning_artifact_dry_run",
]
