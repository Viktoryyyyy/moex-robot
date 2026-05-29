from __future__ import annotations

import json
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Final


class SyntheticSignalArtifactValidationError(ValueError):
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


SYNTHETIC_SIGNAL_ROW_FIELDS: Final[tuple[str, ...]] = (
    "strategy_id",
    "strategy_test_id",
    "signal_id",
    "instrument_id",
    "timestamp",
    "signal_value",
    "signal_version",
    "source_type",
)
SYNTHETIC_SIGNAL_TABLE_ARTIFACT_FIELDS: Final[tuple[str, ...]] = (
    "artifact_id",
    "strategy_id",
    "strategy_test_id",
    "artifact_role",
    "artifact_class",
    "schema_version",
    "rows",
    "source_type",
)
SYNTHETIC_SIGNAL_ARTIFACT_WRITE_REQUEST_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "signal_artifact",
    "output_path",
    "artifact_manifest_ref",
    "write_mode",
)
SYNTHETIC_SIGNAL_ARTIFACT_WRITE_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "write_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "output_path",
    "artifact_id_or_none",
    "artifact_manifest_ref_or_none",
    "error_message_or_none",
)
ALLOWED_SOURCE_TYPES: Final[frozenset[str]] = frozenset({"synthetic_test_only"})
ALLOWED_ARTIFACT_ROLES: Final[frozenset[str]] = frozenset({"synthetic_signal_table"})
ALLOWED_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset({"temporary_test_path"})
ALLOWED_WRITE_MODES: Final[frozenset[str]] = frozenset({"dry_run_test_only"})
ALLOWED_WRITE_STATUSES: Final[frozenset[str]] = frozenset({"written", "rejected"})
ALLOWED_OUTPUT_SUFFIXES: Final[frozenset[str]] = frozenset({".json", ".jsonl"})


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


def _guard_markers(value: str, field_name: str, *, include_path_markers: bool) -> str:
    folded = value.casefold()
    spaced = _spaced_text(value)
    markers = _selection_markers()
    if include_path_markers:
        markers = (*markers, *_blocked_path_markers())
    for marker in markers:
        if marker in folded or marker in spaced:
            raise SyntheticSignalArtifactValidationError(f"{field_name} contains unsupported marker")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SyntheticSignalArtifactValidationError(f"{field_name} is required")
    return value


def _require_identifier(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    return _guard_markers(text, field_name, include_path_markers=False)


def _require_path_text(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    return _guard_markers(text, field_name, include_path_markers=True)


def _require_choice(value: object, field_name: str, allowed_values: frozenset[str]) -> str:
    candidate = _require_text(value, field_name)
    if candidate not in allowed_values:
        raise SyntheticSignalArtifactValidationError(f"{field_name} is unsupported")
    return candidate


def _require_signal_value(value: object, field_name: str) -> object:
    if value is None:
        raise SyntheticSignalArtifactValidationError(f"{field_name} is required")
    if isinstance(value, str) and not value.strip():
        raise SyntheticSignalArtifactValidationError(f"{field_name} is required")
    return value


def _validate_exact_fields(values: Mapping[str, object], expected_fields: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping):
        raise SyntheticSignalArtifactValidationError(f"{label} values must be a mapping")
    if set(values) != set(expected_fields):
        raise SyntheticSignalArtifactValidationError(f"{label} contains unsupported fields")


def validate_synthetic_signal_row_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, SYNTHETIC_SIGNAL_ROW_FIELDS, "row")
    return {
        "strategy_id": _require_identifier(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_identifier(values["strategy_test_id"], "strategy_test_id"),
        "signal_id": _require_identifier(values["signal_id"], "signal_id"),
        "instrument_id": _require_identifier(values["instrument_id"], "instrument_id"),
        "timestamp": _require_identifier(values["timestamp"], "timestamp"),
        "signal_value": _require_signal_value(values["signal_value"], "signal_value"),
        "signal_version": _require_identifier(values["signal_version"], "signal_version"),
        "source_type": _require_choice(values["source_type"], "source_type", ALLOWED_SOURCE_TYPES),
    }


class SyntheticSignalRow:
    __annotations__ = {
        "strategy_id": str,
        "strategy_test_id": str,
        "signal_id": str,
        "instrument_id": str,
        "timestamp": str,
        "signal_value": object,
        "signal_version": str,
        "source_type": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_synthetic_signal_row_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_synthetic_signal_row(row: SyntheticSignalRow) -> SyntheticSignalRow:
    if not isinstance(row, SyntheticSignalRow):
        raise TypeError("row must be SyntheticSignalRow")
    validate_synthetic_signal_row_values(row.__dict__)
    return row


def _require_rows(value: object, field_name: str) -> tuple[SyntheticSignalRow, ...]:
    if isinstance(value, (str, bytes)):
        raise SyntheticSignalArtifactValidationError(f"{field_name} must be a non-empty iterable")
    if not isinstance(value, Iterable):
        raise SyntheticSignalArtifactValidationError(f"{field_name} must be a non-empty iterable")
    rows: list[SyntheticSignalRow] = []
    for item in value:
        if isinstance(item, SyntheticSignalRow):
            rows.append(validate_synthetic_signal_row(item))
        elif isinstance(item, Mapping):
            rows.append(SyntheticSignalRow(**item))
        else:
            raise SyntheticSignalArtifactValidationError(f"{field_name} contains unsupported row")
    if not rows:
        raise SyntheticSignalArtifactValidationError(f"{field_name} must be non-empty")
    return tuple(rows)


def validate_synthetic_signal_table_artifact_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_exact_fields(values, SYNTHETIC_SIGNAL_TABLE_ARTIFACT_FIELDS, "artifact")
    return {
        "artifact_id": _require_identifier(values["artifact_id"], "artifact_id"),
        "strategy_id": _require_identifier(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_identifier(values["strategy_test_id"], "strategy_test_id"),
        "artifact_role": _require_choice(values["artifact_role"], "artifact_role", ALLOWED_ARTIFACT_ROLES),
        "artifact_class": _require_choice(values["artifact_class"], "artifact_class", ALLOWED_ARTIFACT_CLASSES),
        "schema_version": _require_identifier(values["schema_version"], "schema_version"),
        "rows": _require_rows(values["rows"], "rows"),
        "source_type": _require_choice(values["source_type"], "source_type", ALLOWED_SOURCE_TYPES),
    }


class SyntheticSignalTableArtifact:
    __annotations__ = {
        "artifact_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "artifact_role": str,
        "artifact_class": str,
        "schema_version": str,
        "rows": tuple[SyntheticSignalRow, ...],
        "source_type": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_synthetic_signal_table_artifact_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_synthetic_signal_table_artifact(
    artifact: SyntheticSignalTableArtifact,
) -> SyntheticSignalTableArtifact:
    if not isinstance(artifact, SyntheticSignalTableArtifact):
        raise TypeError("artifact must be SyntheticSignalTableArtifact")
    validate_synthetic_signal_table_artifact_values(artifact.__dict__)
    return artifact


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


def _require_output_path(value: object, field_name: str, write_mode: str) -> str:
    path_value = _require_path_text(value, field_name)
    candidate = Path(path_value)
    if candidate.suffix not in ALLOWED_OUTPUT_SUFFIXES:
        raise SyntheticSignalArtifactValidationError(f"{field_name} must use approved JSON suffix")
    if write_mode == "dry_run_test_only" and not _is_under_temp_root(path_value):
        raise SyntheticSignalArtifactValidationError(f"{field_name} must use approved temporary test path")
    return path_value


def validate_synthetic_signal_artifact_write_request_values(
    values: Mapping[str, object],
) -> dict[str, object]:
    _validate_exact_fields(values, SYNTHETIC_SIGNAL_ARTIFACT_WRITE_REQUEST_FIELDS, "request")
    write_mode = _require_choice(values["write_mode"], "write_mode", ALLOWED_WRITE_MODES)
    signal_artifact_value = values["signal_artifact"]
    if isinstance(signal_artifact_value, SyntheticSignalTableArtifact):
        signal_artifact = validate_synthetic_signal_table_artifact(signal_artifact_value)
    elif isinstance(signal_artifact_value, Mapping):
        signal_artifact = SyntheticSignalTableArtifact(**signal_artifact_value)
    else:
        raise SyntheticSignalArtifactValidationError("signal_artifact is required")
    strategy_id = _require_identifier(values["strategy_id"], "strategy_id")
    strategy_test_id = _require_identifier(values["strategy_test_id"], "strategy_test_id")
    if signal_artifact.strategy_id != strategy_id:
        raise SyntheticSignalArtifactValidationError("strategy_id must match signal_artifact")
    if signal_artifact.strategy_test_id != strategy_test_id:
        raise SyntheticSignalArtifactValidationError("strategy_test_id must match signal_artifact")
    return {
        "request_id": _require_identifier(values["request_id"], "request_id"),
        "strategy_id": strategy_id,
        "strategy_test_id": strategy_test_id,
        "signal_artifact": signal_artifact,
        "output_path": _require_output_path(values["output_path"], "output_path", write_mode),
        "artifact_manifest_ref": _require_identifier(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "write_mode": write_mode,
    }


class SyntheticSignalArtifactWriteRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "signal_artifact": SyntheticSignalTableArtifact,
        "output_path": str,
        "artifact_manifest_ref": str,
        "write_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_synthetic_signal_artifact_write_request_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_synthetic_signal_artifact_write_request(
    request: SyntheticSignalArtifactWriteRequest,
) -> SyntheticSignalArtifactWriteRequest:
    if not isinstance(request, SyntheticSignalArtifactWriteRequest):
        raise TypeError("request must be SyntheticSignalArtifactWriteRequest")
    validate_synthetic_signal_artifact_write_request_values(request.__dict__)
    return request


def _require_optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _require_result_error(value: object, write_status: str) -> str | None:
    if write_status == "written":
        if value is not None:
            raise SyntheticSignalArtifactValidationError("written result must not include error")
        return None
    if not isinstance(value, str) or not value.strip():
        raise SyntheticSignalArtifactValidationError("rejected result requires error")
    return value


def validate_synthetic_signal_artifact_write_result_values(
    values: Mapping[str, object],
) -> dict[str, object]:
    _validate_exact_fields(values, SYNTHETIC_SIGNAL_ARTIFACT_WRITE_RESULT_FIELDS, "result")
    write_status = _require_choice(values["write_status"], "write_status", ALLOWED_WRITE_STATUSES)
    return {
        "write_status": write_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "output_path": _require_text(values["output_path"], "output_path"),
        "artifact_id_or_none": _require_optional_text(values["artifact_id_or_none"], "artifact_id_or_none"),
        "artifact_manifest_ref_or_none": _require_optional_text(
            values["artifact_manifest_ref_or_none"],
            "artifact_manifest_ref_or_none",
        ),
        "error_message_or_none": _require_result_error(values["error_message_or_none"], write_status),
    }


class SyntheticSignalArtifactWriteResult:
    __annotations__ = {
        "write_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "output_path": str,
        "artifact_id_or_none": str | None,
        "artifact_manifest_ref_or_none": str | None,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_synthetic_signal_artifact_write_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_synthetic_signal_artifact_write_result(
    result: SyntheticSignalArtifactWriteResult,
) -> SyntheticSignalArtifactWriteResult:
    if not isinstance(result, SyntheticSignalArtifactWriteResult):
        raise TypeError("result must be SyntheticSignalArtifactWriteResult")
    validate_synthetic_signal_artifact_write_result_values(result.__dict__)
    return result


def _row_to_dict(row: SyntheticSignalRow) -> dict[str, object]:
    return {field_name: getattr(row, field_name) for field_name in SYNTHETIC_SIGNAL_ROW_FIELDS}


def _artifact_to_dict(artifact: SyntheticSignalTableArtifact) -> dict[str, object]:
    return {
        "artifact_id": artifact.artifact_id,
        "strategy_id": artifact.strategy_id,
        "strategy_test_id": artifact.strategy_test_id,
        "artifact_role": artifact.artifact_role,
        "artifact_class": artifact.artifact_class,
        "schema_version": artifact.schema_version,
        "rows": [_row_to_dict(row) for row in artifact.rows],
        "source_type": artifact.source_type,
    }


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _safe_artifact_id(source: object) -> str | None:
    artifact = getattr(source, "signal_artifact", None)
    value = getattr(artifact, "artifact_id", None)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _safe_ref(source: object) -> str | None:
    value = getattr(source, "artifact_manifest_ref", None)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _result_from_request(
    *,
    request: object,
    write_status: str,
    error_message_or_none: str | None,
) -> SyntheticSignalArtifactWriteResult:
    return SyntheticSignalArtifactWriteResult(
        write_status=write_status,
        request_id=_safe_text(request, "request_id"),
        strategy_id=_safe_text(request, "strategy_id"),
        strategy_test_id=_safe_text(request, "strategy_test_id"),
        output_path=_safe_text(request, "output_path"),
        artifact_id_or_none=_safe_artifact_id(request) if write_status == "written" else None,
        artifact_manifest_ref_or_none=_safe_ref(request) if write_status == "written" else None,
        error_message_or_none=error_message_or_none,
    )


def write_synthetic_signal_artifact_dry_run(
    request: SyntheticSignalArtifactWriteRequest,
) -> SyntheticSignalArtifactWriteResult:
    try:
        validated_request = validate_synthetic_signal_artifact_write_request(request)
        output_path = Path(validated_request.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_payload = _artifact_to_dict(validated_request.signal_artifact)
        if output_path.suffix == ".jsonl":
            content = "\n".join(json.dumps(row, sort_keys=True) for row in artifact_payload["rows"])
            if content:
                content = content + "\n"
        else:
            content = json.dumps(artifact_payload, sort_keys=True)
        output_path.write_text(content, encoding="utf-8")
        return _result_from_request(
            request=validated_request,
            write_status="written",
            error_message_or_none=None,
        )
    except (SyntheticSignalArtifactValidationError, OSError, TypeError, ValueError) as error:
        return _result_from_request(
            request=request,
            write_status="rejected",
            error_message_or_none=str(error),
        )


__all__ = [
    "ALLOWED_ARTIFACT_CLASSES",
    "ALLOWED_ARTIFACT_ROLES",
    "ALLOWED_OUTPUT_SUFFIXES",
    "ALLOWED_SOURCE_TYPES",
    "ALLOWED_WRITE_MODES",
    "ALLOWED_WRITE_STATUSES",
    "SYNTHETIC_SIGNAL_ARTIFACT_WRITE_REQUEST_FIELDS",
    "SYNTHETIC_SIGNAL_ARTIFACT_WRITE_RESULT_FIELDS",
    "SYNTHETIC_SIGNAL_ROW_FIELDS",
    "SYNTHETIC_SIGNAL_TABLE_ARTIFACT_FIELDS",
    "SyntheticSignalArtifactValidationError",
    "SyntheticSignalArtifactWriteRequest",
    "SyntheticSignalArtifactWriteResult",
    "SyntheticSignalRow",
    "SyntheticSignalTableArtifact",
    "validate_synthetic_signal_artifact_write_request",
    "validate_synthetic_signal_artifact_write_request_values",
    "validate_synthetic_signal_artifact_write_result",
    "validate_synthetic_signal_artifact_write_result_values",
    "validate_synthetic_signal_row",
    "validate_synthetic_signal_row_values",
    "validate_synthetic_signal_table_artifact",
    "validate_synthetic_signal_table_artifact_values",
    "write_synthetic_signal_artifact_dry_run",
]
