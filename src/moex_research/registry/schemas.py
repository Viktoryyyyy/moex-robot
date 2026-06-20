from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final, Iterable, Mapping


class RegistryValidationError(ValueError):
    pass


class RunStatus(str, Enum):
    PLANNED = "planned"
    EXECUTED = "executed"
    BLOCKED = "blocked"
    FAILED = "failed"
    INVALIDATED = "invalidated"
    SUPERSEDED = "superseded"


class ResultStatus(str, Enum):
    SUPPORTED_CANONICAL = "supported_canonical"
    NOT_SUPPORTED_CANONICAL = "not_supported_canonical"
    SUPPORTED_PROVISIONAL = "supported_provisional"
    NOT_SUPPORTED_PROVISIONAL = "not_supported_provisional"
    BLOCKED = "blocked"
    INVALIDATED = "invalidated"
    EVIDENCE_CANONICAL = "evidence_canonical"
    EVIDENCE_PROVISIONAL = "evidence_provisional"


class CanonicalityStatus(str, Enum):
    CANONICAL = "canonical"
    PROVISIONAL = "provisional"
    NON_CANONICAL = "non_canonical"
    BLOCKED = "blocked"


ALLOWED_RUN_STATUSES: Final[frozenset[str]] = frozenset(item.value for item in RunStatus)
# Retained as the legacy public set so existing callers that compare it exactly stay compatible.
ALLOWED_RESULT_STATUSES: Final[frozenset[str]] = frozenset(
    {
        ResultStatus.SUPPORTED_CANONICAL.value,
        ResultStatus.NOT_SUPPORTED_CANONICAL.value,
        ResultStatus.SUPPORTED_PROVISIONAL.value,
        ResultStatus.NOT_SUPPORTED_PROVISIONAL.value,
        ResultStatus.BLOCKED.value,
        ResultStatus.INVALIDATED.value,
    }
)
ACCEPTED_RESULT_STATUSES: Final[frozenset[str]] = frozenset(item.value for item in ResultStatus)
ALLOWED_CANONICALITY_STATUSES: Final[frozenset[str]] = frozenset(item.value for item in CanonicalityStatus)
CANONICAL_RESULT_STATUSES: Final[frozenset[str]] = frozenset(
    {
        ResultStatus.SUPPORTED_CANONICAL.value,
        ResultStatus.NOT_SUPPORTED_CANONICAL.value,
        ResultStatus.EVIDENCE_CANONICAL.value,
    }
)
REQUIRED_CANONICAL_ARTIFACT_ROLES: Final[tuple[str, ...]] = ("run_metadata", "metrics", "primary_result")

REQUIRED_ARTIFACT_MANIFEST_FIELDS: Final[tuple[str, ...]] = (
    "artifact_manifest_id",
    "run_id",
    "schema_version",
    "created_ts",
    "producer_component",
    "repo_commit",
    "artifacts",
)
REQUIRED_ARTIFACT_MANIFEST_ITEM_FIELDS: Final[tuple[str, ...]] = (
    "artifact_id",
    "artifact_role",
    "artifact_class",
    "producer",
    "consumer",
    "format",
    "schema_version",
    "path",
    "required_for_canonical",
)
REQUIRED_EXPERIMENT_REGISTRY_FIELDS: Final[tuple[str, ...]] = (
    "registry_entry_id",
    "run_id",
    "strategy_id",
    "strategy_version",
    "test_type",
    "instrument_scope",
    "timeframe_scope",
    "run_status",
    "result_status",
    "canonicality_status",
    "artifact_manifest_ref",
    "repo_commit",
    "created_ts",
)

_FORBIDDEN_PATH_TOKENS: Final[tuple[str, ...]] = ("*", "?", "[", "]", "{", "}")
_FORBIDDEN_PATH_ALIASES: Final[tuple[str, ...]] = ("latest", "current", "autodetect")
_FORBIDDEN_PROMOTION_METRIC_KEYS: Final[frozenset[str]] = frozenset(
    {"promotion", "promotion_status", "promotion_verdict", "promotion_decision", "promote_to_runtime", "promote_to_live"}
)
_SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-fA-F]{64}$")


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryValidationError(f"{field_name} is required")
    return value


def _require_tuple(value: Iterable[str], field_name: str) -> tuple[str, ...]:
    items = _coerce_string_tuple(value, field_name)
    if not items:
        raise RegistryValidationError(f"{field_name} must be non-empty")
    return items


def _coerce_string_tuple(value: Iterable[str], field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise RegistryValidationError(f"{field_name} must be a tuple of strings")
    try:
        items = tuple(value)
    except TypeError as exc:
        raise RegistryValidationError(f"{field_name} must be a tuple of strings") from exc
    if any(not isinstance(item, str) or not item.strip() for item in items):
        raise RegistryValidationError(f"{field_name} must contain non-empty strings")
    return items


def _require_bool(value: bool, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RegistryValidationError(f"{field_name} must be bool")
    return value


def _validate_explicit_artifact_path(path: str) -> str:
    path = _require_text(path, "path")
    if "\x00" in path:
        raise RegistryValidationError("artifact path must not contain NUL")
    if path.strip().casefold() == "stdout":
        raise RegistryValidationError("stdout-only artifact path is invalid")
    if any(token in path for token in _FORBIDDEN_PATH_TOKENS):
        raise RegistryValidationError("artifact path must be explicit and must not contain glob or template tokens")
    normalized = path.replace("\\", "/")
    if "//" in normalized:
        raise RegistryValidationError("artifact path must not contain empty path segments")
    raw_segments = [segment for segment in normalized.split("/") if segment]
    lowered_segments = [segment.casefold() for segment in raw_segments]
    if any(segment in {".", ".."} for segment in raw_segments):
        raise RegistryValidationError("artifact path must not contain traversal segments")
    if any(alias in segment for segment in lowered_segments for alias in _FORBIDDEN_PATH_ALIASES):
        raise RegistryValidationError("artifact path must not refer to a mutable alias")
    if normalized in {".", "./"} or normalized.endswith("/"):
        raise RegistryValidationError("artifact path must point to an explicit artifact, not a directory")
    return path


def _validate_metrics(metrics: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(metrics, Mapping):
        raise RegistryValidationError("metrics must be a mapping")
    for key, value in metrics.items():
        normalized_key = str(key).casefold()
        if normalized_key in _FORBIDDEN_PROMOTION_METRIC_KEYS or "promotion_verdict" in normalized_key:
            raise RegistryValidationError("promotion verdict must not be embedded in registry metrics")
        if isinstance(value, Mapping):
            _validate_metrics(value)
    return metrics


def _validate_parameter_set(parameter_set: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(parameter_set, Mapping):
        raise RegistryValidationError("parameter_set must be a mapping")
    return parameter_set


def _validate_sha256(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise RegistryValidationError("sha256 must be a 64-character hexadecimal digest")
    return value.lower()


def _validate_size_bytes(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RegistryValidationError("size_bytes must be a non-negative integer")
    return value


@dataclass(frozen=True)
class ArtifactManifestItem:
    artifact_id: str
    artifact_role: str
    artifact_class: str
    producer: str
    consumer: str
    format: str
    schema_version: str
    path: str
    required_for_canonical: bool
    sha256: str | None = None
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_id", _require_text(self.artifact_id, "artifact_id"))
        object.__setattr__(self, "artifact_role", _require_text(self.artifact_role, "artifact_role"))
        object.__setattr__(self, "artifact_class", _require_text(self.artifact_class, "artifact_class"))
        object.__setattr__(self, "producer", _require_text(self.producer, "producer"))
        object.__setattr__(self, "consumer", _require_text(self.consumer, "consumer"))
        object.__setattr__(self, "format", _require_text(self.format, "format"))
        if self.format.casefold() == "stdout":
            raise RegistryValidationError("stdout-only artifact format is invalid")
        object.__setattr__(self, "schema_version", _require_text(self.schema_version, "schema_version"))
        object.__setattr__(self, "path", _validate_explicit_artifact_path(self.path))
        object.__setattr__(self, "required_for_canonical", _require_bool(self.required_for_canonical, "required_for_canonical"))
        object.__setattr__(self, "sha256", _validate_sha256(self.sha256))
        object.__setattr__(self, "size_bytes", _validate_size_bytes(self.size_bytes))


@dataclass(frozen=True)
class ArtifactManifest:
    artifact_manifest_id: str
    run_id: str
    schema_version: str
    created_ts: str
    producer_component: str
    repo_commit: str
    artifacts: tuple[ArtifactManifestItem, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_manifest_id", _require_text(self.artifact_manifest_id, "artifact_manifest_id"))
        object.__setattr__(self, "run_id", _require_text(self.run_id, "run_id"))
        object.__setattr__(self, "schema_version", _require_text(self.schema_version, "schema_version"))
        object.__setattr__(self, "created_ts", _require_text(self.created_ts, "created_ts"))
        object.__setattr__(self, "producer_component", _require_text(self.producer_component, "producer_component"))
        object.__setattr__(self, "repo_commit", _require_text(self.repo_commit, "repo_commit"))
        if isinstance(self.artifacts, (str, bytes)):
            raise RegistryValidationError("artifacts must be a non-empty tuple of ArtifactManifestItem")
        try:
            artifacts = tuple(self.artifacts)
        except TypeError as exc:
            raise RegistryValidationError("artifacts must be a non-empty tuple of ArtifactManifestItem") from exc
        if not artifacts:
            raise RegistryValidationError("artifacts must be non-empty")
        if any(not isinstance(item, ArtifactManifestItem) for item in artifacts):
            raise RegistryValidationError("artifacts must contain ArtifactManifestItem instances")
        object.__setattr__(self, "artifacts", artifacts)


@dataclass(frozen=True)
class ExperimentRegistryEntry:
    registry_entry_id: str
    run_id: str
    strategy_id: str
    strategy_version: str
    test_type: str
    instrument_scope: tuple[str, ...]
    timeframe_scope: tuple[str, ...]
    run_status: str
    result_status: str
    canonicality_status: str
    artifact_manifest_ref: str
    repo_commit: str
    created_ts: str
    metrics: Mapping[str, Any] = field(default_factory=dict)
    promotion_verdict_ref: str | None = None
    dataset_refs: tuple[str, ...] = ()
    feature_refs: tuple[str, ...] = ()
    label_refs: tuple[str, ...] = ()
    parameter_set: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "registry_entry_id", _require_text(self.registry_entry_id, "registry_entry_id"))
        object.__setattr__(self, "run_id", _require_text(self.run_id, "run_id"))
        object.__setattr__(self, "strategy_id", _require_text(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "strategy_version", _require_text(self.strategy_version, "strategy_version"))
        object.__setattr__(self, "test_type", _require_text(self.test_type, "test_type"))
        object.__setattr__(self, "instrument_scope", _require_tuple(self.instrument_scope, "instrument_scope"))
        object.__setattr__(self, "timeframe_scope", _require_tuple(self.timeframe_scope, "timeframe_scope"))
        object.__setattr__(self, "run_status", _require_text(self.run_status, "run_status"))
        object.__setattr__(self, "result_status", _require_text(self.result_status, "result_status"))
        object.__setattr__(self, "canonicality_status", _require_text(self.canonicality_status, "canonicality_status"))
        if self.run_status not in ALLOWED_RUN_STATUSES:
            raise RegistryValidationError("unsupported run_status")
        if self.result_status not in ACCEPTED_RESULT_STATUSES:
            raise RegistryValidationError("unsupported result_status")
        if self.canonicality_status not in ALLOWED_CANONICALITY_STATUSES:
            raise RegistryValidationError("unsupported canonicality_status")
        object.__setattr__(self, "artifact_manifest_ref", _require_text(self.artifact_manifest_ref, "artifact_manifest_ref"))
        object.__setattr__(self, "repo_commit", _require_text(self.repo_commit, "repo_commit"))
        object.__setattr__(self, "created_ts", _require_text(self.created_ts, "created_ts"))
        object.__setattr__(self, "metrics", _validate_metrics(self.metrics))
        if self.promotion_verdict_ref is not None:
            object.__setattr__(self, "promotion_verdict_ref", _require_text(self.promotion_verdict_ref, "promotion_verdict_ref"))
        object.__setattr__(self, "dataset_refs", _coerce_string_tuple(self.dataset_refs, "dataset_refs"))
        object.__setattr__(self, "feature_refs", _coerce_string_tuple(self.feature_refs, "feature_refs"))
        object.__setattr__(self, "label_refs", _coerce_string_tuple(self.label_refs, "label_refs"))
        object.__setattr__(self, "parameter_set", _validate_parameter_set(self.parameter_set))
