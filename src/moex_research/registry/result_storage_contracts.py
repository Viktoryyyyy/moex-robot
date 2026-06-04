from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

ALLOWED_ARTIFACT_CLASSES = frozenset({"repo_relative", "external_pattern", "cli_argument", "env_contract"})
REQUIRED_DATA_REF_FIELDS = (
    "dataset_version_or_hash",
    "source_snapshot_ref",
    "data_refresh_manifest_ref",
    "materialized_partition_ref",
    "quality_report_ref",
)
REQUIRED_RESULT_REF_FIELDS = (
    "parameter_snapshot_ref",
    "canonical_backtest_result_ref",
    "research_runner_ref",
    "research_run_request_ref",
    "strategy_package_ref",
    "strategy_config_ref",
)
REQUIRED_ARTIFACT_ROLES = (
    "parameter_snapshot",
    "canonical_backtest_result",
    "research_run_request",
    "pm_review_closeout",
)
FORBIDDEN_REF_MARKERS = frozenset({"latest", "current", "autodetect"})
FORBIDDEN_PROMOTION_VALUES = frozenset(
    {"approved", "promoted", "live_ready", "runtime_ready", "production_ready", "market_supported"}
)


class ResultStorageValidationError(ValueError):
    pass


def _require_mapping(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ResultStorageValidationError(f"{field_name} must be a mapping")
    return value


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResultStorageValidationError(f"{field_name} is required")
    return value.strip()


def _canonical_json(value: Mapping[str, Any]) -> str:
    _require_mapping(value, "immutable_inputs")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def compute_immutable_inputs_hash(immutable_inputs: Mapping[str, Any]) -> str:
    payload = _canonical_json(immutable_inputs).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def compute_deterministic_run_id(immutable_inputs: Mapping[str, Any]) -> str:
    return "run_" + compute_immutable_inputs_hash(immutable_inputs)[:32]


def _reject_forbidden_text(value: str, field_name: str, *, allow_template_tokens: bool = False) -> str:
    normalized = value.replace("\\", "/")
    if normalized.casefold() == "stdout":
        raise ResultStorageValidationError(f"{field_name} must not be stdout-only")
    parts = [part.casefold() for part in normalized.split("/") if part]
    for marker in FORBIDDEN_REF_MARKERS:
        if marker in parts:
            raise ResultStorageValidationError(f"{field_name} must not use {marker} refs")
    forbidden_tokens = ("*", "?", "[", "]") if allow_template_tokens else ("*", "?", "[", "]", "{", "}")
    if any(token in normalized for token in forbidden_tokens):
        raise ResultStorageValidationError(f"{field_name} must be explicit and must not use glob/template tokens")
    return value


def _is_absolute_or_server_path(value: str) -> bool:
    normalized = value.replace("\\", "/")
    return normalized.startswith("/") or normalized.startswith("~/")


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: str
    role: str
    ref_class: str
    locator: str
    producer: str
    consumer: str
    format: str
    schema_version: str

    def __post_init__(self) -> None:
        artifact_id = _reject_forbidden_text(_require_text(self.artifact_id, "artifact_id"), "artifact_id")
        role = _reject_forbidden_text(_require_text(self.role, "role"), "role")
        ref_class = _require_text(self.ref_class, "ref_class")
        if ref_class not in ALLOWED_ARTIFACT_CLASSES:
            raise ResultStorageValidationError("unsupported artifact ref_class")
        locator = _reject_forbidden_text(
            _require_text(self.locator, "locator"),
            "locator",
            allow_template_tokens=ref_class == "external_pattern",
        )
        producer = _require_text(self.producer, "producer")
        consumer = _require_text(self.consumer, "consumer")
        artifact_format = _reject_forbidden_text(_require_text(self.format, "format"), "format")
        schema_version = _require_text(self.schema_version, "schema_version")
        if ref_class != "external_pattern" and _is_absolute_or_server_path(locator):
            raise ResultStorageValidationError("absolute server paths are not valid artifact proof")
        object.__setattr__(self, "artifact_id", artifact_id)
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "ref_class", ref_class)
        object.__setattr__(self, "locator", locator)
        object.__setattr__(self, "producer", producer)
        object.__setattr__(self, "consumer", consumer)
        object.__setattr__(self, "format", artifact_format)
        object.__setattr__(self, "schema_version", schema_version)


@dataclass(frozen=True)
class ArtifactBundleManifest:
    manifest_id: str
    run_id: str
    schema_version: str
    repo_commit: str
    artifacts: tuple[ArtifactRef, ...]

    def __post_init__(self) -> None:
        manifest_id = _reject_forbidden_text(_require_text(self.manifest_id, "manifest_id"), "manifest_id")
        run_id = _reject_forbidden_text(_require_text(self.run_id, "run_id"), "run_id")
        schema_version = _require_text(self.schema_version, "schema_version")
        repo_commit = _require_text(self.repo_commit, "repo_commit")
        if isinstance(self.artifacts, (str, bytes)):
            raise ResultStorageValidationError("artifacts must be ArtifactRef items")
        artifacts = tuple(self.artifacts)
        if not artifacts:
            raise ResultStorageValidationError("artifacts are required")
        if any(not isinstance(item, ArtifactRef) for item in artifacts):
            raise ResultStorageValidationError("artifacts must contain ArtifactRef items")
        artifact_ids = [item.artifact_id for item in artifacts]
        if len(set(artifact_ids)) != len(artifact_ids):
            raise ResultStorageValidationError("artifact ids must be unique")
        roles = {item.role for item in artifacts}
        missing_roles = set(REQUIRED_ARTIFACT_ROLES).difference(roles)
        if missing_roles:
            raise ResultStorageValidationError("artifact manifest is missing required roles")
        object.__setattr__(self, "manifest_id", manifest_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "repo_commit", repo_commit)
        object.__setattr__(self, "artifacts", artifacts)

    @property
    def artifact_ids(self) -> frozenset[str]:
        return frozenset(item.artifact_id for item in self.artifacts)


@dataclass(frozen=True)
class ResultStorageBundle:
    run_id: str
    schema_version: str
    storage_mode: str
    immutable_inputs: Mapping[str, Any]
    data_refs: Mapping[str, str]
    result_refs: Mapping[str, str]
    artifact_manifest: ArtifactBundleManifest
    pm_review_closeout_ref: str
    finalized: bool = True
    immutable_inputs_hash: str = field(init=False)

    def __post_init__(self) -> None:
        run_id = _reject_forbidden_text(_require_text(self.run_id, "run_id"), "run_id")
        schema_version = _require_text(self.schema_version, "schema_version")
        storage_mode = _require_text(self.storage_mode, "storage_mode")
        data_refs = dict(_require_mapping(self.data_refs, "data_refs"))
        result_refs = dict(_require_mapping(self.result_refs, "result_refs"))
        immutable_inputs = dict(_require_mapping(self.immutable_inputs, "immutable_inputs"))
        if not isinstance(self.artifact_manifest, ArtifactBundleManifest):
            raise ResultStorageValidationError("artifact_manifest must be ArtifactBundleManifest")
        if self.artifact_manifest.run_id != run_id:
            raise ResultStorageValidationError("artifact manifest run_id must match bundle run_id")
        if storage_mode == "production_write":
            raise ResultStorageValidationError("production_write is blocked")
        for field_name in REQUIRED_DATA_REF_FIELDS:
            _reject_forbidden_text(_require_text(data_refs.get(field_name), field_name), field_name)
        for field_name in REQUIRED_RESULT_REF_FIELDS:
            _reject_forbidden_text(_require_text(result_refs.get(field_name), field_name), field_name)
        pm_review_closeout_ref = _reject_forbidden_text(
            _require_text(self.pm_review_closeout_ref, "pm_review_closeout_ref"),
            "pm_review_closeout_ref",
        )
        missing_artifact_refs = set(result_refs.values()).difference(self.artifact_manifest.artifact_ids)
        missing_artifact_refs.add(pm_review_closeout_ref) if pm_review_closeout_ref not in self.artifact_manifest.artifact_ids else None
        if missing_artifact_refs:
            raise ResultStorageValidationError("result bundle contains dangling artifact refs")
        computed_hash = compute_immutable_inputs_hash(immutable_inputs)
        if not isinstance(self.finalized, bool):
            raise ResultStorageValidationError("finalized must be bool")
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "storage_mode", storage_mode)
        object.__setattr__(self, "data_refs", data_refs)
        object.__setattr__(self, "result_refs", result_refs)
        object.__setattr__(self, "immutable_inputs", immutable_inputs)
        object.__setattr__(self, "pm_review_closeout_ref", pm_review_closeout_ref)
        object.__setattr__(self, "immutable_inputs_hash", computed_hash)


def validate_artifact_bundle_manifest(manifest: ArtifactBundleManifest) -> ArtifactBundleManifest:
    if not isinstance(manifest, ArtifactBundleManifest):
        raise TypeError("manifest must be ArtifactBundleManifest")
    return manifest


def validate_result_storage_bundle(bundle: ResultStorageBundle) -> ResultStorageBundle:
    if not isinstance(bundle, ResultStorageBundle):
        raise TypeError("bundle must be ResultStorageBundle")
    return bundle
