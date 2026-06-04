from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .result_storage_contracts import (
    FORBIDDEN_PROMOTION_VALUES,
    ResultStorageBundle,
    ResultStorageValidationError,
    _reject_forbidden_text,
    _require_mapping,
    _require_text,
    validate_result_storage_bundle,
)

ALLOWED_PM_REVIEW_STATUSES = frozenset(
    {
        "pending_pm_review",
        "accepted_final",
        "accepted_as_executed",
        "conditional_pass",
        "rejected",
        "blocked",
        "invalidated",
    }
)
ALLOWED_REGISTRY_RUN_STATUSES = frozenset({"planned", "dry_run_validated", "controlled_written", "blocked", "failed"})
ALLOWED_RESULT_STATUSES = frozenset({"not_evaluated", "supported_provisional", "not_supported_provisional", "blocked"})
FORBIDDEN_PROMOTION_FIELDS = frozenset(
    {
        "promotion_status",
        "promotion_verdict",
        "runtime_status",
        "live_status",
        "market_status",
        "readiness_status",
    }
)


@dataclass(frozen=True)
class PMReviewCloseout:
    review_id: str
    run_id: str
    status: str
    reviewer_role: str
    reviewed_artifact_refs: tuple[str, ...]
    notes: str = ""

    def __post_init__(self) -> None:
        review_id = _reject_forbidden_text(_require_text(self.review_id, "review_id"), "review_id")
        run_id = _reject_forbidden_text(_require_text(self.run_id, "run_id"), "run_id")
        status = _reject_forbidden_text(_require_text(self.status, "status"), "status")
        if status not in ALLOWED_PM_REVIEW_STATUSES:
            raise ResultStorageValidationError("unsupported PM review closeout status")
        reviewer_role = _require_text(self.reviewer_role, "reviewer_role")
        refs = tuple(self.reviewed_artifact_refs)
        if not refs:
            raise ResultStorageValidationError("reviewed_artifact_refs are required")
        for ref in refs:
            _reject_forbidden_text(_require_text(ref, "reviewed_artifact_ref"), "reviewed_artifact_ref")
        object.__setattr__(self, "review_id", review_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "reviewer_role", reviewer_role)
        object.__setattr__(self, "reviewed_artifact_refs", refs)


@dataclass(frozen=True)
class ExperimentRegistryEntry:
    registry_entry_id: str
    run_id: str
    strategy_id: str
    strategy_version: str
    run_status: str
    result_status: str
    result_storage_bundle_ref: str
    artifact_bundle_manifest_ref: str
    pm_review_closeout_ref: str
    repo_commit: str
    data_refs: Mapping[str, str]
    result_refs: Mapping[str, str]
    immutable_inputs_hash: str
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        registry_entry_id = _reject_forbidden_text(
            _require_text(self.registry_entry_id, "registry_entry_id"),
            "registry_entry_id",
        )
        run_id = _reject_forbidden_text(_require_text(self.run_id, "run_id"), "run_id")
        strategy_id = _reject_forbidden_text(_require_text(self.strategy_id, "strategy_id"), "strategy_id")
        strategy_version = _require_text(self.strategy_version, "strategy_version")
        run_status = _require_text(self.run_status, "run_status")
        result_status = _require_text(self.result_status, "result_status")
        if run_status not in ALLOWED_REGISTRY_RUN_STATUSES:
            raise ResultStorageValidationError("unsupported registry run_status")
        if result_status not in ALLOWED_RESULT_STATUSES:
            raise ResultStorageValidationError("unsupported registry result_status")
        for value in (run_status, result_status):
            if value in FORBIDDEN_PROMOTION_VALUES:
                raise ResultStorageValidationError("promotion/readiness status is forbidden")
        result_storage_bundle_ref = _reject_forbidden_text(
            _require_text(self.result_storage_bundle_ref, "result_storage_bundle_ref"),
            "result_storage_bundle_ref",
        )
        artifact_bundle_manifest_ref = _reject_forbidden_text(
            _require_text(self.artifact_bundle_manifest_ref, "artifact_bundle_manifest_ref"),
            "artifact_bundle_manifest_ref",
        )
        pm_review_closeout_ref = _reject_forbidden_text(
            _require_text(self.pm_review_closeout_ref, "pm_review_closeout_ref"),
            "pm_review_closeout_ref",
        )
        data_refs = dict(_require_mapping(self.data_refs, "data_refs"))
        result_refs = dict(_require_mapping(self.result_refs, "result_refs"))
        metadata = dict(_require_mapping(self.metadata, "metadata"))
        _reject_forbidden_promotion_content(metadata)
        object.__setattr__(self, "registry_entry_id", registry_entry_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "strategy_id", strategy_id)
        object.__setattr__(self, "strategy_version", strategy_version)
        object.__setattr__(self, "run_status", run_status)
        object.__setattr__(self, "result_status", result_status)
        object.__setattr__(self, "result_storage_bundle_ref", result_storage_bundle_ref)
        object.__setattr__(self, "artifact_bundle_manifest_ref", artifact_bundle_manifest_ref)
        object.__setattr__(self, "pm_review_closeout_ref", pm_review_closeout_ref)
        object.__setattr__(self, "repo_commit", _require_text(self.repo_commit, "repo_commit"))
        object.__setattr__(
            self,
            "immutable_inputs_hash",
            _require_text(self.immutable_inputs_hash, "immutable_inputs_hash"),
        )
        object.__setattr__(self, "data_refs", data_refs)
        object.__setattr__(self, "result_refs", result_refs)
        object.__setattr__(self, "metadata", metadata)


def _reject_forbidden_promotion_content(values: Mapping[str, Any]) -> None:
    for key, value in values.items():
        key_text = str(key).casefold()
        if key_text in FORBIDDEN_PROMOTION_FIELDS or key_text in FORBIDDEN_PROMOTION_VALUES:
            raise ResultStorageValidationError("promotion/runtime/live readiness values are forbidden")
        if isinstance(value, str) and value.casefold() in FORBIDDEN_PROMOTION_VALUES:
            raise ResultStorageValidationError("promotion/runtime/live readiness values are forbidden")
        if isinstance(value, Mapping):
            _reject_forbidden_promotion_content(value)


def validate_pm_review_closeout(closeout: PMReviewCloseout) -> PMReviewCloseout:
    if not isinstance(closeout, PMReviewCloseout):
        raise TypeError("closeout must be PMReviewCloseout")
    return closeout


def validate_experiment_registry_entry(entry: ExperimentRegistryEntry) -> ExperimentRegistryEntry:
    if not isinstance(entry, ExperimentRegistryEntry):
        raise TypeError("entry must be ExperimentRegistryEntry")
    return entry


def validate_registry_entry_against_bundle(
    entry: ExperimentRegistryEntry,
    bundle: ResultStorageBundle,
    closeout: PMReviewCloseout,
) -> ExperimentRegistryEntry:
    validate_experiment_registry_entry(entry)
    validate_result_storage_bundle(bundle)
    validate_pm_review_closeout(closeout)
    if entry.run_id != bundle.run_id or closeout.run_id != bundle.run_id:
        raise ResultStorageValidationError("registry, bundle, and closeout run_id must match")
    if entry.immutable_inputs_hash != bundle.immutable_inputs_hash:
        raise ResultStorageValidationError("registry immutable input hash must match bundle")
    if entry.artifact_bundle_manifest_ref != bundle.artifact_manifest.manifest_id:
        raise ResultStorageValidationError("artifact manifest ref must match bundle manifest")
    if entry.pm_review_closeout_ref != bundle.pm_review_closeout_ref:
        raise ResultStorageValidationError("PM closeout ref must match bundle")
    if closeout.review_id != entry.pm_review_closeout_ref:
        raise ResultStorageValidationError("PM closeout review_id must match registry ref")
    bundle_artifact_ids = bundle.artifact_manifest.artifact_ids
    dangling = set(entry.result_refs.values()).difference(bundle_artifact_ids)
    if dangling:
        raise ResultStorageValidationError("registry contains dangling result refs")
    return entry
