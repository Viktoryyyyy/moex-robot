from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from moex_research.registry.result_storage_contracts import (
    ArtifactBundleManifest,
    ArtifactRef,
    ResultStorageBundle,
    ResultStorageValidationError,
    _reject_forbidden_text,
    _require_mapping,
    _require_text,
    compute_deterministic_run_id,
)
from moex_research.registry.experiment_registry_contracts import ExperimentRegistryEntry, PMReviewCloseout
from strategies.d1_tsmom_minimal.manifest import MANIFEST, STRATEGY_ID


REQUIRED_DATASET_REF_FIELDS = (
    "dataset_version_or_hash",
    "source_snapshot_ref",
    "data_refresh_manifest_ref",
    "materialized_partition_ref",
    "quality_report_ref",
    "dataset_manifest_ref",
)

REQUIRED_RESULT_STORAGE_CONTRACT_REF_FIELDS = (
    "experiment_registry_entry_contract_ref",
    "result_storage_bundle_contract_ref",
    "artifact_bundle_manifest_contract_ref",
    "pm_review_closeout_contract_ref",
)

D1_TSMOM_MINIMAL_PACKAGE_REF = "src/strategies/d1_tsmom_minimal"
D1_TSMOM_MINIMAL_CONFIG_REF = "configs/strategies/d1_tsmom_minimal.platform_run.v1.yaml"
D1_TSMOM_MINIMAL_BACKTEST_CONFIG_REF = "configs/backtests/d1_tsmom_minimal.platform_run.v1.yaml"
D1_TSMOM_MINIMAL_RUNNER_REF = "src/moex_research/runners/d1_tsmom_minimal_platform.py"
D1_TSMOM_MINIMAL_PARAMETER_SNAPSHOT_REF = "configs/research/d1_tsmom_minimal.parameter_snapshot.v1.yaml"
CANONICAL_BACKTEST_ENGINE_REF = "src/moex_backtest/engine/canonical.py"


class D1TSMOMMinimalRequestValidationError(ValueError):
    pass


@dataclass(frozen=True)
class D1TSMOMMinimalPlatformRunRequest:
    request_id: str
    strategy_package_ref: str
    strategy_config_ref: str
    canonical_backtest_engine_ref: str
    canonical_backtest_config_ref: str
    research_runner_ref: str
    parameter_snapshot_ref: str
    dataset_refs: Mapping[str, str]
    result_storage_contract_refs: Mapping[str, str]
    repo_commit: str
    execution_control: Mapping[str, Any]

    def __post_init__(self) -> None:
        request_id = _clean_text(self.request_id, "request_id")
        strategy_package_ref = _clean_text(self.strategy_package_ref, "strategy_package_ref")
        strategy_config_ref = _clean_text(self.strategy_config_ref, "strategy_config_ref")
        canonical_backtest_engine_ref = _clean_text(self.canonical_backtest_engine_ref, "canonical_backtest_engine_ref")
        canonical_backtest_config_ref = _clean_text(self.canonical_backtest_config_ref, "canonical_backtest_config_ref")
        research_runner_ref = _clean_text(self.research_runner_ref, "research_runner_ref")
        parameter_snapshot_ref = _clean_text(self.parameter_snapshot_ref, "parameter_snapshot_ref")
        repo_commit = _require_text(self.repo_commit, "repo_commit")
        dataset_refs = _clean_mapping(self.dataset_refs, "dataset_refs")
        result_storage_contract_refs = _clean_mapping(self.result_storage_contract_refs, "result_storage_contract_refs")
        execution_control = dict(_require_mapping(self.execution_control, "execution_control"))
        if strategy_package_ref != D1_TSMOM_MINIMAL_PACKAGE_REF:
            raise D1TSMOMMinimalRequestValidationError("unexpected strategy package ref")
        if strategy_config_ref != D1_TSMOM_MINIMAL_CONFIG_REF:
            raise D1TSMOMMinimalRequestValidationError("unexpected strategy config ref")
        if canonical_backtest_engine_ref != CANONICAL_BACKTEST_ENGINE_REF:
            raise D1TSMOMMinimalRequestValidationError("unexpected canonical backtest engine ref")
        if canonical_backtest_config_ref != D1_TSMOM_MINIMAL_BACKTEST_CONFIG_REF:
            raise D1TSMOMMinimalRequestValidationError("unexpected canonical backtest config ref")
        if research_runner_ref != D1_TSMOM_MINIMAL_RUNNER_REF:
            raise D1TSMOMMinimalRequestValidationError("unexpected research runner ref")
        if parameter_snapshot_ref != D1_TSMOM_MINIMAL_PARAMETER_SNAPSHOT_REF:
            raise D1TSMOMMinimalRequestValidationError("unexpected parameter snapshot ref")
        for field_name in REQUIRED_DATASET_REF_FIELDS:
            _clean_text(dataset_refs.get(field_name), "dataset_refs." + field_name)
        for field_name in REQUIRED_RESULT_STORAGE_CONTRACT_REF_FIELDS:
            _clean_text(result_storage_contract_refs.get(field_name), "result_storage_contract_refs." + field_name)
        _reject_forbidden_flags(execution_control)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "strategy_package_ref", strategy_package_ref)
        object.__setattr__(self, "strategy_config_ref", strategy_config_ref)
        object.__setattr__(self, "canonical_backtest_engine_ref", canonical_backtest_engine_ref)
        object.__setattr__(self, "canonical_backtest_config_ref", canonical_backtest_config_ref)
        object.__setattr__(self, "research_runner_ref", research_runner_ref)
        object.__setattr__(self, "parameter_snapshot_ref", parameter_snapshot_ref)
        object.__setattr__(self, "dataset_refs", dataset_refs)
        object.__setattr__(self, "result_storage_contract_refs", result_storage_contract_refs)
        object.__setattr__(self, "repo_commit", repo_commit)
        object.__setattr__(self, "execution_control", execution_control)

    @property
    def immutable_inputs(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "strategy_id": STRATEGY_ID,
            "strategy_version": MANIFEST.version,
            "strategy_package_ref": self.strategy_package_ref,
            "strategy_config_ref": self.strategy_config_ref,
            "canonical_backtest_engine_ref": self.canonical_backtest_engine_ref,
            "canonical_backtest_config_ref": self.canonical_backtest_config_ref,
            "research_runner_ref": self.research_runner_ref,
            "parameter_snapshot_ref": self.parameter_snapshot_ref,
            "dataset_refs": dict(self.dataset_refs),
            "result_storage_contract_refs": dict(self.result_storage_contract_refs),
            "repo_commit": self.repo_commit,
        }


def validate_d1_tsmom_minimal_platform_run_request(
    request: D1TSMOMMinimalPlatformRunRequest,
) -> D1TSMOMMinimalPlatformRunRequest:
    if not isinstance(request, D1TSMOMMinimalPlatformRunRequest):
        raise TypeError("request must be D1TSMOMMinimalPlatformRunRequest")
    return request


def build_planned_result_storage_bundle(request: D1TSMOMMinimalPlatformRunRequest) -> tuple[
    ExperimentRegistryEntry,
    ResultStorageBundle,
    PMReviewCloseout,
]:
    validate_d1_tsmom_minimal_platform_run_request(request)
    immutable_inputs = request.immutable_inputs
    run_id = compute_deterministic_run_id(immutable_inputs)
    artifacts = _planned_artifacts(run_id, request)
    manifest = ArtifactBundleManifest(
        manifest_id="manifest." + run_id + ".v1",
        run_id=run_id,
        schema_version="artifact_bundle_manifest.v1",
        repo_commit=request.repo_commit,
        artifacts=artifacts,
    )
    data_refs = dict(request.dataset_refs)
    result_refs = {
        "parameter_snapshot_ref": "artifact.d1_tsmom_minimal.parameter_snapshot.v1",
        "canonical_backtest_result_ref": "artifact.d1_tsmom_minimal.canonical_backtest_result.v1",
        "research_runner_ref": "artifact.d1_tsmom_minimal.research_runner.v1",
        "research_run_request_ref": "artifact.d1_tsmom_minimal.research_run_request.v1",
        "strategy_package_ref": "artifact.d1_tsmom_minimal.strategy_package.v1",
        "strategy_config_ref": "artifact.d1_tsmom_minimal.strategy_config.v1",
    }
    bundle = ResultStorageBundle(
        run_id=run_id,
        schema_version="result_storage_bundle.v1",
        storage_mode="dry_run",
        immutable_inputs=immutable_inputs,
        data_refs=data_refs,
        result_refs=result_refs,
        artifact_manifest=manifest,
        pm_review_closeout_ref="artifact.d1_tsmom_minimal.pm_review_closeout.v1",
        finalized=True,
    )
    closeout = PMReviewCloseout(
        review_id="artifact.d1_tsmom_minimal.pm_review_closeout.v1",
        run_id=run_id,
        status="pending_pm_review",
        reviewer_role="PM_L3_DELIVERY_VALIDATION_OWNER",
        reviewed_artifact_refs=(
            "artifact.d1_tsmom_minimal.research_run_request.v1",
            "artifact.d1_tsmom_minimal.parameter_snapshot.v1",
        ),
    )
    entry = ExperimentRegistryEntry(
        registry_entry_id="registry." + run_id + ".v1",
        run_id=run_id,
        strategy_id=STRATEGY_ID,
        strategy_version=MANIFEST.version,
        run_status="planned",
        result_status="not_evaluated",
        result_storage_bundle_ref="bundle." + run_id + ".v1",
        artifact_bundle_manifest_ref=manifest.manifest_id,
        pm_review_closeout_ref=closeout.review_id,
        repo_commit=request.repo_commit,
        data_refs=bundle.data_refs,
        result_refs=bundle.result_refs,
        immutable_inputs_hash=bundle.immutable_inputs_hash,
        metadata={"pm_review_status": "pending_pm_review", "real_run_executed": False},
    )
    return entry, bundle, closeout


def _planned_artifacts(run_id: str, request: D1TSMOMMinimalPlatformRunRequest) -> tuple[ArtifactRef, ...]:
    run_root = "artifacts/research/d1_tsmom_minimal/" + run_id
    return (
        _artifact("artifact.d1_tsmom_minimal.parameter_snapshot.v1", "parameter_snapshot", request.parameter_snapshot_ref),
        _artifact(
            "artifact.d1_tsmom_minimal.canonical_backtest_result.v1",
            "canonical_backtest_result",
            run_root + "/canonical_backtest_result.json",
        ),
        _artifact("artifact.d1_tsmom_minimal.research_run_request.v1", "research_run_request", "configs/research/d1_tsmom_minimal.platform_run_request.v1.yaml"),
        _artifact("artifact.d1_tsmom_minimal.research_runner.v1", "research_runner", request.research_runner_ref),
        _artifact("artifact.d1_tsmom_minimal.strategy_package.v1", "strategy_package", request.strategy_package_ref),
        _artifact("artifact.d1_tsmom_minimal.strategy_config.v1", "strategy_config", request.strategy_config_ref),
        _artifact("artifact.d1_tsmom_minimal.dataset_manifest.v1", "dataset_manifest", request.dataset_refs["dataset_manifest_ref"]),
        _artifact(
            "artifact.d1_tsmom_minimal.pm_review_closeout.v1",
            "pm_review_closeout",
            run_root + "/pm_review_closeout.json",
        ),
    )


def _artifact(artifact_id: str, role: str, locator: str) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        role=role,
        ref_class="repo_relative",
        locator=locator,
        producer="moex_research.runners.d1_tsmom_minimal_platform",
        consumer="PM_L3_DELIVERY_VALIDATION_OWNER",
        format="json" if locator.endswith(".json") else "repo_ref",
        schema_version="d1_tsmom_minimal_platform_run.v1",
    )


def _clean_text(value: Any, field_name: str) -> str:
    try:
        return _reject_forbidden_text(_require_text(value, field_name), field_name)
    except ResultStorageValidationError as exc:
        raise D1TSMOMMinimalRequestValidationError(str(exc)) from exc


def _clean_mapping(value: Mapping[str, str], field_name: str) -> dict[str, str]:
    mapping = dict(_require_mapping(value, field_name))
    for key, item in mapping.items():
        _clean_text(str(key), field_name + ".key")
        _clean_text(item, field_name + "." + str(key))
    return mapping


def _reject_forbidden_flags(values: Mapping[str, Any]) -> None:
    blocked_true_flags = (
        "real_run_allowed_in_this_slice",
        "production_write_allowed",
        "runtime_live_allowed",
        "broker_integration_allowed",
        "parameter_optimization_allowed",
        "market_conclusion_allowed",
    )
    for flag in blocked_true_flags:
        if values.get(flag) is not False:
            raise D1TSMOMMinimalRequestValidationError(flag + " must be false")


__all__ = [
    "D1TSMOMMinimalPlatformRunRequest",
    "D1TSMOMMinimalRequestValidationError",
    "build_planned_result_storage_bundle",
    "validate_d1_tsmom_minimal_platform_run_request",
]
