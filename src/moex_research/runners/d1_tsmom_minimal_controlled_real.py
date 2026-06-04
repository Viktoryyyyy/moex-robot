from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from moex_research.registry.experiment_registry_contracts import ExperimentRegistryEntry, PMReviewCloseout
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
from moex_research.runners.d1_tsmom_minimal_platform import (
    CANONICAL_BACKTEST_ENGINE_REF,
    D1_TSMOM_MINIMAL_BACKTEST_CONFIG_REF,
    D1_TSMOM_MINIMAL_CONFIG_REF,
    D1_TSMOM_MINIMAL_PACKAGE_REF,
    D1_TSMOM_MINIMAL_PARAMETER_SNAPSHOT_REF,
)
from strategies.d1_tsmom_minimal.manifest import MANIFEST, STRATEGY_ID


CONTROLLED_REAL_RUN_REQUEST_ID = "d1_tsmom_minimal.controlled_real_run_request.v1"
CONTROLLED_REAL_RUN_EXECUTION_MODE = "controlled_real_run"
CONTROLLED_REAL_RUNNER_REF = "src/moex_research/runners/d1_tsmom_minimal_controlled_real.py"
CONTROLLED_REQUEST_CONFIG_REF = "configs/research/d1_tsmom_minimal.controlled_real_run_request.v1.yaml"
CONTROLLED_WRITE_STORAGE_MODES = frozenset({"controlled_fixture", "controlled_tempdir", "controlled_local_test"})
CONTROLLED_ARTIFACT_OUTPUT_CONTRACT_CLASSES = frozenset({"cli_argument", "env_contract"})
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


class D1TSMOMMinimalControlledRealRunValidationError(ValueError):
    pass


@dataclass(frozen=True)
class D1TSMOMMinimalControlledRealRunRequest:
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
        if request_id != CONTROLLED_REAL_RUN_REQUEST_ID:
            raise D1TSMOMMinimalControlledRealRunValidationError("unsupported controlled request_id")
        if strategy_package_ref != D1_TSMOM_MINIMAL_PACKAGE_REF:
            raise D1TSMOMMinimalControlledRealRunValidationError("unexpected strategy package ref")
        if strategy_config_ref != D1_TSMOM_MINIMAL_CONFIG_REF:
            raise D1TSMOMMinimalControlledRealRunValidationError("unexpected strategy config ref")
        if canonical_backtest_engine_ref != CANONICAL_BACKTEST_ENGINE_REF:
            raise D1TSMOMMinimalControlledRealRunValidationError("unexpected canonical backtest engine ref")
        if canonical_backtest_config_ref != D1_TSMOM_MINIMAL_BACKTEST_CONFIG_REF:
            raise D1TSMOMMinimalControlledRealRunValidationError("unexpected canonical backtest config ref")
        if research_runner_ref != CONTROLLED_REAL_RUNNER_REF:
            raise D1TSMOMMinimalControlledRealRunValidationError("unexpected controlled research runner ref")
        if parameter_snapshot_ref != D1_TSMOM_MINIMAL_PARAMETER_SNAPSHOT_REF:
            raise D1TSMOMMinimalControlledRealRunValidationError("unexpected parameter snapshot ref")
        for field_name in REQUIRED_DATASET_REF_FIELDS:
            _clean_text(dataset_refs.get(field_name), "dataset_refs." + field_name)
        for field_name in REQUIRED_RESULT_STORAGE_CONTRACT_REF_FIELDS:
            _clean_text(result_storage_contract_refs.get(field_name), "result_storage_contract_refs." + field_name)
        _validate_execution_control(execution_control)
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
    def artifact_output_contract(self) -> dict[str, Any]:
        return dict(_require_mapping(self.execution_control.get("artifact_output_contract"), "artifact_output_contract"))

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
            "execution_mode": CONTROLLED_REAL_RUN_EXECUTION_MODE,
            "artifact_output_contract": self.artifact_output_contract,
        }


def validate_d1_tsmom_minimal_controlled_real_run_request(
    request: D1TSMOMMinimalControlledRealRunRequest,
) -> D1TSMOMMinimalControlledRealRunRequest:
    if not isinstance(request, D1TSMOMMinimalControlledRealRunRequest):
        raise TypeError("request must be D1TSMOMMinimalControlledRealRunRequest")
    return request


def build_controlled_real_run_result_storage_bundle(
    request: D1TSMOMMinimalControlledRealRunRequest,
) -> tuple[ExperimentRegistryEntry, ResultStorageBundle, PMReviewCloseout]:
    validate_d1_tsmom_minimal_controlled_real_run_request(request)
    immutable_inputs = request.immutable_inputs
    run_id = compute_deterministic_run_id(immutable_inputs)
    artifacts = _controlled_artifacts(run_id, request)
    manifest = ArtifactBundleManifest(
        manifest_id="manifest." + run_id + ".v1",
        run_id=run_id,
        schema_version="artifact_bundle_manifest.v1",
        repo_commit=request.repo_commit,
        artifacts=artifacts,
    )
    data_refs = dict(request.dataset_refs)
    result_refs = _standard_result_refs()
    bundle = ResultStorageBundle(
        run_id=run_id,
        schema_version="result_storage_bundle.v1",
        storage_mode=str(request.execution_control["storage_mode"]),
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
            "artifact.d1_tsmom_minimal.canonical_backtest_result.v1",
        ),
    )
    entry = ExperimentRegistryEntry(
        registry_entry_id="registry." + run_id + ".v1",
        run_id=run_id,
        strategy_id=STRATEGY_ID,
        strategy_version=MANIFEST.version,
        run_status="controlled_written",
        result_status="not_evaluated",
        result_storage_bundle_ref="bundle." + run_id + ".v1",
        artifact_bundle_manifest_ref=manifest.manifest_id,
        pm_review_closeout_ref=closeout.review_id,
        repo_commit=request.repo_commit,
        data_refs=bundle.data_refs,
        result_refs=bundle.result_refs,
        immutable_inputs_hash=bundle.immutable_inputs_hash,
        metadata={
            "pm_review_status": "pending_pm_review",
            "real_run_executed": True,
            "execution_mode": CONTROLLED_REAL_RUN_EXECUTION_MODE,
        },
    )
    return entry, bundle, closeout


def _standard_result_refs() -> dict[str, str]:
    return {
        "parameter_snapshot_ref": "artifact.d1_tsmom_minimal.parameter_snapshot.v1",
        "canonical_backtest_result_ref": "artifact.d1_tsmom_minimal.canonical_backtest_result.v1",
        "research_runner_ref": "artifact.d1_tsmom_minimal.research_runner.v1",
        "research_run_request_ref": "artifact.d1_tsmom_minimal.research_run_request.v1",
        "strategy_package_ref": "artifact.d1_tsmom_minimal.strategy_package.v1",
        "strategy_config_ref": "artifact.d1_tsmom_minimal.strategy_config.v1",
    }


def _controlled_artifacts(run_id: str, request: D1TSMOMMinimalControlledRealRunRequest) -> tuple[ArtifactRef, ...]:
    run_root = "artifacts/research/d1_tsmom_minimal/" + run_id
    return (
        _artifact("artifact.d1_tsmom_minimal.parameter_snapshot.v1", "parameter_snapshot", request.parameter_snapshot_ref),
        _artifact(
            "artifact.d1_tsmom_minimal.canonical_backtest_result.v1",
            "canonical_backtest_result",
            run_root + "/canonical_backtest_result.json",
        ),
        _artifact("artifact.d1_tsmom_minimal.research_run_request.v1", "research_run_request", CONTROLLED_REQUEST_CONFIG_REF),
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
        producer="moex_research.runners.d1_tsmom_minimal_controlled_real",
        consumer="PM_L3_DELIVERY_VALIDATION_OWNER",
        format="json" if locator.endswith(".json") else "repo_ref",
        schema_version="d1_tsmom_minimal_controlled_real_run.v1",
    )


def _validate_execution_control(values: Mapping[str, Any]) -> None:
    _clean_nested_text_values(values, "execution_control")
    if values.get("execution_mode") != CONTROLLED_REAL_RUN_EXECUTION_MODE:
        raise D1TSMOMMinimalControlledRealRunValidationError("execution_mode must be controlled_real_run")
    if values.get("real_run_allowed_in_this_slice") is not True:
        raise D1TSMOMMinimalControlledRealRunValidationError("real_run_allowed_in_this_slice must be true")
    for flag in (
        "production_write_allowed",
        "runtime_live_allowed",
        "broker_integration_allowed",
        "parameter_optimization_allowed",
        "market_conclusion_allowed",
    ):
        if values.get(flag) is not False:
            raise D1TSMOMMinimalControlledRealRunValidationError(flag + " must be false")
    storage_mode = _clean_text(values.get("storage_mode"), "execution_control.storage_mode")
    if storage_mode not in CONTROLLED_WRITE_STORAGE_MODES:
        raise D1TSMOMMinimalControlledRealRunValidationError("storage_mode must be controlled")
    _validate_artifact_output_contract(values.get("artifact_output_contract"), storage_mode)
    if values.get("result_storage_bundle_validation_required") is not True:
        raise D1TSMOMMinimalControlledRealRunValidationError("result storage bundle validation must be required")
    if values.get("pm_review_artifact_required") is not True:
        raise D1TSMOMMinimalControlledRealRunValidationError("PM review artifact must be required")
    if values.get("stdout_only_result_allowed") is not False:
        raise D1TSMOMMinimalControlledRealRunValidationError("stdout-only result must be rejected")


def _validate_artifact_output_contract(value: Any, storage_mode: str) -> None:
    contract = dict(_require_mapping(value, "artifact_output_contract"))
    _clean_nested_text_values(contract, "artifact_output_contract")
    contract_class = _clean_text(contract.get("contract_class"), "artifact_output_contract.contract_class")
    if contract_class not in CONTROLLED_ARTIFACT_OUTPUT_CONTRACT_CLASSES:
        raise D1TSMOMMinimalControlledRealRunValidationError("artifact output contract must be cli_argument or env_contract")
    contract_storage_mode = _clean_text(contract.get("storage_mode"), "artifact_output_contract.storage_mode")
    if contract_storage_mode != storage_mode:
        raise D1TSMOMMinimalControlledRealRunValidationError("artifact output contract storage_mode must match")
    if contract.get("required") is not True:
        raise D1TSMOMMinimalControlledRealRunValidationError("artifact output contract must be required")
    if contract_class == "cli_argument":
        argument_name = _clean_text(contract.get("argument_name"), "artifact_output_contract.argument_name")
        if not argument_name.startswith("--"):
            raise D1TSMOMMinimalControlledRealRunValidationError("cli artifact output contract requires -- argument name")
    if contract_class == "env_contract":
        env_var = _clean_text(contract.get("env_var"), "artifact_output_contract.env_var")
        if not env_var.isidentifier() or env_var.upper() != env_var:
            raise D1TSMOMMinimalControlledRealRunValidationError("env artifact output contract requires uppercase env_var")
    path_rules = dict(_require_mapping(contract.get("path_rules"), "artifact_output_contract.path_rules"))
    for flag in (
        "hardcoded_server_path_allowed",
        "implicit_file_selection_allowed",
        "latest_current_autodetect_allowed",
    ):
        if path_rules.get(flag) is not False:
            raise D1TSMOMMinimalControlledRealRunValidationError("artifact output path rule must be false: " + flag)


def _clean_text(value: Any, field_name: str) -> str:
    try:
        return _reject_forbidden_text(_require_text(value, field_name), field_name)
    except ResultStorageValidationError as exc:
        raise D1TSMOMMinimalControlledRealRunValidationError(str(exc)) from exc


def _clean_mapping(value: Mapping[str, str], field_name: str) -> dict[str, str]:
    mapping = dict(_require_mapping(value, field_name))
    for key, item in mapping.items():
        _clean_text(str(key), field_name + ".key")
        _clean_text(item, field_name + "." + str(key))
    return mapping


def _clean_nested_text_values(value: Mapping[str, Any], field_name: str) -> None:
    for key, item in value.items():
        key_text = _clean_text(str(key), field_name + ".key")
        item_name = field_name + "." + key_text
        if isinstance(item, str):
            _clean_text(item, item_name)
        elif isinstance(item, Mapping):
            _clean_nested_text_values(item, item_name)
        elif isinstance(item, bool):
            continue
        elif item is None:
            raise D1TSMOMMinimalControlledRealRunValidationError(item_name + " is required")


__all__ = [
    "CONTROLLED_REAL_RUN_EXECUTION_MODE",
    "CONTROLLED_REAL_RUN_REQUEST_ID",
    "D1TSMOMMinimalControlledRealRunRequest",
    "D1TSMOMMinimalControlledRealRunValidationError",
    "build_controlled_real_run_result_storage_bundle",
    "validate_d1_tsmom_minimal_controlled_real_run_request",
]
