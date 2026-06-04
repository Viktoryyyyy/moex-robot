from pathlib import Path

import pytest

from moex_research.registry.controlled_writer import CONTROLLED_TEMPDIR, PRODUCTION_WRITE, write_controlled_result_storage
from moex_research.registry.experiment_registry_contracts import PMReviewCloseout
from moex_research.registry.result_storage_contracts import ResultStorageValidationError
from moex_research.runners.d1_tsmom_minimal_controlled_real import (
    CONTROLLED_REAL_RUN_EXECUTION_MODE,
    CONTROLLED_REAL_RUN_REQUEST_ID,
    D1TSMOMMinimalControlledRealRunRequest,
    D1TSMOMMinimalControlledRealRunValidationError,
    build_controlled_real_run_result_storage_bundle,
    validate_d1_tsmom_minimal_controlled_real_run_request,
)
from moex_research.runners.d1_tsmom_minimal_platform import (
    D1TSMOMMinimalPlatformRunRequest,
    D1TSMOMMinimalRequestValidationError,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTROLLED_CONFIG_REF = "configs/research/d1_tsmom_minimal.controlled_real_run_request.v1.yaml"
CONTROLLED_CONTRACT_REF = "contracts/experiments/d1_tsmom_minimal_controlled_real_run_request.v1.yaml"


def _dataset_refs(**overrides):
    values = {
        "dataset_version_or_hash": "futures_d1_tsmom_minimal.contract_only.v1",
        "source_snapshot_ref": "contracts/datasets/futures_source_snapshot.v1.yaml",
        "data_refresh_manifest_ref": "contracts/datasets/futures_data_refresh_manifest.v1.yaml",
        "materialized_partition_ref": "contracts/datasets/futures_derived_d1.v1.yaml",
        "quality_report_ref": "contracts/datasets/futures_quality_report.v1.yaml",
        "dataset_manifest_ref": "contracts/datasets/futures_d1_tsmom_minimal_manifest.v1.yaml",
    }
    values.update(overrides)
    return values


def _result_storage_contract_refs(**overrides):
    values = {
        "experiment_registry_entry_contract_ref": "contracts/experiments/experiment_registry_entry.v1.yaml",
        "result_storage_bundle_contract_ref": "contracts/experiments/result_storage_bundle.v1.yaml",
        "artifact_bundle_manifest_contract_ref": "contracts/experiments/artifact_bundle_manifest.v1.yaml",
        "pm_review_closeout_contract_ref": "contracts/experiments/pm_review_closeout.v1.yaml",
    }
    values.update(overrides)
    return values


def _artifact_output_contract(**overrides):
    values = {
        "contract_class": "cli_argument",
        "argument_name": "--artifact-bundle-root",
        "storage_mode": CONTROLLED_TEMPDIR,
        "required": True,
        "path_rules": {
            "hardcoded_server_path_allowed": False,
            "implicit_file_selection_allowed": False,
            "latest_current_autodetect_allowed": False,
        },
    }
    values.update(overrides)
    return values


def _controlled_execution_control(**overrides):
    values = {
        "execution_mode": CONTROLLED_REAL_RUN_EXECUTION_MODE,
        "real_run_allowed_in_this_slice": True,
        "storage_mode": CONTROLLED_TEMPDIR,
        "artifact_output_contract": _artifact_output_contract(),
        "result_storage_bundle_validation_required": True,
        "pm_review_artifact_required": True,
        "stdout_only_result_allowed": False,
        "production_write_allowed": False,
        "runtime_live_allowed": False,
        "broker_integration_allowed": False,
        "parameter_optimization_allowed": False,
        "market_conclusion_allowed": False,
    }
    values.update(overrides)
    return values


def _planned_execution_control(**overrides):
    values = {
        "real_run_allowed_in_this_slice": False,
        "production_write_allowed": False,
        "runtime_live_allowed": False,
        "broker_integration_allowed": False,
        "parameter_optimization_allowed": False,
        "market_conclusion_allowed": False,
    }
    values.update(overrides)
    return values


def _controlled_request(**overrides):
    values = {
        "request_id": CONTROLLED_REAL_RUN_REQUEST_ID,
        "strategy_package_ref": "src/strategies/d1_tsmom_minimal",
        "strategy_config_ref": "configs/strategies/d1_tsmom_minimal.platform_run.v1.yaml",
        "canonical_backtest_engine_ref": "src/moex_backtest/engine/canonical.py",
        "canonical_backtest_config_ref": "configs/backtests/d1_tsmom_minimal.platform_run.v1.yaml",
        "research_runner_ref": "src/moex_research/runners/d1_tsmom_minimal_controlled_real.py",
        "parameter_snapshot_ref": "configs/research/d1_tsmom_minimal.parameter_snapshot.v1.yaml",
        "dataset_refs": _dataset_refs(),
        "result_storage_contract_refs": _result_storage_contract_refs(),
        "repo_commit": "2d782c3a264603412e38a83c357ba6330c1b4e11",
        "execution_control": _controlled_execution_control(),
    }
    values.update(overrides)
    return D1TSMOMMinimalControlledRealRunRequest(**values)


def _planned_request(**overrides):
    values = {
        "request_id": "d1_tsmom_minimal.platform_run_request.v1",
        "strategy_package_ref": "src/strategies/d1_tsmom_minimal",
        "strategy_config_ref": "configs/strategies/d1_tsmom_minimal.platform_run.v1.yaml",
        "canonical_backtest_engine_ref": "src/moex_backtest/engine/canonical.py",
        "canonical_backtest_config_ref": "configs/backtests/d1_tsmom_minimal.platform_run.v1.yaml",
        "research_runner_ref": "src/moex_research/runners/d1_tsmom_minimal_platform.py",
        "parameter_snapshot_ref": "configs/research/d1_tsmom_minimal.parameter_snapshot.v1.yaml",
        "dataset_refs": _dataset_refs(),
        "result_storage_contract_refs": _result_storage_contract_refs(),
        "repo_commit": "2d782c3a264603412e38a83c357ba6330c1b4e11",
        "execution_control": _planned_execution_control(),
    }
    values.update(overrides)
    return D1TSMOMMinimalPlatformRunRequest(**values)


def test_controlled_real_run_declared_refs_exist():
    assert (REPO_ROOT / CONTROLLED_CONFIG_REF).is_file()
    assert (REPO_ROOT / CONTROLLED_CONTRACT_REF).is_file()
    assert (REPO_ROOT / "src/moex_research/runners/d1_tsmom_minimal_controlled_real.py").is_file()


def test_planned_dry_run_request_still_rejects_accidental_real_execution():
    with pytest.raises(D1TSMOMMinimalRequestValidationError):
        _planned_request(execution_control=_planned_execution_control(real_run_allowed_in_this_slice=True))


def test_controlled_real_run_accepts_real_execution_only_with_artifact_output_contract():
    request = validate_d1_tsmom_minimal_controlled_real_run_request(_controlled_request())
    entry, bundle, closeout = build_controlled_real_run_result_storage_bundle(request)
    assert request.execution_control["real_run_allowed_in_this_slice"] is True
    assert entry.run_status == "controlled_written"
    assert entry.result_status == "not_evaluated"
    assert entry.metadata["real_run_executed"] is True
    assert bundle.storage_mode == CONTROLLED_TEMPDIR
    assert closeout.status == "pending_pm_review"
    with pytest.raises(D1TSMOMMinimalControlledRealRunValidationError):
        _controlled_request(execution_control=_controlled_execution_control(artifact_output_contract={}))


@pytest.mark.parametrize(
    "flag_name",
    [
        "production_write_allowed",
        "runtime_live_allowed",
        "broker_integration_allowed",
        "parameter_optimization_allowed",
        "market_conclusion_allowed",
    ],
)
def test_controlled_real_run_preserves_guardrail_flag_rejections(flag_name):
    with pytest.raises(D1TSMOMMinimalControlledRealRunValidationError):
        _controlled_request(execution_control=_controlled_execution_control(**{flag_name: True}))


def test_controlled_real_run_rejects_production_write_storage_mode():
    with pytest.raises(D1TSMOMMinimalControlledRealRunValidationError):
        _controlled_request(
            execution_control=_controlled_execution_control(
                storage_mode=PRODUCTION_WRITE,
                artifact_output_contract=_artifact_output_contract(storage_mode=PRODUCTION_WRITE),
            )
        )


@pytest.mark.parametrize("bad_ref", ["latest", "current", "autodetect", "stdout"])
def test_controlled_real_run_rejects_latest_current_autodetect_and_stdout_refs(bad_ref):
    with pytest.raises(D1TSMOMMinimalControlledRealRunValidationError):
        _controlled_request(
            execution_control=_controlled_execution_control(
                artifact_output_contract=_artifact_output_contract(argument_name=bad_ref)
            )
        )


def test_controlled_real_run_rejects_stdout_only_result():
    with pytest.raises(D1TSMOMMinimalControlledRealRunValidationError):
        _controlled_request(execution_control=_controlled_execution_control(stdout_only_result_allowed=True))


@pytest.mark.parametrize("required_flag", ["result_storage_bundle_validation_required", "pm_review_artifact_required"])
def test_controlled_real_run_requires_result_storage_validation_and_pm_review_artifact(required_flag):
    with pytest.raises(D1TSMOMMinimalControlledRealRunValidationError):
        _controlled_request(execution_control=_controlled_execution_control(**{required_flag: False}))


def test_controlled_real_run_requires_pm_review_contract_ref():
    with pytest.raises(D1TSMOMMinimalControlledRealRunValidationError):
        _controlled_request(result_storage_contract_refs=_result_storage_contract_refs(pm_review_closeout_contract_ref=""))


def test_result_storage_bundle_validation_is_required_before_controlled_write(tmp_path):
    entry, bundle, closeout = build_controlled_real_run_result_storage_bundle(_controlled_request())
    result = write_controlled_result_storage(
        registry_entry=entry,
        bundle=bundle,
        pm_review_closeout=closeout,
        storage_mode=CONTROLLED_TEMPDIR,
        storage_root=tmp_path,
    )
    assert result.persisted is True
    bad_closeout = PMReviewCloseout(
        review_id=closeout.review_id,
        run_id="run_mismatch",
        status=closeout.status,
        reviewer_role=closeout.reviewer_role,
        reviewed_artifact_refs=closeout.reviewed_artifact_refs,
    )
    with pytest.raises(ResultStorageValidationError):
        write_controlled_result_storage(
            registry_entry=entry,
            bundle=bundle,
            pm_review_closeout=bad_closeout,
            storage_mode=CONTROLLED_TEMPDIR,
            storage_root=tmp_path,
        )
