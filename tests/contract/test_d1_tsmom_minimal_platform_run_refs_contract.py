from pathlib import Path

import pytest

from moex_backtest.engine.canonical import CanonicalBacktestInput
from moex_research.registry.controlled_writer import PRODUCTION_WRITE, write_controlled_result_storage
from moex_research.registry.result_storage_contracts import ResultStorageValidationError
from moex_research.runners.d1_tsmom_minimal_platform import (
    D1TSMOMMinimalPlatformRunRequest,
    D1TSMOMMinimalRequestValidationError,
    build_planned_result_storage_bundle,
    validate_d1_tsmom_minimal_platform_run_request,
)
from moex_strategy_sdk import REQUIRED_MANIFEST_FIELDS
from moex_strategy_sdk.errors import ConfigValidationError, InterfaceValidationError, UnsupportedModeError
from strategies.d1_tsmom_minimal.artifact_contracts import ARTIFACT_CONTRACTS
from strategies.d1_tsmom_minimal.backtest_adapter import D1TSMOMMinimalBacktestAdapter
from strategies.d1_tsmom_minimal.config import build_config_schema, validate_config
from strategies.d1_tsmom_minimal.live_adapter import D1TSMOMMinimalLiveAdapter
from strategies.d1_tsmom_minimal.manifest import MANIFEST, STRATEGY_ID
from strategies.d1_tsmom_minimal.signal_engine import D1TSMOMMinimalSignalEngine


REPO_ROOT = Path(__file__).resolve().parents[2]
STRATEGY_ROOT = REPO_ROOT / "src" / "strategies" / "d1_tsmom_minimal"
REQUIRED_PACKAGE_FILES = (
    "manifest.py",
    "config.py",
    "signal_engine.py",
    "backtest_adapter.py",
    "live_adapter.py",
    "artifact_contracts.py",
)
REQUIRED_REFS = (
    "configs/strategies/d1_tsmom_minimal.platform_run.v1.yaml",
    "configs/backtests/d1_tsmom_minimal.platform_run.v1.yaml",
    "configs/research/d1_tsmom_minimal.platform_run_request.v1.yaml",
    "configs/research/d1_tsmom_minimal.parameter_snapshot.v1.yaml",
    "contracts/datasets/futures_d1_tsmom_minimal_manifest.v1.yaml",
    "contracts/datasets/futures_source_snapshot.v1.yaml",
    "contracts/experiments/d1_tsmom_minimal_platform_run_request.v1.yaml",
)


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


def _execution_control(**overrides):
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


def _request(**overrides):
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
        "repo_commit": "fff04fafdfc1431dbd25ef02bd9042854dfcdb3f",
        "execution_control": _execution_control(),
    }
    values.update(overrides)
    return D1TSMOMMinimalPlatformRunRequest(**values)


def test_d1_tsmom_minimal_required_package_and_repo_refs_exist():
    for file_name in REQUIRED_PACKAGE_FILES:
        assert (STRATEGY_ROOT / file_name).is_file()
    for path in REQUIRED_REFS:
        assert (REPO_ROOT / path).is_file()


def test_manifest_declares_real_strategy_contract_without_live_or_promotion():
    for field_name in REQUIRED_MANIFEST_FIELDS:
        assert hasattr(MANIFEST, field_name)
    assert MANIFEST.strategy_id == STRATEGY_ID
    assert MANIFEST.version == "0.1.0"
    assert MANIFEST.instrument_scope == ("Si", "USDRUBF")
    assert MANIFEST.timeframe == "D1"
    assert MANIFEST.required_datasets == ("dataset.futures_derived_d1.v1",)
    assert MANIFEST.required_features == ("feature.d1_tsmom_minimal_returns.v1",)
    assert MANIFEST.required_labels == ("label.d1_tsmom_minimal_forward_return.v1",)
    assert MANIFEST.supports_backtest is True
    assert MANIFEST.supports_live is False


def test_config_is_fixed_minimal_and_rejects_parameter_optimization():
    config = validate_config(build_config_schema().defaults)
    assert config.lookback_days == 20
    assert config.signal_threshold == 0.0
    with pytest.raises(ConfigValidationError):
        validate_config({"lookback_days": 10})
    with pytest.raises(ConfigValidationError):
        validate_config({"signal_threshold": 0.2})


def test_signal_engine_maps_20d_return_sign_to_target_position():
    features = {
        "rows": (
            {"timestamp": "2026-01-01", "lookback_return": 0.01},
            {"timestamp": "2026-01-02", "lookback_return": -0.01},
            {"timestamp": "2026-01-03", "lookback_return": 0.0},
        )
    }
    signals = D1TSMOMMinimalSignalEngine().generate_signals(features)
    assert tuple(row["target_position"] for row in signals) == (1, -1, 0)
    with pytest.raises(InterfaceValidationError):
        D1TSMOMMinimalSignalEngine().generate_signals({"rows": ({"timestamp": "bad"},)})


def test_backtest_adapter_maps_to_canonical_engine_input_only():
    signals = D1TSMOMMinimalSignalEngine().generate_signals({"rows": ({"timestamp": 0, "lookback_return": 0.01},)})
    contract = D1TSMOMMinimalBacktestAdapter().to_backtest_inputs(
        signals=signals,
        context={"bars": ({"timestamp": 0, "open": 100.0, "close": 101.0}, {"timestamp": 1, "open": 101.0, "close": 102.0})},
    )
    assert isinstance(contract, CanonicalBacktestInput)
    assert contract.signals == ({"timestamp": 0, "target_position": 1},)
    with pytest.raises(InterfaceValidationError):
        D1TSMOMMinimalBacktestAdapter().to_backtest_inputs(signals=signals, context={})


def test_live_adapter_is_blocked_fail_closed():
    adapter = D1TSMOMMinimalLiveAdapter()
    assert adapter.supports_live is False
    with pytest.raises(UnsupportedModeError):
        adapter.to_live_intents(signals=(), context={})


def test_artifact_contracts_are_explicit_and_include_real_data_input_ref():
    artifact_ids = tuple(contract.artifact_id for contract in ARTIFACT_CONTRACTS)
    assert artifact_ids == (
        "d1_tsmom_minimal.input_features.v1",
        "d1_tsmom_minimal.output_signals.v1",
        "d1_tsmom_minimal.backtest_input.v1",
        "d1_tsmom_minimal.live_blocked.v1",
    )
    assert ARTIFACT_CONTRACTS[0].contract_class == "external_pattern"
    assert "MOEX_DATA_ROOT" in ARTIFACT_CONTRACTS[0].locator_ref


def test_platform_run_request_accepts_exact_refs_and_result_storage_contracts():
    request = validate_d1_tsmom_minimal_platform_run_request(_request())
    assert request.strategy_package_ref == "src/strategies/d1_tsmom_minimal"
    assert request.dataset_refs["dataset_manifest_ref"] == "contracts/datasets/futures_d1_tsmom_minimal_manifest.v1.yaml"
    entry, bundle, closeout = build_planned_result_storage_bundle(request)
    assert entry.strategy_id == STRATEGY_ID
    assert entry.run_status == "planned"
    assert entry.result_status == "not_evaluated"
    assert bundle.data_refs["source_snapshot_ref"] == "contracts/datasets/futures_source_snapshot.v1.yaml"
    assert bundle.result_refs["canonical_backtest_result_ref"] == "artifact.d1_tsmom_minimal.canonical_backtest_result.v1"
    assert closeout.status == "pending_pm_review"


@pytest.mark.parametrize("bad_ref", ["artifacts/latest/result.json", "artifacts/current/result.json", "artifacts/autodetect/result.json", "stdout"])
def test_platform_run_request_rejects_dynamic_or_stdout_refs(bad_ref):
    with pytest.raises(D1TSMOMMinimalRequestValidationError):
        _request(parameter_snapshot_ref=bad_ref)


@pytest.mark.parametrize(
    "field_name",
    ["source_snapshot_ref", "data_refresh_manifest_ref", "materialized_partition_ref", "quality_report_ref", "dataset_manifest_ref"],
)
def test_platform_run_request_rejects_missing_dataset_refs(field_name):
    with pytest.raises(D1TSMOMMinimalRequestValidationError):
        _request(dataset_refs=_dataset_refs(**{field_name: ""}))


@pytest.mark.parametrize(
    "flag_name",
    [
        "real_run_allowed_in_this_slice",
        "production_write_allowed",
        "runtime_live_allowed",
        "broker_integration_allowed",
        "parameter_optimization_allowed",
        "market_conclusion_allowed",
    ],
)
def test_platform_run_request_rejects_forbidden_execution_flags(flag_name):
    with pytest.raises(D1TSMOMMinimalRequestValidationError):
        _request(execution_control=_execution_control(**{flag_name: True}))


def test_planned_result_storage_preserves_production_write_rejection(tmp_path):
    entry, bundle, closeout = build_planned_result_storage_bundle(_request())
    with pytest.raises(ResultStorageValidationError):
        write_controlled_result_storage(
            registry_entry=entry,
            bundle=bundle,
            pm_review_closeout=closeout,
            storage_mode=PRODUCTION_WRITE,
            storage_root=tmp_path,
        )


def test_strategy_package_source_has_no_forbidden_io_network_runtime_registry_or_promotion_terms():
    forbidden_terms = (
        "open(",
        ".read_text(",
        ".write_text(",
        "pathlib",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "http",
        "glob(",
        "os.",
        "order_send",
        "broker",
    )
    for path in STRATEGY_ROOT.glob("*.py"):
        source = path.read_text(encoding="utf-8").casefold()
        for term in forbidden_terms:
            assert term not in source


def test_reference_fixture_process_proof_file_still_exists():
    assert (REPO_ROOT / "configs/strategies/reference_fixture_strategy.process_proof.v1.yaml").is_file()
    assert (REPO_ROOT / "configs/backtests/reference_fixture_strategy.process_proof.v1.yaml").is_file()
