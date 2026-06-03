from pathlib import Path

import pytest

from moex_backtest.engine.canonical import CanonicalBacktestInput
from moex_strategy_sdk import REQUIRED_MANIFEST_FIELDS
from moex_strategy_sdk.errors import ConfigValidationError, InterfaceValidationError, UnsupportedModeError
from strategies.reference_fixture_strategy.artifact_contracts import ARTIFACT_CONTRACTS
from strategies.reference_fixture_strategy.backtest_adapter import ReferenceFixtureBacktestAdapter
from strategies.reference_fixture_strategy.config import build_config_schema, validate_config
from strategies.reference_fixture_strategy.live_adapter import ReferenceFixtureLiveAdapter
from strategies.reference_fixture_strategy.manifest import MANIFEST, STRATEGY_ID
from strategies.reference_fixture_strategy.signal_engine import ReferenceFixtureSignalEngine

REPO_ROOT = Path(__file__).resolve().parents[2]
STRATEGY_ROOT = REPO_ROOT / "src" / "strategies" / "reference_fixture_strategy"
REQUIRED_PACKAGE_FILES = (
    "manifest.py",
    "config.py",
    "signal_engine.py",
    "backtest_adapter.py",
    "live_adapter.py",
    "artifact_contracts.py",
)


def test_reference_fixture_strategy_required_files_exist():
    for file_name in REQUIRED_PACKAGE_FILES:
        assert (STRATEGY_ROOT / file_name).is_file()


def test_manifest_declares_required_contract_fields_and_blocks_live():
    for field_name in REQUIRED_MANIFEST_FIELDS:
        assert hasattr(MANIFEST, field_name)
    assert MANIFEST.strategy_id == STRATEGY_ID
    assert MANIFEST.version == "0.1.0"
    assert MANIFEST.instrument_scope == ("Si",)
    assert MANIFEST.timeframe == "D1"
    assert MANIFEST.required_datasets == ("dataset.futures_derived_d1.v1",)
    assert MANIFEST.required_features == ("feature.d1_tsmom_inputs.v1",)
    assert MANIFEST.required_labels == ("label.reference_fixture_none.v1",)
    assert MANIFEST.supports_backtest is True
    assert MANIFEST.supports_live is False
    assert MANIFEST.report_schema_version == "reference_fixture_report.v1"
    assert MANIFEST.artifact_contract_version == "reference_fixture_artifacts.v1"


def test_config_is_typed_validated_and_fail_closed():
    schema = build_config_schema()
    config = validate_config(schema.defaults)
    assert config.strategy_id == STRATEGY_ID
    assert config.default_target_position == 0
    with pytest.raises(ConfigValidationError):
        validate_config({"default_target_position": 2})
    with pytest.raises(ConfigValidationError):
        validate_config({"default_target_position": 1})
    non_flat_config = validate_config(
        {
            "default_target_position": 1,
            "allow_non_flat_fixture_positions": True,
        }
    )
    assert non_flat_config.default_target_position == 1


def test_signal_engine_is_deterministic_fixture_only():
    features = {"rows": ({"timestamp": "2026-01-01"}, {"timestamp": "2026-01-02"})}
    engine = ReferenceFixtureSignalEngine()
    first = engine.generate_signals(features)
    second = engine.generate_signals(features)
    assert first == second
    assert first == (
        {
            "strategy_id": STRATEGY_ID,
            "timestamp": "2026-01-01",
            "target_position": 0,
            "reason_code": "reference_fixture_noop",
        },
        {
            "strategy_id": STRATEGY_ID,
            "timestamp": "2026-01-02",
            "target_position": 0,
            "reason_code": "reference_fixture_noop",
        },
    )
    with pytest.raises(InterfaceValidationError):
        engine.generate_signals({"rows": ({"not_timestamp": "bad"},)})


def test_backtest_adapter_maps_only_to_canonical_contract():
    signals = ReferenceFixtureSignalEngine().generate_signals({"rows": ({"timestamp": 0},)})
    request = ReferenceFixtureBacktestAdapter().to_backtest_inputs(
        signals=signals,
        context={"bars": ({"timestamp": 0, "open": 100.0, "close": 100.0},)},
    )
    assert isinstance(request, CanonicalBacktestInput)
    assert request.signals == ({"timestamp": 0, "target_position": 0},)
    with pytest.raises(InterfaceValidationError):
        ReferenceFixtureBacktestAdapter().to_backtest_inputs(signals=signals, context={})


def test_live_adapter_is_blocked_by_default_fail_closed():
    adapter = ReferenceFixtureLiveAdapter()
    assert adapter.supports_live is False
    with pytest.raises(UnsupportedModeError):
        adapter.to_live_intents(signals=(), context={})


def test_artifact_contracts_are_explicit():
    artifact_ids = tuple(contract.artifact_id for contract in ARTIFACT_CONTRACTS)
    assert artifact_ids == (
        "reference_fixture_strategy.input_features.v1",
        "reference_fixture_strategy.output_signals.v1",
        "reference_fixture_strategy.backtest_input.v1",
        "reference_fixture_strategy.live_blocked.v1",
    )
    assert all(contract.contract_class in {"repo_relative", "cli_argument"} for contract in ARTIFACT_CONTRACTS)
    assert all(contract.locator_ref for contract in ARTIFACT_CONTRACTS)


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
        "registry_mutation_allowed",
        "promotion_verdict",
        "approved",
        "broker",
        "order_send",
    )
    for path in STRATEGY_ROOT.glob("*.py"):
        source = path.read_text(encoding="utf-8").casefold()
        for term in forbidden_terms:
            assert term not in source


def test_strategy_declares_existing_registry_dependencies_without_execution():
    assert MANIFEST.required_datasets == ("dataset.futures_derived_d1.v1",)
    assert MANIFEST.required_features == ("feature.d1_tsmom_inputs.v1",)
    assert MANIFEST.instrument_scope == ("Si",)
