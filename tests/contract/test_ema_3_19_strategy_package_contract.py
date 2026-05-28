from pathlib import Path

import pytest

from moex_strategy_sdk import ArtifactContract, StrategyManifest, validate_strategy_manifest
from moex_strategy_sdk.errors import ConfigValidationError, UnsupportedModeError


PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "strategies" / "ema_3_19"
REQUIRED_FILES = {
    "__init__.py",
    "manifest.py",
    "config.py",
    "signal_engine.py",
    "backtest_adapter.py",
    "live_adapter.py",
    "artifact_contracts.py",
}
OPTIONAL_OUT_OF_SCOPE_FILES = {"reports.py", "risk_policy.py", "feature_mapping.py"}


def _source(path: str) -> str:
    return (PACKAGE_ROOT / path).read_text(encoding="utf-8").casefold()


def test_ema_3_19_package_imports():
    import strategies.ema_3_19 as package
    from strategies.ema_3_19.artifact_contracts import ARTIFACT_CONTRACTS
    from strategies.ema_3_19.backtest_adapter import to_backtest_inputs
    from strategies.ema_3_19.config import DEFAULT_CONFIG, EMA319Config, build_config
    from strategies.ema_3_19.live_adapter import SUPPORTS_LIVE, to_live_intents
    from strategies.ema_3_19.manifest import STRATEGY_ID, STRATEGY_MANIFEST
    from strategies.ema_3_19.signal_engine import generate_signals

    assert package.STRATEGY_ID == "ema_3_19"
    assert STRATEGY_ID == "ema_3_19"
    assert isinstance(STRATEGY_MANIFEST, StrategyManifest)
    assert isinstance(DEFAULT_CONFIG, EMA319Config)
    assert build_config().fast_period == 3
    assert callable(generate_signals)
    assert callable(to_backtest_inputs)
    assert callable(to_live_intents)
    assert SUPPORTS_LIVE is False
    assert ARTIFACT_CONTRACTS


def test_package_has_exact_required_files_only():
    file_names = {path.name for path in PACKAGE_ROOT.iterdir() if path.is_file()}

    assert file_names == REQUIRED_FILES
    assert file_names.isdisjoint(OPTIONAL_OUT_OF_SCOPE_FILES)


def test_manifest_validates_and_blocks_live():
    from strategies.ema_3_19.manifest import STRATEGY_MANIFEST

    manifest = validate_strategy_manifest(STRATEGY_MANIFEST)

    assert manifest.strategy_id == "ema_3_19"
    assert manifest.version == "0.1.0"
    assert manifest.instrument_scope == ("Si",)
    assert manifest.timeframe == "15m"
    assert manifest.required_datasets == ("futures_continuous_5m",)
    assert manifest.required_features == ("ema_3", "ema_19")
    assert manifest.required_labels == ("signal_direction",)
    assert manifest.supports_backtest is True
    assert manifest.supports_live is False
    assert manifest.report_schema_version == "strategy_report.v1"
    assert manifest.artifact_contract_version == "strategy_artifacts.v1"


def test_config_defaults_and_validation():
    from strategies.ema_3_19.config import DEFAULT_CONFIG, build_config

    assert DEFAULT_CONFIG.strategy_id == "ema_3_19"
    assert DEFAULT_CONFIG.version == "0.1.0"
    assert DEFAULT_CONFIG.fast_period == 3
    assert DEFAULT_CONFIG.slow_period == 19
    assert build_config().to_mapping()["fast_period"] == 3

    with pytest.raises(ConfigValidationError):
        build_config(fast_period=19, slow_period=19)
    with pytest.raises(ConfigValidationError):
        build_config(fast_period=20, slow_period=19)


def test_signal_engine_boundary_only():
    from strategies.ema_3_19.signal_engine import generate_signals

    signals = generate_signals(
        (
            {"ema_3": 12.0, "ema_19": 10.0, "feature_timestamp": "t1"},
            {"ema_3": 8.0, "ema_19": 10.0, "feature_timestamp": "t2"},
            {"ema_3": 10.0, "ema_19": 10.0, "feature_timestamp": "t3"},
        )
    )

    assert signals == (
        {
            "strategy_id": "ema_3_19",
            "signal_name": "ema_3_19_direction",
            "signal_value": 1,
            "feature_timestamp": "t1",
        },
        {
            "strategy_id": "ema_3_19",
            "signal_name": "ema_3_19_direction",
            "signal_value": -1,
            "feature_timestamp": "t2",
        },
        {
            "strategy_id": "ema_3_19",
            "signal_name": "ema_3_19_direction",
            "signal_value": 0,
            "feature_timestamp": "t3",
        },
    )

    source = _source("signal_engine.py")
    forbidden = (
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "open(",
        "read_text",
        "write_text",
        "fill",
        "pnl",
        "lock",
        "server",
    )
    for marker in forbidden:
        assert marker not in source


def test_backtest_adapter_mapping_only():
    from strategies.ema_3_19.backtest_adapter import to_backtest_inputs

    mapped = to_backtest_inputs(
        ({"signal_value": 1},),
        {"calendar_contract": "canonical_backtest_semantics.v1"},
    )

    assert mapped == {
        "strategy_id": "ema_3_19",
        "signal_rows": ({"signal_value": 1},),
        "context": {"calendar_contract": "canonical_backtest_semantics.v1"},
        "adapter_contract": "canonical_backtest_input.v1",
    }

    source = _source("backtest_adapter.py")
    forbidden = ("pnl", "fill", "commission", "slippage", "cost", "dataset", "discover", "subprocess")
    for marker in forbidden:
        assert marker not in source


def test_live_adapter_is_blocked_interface_only():
    from strategies.ema_3_19.live_adapter import SUPPORTS_LIVE, assert_live_blocked, to_live_intents

    assert SUPPORTS_LIVE is False
    with pytest.raises(UnsupportedModeError):
        assert_live_blocked()
    with pytest.raises(UnsupportedModeError):
        to_live_intents(({"signal_value": 1},), {})

    source = _source("live_adapter.py")
    forbidden = ("order", "broker", "scheduler", "submit", "route", "position", "fill", "subprocess")
    for marker in forbidden:
        assert marker not in source


def test_artifact_contracts_are_explicit():
    from strategies.ema_3_19.artifact_contracts import ARTIFACT_CONTRACTS

    assert len(ARTIFACT_CONTRACTS) == 3
    assert all(isinstance(contract, ArtifactContract) for contract in ARTIFACT_CONTRACTS)

    expected = {
        "ema_3_19.input_features.v1": ("input", "moex_features.daily", "strategies.ema_3_19.signal_engine", "table"),
        "ema_3_19.signal_output.v1": ("signal_output", "strategies.ema_3_19.signal_engine", "moex_backtest.engine", "table"),
        "ema_3_19.report.v1": ("report", "moex_backtest.reports", "moex_research.publishers", "json"),
    }
    actual = {
        contract.artifact_id: (
            contract.artifact_class,
            contract.producer,
            contract.consumer,
            contract.format,
        )
        for contract in ARTIFACT_CONTRACTS
    }

    assert actual == expected
    for contract in ARTIFACT_CONTRACTS:
        assert contract.schema_version
        assert contract.contract_class == "external_pattern"


def test_no_forbidden_markers_or_d1_tsmom_references():
    full_source = "\n".join(
        path.read_text(encoding="utf-8").casefold()
        for path in PACKAGE_ROOT.iterdir()
        if path.is_file()
    )

    forbidden = (
        "latest",
        "current",
        "autodetect",
        "d1_tsmom",
        "tsmom",
        "/home/",
        "/var/",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "open(",
    )
    for marker in forbidden:
        assert marker not in full_source
