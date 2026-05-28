from pathlib import Path

import pytest

from moex_research.contracts.strategy_test_package import StrategyTestPackageValidationError
from moex_research.runners.dry_validate_strategy_test_package import dry_validate_strategy_test_package
from tests.fixtures.strategy_testing.ema_3_19_package import build_ema_3_19_strategy_test_package


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "dry_validate_strategy_test_package.py"


def test_valid_ema_fixture_passes_dry_validation():
    package = build_ema_3_19_strategy_test_package()

    assert dry_validate_strategy_test_package(package) is package


def test_invalid_package_type_fails_closed():
    with pytest.raises(TypeError):
        dry_validate_strategy_test_package(object())


def test_invalid_package_state_fails_closed():
    package = build_ema_3_19_strategy_test_package()
    package.artifact_manifest_ref = "latest"

    with pytest.raises(StrategyTestPackageValidationError):
        dry_validate_strategy_test_package(package)


def test_dry_validation_runner_exposes_only_pure_boundary():
    import moex_research.runners.dry_validate_strategy_test_package as runner

    public_names = set(runner.__all__)

    assert public_names == {"dry_validate_strategy_test_package"}
    assert callable(runner.dry_validate_strategy_test_package)

    forbidden_names = {
        "run_backtest",
        "execute",
        "live",
        "broker",
        "order",
        "pnl",
        "metrics",
        "report",
        "registry_write",
    }
    assert public_names.isdisjoint(forbidden_names)
    for name in forbidden_names:
        assert not hasattr(runner, name)


def test_dry_validation_runner_source_has_no_forbidden_responsibilities():
    source = RUNNER_PATH.read_text(encoding="utf-8").casefold()
    forbidden_terms = (
        "run_backtest",
        "execute",
        "live",
        "broker",
        "order",
        "pnl",
        "metrics",
        "report",
        "registry_write",
        "server",
        "data_root",
        "latest",
        "current",
        "autodetect",
    )

    for term in forbidden_terms:
        assert term not in source
