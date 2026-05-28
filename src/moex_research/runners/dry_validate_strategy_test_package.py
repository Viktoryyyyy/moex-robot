from __future__ import annotations

from moex_research.contracts.strategy_test_package import (
    StrategyTestPackage,
    validate_strategy_test_package,
)


def dry_validate_strategy_test_package(package: StrategyTestPackage) -> StrategyTestPackage:
    return validate_strategy_test_package(package)


__all__ = ["dry_validate_strategy_test_package"]
