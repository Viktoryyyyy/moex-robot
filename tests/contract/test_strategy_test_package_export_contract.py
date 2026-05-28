from moex_research import contracts
from moex_research.contracts import (
    STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS,
    StrategyTestPackage,
    StrategyTestPackageValidationError,
    validate_strategy_test_package,
    validate_strategy_test_package_values,
)
from moex_research.contracts.strategy_test_package import (
    STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS as DIRECT_REQUIRED_FIELDS,
)
from moex_research.contracts.strategy_test_package import (
    StrategyTestPackage as DirectStrategyTestPackage,
)
from moex_research.contracts.strategy_test_package import (
    StrategyTestPackageValidationError as DirectStrategyTestPackageValidationError,
)
from moex_research.contracts.strategy_test_package import (
    validate_strategy_test_package as direct_validate_strategy_test_package,
)
from moex_research.contracts.strategy_test_package import (
    validate_strategy_test_package_values as direct_validate_strategy_test_package_values,
)


EXPECTED_PACKAGE_EXPORTS = (
    "STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS",
    "StrategyTestPackage",
    "StrategyTestPackageValidationError",
    "validate_strategy_test_package",
    "validate_strategy_test_package_values",
)


def test_strategy_test_package_surface_is_exported_from_contracts_package():
    for export_name in EXPECTED_PACKAGE_EXPORTS:
        assert export_name in contracts.__all__
        assert hasattr(contracts, export_name)


def test_strategy_test_package_exports_match_direct_module_objects():
    assert STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS is DIRECT_REQUIRED_FIELDS
    assert StrategyTestPackage is DirectStrategyTestPackage
    assert StrategyTestPackageValidationError is DirectStrategyTestPackageValidationError
    assert validate_strategy_test_package is direct_validate_strategy_test_package
    assert validate_strategy_test_package_values is direct_validate_strategy_test_package_values


def test_strategy_test_package_export_contract_does_not_add_execution_surface():
    forbidden_export_markers = (
        "runner",
        "metrics",
        "engine",
        "execute",
        "execution",
        "promotion_verdict",
        "server",
        "runtime",
        "live",
    )

    package_exports = tuple(
        export_name
        for export_name in contracts.__all__
        if export_name in EXPECTED_PACKAGE_EXPORTS
    )
    assert package_exports == EXPECTED_PACKAGE_EXPORTS

    for export_name in package_exports:
        normalized = export_name.casefold()
        for marker in forbidden_export_markers:
            assert marker not in normalized
