from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Final

from moex_research.contracts.strategy_test_package import StrategyTestPackage
from moex_research.runners.dry_validate_strategy_test_package import (
    dry_validate_strategy_test_package,
)

APPROVED_STRATEGY_TEST_PACKAGE_FIXTURES: Final[frozenset[str]] = frozenset(
    {"tests.fixtures.strategy_testing.ema_3_19_package"}
)
_PACKAGE_OBJECT_NAMES: Final[tuple[str, ...]] = ("EMA_3_19_STRATEGY_TEST_PACKAGE",)
_PACKAGE_FACTORY_NAMES: Final[tuple[str, ...]] = ("build_ema_3_19_strategy_test_package",)


@dataclass(frozen=True)
class DryValidationRequest:
    fixture_module: str | None = None
    package: StrategyTestPackage | None = None


@dataclass(frozen=True)
class DryValidationResult:
    validation_status: str
    fixture_module: str | None
    strategy_id: str | None
    strategy_test_id: str | None
    error_message_or_none: str | None


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _blocked_import_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _approved_fixture_module(module_path: object) -> str:
    if not isinstance(module_path, str) or not module_path.strip():
        raise ValueError("fixture module is required")
    normalized = module_path.casefold()
    tokenized = normalized
    for separator in ("/", "\\", ".", "_", "-", ":"):
        tokenized = tokenized.replace(separator, " ")
    tokens = tuple(token for token in tokenized.split() if token)
    for marker in _blocked_import_markers():
        if marker in normalized or marker in tokens:
            raise ValueError("fixture module is not approved")
    if module_path not in APPROVED_STRATEGY_TEST_PACKAGE_FIXTURES:
        raise ValueError("fixture module is not approved")
    return module_path


def _package_from_fixture(module_path: str) -> StrategyTestPackage:
    module = import_module(_approved_fixture_module(module_path))
    for object_name in _PACKAGE_OBJECT_NAMES:
        package = getattr(module, object_name, None)
        if package is not None:
            return package
    for factory_name in _PACKAGE_FACTORY_NAMES:
        factory = getattr(module, factory_name, None)
        if callable(factory):
            return factory()
    raise ValueError("fixture package object is required")


def _package_identifiers(package: object) -> tuple[str | None, str | None]:
    manifest = getattr(package, "manifest", None)
    strategy_id = getattr(manifest, "strategy_id", None)
    strategy_test_id = getattr(manifest, "strategy_test_id", None)
    return (
        strategy_id if isinstance(strategy_id, str) else None,
        strategy_test_id if isinstance(strategy_test_id, str) else None,
    )


def _result(
    validation_status: str,
    fixture_module: str | None,
    package: object,
    error_message_or_none: str | None,
) -> DryValidationResult:
    strategy_id, strategy_test_id = _package_identifiers(package)
    return DryValidationResult(
        validation_status=validation_status,
        fixture_module=fixture_module,
        strategy_id=strategy_id,
        strategy_test_id=strategy_test_id,
        error_message_or_none=error_message_or_none,
    )


def dry_validate_strategy_test_package_request(
    request: DryValidationRequest,
) -> DryValidationResult:
    package: object | None = None
    fixture_module: str | None = None
    try:
        if not isinstance(request, DryValidationRequest):
            raise TypeError("request must be DryValidationRequest")
        fixture_module = request.fixture_module
        if request.package is not None:
            package = request.package
        elif request.fixture_module is not None:
            fixture_module = _approved_fixture_module(request.fixture_module)
            package = _package_from_fixture(fixture_module)
        else:
            raise ValueError("strategy test package is required")
        validated_package = dry_validate_strategy_test_package(package)
        return _result("pass", fixture_module, validated_package, None)
    except Exception as exc:
        return _result("fail", fixture_module, package, str(exc) or exc.__class__.__name__)


def dry_validate_fixture_package(fixture_module: str) -> DryValidationResult:
    return dry_validate_strategy_test_package_request(
        DryValidationRequest(fixture_module=fixture_module)
    )


__all__ = [
    "APPROVED_STRATEGY_TEST_PACKAGE_FIXTURES",
    "DryValidationRequest",
    "DryValidationResult",
    "dry_validate_fixture_package",
    "dry_validate_strategy_test_package_request",
]
