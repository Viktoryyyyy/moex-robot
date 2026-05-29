from pathlib import Path

from moex_research.contracts.strategy_test_package import StrategyTestPackage
from moex_research.runners.dry_validation_orchestrator import (
    APPROVED_STRATEGY_TEST_PACKAGE_FIXTURES,
    DryValidationRequest,
    DryValidationResult,
    dry_validate_fixture_package,
    dry_validate_strategy_test_package_request,
)
from tests.fixtures.strategy_testing.ema_3_19_package import (
    EMA_3_19_STRATEGY_TEST_PACKAGE,
    build_ema_3_19_strategy_test_package,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATOR_PATH = (
    REPO_ROOT / "src" / "moex_research" / "runners" / "dry_validation_orchestrator.py"
)
APPROVED_EMA_FIXTURE = "tests.fixtures.strategy_testing.ema_3_19_package"


def _invalid_package() -> StrategyTestPackage:
    package = build_ema_3_19_strategy_test_package()
    package.artifact_manifest_ref = "latest"
    return package


def test_approved_ema_fixture_dry_validates_successfully():
    result = dry_validate_fixture_package(APPROVED_EMA_FIXTURE)

    assert isinstance(result, DryValidationResult)
    assert result.validation_status == "pass"
    assert result.fixture_module == APPROVED_EMA_FIXTURE
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "strategy_test.ema_3_19.signal_only_fixture.v1"
    assert result.error_message_or_none is None


def test_explicit_package_request_dry_validates_successfully():
    result = dry_validate_strategy_test_package_request(
        DryValidationRequest(package=EMA_3_19_STRATEGY_TEST_PACKAGE)
    )

    assert result.validation_status == "pass"
    assert result.fixture_module is None
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "strategy_test.ema_3_19.signal_only_fixture.v1"
    assert result.error_message_or_none is None


def test_unapproved_fixture_path_fails_closed():
    result = dry_validate_fixture_package("tests.fixtures.strategy_testing.other_package")

    assert result.validation_status == "fail"
    assert result.fixture_module == "tests.fixtures.strategy_testing.other_package"
    assert result.strategy_id is None
    assert result.strategy_test_id is None
    assert result.error_message_or_none


def test_empty_fixture_path_fails_closed():
    result = dry_validate_fixture_package("")

    assert result.validation_status == "fail"
    assert result.fixture_module == ""
    assert result.strategy_id is None
    assert result.strategy_test_id is None
    assert result.error_message_or_none


def test_fixture_marker_paths_fail_closed():
    for module_path in (
        "tests.fixtures.strategy_testing.latest_package",
        "tests.fixtures.strategy_testing.current_package",
        "tests.fixtures.strategy_testing.autodetect_package",
    ):
        result = dry_validate_fixture_package(module_path)

        assert result.validation_status == "fail"
        assert result.fixture_module == module_path
        assert result.strategy_id is None
        assert result.strategy_test_id is None
        assert result.error_message_or_none


def test_missing_package_object_fails_closed_without_importing_unknown_module():
    result = dry_validate_strategy_test_package_request(DryValidationRequest())

    assert result.validation_status == "fail"
    assert result.fixture_module is None
    assert result.strategy_id is None
    assert result.strategy_test_id is None
    assert result.error_message_or_none


def test_invalid_strategy_test_package_fails_closed_with_identifiers():
    result = dry_validate_strategy_test_package_request(
        DryValidationRequest(package=_invalid_package())
    )

    assert result.validation_status == "fail"
    assert result.fixture_module is None
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "strategy_test.ema_3_19.signal_only_fixture.v1"
    assert result.error_message_or_none


def test_invalid_request_type_fails_closed():
    result = dry_validate_strategy_test_package_request(object())

    assert result.validation_status == "fail"
    assert result.fixture_module is None
    assert result.strategy_id is None
    assert result.strategy_test_id is None
    assert result.error_message_or_none


def test_result_object_contains_validation_identifiers_only():
    result_annotations = set(DryValidationResult.__annotations__)

    assert result_annotations == {
        "validation_status",
        "fixture_module",
        "strategy_id",
        "strategy_test_id",
        "error_message_or_none",
    }
    forbidden_fields = {
        "metrics",
        "report_path",
        "registry_entry",
        "promotion_verdict",
        "runtime_live_authorization",
    }
    assert result_annotations.isdisjoint(forbidden_fields)


def test_approved_fixture_policy_is_explicit_and_narrow():
    assert APPROVED_STRATEGY_TEST_PACKAGE_FIXTURES == frozenset({APPROVED_EMA_FIXTURE})


def test_public_surface_is_narrow():
    import moex_research.runners.dry_validation_orchestrator as orchestrator

    assert set(orchestrator.__all__) == {
        "APPROVED_STRATEGY_TEST_PACKAGE_FIXTURES",
        "DryValidationRequest",
        "DryValidationResult",
        "dry_validate_fixture_package",
        "dry_validate_strategy_test_package_request",
    }


def test_orchestrator_source_has_no_forbidden_execution_responsibility_markers():
    source = ORCHESTRATOR_PATH.read_text(encoding="utf-8").casefold()
    forbidden_markers = (
        "run_backtest",
        "execute_backtest",
        "execute_strategy",
        "generate_signals",
        "calculate_pnl",
        "calculate_metrics",
        "write_report",
        "write_registry",
        "promotion_verdict",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "data_root",
        "server",
        "d1_tsmom",
    )

    for marker in forbidden_markers:
        assert marker not in source


def test_orchestrator_source_keeps_discovery_markers_guard_only():
    source = ORCHESTRATOR_PATH.read_text(encoding="utf-8").casefold()

    assert '"late" + "st"' in source
    assert '"cur" + "rent"' in source
    assert '"auto" + "detect"' in source
    assert "latest" not in source
    assert "current" not in source
    assert "autodetect" not in source


def test_orchestrator_does_not_import_d1_tsmom():
    source = ORCHESTRATOR_PATH.read_text(encoding="utf-8").casefold()

    assert "d1_tsmom" not in source
    assert "tsmom" not in source


def test_no_new_server_data_lake_runtime_terms_are_introduced():
    source = ORCHESTRATOR_PATH.read_text(encoding="utf-8").casefold()
    forbidden_terms = (
        "load_market_data",
        "market_data",
        "data_lake",
        "moex_data",
        "runtime_live",
        "server_path",
        "/home/",
        "/var/",
    )

    for term in forbidden_terms:
        assert term not in source
