from pathlib import Path

from moex_research.contracts import StrategyTestPackage, validate_strategy_test_package


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "strategy_testing"
    / "d1_large_move_mean_reversion_package.py"
)


def test_reference_strategy_test_package_fixture_imports():
    from tests.fixtures.strategy_testing.d1_large_move_mean_reversion_package import (
        REFERENCE_STRATEGY_TEST_PACKAGE,
        build_reference_strategy_test_package,
    )

    assert isinstance(REFERENCE_STRATEGY_TEST_PACKAGE, StrategyTestPackage)
    assert isinstance(build_reference_strategy_test_package(), StrategyTestPackage)


def test_reference_strategy_test_package_fixture_validates():
    from tests.fixtures.strategy_testing.d1_large_move_mean_reversion_package import (
        REFERENCE_STRATEGY_TEST_PACKAGE,
    )

    assert validate_strategy_test_package(REFERENCE_STRATEGY_TEST_PACKAGE) is REFERENCE_STRATEGY_TEST_PACKAGE


def test_reference_strategy_test_package_fixture_identity_and_scope():
    from tests.fixtures.strategy_testing.d1_large_move_mean_reversion_package import (
        REFERENCE_DATASET_REF_ID,
        REFERENCE_FEATURE_REF_ID,
        REFERENCE_PRIMARY_LABEL_REF_ID,
        REFERENCE_SECONDARY_LABEL_REF_ID,
        REFERENCE_SIGNAL_REF_ID,
        REFERENCE_STRATEGY_ID,
        REFERENCE_STRATEGY_TEST_ID,
        REFERENCE_STRATEGY_TEST_PACKAGE,
    )

    package = REFERENCE_STRATEGY_TEST_PACKAGE

    assert package.manifest.strategy_test_id == REFERENCE_STRATEGY_TEST_ID
    assert package.manifest.strategy_id == REFERENCE_STRATEGY_ID
    assert package.manifest.test_type == "event_study_research"
    assert package.manifest.instrument_scope == ("Si",)
    assert package.manifest.timeframe_scope == ("D1",)
    assert package.manifest.dataset_refs == (REFERENCE_DATASET_REF_ID,)
    assert package.manifest.feature_refs == (REFERENCE_FEATURE_REF_ID,)
    assert package.manifest.label_refs == (
        REFERENCE_PRIMARY_LABEL_REF_ID,
        REFERENCE_SECONDARY_LABEL_REF_ID,
    )
    assert package.manifest.signal_refs == (REFERENCE_SIGNAL_REF_ID,)
    assert package.registry_entry_ref_or_none == (
        "registry_entry.strategy_test.d1_large_move_mr.reference_fixture.v1"
    )
    assert package.promotion_verdict_ref_or_none is None


def test_reference_strategy_test_package_fixture_preserves_label_class_separation():
    from tests.fixtures.strategy_testing.d1_large_move_mean_reversion_package import (
        REFERENCE_STRATEGY_TEST_PACKAGE,
    )

    label_classes = tuple(label_ref.label_class for label_ref in REFERENCE_STRATEGY_TEST_PACKAGE.label_refs)

    assert label_classes == ("primary_research", "secondary_execution_compatible")


def test_reference_strategy_test_package_fixture_dependencies_are_declared():
    from tests.fixtures.strategy_testing.d1_large_move_mean_reversion_package import (
        REFERENCE_DATASET_REF_ID,
        REFERENCE_FEATURE_REF_ID,
        REFERENCE_STRATEGY_TEST_PACKAGE,
    )

    package = REFERENCE_STRATEGY_TEST_PACKAGE
    feature_ref = package.feature_refs[0]
    signal_ref = package.signal_refs[0]

    assert feature_ref.input_dataset_refs == (REFERENCE_DATASET_REF_ID,)
    assert signal_ref.input_feature_refs == (REFERENCE_FEATURE_REF_ID,)


def test_reference_strategy_test_package_fixture_has_no_forbidden_execution_terms():
    source = FIXTURE_PATH.read_text(encoding="utf-8").casefold()
    forbidden_terms = (
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "open(",
        "strategy implementation",
        "research execution",
        "backtest engine",
    )

    for term in forbidden_terms:
        assert term not in source
