from pathlib import Path

from moex_research.contracts import StrategyTestPackage, validate_strategy_test_package


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "strategy_testing" / "ema_3_19_package.py"


def test_ema_3_19_strategy_test_package_fixture_imports():
    from tests.fixtures.strategy_testing.ema_3_19_package import (
        EMA_3_19_STRATEGY_TEST_PACKAGE,
        build_ema_3_19_strategy_test_package,
    )

    assert isinstance(EMA_3_19_STRATEGY_TEST_PACKAGE, StrategyTestPackage)
    assert isinstance(build_ema_3_19_strategy_test_package(), StrategyTestPackage)


def test_ema_3_19_strategy_test_package_fixture_validates():
    from tests.fixtures.strategy_testing.ema_3_19_package import EMA_3_19_STRATEGY_TEST_PACKAGE

    assert validate_strategy_test_package(EMA_3_19_STRATEGY_TEST_PACKAGE) is EMA_3_19_STRATEGY_TEST_PACKAGE


def test_ema_3_19_strategy_test_package_fixture_identity_and_scope():
    from tests.fixtures.strategy_testing.ema_3_19_package import (
        EMA_DATASET_REF_ID,
        EMA_FEATURE_REF_ID,
        EMA_PRIMARY_LABEL_REF_ID,
        EMA_SECONDARY_LABEL_REF_ID,
        EMA_SIGNAL_REF_ID,
        EMA_STRATEGY_ID,
        EMA_STRATEGY_TEST_ID,
        EMA_3_19_STRATEGY_TEST_PACKAGE,
    )

    package = EMA_3_19_STRATEGY_TEST_PACKAGE

    assert package.manifest.strategy_test_id == EMA_STRATEGY_TEST_ID
    assert package.manifest.strategy_id == EMA_STRATEGY_ID
    assert package.manifest.strategy_id == "ema_3_19"
    assert package.manifest.strategy_version == "0.1.0"
    assert package.manifest.test_type == "signal_only_research"
    assert package.manifest.instrument_scope == ("Si",)
    assert package.manifest.timeframe_scope == ("15m",)
    assert package.manifest.dataset_refs == (EMA_DATASET_REF_ID,)
    assert package.manifest.feature_refs == (EMA_FEATURE_REF_ID,)
    assert package.manifest.label_refs == (EMA_PRIMARY_LABEL_REF_ID, EMA_SECONDARY_LABEL_REF_ID)
    assert package.manifest.signal_refs == (EMA_SIGNAL_REF_ID,)
    assert package.registry_entry_ref_or_none is None
    assert package.promotion_verdict_ref_or_none is None


def test_ema_3_19_strategy_test_package_fixture_blocks_runtime_permission():
    from tests.fixtures.strategy_testing.ema_3_19_package import EMA_3_19_STRATEGY_TEST_PACKAGE

    permission_value = getattr(EMA_3_19_STRATEGY_TEST_PACKAGE.manifest, "runtime_live_allowed")

    assert permission_value is False


def test_ema_3_19_strategy_test_package_fixture_has_explicit_refs():
    from tests.fixtures.strategy_testing.ema_3_19_package import EMA_3_19_STRATEGY_TEST_PACKAGE

    package = EMA_3_19_STRATEGY_TEST_PACKAGE

    assert package.artifact_manifest_ref == "artifact_manifest.strategy_test.ema_3_19.signal_only_fixture.v1"
    assert package.dataset_refs
    assert package.feature_refs
    assert package.label_refs
    assert package.signal_refs
    assert package.manifest.backtest_semantics_ref
    assert package.manifest.cost_slippage_ref
    assert package.manifest.artifact_contract_ref


def test_ema_3_19_strategy_test_package_fixture_dependencies_are_declared():
    from tests.fixtures.strategy_testing.ema_3_19_package import (
        EMA_DATASET_REF_ID,
        EMA_FEATURE_REF_ID,
        EMA_3_19_STRATEGY_TEST_PACKAGE,
    )

    package = EMA_3_19_STRATEGY_TEST_PACKAGE
    feature_ref = package.feature_refs[0]
    signal_ref = package.signal_refs[0]

    assert feature_ref.input_dataset_refs == (EMA_DATASET_REF_ID,)
    assert feature_ref.known_by_when
    assert feature_ref.anti_leakage_rule
    assert signal_ref.input_feature_refs == (EMA_FEATURE_REF_ID,)
    assert signal_ref.known_by_when
    assert signal_ref.signal_timestamp_rule


def test_ema_3_19_strategy_test_package_fixture_preserves_label_class_separation():
    from tests.fixtures.strategy_testing.ema_3_19_package import EMA_3_19_STRATEGY_TEST_PACKAGE

    label_classes = tuple(label_ref.label_class for label_ref in EMA_3_19_STRATEGY_TEST_PACKAGE.label_refs)

    assert label_classes == ("primary_research", "secondary_execution_compatible")


def test_ema_3_19_strategy_test_package_fixture_has_no_ref_marker_drift():
    from tests.fixtures.strategy_testing.ema_3_19_package import EMA_3_19_STRATEGY_TEST_PACKAGE

    package = EMA_3_19_STRATEGY_TEST_PACKAGE
    values = [
        package.artifact_manifest_ref,
        *package.manifest.dataset_refs,
        *package.manifest.feature_refs,
        *package.manifest.label_refs,
        *package.manifest.signal_refs,
        package.manifest.backtest_semantics_ref,
        package.manifest.cost_slippage_ref,
        package.manifest.artifact_contract_ref,
    ]

    for value in values:
        normalized = str(value).casefold()
        tokens = normalized.replace(".", " ").replace("_", " ").replace("-", " ").split()
        assert "latest" not in tokens
        assert "current" not in tokens
        assert "autodetect" not in tokens


def test_ema_3_19_strategy_test_package_fixture_has_no_d1_tsmom_references():
    source = FIXTURE_PATH.read_text(encoding="utf-8").casefold()

    assert "d1_tsmom" not in source
    assert "tsmom" not in source


def test_ema_3_19_strategy_test_package_fixture_source_has_no_real_strategy_terms():
    source = FIXTURE_PATH.read_text(encoding="utf-8").casefold()
    forbidden_terms = (
        "generate_signals",
        "load_data",
        "read_text",
        "write_text",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "open(",
        "/home/",
        "/var/",
        "registry_write",
        "promotion_verdict(",
        "to_backtest_inputs",
        "to_live_intents",
    )

    for term in forbidden_terms:
        assert term not in source
