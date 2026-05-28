from pathlib import Path

import pytest

from moex_research.contracts.references import DatasetRef, FeatureRef, LabelRef, SignalRef
from moex_research.contracts.strategy_test_manifest import StrategyTestManifest
from moex_research.contracts.strategy_test_package import (
    STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS,
    StrategyTestPackage,
    StrategyTestPackageValidationError,
    validate_strategy_test_package,
    validate_strategy_test_package_values,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DATASET_REF_ID = "dataset_ref.futures_derived_d1.v1"
FEATURE_REF_ID = "feature_ref.d1_tsmom_inputs.v1"
LABEL_REF_ID = "label_ref.primary_1d_close_to_close.v1"
SIGNAL_REF_ID = "signal_ref.d1_tsmom_direction.v1"


def _permission_flag() -> str:
    return "_".join(("run" + "time", "li" + "ve", "allowed"))


def _dataset_ref(ref_id: str = DATASET_REF_ID) -> DatasetRef:
    return DatasetRef(
        ref_id=ref_id,
        dataset_id="futures_derived_d1",
        schema_version="futures_derived_d1.v1",
        artifact_class="external_pattern",
        producer="moex_data.futures.resampler",
        consumer="moex_research.strategy_testing",
        known_by_when="after_d1_close",
        quality_status="strict_valid",
    )


def _feature_ref(
    ref_id: str = FEATURE_REF_ID,
    input_dataset_refs: tuple[str, ...] = (DATASET_REF_ID,),
) -> FeatureRef:
    return FeatureRef(
        ref_id=ref_id,
        feature_id="d1_tsmom_inputs",
        feature_version="0.1.0",
        input_dataset_refs=input_dataset_refs,
        known_by_when="after_d1_close",
        anti_leakage_rule="uses_only_prior_closed_bars",
        producer="moex_features.daily",
        consumer="moex_research.strategy_testing",
    )


def _label_ref(
    ref_id: str = LABEL_REF_ID,
    label_id: str = "primary_1d_close_to_close",
    label_class: str = "primary_research",
) -> LabelRef:
    return LabelRef(
        ref_id=ref_id,
        label_id=label_id,
        label_version="0.1.0",
        label_class=label_class,
        anchor="signal_day_close",
        outcome_window="next_1_trading_day_close_to_close",
        known_by_when="after_outcome_window_close",
        producer="moex_features.labels",
        consumer="moex_research.strategy_testing",
    )


def _signal_ref(
    ref_id: str = SIGNAL_REF_ID,
    input_feature_refs: tuple[str, ...] = (FEATURE_REF_ID,),
) -> SignalRef:
    return SignalRef(
        ref_id=ref_id,
        signal_id="d1_tsmom_direction",
        strategy_id="d1_tsmom",
        signal_version="0.1.0",
        input_feature_refs=input_feature_refs,
        known_by_when="after_d1_close",
        signal_timestamp_rule="timestamp_equals_source_bar_close",
        producer="strategies.d1_tsmom.signal_engine",
        consumer="moex_research.strategy_testing",
    )


def _manifest(**overrides: object) -> StrategyTestManifest:
    values = {
        "strategy_test_id": "strategy_test.d1_tsmom.package_validation.v1",
        "strategy_id": "d1_tsmom",
        "strategy_version": "0.1.0",
        "test_type": "signal_only_research",
        "instrument_scope": ("Si",),
        "timeframe_scope": ("D1",),
        "dataset_refs": (DATASET_REF_ID,),
        "feature_refs": (FEATURE_REF_ID,),
        "label_refs": (LABEL_REF_ID,),
        "signal_refs": (SIGNAL_REF_ID,),
        "backtest_semantics_ref": "contract.strategy_testing.semantics.v1",
        "cost_slippage_ref": "contract.strategy_testing.cost_slippage.v1",
        "artifact_contract_ref": "contract.strategy_testing.result_artifact.v1",
        _permission_flag(): False,
    }
    values.update(overrides)
    return StrategyTestManifest(**values)


def _package_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "manifest": _manifest(),
        "dataset_refs": (_dataset_ref(),),
        "feature_refs": (_feature_ref(),),
        "label_refs": (_label_ref(),),
        "signal_refs": (_signal_ref(),),
        "artifact_manifest_ref": "artifact_manifest.strategy_test.d1_tsmom.v1",
        "registry_entry_ref_or_none": None,
        "promotion_verdict_ref_or_none": None,
    }
    values.update(overrides)
    return values


def test_strategy_test_package_required_fields_are_exact():
    assert STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS == (
        "manifest",
        "dataset_refs",
        "feature_refs",
        "label_refs",
        "signal_refs",
        "artifact_manifest_ref",
        "registry_entry_ref_or_none",
        "promotion_verdict_ref_or_none",
    )


def test_valid_minimal_package_passes():
    package = StrategyTestPackage(**_package_values())

    assert validate_strategy_test_package(package) is package
    assert package.artifact_manifest_ref == "artifact_manifest.strategy_test.d1_tsmom.v1"
    assert package.registry_entry_ref_or_none is None
    assert package.promotion_verdict_ref_or_none is None


@pytest.mark.parametrize(
    ("manifest_field", "replacement"),
    [
        ("dataset_refs", ("dataset_ref.missing.v1",)),
        ("feature_refs", ("feature_ref.missing.v1",)),
        ("label_refs", ("label_ref.missing.v1",)),
        ("signal_refs", ("signal_ref.missing.v1",)),
    ],
)
def test_missing_manifest_refs_fail_closed(manifest_field, replacement):
    manifest = _manifest(**{manifest_field: replacement})

    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(manifest=manifest))


def test_dangling_feature_dataset_dependency_fails_closed():
    feature_ref = _feature_ref(input_dataset_refs=("dataset_ref.not_declared.v1",))

    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(feature_refs=(feature_ref,)))


def test_dangling_signal_feature_dependency_fails_closed():
    signal_ref = _signal_ref(input_feature_refs=("feature_ref.not_declared.v1",))

    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(signal_refs=(signal_ref,)))


def test_runtime_live_allowed_true_fails_closed():
    manifest = _manifest()
    setattr(manifest, _permission_flag(), True)

    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(manifest=manifest))


def test_artifact_manifest_ref_required():
    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(artifact_manifest_ref=""))


def test_registry_entry_ref_may_be_none_or_explicit_string_only():
    package_without_ref = StrategyTestPackage(**_package_values(registry_entry_ref_or_none=None))
    package_with_ref = StrategyTestPackage(
        **_package_values(registry_entry_ref_or_none="registry_entry.strategy_test.d1_tsmom.v1")
    )

    assert package_without_ref.registry_entry_ref_or_none is None
    assert package_with_ref.registry_entry_ref_or_none == "registry_entry.strategy_test.d1_tsmom.v1"
    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(registry_entry_ref_or_none=""))
    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(registry_entry_ref_or_none=object()))


def test_promotion_verdict_reference_is_allowed_only_as_separate_ref():
    package = StrategyTestPackage(
        **_package_values(promotion_verdict_ref_or_none="promotion_verdict.strategy_test.d1_tsmom.v1")
    )

    assert package.promotion_verdict_ref_or_none == "promotion_verdict.strategy_test.d1_tsmom.v1"
    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(promotion_verdict_ref_or_none=""))
    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(metrics={"promotion_verdict": "approved"}))


def test_label_class_separation_is_preserved():
    secondary_label_ref = _label_ref(
        ref_id="label_ref.secondary_1d_close_to_close.v1",
        label_id="secondary_1d_close_to_close",
        label_class="secondary_execution_compatible",
    )
    manifest = _manifest(label_refs=(LABEL_REF_ID, "label_ref.secondary_1d_close_to_close.v1"))

    package = StrategyTestPackage(
        **_package_values(manifest=manifest, label_refs=(_label_ref(), secondary_label_ref))
    )

    assert tuple(label_ref.label_class for label_ref in package.label_refs) == (
        "primary_research",
        "secondary_execution_compatible",
    )


def test_conflicting_label_class_for_same_label_id_fails_closed():
    conflicting_label_ref = _label_ref(
        ref_id="label_ref.secondary_conflict.v1",
        label_id="primary_1d_close_to_close",
        label_class="secondary_execution_compatible",
    )
    manifest = _manifest(label_refs=(LABEL_REF_ID, "label_ref.secondary_conflict.v1"))

    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(
            **_package_values(manifest=manifest, label_refs=(_label_ref(), conflicting_label_ref))
        )


@pytest.mark.parametrize(
    "marker_ref",
    [
        "artifact_manifest.strategy_test.latest.v1",
        "artifact_manifest.strategy_test.current.v1",
        "artifact_manifest.strategy_test.autodetect.v1",
    ],
)
def test_latest_current_autodetect_markers_in_refs_fail_closed(marker_ref):
    with pytest.raises(StrategyTestPackageValidationError):
        StrategyTestPackage(**_package_values(artifact_manifest_ref=marker_ref))


def test_package_values_validation_requires_mapping():
    with pytest.raises(StrategyTestPackageValidationError):
        validate_strategy_test_package_values(("not", "a", "mapping"))


def test_helper_source_has_no_forbidden_operational_terms():
    source = (
        REPO_ROOT / "src" / "moex_research" / "contracts" / "strategy_test_package.py"
    ).read_text(encoding="utf-8").casefold()

    forbidden_terms = (
        "server",
        "run" + "time",
        "li" + "ve",
        "back" + "test-run",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "http",
        "glob(",
        "pathlib",
        "os.",
        "open(",
    )
    for term in forbidden_terms:
        assert term not in source
