from pathlib import Path

import pytest

from moex_research.contracts.strategy_test_manifest import (
    ALLOWED_TEST_TYPES,
    STRATEGY_TEST_MANIFEST_REQUIRED_FIELDS,
    StrategyTestManifest,
    StrategyTestManifestValidationError,
    StrategyTestReference,
    validate_strategy_test_manifest,
    validate_strategy_test_manifest_values,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
FLAG_FIELD = "runtime_live_allowed"


VALID_MANIFEST_VALUES = {
    "strategy_test_id": "test.d1_tsmom.fragility.v1",
    "strategy_id": "d1_tsmom",
    "strategy_version": "0.1.0",
    "test_type": "fragility_robustness_package",
    "instrument_scope": ("Si", "USDRUBF"),
    "timeframe_scope": ("D1",),
    "dataset_refs": ("dataset.futures_derived_d1.v1",),
    "feature_refs": ("feature.d1_tsmom_signal.v1",),
    "label_refs": ("label.primary_1d_close_to_close.v1",),
    "signal_refs": ("signal.d1_tsmom.v1",),
    "backtest_semantics_ref": "contracts/strategy_testing/canonical_backtest_semantics.v1.md",
    "cost_slippage_ref": "contracts/strategy_testing/cost_slippage_contract.v1.md",
    "artifact_contract_ref": "contracts/strategy_testing/result_artifact_contract.v1.md",
    FLAG_FIELD: False,
}


def test_strategy_test_manifest_required_fields_are_exact():
    assert STRATEGY_TEST_MANIFEST_REQUIRED_FIELDS == (
        "strategy_test_id",
        "strategy_id",
        "strategy_version",
        "test_type",
        "instrument_scope",
        "timeframe_scope",
        "dataset_refs",
        "feature_refs",
        "label_refs",
        "signal_refs",
        "backtest_semantics_ref",
        "cost_slippage_ref",
        "artifact_contract_ref",
        FLAG_FIELD,
    )


def test_strategy_test_manifest_accepts_valid_values():
    manifest = StrategyTestManifest(**VALID_MANIFEST_VALUES)

    assert validate_strategy_test_manifest(manifest) is manifest
    assert manifest.strategy_test_id == "test.d1_tsmom.fragility.v1"
    assert manifest.test_type == "fragility_robustness_package"
    assert manifest.instrument_scope == ("Si", "USDRUBF")
    assert getattr(manifest, FLAG_FIELD) is False


def test_strategy_test_manifest_defaults_runtime_live_allowed_to_false():
    values = dict(VALID_MANIFEST_VALUES)
    values.pop(FLAG_FIELD)

    manifest = StrategyTestManifest(**values)

    assert getattr(manifest, FLAG_FIELD) is False


@pytest.mark.parametrize("test_type", sorted(ALLOWED_TEST_TYPES))
def test_strategy_test_manifest_allows_exact_test_type_set(test_type):
    values = dict(VALID_MANIFEST_VALUES)
    values["test_type"] = test_type

    manifest = StrategyTestManifest(**values)

    assert manifest.test_type == test_type


@pytest.mark.parametrize(
    "field",
    [
        "strategy_test_id",
        "strategy_id",
        "strategy_version",
        "test_type",
        "instrument_scope",
        "timeframe_scope",
        "dataset_refs",
        "feature_refs",
        "label_refs",
        "signal_refs",
        "backtest_semantics_ref",
        "cost_slippage_ref",
        "artifact_contract_ref",
    ],
)
def test_missing_required_field_fails_closed(field):
    values = dict(VALID_MANIFEST_VALUES)
    values.pop(field)

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


@pytest.mark.parametrize("field", ["strategy_test_id", "strategy_id"])
def test_empty_identifier_fields_fail_closed(field):
    values = dict(VALID_MANIFEST_VALUES)
    values[field] = ""

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


def test_unsupported_test_type_fails_closed():
    values = dict(VALID_MANIFEST_VALUES)
    values["test_type"] = "production"

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


@pytest.mark.parametrize("field", ["instrument_scope", "timeframe_scope"])
def test_empty_scope_fails_closed(field):
    values = dict(VALID_MANIFEST_VALUES)
    values[field] = ()

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


def test_empty_dataset_refs_fails_closed():
    values = dict(VALID_MANIFEST_VALUES)
    values["dataset_refs"] = ()

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


@pytest.mark.parametrize("field", ["feature_refs", "label_refs", "signal_refs"])
def test_empty_explicit_ref_lists_fail_closed(field):
    values = dict(VALID_MANIFEST_VALUES)
    values[field] = ()

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


@pytest.mark.parametrize(
    "field",
    ["backtest_semantics_ref", "cost_slippage_ref", "artifact_contract_ref"],
)
def test_missing_single_ref_fields_fail_closed(field):
    values = dict(VALID_MANIFEST_VALUES)
    values.pop(field)

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


def test_runtime_live_allowed_true_fails_closed():
    values = dict(VALID_MANIFEST_VALUES)
    values[FLAG_FIELD] = True

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestManifest(**values)


def test_ref_objects_are_allowed_but_must_be_non_empty():
    values = dict(VALID_MANIFEST_VALUES)
    values["dataset_refs"] = (StrategyTestReference("dataset.explicit.v1"),)
    values["backtest_semantics_ref"] = StrategyTestReference("semantics.explicit.v1")

    manifest = StrategyTestManifest(**values)

    assert isinstance(manifest.dataset_refs[0], StrategyTestReference)
    assert isinstance(manifest.backtest_semantics_ref, StrategyTestReference)

    with pytest.raises(StrategyTestManifestValidationError):
        StrategyTestReference("")


def test_unknown_manifest_field_fails_closed():
    values = dict(VALID_MANIFEST_VALUES)
    values["unexpected_field"] = "not_allowed"

    with pytest.raises(StrategyTestManifestValidationError):
        validate_strategy_test_manifest_values(values)


def test_strategy_test_manifest_schema_has_no_execution_or_discovery_terms():
    source = (REPO_ROOT / "src" / "moex_research" / "contracts" / "strategy_test_manifest.py").read_text(
        encoding="utf-8"
    ).casefold()

    forbidden_terms = (
        "subprocess",
        "requests",
        "urllib",
        "glob(",
        "pathlib",
        "os.",
        "open(",
        "backtest.run",
        "registryentry(",
        "promotion_verdict(",
        "old research",
    )
    for term in forbidden_terms:
        assert term not in source
