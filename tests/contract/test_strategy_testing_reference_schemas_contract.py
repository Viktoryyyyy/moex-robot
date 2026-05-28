from pathlib import Path

import pytest

from moex_research.contracts.references import (
    ALLOWED_ARTIFACT_CLASSES,
    ALLOWED_LABEL_CLASSES,
    DATASET_REF_REQUIRED_FIELDS,
    FEATURE_REF_REQUIRED_FIELDS,
    LABEL_REF_REQUIRED_FIELDS,
    SIGNAL_REF_REQUIRED_FIELDS,
    ArtifactClass,
    DatasetRef,
    FeatureRef,
    LabelClass,
    LabelRef,
    ReferenceValidationError,
    SignalRef,
    validate_dataset_ref,
    validate_dataset_ref_values,
    validate_feature_ref,
    validate_label_ref,
    validate_signal_ref,
)


REPO_ROOT = Path(__file__).resolve().parents[2]

VALID_DATASET_REF_VALUES = {
    "ref_id": "dataset_ref.futures_derived_d1.v1",
    "dataset_id": "futures_derived_d1",
    "schema_version": "futures_derived_d1.v1",
    "artifact_class": "external_pattern",
    "producer": "moex_data.futures.resampler",
    "consumer": "moex_research.strategy_testing",
    "known_by_when": "after_d1_close",
    "quality_status": "strict_valid",
}

VALID_FEATURE_REF_VALUES = {
    "ref_id": "feature_ref.d1_tsmom.v1",
    "feature_id": "d1_tsmom_signal_inputs",
    "feature_version": "0.1.0",
    "input_dataset_refs": ("dataset_ref.futures_derived_d1.v1",),
    "known_by_when": "after_d1_close",
    "anti_leakage_rule": "uses_only_prior_closed_bars",
    "producer": "moex_features.daily",
    "consumer": "moex_research.strategy_testing",
}

VALID_LABEL_REF_VALUES = {
    "ref_id": "label_ref.primary_1d_close_to_close.v1",
    "label_id": "primary_1d_close_to_close",
    "label_version": "0.1.0",
    "label_class": "primary_research",
    "anchor": "signal_day_close",
    "outcome_window": "next_1_trading_day_close_to_close",
    "known_by_when": "after_outcome_window_close",
    "producer": "moex_features.labels",
    "consumer": "moex_research.strategy_testing",
}

VALID_SIGNAL_REF_VALUES = {
    "ref_id": "signal_ref.d1_tsmom.v1",
    "signal_id": "d1_tsmom_direction",
    "strategy_id": "d1_tsmom",
    "signal_version": "0.1.0",
    "input_feature_refs": ("feature_ref.d1_tsmom.v1",),
    "known_by_when": "after_d1_close",
    "signal_timestamp_rule": "timestamp_equals_source_bar_close",
    "producer": "strategies.d1_tsmom.signal_engine",
    "consumer": "moex_research.strategy_testing",
}


def test_reference_required_fields_are_exact():
    assert DATASET_REF_REQUIRED_FIELDS == (
        "ref_id",
        "dataset_id",
        "schema_version",
        "artifact_class",
        "producer",
        "consumer",
        "known_by_when",
        "quality_status",
    )
    assert FEATURE_REF_REQUIRED_FIELDS == (
        "ref_id",
        "feature_id",
        "feature_version",
        "input_dataset_refs",
        "known_by_when",
        "anti_leakage_rule",
        "producer",
        "consumer",
    )
    assert LABEL_REF_REQUIRED_FIELDS == (
        "ref_id",
        "label_id",
        "label_version",
        "label_class",
        "anchor",
        "outcome_window",
        "known_by_when",
        "producer",
        "consumer",
    )
    assert SIGNAL_REF_REQUIRED_FIELDS == (
        "ref_id",
        "signal_id",
        "strategy_id",
        "signal_version",
        "input_feature_refs",
        "known_by_when",
        "signal_timestamp_rule",
        "producer",
        "consumer",
    )


def test_valid_refs_pass():
    dataset_ref = DatasetRef(**VALID_DATASET_REF_VALUES)
    feature_ref = FeatureRef(**VALID_FEATURE_REF_VALUES)
    label_ref = LabelRef(**VALID_LABEL_REF_VALUES)
    signal_ref = SignalRef(**VALID_SIGNAL_REF_VALUES)

    assert validate_dataset_ref(dataset_ref) is dataset_ref
    assert validate_feature_ref(feature_ref) is feature_ref
    assert validate_label_ref(label_ref) is label_ref
    assert validate_signal_ref(signal_ref) is signal_ref
    assert dataset_ref.artifact_class == "external_pattern"
    assert feature_ref.input_dataset_refs == ("dataset_ref.futures_derived_d1.v1",)
    assert label_ref.label_class == "primary_research"
    assert signal_ref.signal_timestamp_rule == "timestamp_equals_source_bar_close"


@pytest.mark.parametrize("artifact_class", sorted(ALLOWED_ARTIFACT_CLASSES))
def test_dataset_ref_allows_explicit_artifact_class_set(artifact_class):
    values = dict(VALID_DATASET_REF_VALUES)
    values["artifact_class"] = artifact_class

    dataset_ref = DatasetRef(**values)

    assert dataset_ref.artifact_class == artifact_class


def test_dataset_ref_accepts_artifact_class_enum():
    values = dict(VALID_DATASET_REF_VALUES)
    values["artifact_class"] = ArtifactClass.ENV_CONTRACT

    dataset_ref = DatasetRef(**values)

    assert dataset_ref.artifact_class == "env_contract"


@pytest.mark.parametrize("label_class", sorted(ALLOWED_LABEL_CLASSES))
def test_label_class_separation_is_explicit(label_class):
    values = dict(VALID_LABEL_REF_VALUES)
    values["label_class"] = label_class

    label_ref = LabelRef(**values)

    assert label_ref.label_class == label_class


def test_label_ref_accepts_label_class_enum():
    values = dict(VALID_LABEL_REF_VALUES)
    values["label_class"] = LabelClass.SECONDARY_EXECUTION_COMPATIBLE

    label_ref = LabelRef(**values)

    assert label_ref.label_class == "secondary_execution_compatible"


@pytest.mark.parametrize(
    ("factory", "values", "field"),
    [
        (DatasetRef, VALID_DATASET_REF_VALUES, "ref_id"),
        (DatasetRef, VALID_DATASET_REF_VALUES, "dataset_id"),
        (DatasetRef, VALID_DATASET_REF_VALUES, "schema_version"),
        (FeatureRef, VALID_FEATURE_REF_VALUES, "ref_id"),
        (FeatureRef, VALID_FEATURE_REF_VALUES, "feature_id"),
        (FeatureRef, VALID_FEATURE_REF_VALUES, "feature_version"),
        (LabelRef, VALID_LABEL_REF_VALUES, "ref_id"),
        (LabelRef, VALID_LABEL_REF_VALUES, "label_id"),
        (LabelRef, VALID_LABEL_REF_VALUES, "label_version"),
        (SignalRef, VALID_SIGNAL_REF_VALUES, "ref_id"),
        (SignalRef, VALID_SIGNAL_REF_VALUES, "signal_id"),
        (SignalRef, VALID_SIGNAL_REF_VALUES, "strategy_id"),
        (SignalRef, VALID_SIGNAL_REF_VALUES, "signal_version"),
    ],
)
def test_empty_required_identifiers_fail_closed(factory, values, field):
    invalid_values = dict(values)
    invalid_values[field] = ""

    with pytest.raises(ReferenceValidationError):
        factory(**invalid_values)


@pytest.mark.parametrize(
    ("factory", "values"),
    [
        (DatasetRef, VALID_DATASET_REF_VALUES),
        (FeatureRef, VALID_FEATURE_REF_VALUES),
        (LabelRef, VALID_LABEL_REF_VALUES),
        (SignalRef, VALID_SIGNAL_REF_VALUES),
    ],
)
def test_empty_known_by_when_fails_closed(factory, values):
    invalid_values = dict(values)
    invalid_values["known_by_when"] = ""

    with pytest.raises(ReferenceValidationError):
        factory(**invalid_values)


@pytest.mark.parametrize("field", ["producer", "consumer"])
@pytest.mark.parametrize(
    ("factory", "values"),
    [
        (DatasetRef, VALID_DATASET_REF_VALUES),
        (FeatureRef, VALID_FEATURE_REF_VALUES),
        (LabelRef, VALID_LABEL_REF_VALUES),
        (SignalRef, VALID_SIGNAL_REF_VALUES),
    ],
)
def test_empty_producer_or_consumer_fails_closed(factory, values, field):
    invalid_values = dict(values)
    invalid_values[field] = ""

    with pytest.raises(ReferenceValidationError):
        factory(**invalid_values)


def test_unsupported_artifact_class_fails_closed():
    values = dict(VALID_DATASET_REF_VALUES)
    values["artifact_class"] = "implicit_latest_file"

    with pytest.raises(ReferenceValidationError):
        DatasetRef(**values)


def test_unsupported_label_class_fails_closed():
    values = dict(VALID_LABEL_REF_VALUES)
    values["label_class"] = "mixed_label"

    with pytest.raises(ReferenceValidationError):
        LabelRef(**values)


def test_feature_refs_require_non_empty_input_dataset_refs():
    values = dict(VALID_FEATURE_REF_VALUES)
    values["input_dataset_refs"] = ()

    with pytest.raises(ReferenceValidationError):
        FeatureRef(**values)


def test_signal_refs_require_non_empty_input_feature_refs():
    values = dict(VALID_SIGNAL_REF_VALUES)
    values["input_feature_refs"] = ()

    with pytest.raises(ReferenceValidationError):
        SignalRef(**values)


def test_feature_refs_require_anti_leakage_rule():
    missing_values = dict(VALID_FEATURE_REF_VALUES)
    missing_values.pop("anti_leakage_rule")
    empty_values = dict(VALID_FEATURE_REF_VALUES)
    empty_values["anti_leakage_rule"] = ""

    with pytest.raises(ReferenceValidationError):
        FeatureRef(**missing_values)
    with pytest.raises(ReferenceValidationError):
        FeatureRef(**empty_values)


def test_signal_refs_require_timestamp_semantics():
    missing_values = dict(VALID_SIGNAL_REF_VALUES)
    missing_values.pop("signal_timestamp_rule")
    empty_values = dict(VALID_SIGNAL_REF_VALUES)
    empty_values["signal_timestamp_rule"] = ""

    with pytest.raises(ReferenceValidationError):
        SignalRef(**missing_values)
    with pytest.raises(ReferenceValidationError):
        SignalRef(**empty_values)


@pytest.mark.parametrize(
    ("factory", "values"),
    [
        (DatasetRef, VALID_DATASET_REF_VALUES),
        (FeatureRef, VALID_FEATURE_REF_VALUES),
        (LabelRef, VALID_LABEL_REF_VALUES),
        (SignalRef, VALID_SIGNAL_REF_VALUES),
    ],
)
def test_unknown_reference_field_fails_closed(factory, values):
    invalid_values = dict(values)
    invalid_values["unexpected_field"] = "not_allowed"

    with pytest.raises(ReferenceValidationError):
        factory(**invalid_values)


def test_dataset_ref_values_validation_requires_mapping():
    with pytest.raises(ReferenceValidationError):
        validate_dataset_ref_values(("not", "a", "mapping"))


def test_reference_schema_source_has_no_forbidden_operational_terms():
    source = (REPO_ROOT / "src" / "moex_research" / "contracts" / "references.py").read_text(
        encoding="utf-8"
    ).casefold()

    forbidden_terms = (
        "server",
        "runtime",
        "live",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "http",
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
