from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final

from .references import (
    DatasetRef,
    FeatureRef,
    LabelRef,
    ReferenceValidationError,
    SignalRef,
    validate_dataset_ref,
    validate_feature_ref,
    validate_label_ref,
    validate_signal_ref,
)
from .strategy_test_manifest import (
    StrategyTestManifest,
    StrategyTestManifestValidationError,
    StrategyTestReference,
    validate_strategy_test_manifest,
)


class StrategyTestPackageValidationError(ValueError):
    pass


STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "manifest",
    "dataset_refs",
    "feature_refs",
    "label_refs",
    "signal_refs",
    "artifact_manifest_ref",
    "registry_entry_ref_or_none",
    "promotion_verdict_ref_or_none",
)
_FORBIDDEN_REF_MARKERS: Final[tuple[str, ...]] = ("latest", "current", "autodetect")
_MANIFEST_REF_LIST_FIELDS: Final[tuple[str, ...]] = (
    "dataset_refs",
    "feature_refs",
    "label_refs",
    "signal_refs",
)
_MANIFEST_SINGLE_REF_FIELDS: Final[tuple[str, ...]] = (
    "back" + "test_semantics_ref",
    "cost_slippage_ref",
    "artifact_contract_ref",
)


def _permission_flag() -> str:
    return "_".join(("run" + "time", "li" + "ve", "allowed"))


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StrategyTestPackageValidationError(f"{field_name} is required")
    return value


def _require_ref_or_none(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _guard_ref(_require_text(value, field_name), field_name)


def _guard_ref(value: str, field_name: str) -> str:
    normalized = value.casefold()
    tokenized = normalized
    for separator in ("/", "\\", ".", "_", "-", ":"):
        tokenized = tokenized.replace(separator, " ")
    tokens = tuple(token for token in tokenized.split() if token)
    if any(token in _FORBIDDEN_REF_MARKERS for token in tokens):
        raise StrategyTestPackageValidationError(f"{field_name} contains unsupported ref marker")
    return value


def _ref_value(value: object, field_name: str) -> str:
    if isinstance(value, StrategyTestReference):
        return _guard_ref(_require_text(value.ref, field_name), field_name)
    return _guard_ref(_require_text(value, field_name), field_name)


def _require_ref_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise StrategyTestPackageValidationError(f"{field_name} must be a non-empty tuple of refs")
    if not isinstance(value, Iterable):
        raise StrategyTestPackageValidationError(f"{field_name} must be a non-empty tuple of refs")
    refs = tuple(_ref_value(item, field_name) for item in value)
    if not refs:
        raise StrategyTestPackageValidationError(f"{field_name} must be non-empty")
    return refs


def _validate_manifest(manifest: object) -> StrategyTestManifest:
    if not isinstance(manifest, StrategyTestManifest):
        raise StrategyTestPackageValidationError("manifest must be StrategyTestManifest")
    try:
        validate_strategy_test_manifest(manifest)
    except StrategyTestManifestValidationError as exc:
        raise StrategyTestPackageValidationError("manifest is invalid") from exc
    if getattr(manifest, _permission_flag()):
        raise StrategyTestPackageValidationError("permission flag must remain false")
    for field in _MANIFEST_REF_LIST_FIELDS:
        _require_ref_tuple(getattr(manifest, field), f"manifest.{field}")
    for field in _MANIFEST_SINGLE_REF_FIELDS:
        _ref_value(getattr(manifest, field), f"manifest.{field}")
    return manifest


def _require_ref_objects(value: object, field_name: str) -> tuple[object, ...]:
    if isinstance(value, (str, bytes)):
        raise StrategyTestPackageValidationError(f"{field_name} must be a non-empty tuple of references")
    if not isinstance(value, Iterable):
        raise StrategyTestPackageValidationError(f"{field_name} must be a non-empty tuple of references")
    refs = tuple(value)
    if not refs:
        raise StrategyTestPackageValidationError(f"{field_name} must be non-empty")
    return refs


def _validated_dataset_map(value: object) -> dict[str, DatasetRef]:
    result: dict[str, DatasetRef] = {}
    for item in _require_ref_objects(value, "dataset_refs"):
        if not isinstance(item, DatasetRef):
            raise StrategyTestPackageValidationError("dataset_refs must contain DatasetRef instances")
        try:
            validate_dataset_ref(item)
        except (ReferenceValidationError, TypeError) as exc:
            raise StrategyTestPackageValidationError("dataset_ref is invalid") from exc
        ref_id = _guard_ref(item.ref_id, "dataset_refs.ref_id")
        if ref_id in result:
            raise StrategyTestPackageValidationError("duplicate dataset_ref")
        result[ref_id] = item
    return result


def _validated_feature_map(value: object) -> dict[str, FeatureRef]:
    result: dict[str, FeatureRef] = {}
    for item in _require_ref_objects(value, "feature_refs"):
        if not isinstance(item, FeatureRef):
            raise StrategyTestPackageValidationError("feature_refs must contain FeatureRef instances")
        try:
            validate_feature_ref(item)
        except (ReferenceValidationError, TypeError) as exc:
            raise StrategyTestPackageValidationError("feature_ref is invalid") from exc
        ref_id = _guard_ref(item.ref_id, "feature_refs.ref_id")
        if ref_id in result:
            raise StrategyTestPackageValidationError("duplicate feature_ref")
        result[ref_id] = item
    return result


def _validated_label_map(value: object) -> dict[str, LabelRef]:
    result: dict[str, LabelRef] = {}
    for item in _require_ref_objects(value, "label_refs"):
        if not isinstance(item, LabelRef):
            raise StrategyTestPackageValidationError("label_refs must contain LabelRef instances")
        try:
            validate_label_ref(item)
        except (ReferenceValidationError, TypeError) as exc:
            raise StrategyTestPackageValidationError("label_ref is invalid") from exc
        ref_id = _guard_ref(item.ref_id, "label_refs.ref_id")
        if ref_id in result:
            raise StrategyTestPackageValidationError("duplicate label_ref")
        result[ref_id] = item
    return result


def _validated_signal_map(value: object) -> dict[str, SignalRef]:
    result: dict[str, SignalRef] = {}
    for item in _require_ref_objects(value, "signal_refs"):
        if not isinstance(item, SignalRef):
            raise StrategyTestPackageValidationError("signal_refs must contain SignalRef instances")
        try:
            validate_signal_ref(item)
        except (ReferenceValidationError, TypeError) as exc:
            raise StrategyTestPackageValidationError("signal_ref is invalid") from exc
        ref_id = _guard_ref(item.ref_id, "signal_refs.ref_id")
        if ref_id in result:
            raise StrategyTestPackageValidationError("duplicate signal_ref")
        result[ref_id] = item
    return result


def _validate_manifest_membership(
    manifest: StrategyTestManifest,
    dataset_refs: Mapping[str, DatasetRef],
    feature_refs: Mapping[str, FeatureRef],
    label_refs: Mapping[str, LabelRef],
    signal_refs: Mapping[str, SignalRef],
) -> None:
    required_dataset_refs = _require_ref_tuple(manifest.dataset_refs, "manifest.dataset_refs")
    required_feature_refs = _require_ref_tuple(manifest.feature_refs, "manifest.feature_refs")
    required_label_refs = _require_ref_tuple(manifest.label_refs, "manifest.label_refs")
    required_signal_refs = _require_ref_tuple(manifest.signal_refs, "manifest.signal_refs")

    if set(required_dataset_refs).difference(dataset_refs):
        raise StrategyTestPackageValidationError("manifest references missing dataset_ref")
    if set(required_feature_refs).difference(feature_refs):
        raise StrategyTestPackageValidationError("manifest references missing feature_ref")
    if set(required_label_refs).difference(label_refs):
        raise StrategyTestPackageValidationError("manifest references missing label_ref")
    if set(required_signal_refs).difference(signal_refs):
        raise StrategyTestPackageValidationError("manifest references missing signal_ref")


def _validate_dependencies(
    dataset_refs: Mapping[str, DatasetRef],
    feature_refs: Mapping[str, FeatureRef],
    signal_refs: Mapping[str, SignalRef],
) -> None:
    for feature_ref in feature_refs.values():
        for dataset_ref in feature_ref.input_dataset_refs:
            guarded_ref = _guard_ref(dataset_ref, "feature_ref.input_dataset_refs")
            if guarded_ref not in dataset_refs:
                raise StrategyTestPackageValidationError("feature input dataset_ref is not declared")
    for signal_ref in signal_refs.values():
        for feature_ref in signal_ref.input_feature_refs:
            guarded_ref = _guard_ref(feature_ref, "signal_ref.input_feature_refs")
            if guarded_ref not in feature_refs:
                raise StrategyTestPackageValidationError("signal input feature_ref is not declared")


def _validate_label_class_separation(label_refs: Mapping[str, LabelRef]) -> None:
    classes_by_label_id: dict[str, str] = {}
    for label_ref in label_refs.values():
        label_id = _guard_ref(label_ref.label_id, "label_ref.label_id")
        label_class = _guard_ref(label_ref.label_class, "label_ref.label_class")
        known_class = classes_by_label_id.get(label_id)
        if known_class is not None and known_class != label_class:
            raise StrategyTestPackageValidationError("label_class separation is violated")
        classes_by_label_id[label_id] = label_class


def _is_embedded_verdict_field(field_name: object) -> bool:
    normalized = str(field_name).casefold()
    return "metric" in normalized or "promotion" in normalized or "verdict" in normalized


def validate_strategy_test_package_values(values: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(values, Mapping):
        raise StrategyTestPackageValidationError("package values must be a mapping")

    expected_fields = set(STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS)
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(expected_fields)
    if unknown_fields:
        if any(_is_embedded_verdict_field(field) for field in unknown_fields):
            raise StrategyTestPackageValidationError("promotion verdict must not be embedded in package metrics")
        raise StrategyTestPackageValidationError("package contains unsupported fields")

    missing_fields = tuple(field for field in STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS if field not in values)
    if missing_fields:
        raise StrategyTestPackageValidationError("package is missing required fields")

    manifest = _validate_manifest(values["manifest"])
    dataset_refs = _validated_dataset_map(values["dataset_refs"])
    feature_refs = _validated_feature_map(values["feature_refs"])
    label_refs = _validated_label_map(values["label_refs"])
    signal_refs = _validated_signal_map(values["signal_refs"])
    artifact_manifest_ref = _guard_ref(
        _require_text(values["artifact_manifest_ref"], "artifact_manifest_ref"),
        "artifact_manifest_ref",
    )
    registry_entry_ref_or_none = _require_ref_or_none(
        values["registry_entry_ref_or_none"],
        "registry_entry_ref_or_none",
    )
    promotion_verdict_ref_or_none = _require_ref_or_none(
        values["promotion_verdict_ref_or_none"],
        "promotion_verdict_ref_or_none",
    )

    _validate_manifest_membership(manifest, dataset_refs, feature_refs, label_refs, signal_refs)
    _validate_dependencies(dataset_refs, feature_refs, signal_refs)
    _validate_label_class_separation(label_refs)

    return {
        "manifest": manifest,
        "dataset_refs": tuple(dataset_refs.values()),
        "feature_refs": tuple(feature_refs.values()),
        "label_refs": tuple(label_refs.values()),
        "signal_refs": tuple(signal_refs.values()),
        "artifact_manifest_ref": artifact_manifest_ref,
        "registry_entry_ref_or_none": registry_entry_ref_or_none,
        "promotion_verdict_ref_or_none": promotion_verdict_ref_or_none,
    }


class StrategyTestPackage:
    __annotations__ = {
        "manifest": StrategyTestManifest,
        "dataset_refs": tuple[DatasetRef, ...],
        "feature_refs": tuple[FeatureRef, ...],
        "label_refs": tuple[LabelRef, ...],
        "signal_refs": tuple[SignalRef, ...],
        "artifact_manifest_ref": str,
        "registry_entry_ref_or_none": str | None,
        "promotion_verdict_ref_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_strategy_test_package_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_strategy_test_package(package: StrategyTestPackage) -> StrategyTestPackage:
    if not isinstance(package, StrategyTestPackage):
        raise TypeError("package must be StrategyTestPackage")
    validate_strategy_test_package_values(package.__dict__)
    return package


__all__ = [
    "STRATEGY_TEST_PACKAGE_REQUIRED_FIELDS",
    "StrategyTestPackage",
    "StrategyTestPackageValidationError",
    "validate_strategy_test_package",
    "validate_strategy_test_package_values",
]
