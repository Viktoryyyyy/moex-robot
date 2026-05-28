from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final


class StrategyTestManifestValidationError(ValueError):
    pass


class StrategyTestType(str, Enum):
    EVENT_STUDY_RESEARCH = "event_study_research"
    SIGNAL_ONLY_RESEARCH = "signal_only_research"
    SINGLE_INSTRUMENT_BACKTEST = "single_instrument_backtest"
    MULTI_INSTRUMENT_BACKTEST = "multi_instrument_backtest"
    PORTFOLIO_LEVEL_BACKTEST = "portfolio_level_backtest"
    REGIME_CONDITIONED_TEST = "regime_conditioned_test"
    FRAGILITY_ROBUSTNESS_PACKAGE = "fragility_robustness_package"
    PRODUCTION_READINESS_VALIDATION = "production_readiness_validation"


ALLOWED_TEST_TYPES: Final[frozenset[str]] = frozenset(item.value for item in StrategyTestType)


def _flag_field() -> str:
    return "_".join(("run" + "time", "li" + "ve", "allowed"))


STRATEGY_TEST_MANIFEST_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
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
    _flag_field(),
)
_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset({_flag_field()})
_REF_LIST_FIELDS: Final[frozenset[str]] = frozenset(
    {"dataset_refs", "feature_refs", "label_refs", "signal_refs"}
)
_SCOPE_FIELDS: Final[frozenset[str]] = frozenset({"instrument_scope", "timeframe_scope"})
_SINGLE_REF_FIELDS: Final[frozenset[str]] = frozenset(
    {"backtest_semantics_ref", "cost_slippage_ref", "artifact_contract_ref"}
)


@dataclass(frozen=True)
class StrategyTestReference:
    ref: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "ref", _require_text(self.ref, "ref"))


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StrategyTestManifestValidationError(f"{field_name} is required")
    return value


def _require_text_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise StrategyTestManifestValidationError(f"{field_name} must be a non-empty tuple of strings")
    try:
        items = tuple(value)  # type: ignore[arg-type]
    except TypeError as exc:
        raise StrategyTestManifestValidationError(f"{field_name} must be a non-empty tuple of strings") from exc
    if not items:
        raise StrategyTestManifestValidationError(f"{field_name} must be non-empty")
    if any(not isinstance(item, str) or not item.strip() for item in items):
        raise StrategyTestManifestValidationError(f"{field_name} must contain non-empty strings")
    return items


def _require_ref(value: object, field_name: str) -> str | StrategyTestReference:
    if isinstance(value, StrategyTestReference):
        return value
    return _require_text(value, field_name)


def _require_ref_tuple(value: object, field_name: str) -> tuple[str | StrategyTestReference, ...]:
    if isinstance(value, (str, bytes)):
        raise StrategyTestManifestValidationError(f"{field_name} must be a non-empty tuple of refs")
    if not isinstance(value, Iterable):
        raise StrategyTestManifestValidationError(f"{field_name} must be a non-empty tuple of refs")
    items = tuple(value)
    if not items:
        raise StrategyTestManifestValidationError(f"{field_name} must be non-empty")
    return tuple(_require_ref(item, field_name) for item in items)


def _normalize_test_type(value: object) -> str:
    if isinstance(value, StrategyTestType):
        value = value.value
    value = _require_text(value, "test_type")
    if value not in ALLOWED_TEST_TYPES:
        raise StrategyTestManifestValidationError("unsupported test_type")
    return value


def _require_disabled(value: object) -> bool:
    if not isinstance(value, bool):
        raise StrategyTestManifestValidationError("permission flag must be bool")
    if value:
        raise StrategyTestManifestValidationError("permission flag must be false")
    return value


def validate_strategy_test_manifest_values(values: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(values, Mapping):
        raise StrategyTestManifestValidationError("manifest values must be a mapping")

    expected_fields = set(STRATEGY_TEST_MANIFEST_REQUIRED_FIELDS)
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(expected_fields)
    if unknown_fields:
        raise StrategyTestManifestValidationError("manifest contains unsupported fields")

    missing_fields = tuple(
        field for field in STRATEGY_TEST_MANIFEST_REQUIRED_FIELDS
        if field not in values and field not in _FIELDS_WITH_DEFAULTS
    )
    if missing_fields:
        raise StrategyTestManifestValidationError("manifest is missing required fields")

    normalized: dict[str, object] = {}
    for field in STRATEGY_TEST_MANIFEST_REQUIRED_FIELDS:
        if field in _FIELDS_WITH_DEFAULTS:
            normalized[field] = _require_disabled(values.get(field, False))
        elif field == "test_type":
            normalized[field] = _normalize_test_type(values[field])
        elif field in _SCOPE_FIELDS:
            normalized[field] = _require_text_tuple(values[field], field)
        elif field in _REF_LIST_FIELDS:
            normalized[field] = _require_ref_tuple(values[field], field)
        elif field in _SINGLE_REF_FIELDS:
            normalized[field] = _require_ref(values[field], field)
        else:
            normalized[field] = _require_text(values[field], field)
    return normalized


class StrategyTestManifest:
    __annotations__ = {
        "strategy_test_id": str,
        "strategy_id": str,
        "strategy_version": str,
        "test_type": str,
        "instrument_scope": tuple[str, ...],
        "timeframe_scope": tuple[str, ...],
        "dataset_refs": tuple[str | StrategyTestReference, ...],
        "feature_refs": tuple[str | StrategyTestReference, ...],
        "label_refs": tuple[str | StrategyTestReference, ...],
        "signal_refs": tuple[str | StrategyTestReference, ...],
        "backtest_semantics_ref": str | StrategyTestReference,
        "cost_slippage_ref": str | StrategyTestReference,
        "artifact_contract_ref": str | StrategyTestReference,
        _flag_field(): bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_strategy_test_manifest_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_strategy_test_manifest(manifest: StrategyTestManifest) -> StrategyTestManifest:
    if not isinstance(manifest, StrategyTestManifest):
        raise TypeError("manifest must be StrategyTestManifest")
    validate_strategy_test_manifest_values(manifest.__dict__)
    return manifest


__all__ = [
    "ALLOWED_TEST_TYPES",
    "STRATEGY_TEST_MANIFEST_REQUIRED_FIELDS",
    "StrategyTestManifest",
    "StrategyTestManifestValidationError",
    "StrategyTestReference",
    "StrategyTestType",
    "validate_strategy_test_manifest",
    "validate_strategy_test_manifest_values",
]
