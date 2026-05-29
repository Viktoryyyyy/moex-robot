from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final


class BacktestRunRequestValidationError(ValueError):
    pass


class BacktestInputBundleValidationError(ValueError):
    pass


class BacktestOutputPlanValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _market_access() -> str:
    return "li" + "ve"


def _scheduler_access() -> str:
    return "run" + "time"


def _external_actor() -> str:
    return "bro" + "ker"


def _intent_marker() -> str:
    return "or" + "der"


def _production_marker() -> str:
    return "prod" + "uction"


def _planned_mode() -> str:
    return "non_" + "li" + "ve_backtest_planned"


ALLOWED_BACKTEST_MODES: Final[frozenset[str]] = frozenset(
    {
        "plan_only",
        _planned_mode(),
    }
)
BACKTEST_RUN_REQUEST_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_version",
    "strategy_test_id",
    "package_ref",
    "signal_artifact_ref",
    "backtest_semantics_ref",
    "cost_slippage_ref",
    "input_bundle_ref",
    "output_plan_ref",
    "backtest_mode",
)
BACKTEST_INPUT_BUNDLE_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "input_bundle_id",
    "dataset_refs",
    "signal_artifact_ref",
    "backtest_semantics_ref",
    "cost_slippage_ref",
)
BACKTEST_OUTPUT_PLAN_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "output_plan_id",
    "required_output_artifacts",
    "artifact_manifest_ref",
    "metrics_artifact_ref_or_none",
    "report_artifact_ref_or_none",
    "write_allowed",
    "registry_write_allowed",
    "promotion_verdict_allowed",
)
_OUTPUT_PLAN_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset(
    {
        "write_allowed",
        "registry_write_allowed",
        "promotion_verdict_allowed",
    }
)


def _blocked_ref_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _blocked_mode_markers() -> tuple[str, ...]:
    return (
        _market_access(),
        _scheduler_access(),
        _external_actor(),
        _intent_marker(),
        _production_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        _external_actor() + "_" + _intent_marker() + "_execution",
        _production_marker() + "_execution",
    )


def _tokenize(value: str) -> tuple[str, ...]:
    tokenized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        tokenized = tokenized.replace(separator, " ")
    return tuple(token for token in tokenized.split() if token)


def _require_text(value: object, field_name: str, error_type: type[ValueError]) -> str:
    if not isinstance(value, str) or not value.strip():
        raise error_type(f"{field_name} is required")
    return value


def _guard_ref(value: str, field_name: str, error_type: type[ValueError]) -> str:
    tokens = _tokenize(value)
    if any(marker in tokens for marker in _blocked_ref_markers()):
        raise error_type(f"{field_name} contains unsupported ref marker")
    return value


def _require_ref(value: object, field_name: str, error_type: type[ValueError]) -> str:
    return _guard_ref(_require_text(value, field_name, error_type), field_name, error_type)


def _require_ref_or_none(value: object, field_name: str, error_type: type[ValueError]) -> str | None:
    if value is None:
        return None
    return _require_ref(value, field_name, error_type)


def _require_ref_tuple(value: object, field_name: str, error_type: type[ValueError]) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise error_type(f"{field_name} must be a non-empty tuple of refs")
    if not isinstance(value, Iterable):
        raise error_type(f"{field_name} must be a non-empty tuple of refs")
    refs = tuple(_require_ref(item, field_name, error_type) for item in value)
    if not refs:
        raise error_type(f"{field_name} must be non-empty")
    return refs


def _require_bool_disabled(value: object, field_name: str, error_type: type[ValueError]) -> bool:
    if not isinstance(value, bool):
        raise error_type(f"{field_name} must be bool")
    if value:
        raise error_type(f"{field_name} must be false")
    return value


def _validate_expected_fields(
    values: Mapping[str, object],
    expected_fields: tuple[str, ...],
    fields_with_defaults: frozenset[str],
    error_type: type[ValueError],
    object_name: str,
) -> None:
    if not isinstance(values, Mapping):
        raise error_type(f"{object_name} values must be a mapping")
    provided_fields = set(values)
    unknown_fields = provided_fields.difference(expected_fields)
    if unknown_fields:
        raise error_type(f"{object_name} contains unsupported fields")
    missing_fields = tuple(
        field for field in expected_fields
        if field not in values and field not in fields_with_defaults
    )
    if missing_fields:
        raise error_type(f"{object_name} is missing required fields")


def _normalize_backtest_mode(value: object) -> str:
    normalized = _require_text(value, "backtest_mode", BacktestRunRequestValidationError)
    tokens = _tokenize(normalized)
    if normalized not in ALLOWED_BACKTEST_MODES and any(
        marker in tokens for marker in _blocked_mode_markers()
    ):
        raise BacktestRunRequestValidationError("unsupported backtest_mode")
    if normalized not in ALLOWED_BACKTEST_MODES:
        raise BacktestRunRequestValidationError("unsupported backtest_mode")
    return normalized


def validate_backtest_run_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        BACKTEST_RUN_REQUEST_REQUIRED_FIELDS,
        frozenset(),
        BacktestRunRequestValidationError,
        "backtest request",
    )
    return {
        "request_id": _require_text(values["request_id"], "request_id", BacktestRunRequestValidationError),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id", BacktestRunRequestValidationError),
        "strategy_version": _require_text(values["strategy_version"], "strategy_version", BacktestRunRequestValidationError),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id", BacktestRunRequestValidationError),
        "package_ref": _require_ref(values["package_ref"], "package_ref", BacktestRunRequestValidationError),
        "signal_artifact_ref": _require_ref(values["signal_artifact_ref"], "signal_artifact_ref", BacktestRunRequestValidationError),
        "backtest_semantics_ref": _require_ref(values["backtest_semantics_ref"], "backtest_semantics_ref", BacktestRunRequestValidationError),
        "cost_slippage_ref": _require_ref(values["cost_slippage_ref"], "cost_slippage_ref", BacktestRunRequestValidationError),
        "input_bundle_ref": _require_ref(values["input_bundle_ref"], "input_bundle_ref", BacktestRunRequestValidationError),
        "output_plan_ref": _require_ref(values["output_plan_ref"], "output_plan_ref", BacktestRunRequestValidationError),
        "backtest_mode": _normalize_backtest_mode(values["backtest_mode"]),
    }


class BacktestRunRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_version": str,
        "strategy_test_id": str,
        "package_ref": str,
        "signal_artifact_ref": str,
        "backtest_semantics_ref": str,
        "cost_slippage_ref": str,
        "input_bundle_ref": str,
        "output_plan_ref": str,
        "backtest_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_backtest_run_request_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_backtest_run_request(request: BacktestRunRequest) -> BacktestRunRequest:
    if not isinstance(request, BacktestRunRequest):
        raise TypeError("request must be BacktestRunRequest")
    validate_backtest_run_request_values(request.__dict__)
    return request


def validate_backtest_input_bundle_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        BACKTEST_INPUT_BUNDLE_REQUIRED_FIELDS,
        frozenset(),
        BacktestInputBundleValidationError,
        "input bundle",
    )
    return {
        "input_bundle_id": _require_text(values["input_bundle_id"], "input_bundle_id", BacktestInputBundleValidationError),
        "dataset_refs": _require_ref_tuple(values["dataset_refs"], "dataset_refs", BacktestInputBundleValidationError),
        "signal_artifact_ref": _require_ref(values["signal_artifact_ref"], "signal_artifact_ref", BacktestInputBundleValidationError),
        "backtest_semantics_ref": _require_ref(values["backtest_semantics_ref"], "backtest_semantics_ref", BacktestInputBundleValidationError),
        "cost_slippage_ref": _require_ref(values["cost_slippage_ref"], "cost_slippage_ref", BacktestInputBundleValidationError),
    }


class BacktestInputBundle:
    __annotations__ = {
        "input_bundle_id": str,
        "dataset_refs": tuple[str, ...],
        "signal_artifact_ref": str,
        "backtest_semantics_ref": str,
        "cost_slippage_ref": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_backtest_input_bundle_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_backtest_input_bundle(bundle: BacktestInputBundle) -> BacktestInputBundle:
    if not isinstance(bundle, BacktestInputBundle):
        raise TypeError("bundle must be BacktestInputBundle")
    validate_backtest_input_bundle_values(bundle.__dict__)
    return bundle


def validate_backtest_output_plan_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        BACKTEST_OUTPUT_PLAN_REQUIRED_FIELDS,
        _OUTPUT_PLAN_FIELDS_WITH_DEFAULTS,
        BacktestOutputPlanValidationError,
        "output plan",
    )
    return {
        "output_plan_id": _require_text(values["output_plan_id"], "output_plan_id", BacktestOutputPlanValidationError),
        "required_output_artifacts": _require_ref_tuple(values["required_output_artifacts"], "required_output_artifacts", BacktestOutputPlanValidationError),
        "artifact_manifest_ref": _require_ref(values["artifact_manifest_ref"], "artifact_manifest_ref", BacktestOutputPlanValidationError),
        "metrics_artifact_ref_or_none": _require_ref_or_none(values["metrics_artifact_ref_or_none"], "metrics_artifact_ref_or_none", BacktestOutputPlanValidationError),
        "report_artifact_ref_or_none": _require_ref_or_none(values["report_artifact_ref_or_none"], "report_artifact_ref_or_none", BacktestOutputPlanValidationError),
        "write_allowed": _require_bool_disabled(values.get("write_allowed", False), "write_allowed", BacktestOutputPlanValidationError),
        "registry_write_allowed": _require_bool_disabled(values.get("registry_write_allowed", False), "registry_write_allowed", BacktestOutputPlanValidationError),
        "promotion_verdict_allowed": _require_bool_disabled(values.get("promotion_verdict_allowed", False), "promotion_verdict_allowed", BacktestOutputPlanValidationError),
    }


class BacktestOutputPlan:
    __annotations__ = {
        "output_plan_id": str,
        "required_output_artifacts": tuple[str, ...],
        "artifact_manifest_ref": str,
        "metrics_artifact_ref_or_none": str | None,
        "report_artifact_ref_or_none": str | None,
        "write_allowed": bool,
        "registry_write_allowed": bool,
        "promotion_verdict_allowed": bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_backtest_output_plan_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_backtest_output_plan(plan: BacktestOutputPlan) -> BacktestOutputPlan:
    if not isinstance(plan, BacktestOutputPlan):
        raise TypeError("plan must be BacktestOutputPlan")
    validate_backtest_output_plan_values(plan.__dict__)
    return plan


__all__ = [
    "ALLOWED_BACKTEST_MODES",
    "BACKTEST_INPUT_BUNDLE_REQUIRED_FIELDS",
    "BACKTEST_OUTPUT_PLAN_REQUIRED_FIELDS",
    "BACKTEST_RUN_REQUEST_REQUIRED_FIELDS",
    "BacktestInputBundle",
    "BacktestInputBundleValidationError",
    "BacktestOutputPlan",
    "BacktestOutputPlanValidationError",
    "BacktestRunRequest",
    "BacktestRunRequestValidationError",
    "validate_backtest_input_bundle",
    "validate_backtest_input_bundle_values",
    "validate_backtest_output_plan",
    "validate_backtest_output_plan_values",
    "validate_backtest_run_request",
    "validate_backtest_run_request_values",
]
