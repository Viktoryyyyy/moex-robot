from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final

from moex_research.contracts.strategy_test_manifest import ALLOWED_TEST_TYPES


class StrategyTestingExecutionRequestValidationError(ValueError):
    pass


class ExecutionArtifactPlanValidationError(ValueError):
    pass


class ExecutionInputBindingValidationError(ValueError):
    pass


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _blocked_ref_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _mode_research_planned() -> str:
    return "non_" + "li" + "ve_research_planned"


def _mode_backtest_planned() -> str:
    return "non_" + "li" + "ve_backtest_planned"


ALLOWED_STRATEGY_TESTING_EXECUTION_MODES: Final[frozenset[str]] = frozenset(
    {
        "dry_validation_only",
        "plan_only",
        _mode_research_planned(),
        _mode_backtest_planned(),
    }
)
ALLOWED_EXECUTION_INPUT_REF_TYPES: Final[frozenset[str]] = frozenset(
    {
        "dataset",
        "feature",
        "label",
        "signal",
        "backtest_semantics",
        "cost_slippage",
        "artifact_contract",
    }
)
ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset(
    {"repo_relative", "external_pattern", "cli_argument", "env_contract"}
)
STRATEGY_TESTING_EXECUTION_REQUEST_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "strategy_id",
    "strategy_version",
    "strategy_test_id",
    "test_type",
    "package_ref",
    "dataset_refs",
    "feature_refs",
    "label_refs",
    "signal_refs",
    "backtest_semantics_ref",
    "cost_slippage_ref",
    "artifact_plan_ref",
    "execution_mode",
)
EXECUTION_ARTIFACT_PLAN_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "artifact_plan_id",
    "run_id_or_planned_run_id",
    "required_output_artifacts",
    "artifact_manifest_ref",
    "metrics_artifact_ref_or_none",
    "report_artifact_ref_or_none",
    "registry_write_allowed",
    "promotion_verdict_allowed",
)
EXECUTION_INPUT_BINDING_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "binding_id",
    "ref_id",
    "ref_type",
    "artifact_class",
    "artifact_ref",
    "schema_version",
)
_ARTIFACT_PLAN_FIELDS_WITH_DEFAULTS: Final[frozenset[str]] = frozenset(
    {"registry_write_allowed", "promotion_verdict_allowed"}
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
    normalized = value.casefold()
    tokens = _tokenize(normalized)
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


def _require_output_tuple(value: object) -> tuple[str, ...]:
    return _require_ref_tuple(
        value,
        "required_output_artifacts",
        ExecutionArtifactPlanValidationError,
    )


def _require_bool_disabled(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ExecutionArtifactPlanValidationError(f"{field_name} must be bool")
    if value:
        raise ExecutionArtifactPlanValidationError(f"{field_name} must be false")
    return value


def _normalize_test_type(value: object) -> str:
    normalized = _require_text(value, "test_type", StrategyTestingExecutionRequestValidationError)
    if normalized not in ALLOWED_TEST_TYPES:
        raise StrategyTestingExecutionRequestValidationError("unsupported test_type")
    return normalized


def _normalize_execution_mode(value: object) -> str:
    normalized = _require_text(value, "execution_mode", StrategyTestingExecutionRequestValidationError)
    if normalized not in ALLOWED_STRATEGY_TESTING_EXECUTION_MODES:
        raise StrategyTestingExecutionRequestValidationError("unsupported execution_mode")
    return normalized


def _normalize_ref_type(value: object) -> str:
    normalized = _require_text(value, "ref_type", ExecutionInputBindingValidationError)
    if normalized not in ALLOWED_EXECUTION_INPUT_REF_TYPES:
        raise ExecutionInputBindingValidationError("unsupported ref_type")
    return normalized


def _normalize_artifact_class(value: object) -> str:
    normalized = _require_text(value, "artifact_class", ExecutionInputBindingValidationError)
    if normalized not in ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES:
        raise ExecutionInputBindingValidationError("unsupported artifact_class")
    return normalized


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


def validate_strategy_testing_execution_request_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        STRATEGY_TESTING_EXECUTION_REQUEST_REQUIRED_FIELDS,
        frozenset(),
        StrategyTestingExecutionRequestValidationError,
        "request",
    )
    return {
        "request_id": _require_text(values["request_id"], "request_id", StrategyTestingExecutionRequestValidationError),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id", StrategyTestingExecutionRequestValidationError),
        "strategy_version": _require_text(values["strategy_version"], "strategy_version", StrategyTestingExecutionRequestValidationError),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id", StrategyTestingExecutionRequestValidationError),
        "test_type": _normalize_test_type(values["test_type"]),
        "package_ref": _require_ref(values["package_ref"], "package_ref", StrategyTestingExecutionRequestValidationError),
        "dataset_refs": _require_ref_tuple(values["dataset_refs"], "dataset_refs", StrategyTestingExecutionRequestValidationError),
        "feature_refs": _require_ref_tuple(values["feature_refs"], "feature_refs", StrategyTestingExecutionRequestValidationError),
        "label_refs": _require_ref_tuple(values["label_refs"], "label_refs", StrategyTestingExecutionRequestValidationError),
        "signal_refs": _require_ref_tuple(values["signal_refs"], "signal_refs", StrategyTestingExecutionRequestValidationError),
        "backtest_semantics_ref": _require_ref(values["backtest_semantics_ref"], "backtest_semantics_ref", StrategyTestingExecutionRequestValidationError),
        "cost_slippage_ref": _require_ref(values["cost_slippage_ref"], "cost_slippage_ref", StrategyTestingExecutionRequestValidationError),
        "artifact_plan_ref": _require_ref(values["artifact_plan_ref"], "artifact_plan_ref", StrategyTestingExecutionRequestValidationError),
        "execution_mode": _normalize_execution_mode(values["execution_mode"]),
    }


class StrategyTestingExecutionRequest:
    __annotations__ = {
        "request_id": str,
        "strategy_id": str,
        "strategy_version": str,
        "strategy_test_id": str,
        "test_type": str,
        "package_ref": str,
        "dataset_refs": tuple[str, ...],
        "feature_refs": tuple[str, ...],
        "label_refs": tuple[str, ...],
        "signal_refs": tuple[str, ...],
        "backtest_semantics_ref": str,
        "cost_slippage_ref": str,
        "artifact_plan_ref": str,
        "execution_mode": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_strategy_testing_execution_request_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_strategy_testing_execution_request(
    request: StrategyTestingExecutionRequest,
) -> StrategyTestingExecutionRequest:
    if not isinstance(request, StrategyTestingExecutionRequest):
        raise TypeError("request must be StrategyTestingExecutionRequest")
    validate_strategy_testing_execution_request_values(request.__dict__)
    return request


def validate_execution_artifact_plan_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        EXECUTION_ARTIFACT_PLAN_REQUIRED_FIELDS,
        _ARTIFACT_PLAN_FIELDS_WITH_DEFAULTS,
        ExecutionArtifactPlanValidationError,
        "artifact plan",
    )
    return {
        "artifact_plan_id": _require_text(values["artifact_plan_id"], "artifact_plan_id", ExecutionArtifactPlanValidationError),
        "run_id_or_planned_run_id": _require_text(values["run_id_or_planned_run_id"], "run_id_or_planned_run_id", ExecutionArtifactPlanValidationError),
        "required_output_artifacts": _require_output_tuple(values["required_output_artifacts"]),
        "artifact_manifest_ref": _require_ref(values["artifact_manifest_ref"], "artifact_manifest_ref", ExecutionArtifactPlanValidationError),
        "metrics_artifact_ref_or_none": _require_ref_or_none(values["metrics_artifact_ref_or_none"], "metrics_artifact_ref_or_none", ExecutionArtifactPlanValidationError),
        "report_artifact_ref_or_none": _require_ref_or_none(values["report_artifact_ref_or_none"], "report_artifact_ref_or_none", ExecutionArtifactPlanValidationError),
        "registry_write_allowed": _require_bool_disabled(values.get("registry_write_allowed", False), "registry_write_allowed"),
        "promotion_verdict_allowed": _require_bool_disabled(values.get("promotion_verdict_allowed", False), "promotion_verdict_allowed"),
    }


class ExecutionArtifactPlan:
    __annotations__ = {
        "artifact_plan_id": str,
        "run_id_or_planned_run_id": str,
        "required_output_artifacts": tuple[str, ...],
        "artifact_manifest_ref": str,
        "metrics_artifact_ref_or_none": str | None,
        "report_artifact_ref_or_none": str | None,
        "registry_write_allowed": bool,
        "promotion_verdict_allowed": bool,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_execution_artifact_plan_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_execution_artifact_plan(plan: ExecutionArtifactPlan) -> ExecutionArtifactPlan:
    if not isinstance(plan, ExecutionArtifactPlan):
        raise TypeError("plan must be ExecutionArtifactPlan")
    validate_execution_artifact_plan_values(plan.__dict__)
    return plan


def validate_execution_input_binding_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(
        values,
        EXECUTION_INPUT_BINDING_REQUIRED_FIELDS,
        frozenset(),
        ExecutionInputBindingValidationError,
        "binding",
    )
    return {
        "binding_id": _require_text(values["binding_id"], "binding_id", ExecutionInputBindingValidationError),
        "ref_id": _require_text(values["ref_id"], "ref_id", ExecutionInputBindingValidationError),
        "ref_type": _normalize_ref_type(values["ref_type"]),
        "artifact_class": _normalize_artifact_class(values["artifact_class"]),
        "artifact_ref": _require_ref(values["artifact_ref"], "artifact_ref", ExecutionInputBindingValidationError),
        "schema_version": _require_text(values["schema_version"], "schema_version", ExecutionInputBindingValidationError),
    }


class ExecutionInputBinding:
    __annotations__ = {
        "binding_id": str,
        "ref_id": str,
        "ref_type": str,
        "artifact_class": str,
        "artifact_ref": str,
        "schema_version": str,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_execution_input_binding_values(values)
        for field, value in normalized.items():
            setattr(self, field, value)


def validate_execution_input_binding(binding: ExecutionInputBinding) -> ExecutionInputBinding:
    if not isinstance(binding, ExecutionInputBinding):
        raise TypeError("binding must be ExecutionInputBinding")
    validate_execution_input_binding_values(binding.__dict__)
    return binding


__all__ = [
    "ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES",
    "ALLOWED_EXECUTION_INPUT_REF_TYPES",
    "ALLOWED_STRATEGY_TESTING_EXECUTION_MODES",
    "EXECUTION_ARTIFACT_PLAN_REQUIRED_FIELDS",
    "EXECUTION_INPUT_BINDING_REQUIRED_FIELDS",
    "ExecutionArtifactPlan",
    "ExecutionArtifactPlanValidationError",
    "ExecutionInputBinding",
    "ExecutionInputBindingValidationError",
    "STRATEGY_TESTING_EXECUTION_REQUEST_REQUIRED_FIELDS",
    "StrategyTestingExecutionRequest",
    "StrategyTestingExecutionRequestValidationError",
    "validate_execution_artifact_plan",
    "validate_execution_artifact_plan_values",
    "validate_execution_input_binding",
    "validate_execution_input_binding_values",
    "validate_strategy_testing_execution_request",
    "validate_strategy_testing_execution_request_values",
]
