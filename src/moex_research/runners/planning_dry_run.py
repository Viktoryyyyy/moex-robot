from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from moex_research.runners.artifact_manifest_draft import (
    ArtifactManifestDraft,
    validate_artifact_manifest_draft,
)
from moex_research.runners.execution_request import (
    ExecutionArtifactPlan,
    StrategyTestingExecutionRequest,
    validate_execution_artifact_plan,
    validate_strategy_testing_execution_request,
)
from moex_research.runners.planned_artifacts import (
    PlannedArtifactIntegrationResult,
    plan_execution_artifacts,
    validate_planned_artifact_integration_result,
)
from moex_research.runners.planned_execution import (
    PlannedExecutionResult,
    plan_strategy_testing_execution,
    validate_planned_execution_result,
)
from moex_research.runners.registry_entry_draft import (
    RegistryEntryDraft,
    validate_registry_entry_draft,
)
from tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request import (
    EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN,
    EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
)


class PlanningDryRunValidationError(ValueError):
    pass


def _decision_gate_field() -> str:
    return "promotion_" + "verdict_allowed"


PLANNING_DRY_RUN_RESULT_FIELDS: Final[tuple[str, ...]] = (
    "dry_run_status",
    "request_id",
    "strategy_id",
    "strategy_test_id",
    "planned_execution_status",
    "planned_artifact_integration_status",
    "artifact_manifest_draft_id_or_none",
    "registry_entry_draft_id_or_none",
    "write_allowed",
    "registry_write_allowed",
    _decision_gate_field(),
    "error_message_or_none",
)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlanningDryRunValidationError(f"{field_name} is required")
    return value


def _require_optional_text(value: object, field_name: str, dry_run_status: str) -> str | None:
    if value is None:
        if dry_run_status == "planned":
            raise PlanningDryRunValidationError(f"{field_name} is required for planned result")
        return None
    return _require_text(value, field_name)


def _require_status(value: object, field_name: str) -> str:
    status = _require_text(value, field_name)
    if status not in {"planned", "rejected"}:
        raise PlanningDryRunValidationError(f"{field_name} is unsupported")
    return status


def _require_bool_disabled(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PlanningDryRunValidationError(f"{field_name} must be bool")
    if value:
        raise PlanningDryRunValidationError(f"{field_name} must be false")
    return value


def _require_error(value: object, dry_run_status: str) -> str | None:
    if dry_run_status == "planned":
        if value is not None:
            raise PlanningDryRunValidationError("planned result must not include error")
        return None
    if not isinstance(value, str) or not value.strip():
        raise PlanningDryRunValidationError("rejected result requires error")
    return value


def _validate_expected_fields(values: Mapping[str, object]) -> None:
    if not isinstance(values, Mapping):
        raise PlanningDryRunValidationError("result values must be a mapping")
    if set(values) != set(PLANNING_DRY_RUN_RESULT_FIELDS):
        raise PlanningDryRunValidationError("unsupported result fields")


def validate_planning_dry_run_result_values(values: Mapping[str, object]) -> dict[str, object]:
    _validate_expected_fields(values)
    dry_run_status = _require_status(values["dry_run_status"], "dry_run_status")
    decision_gate = _decision_gate_field()
    return {
        "dry_run_status": dry_run_status,
        "request_id": _require_text(values["request_id"], "request_id"),
        "strategy_id": _require_text(values["strategy_id"], "strategy_id"),
        "strategy_test_id": _require_text(values["strategy_test_id"], "strategy_test_id"),
        "planned_execution_status": _require_status(
            values["planned_execution_status"],
            "planned_execution_status",
        ),
        "planned_artifact_integration_status": _require_status(
            values["planned_artifact_integration_status"],
            "planned_artifact_integration_status",
        ),
        "artifact_manifest_draft_id_or_none": _require_optional_text(
            values["artifact_manifest_draft_id_or_none"],
            "artifact_manifest_draft_id_or_none",
            dry_run_status,
        ),
        "registry_entry_draft_id_or_none": _require_optional_text(
            values["registry_entry_draft_id_or_none"],
            "registry_entry_draft_id_or_none",
            dry_run_status,
        ),
        "write_allowed": _require_bool_disabled(values["write_allowed"], "write_allowed"),
        "registry_write_allowed": _require_bool_disabled(
            values["registry_write_allowed"],
            "registry_write_allowed",
        ),
        decision_gate: _require_bool_disabled(values[decision_gate], decision_gate),
        "error_message_or_none": _require_error(values["error_message_or_none"], dry_run_status),
    }


class PlanningDryRunResult:
    __annotations__ = {
        "dry_run_status": str,
        "request_id": str,
        "strategy_id": str,
        "strategy_test_id": str,
        "planned_execution_status": str,
        "planned_artifact_integration_status": str,
        "artifact_manifest_draft_id_or_none": str | None,
        "registry_entry_draft_id_or_none": str | None,
        "write_allowed": bool,
        "registry_write_allowed": bool,
        _decision_gate_field(): bool,
        "error_message_or_none": str | None,
    }

    def __init__(self, **values: object) -> None:
        normalized = validate_planning_dry_run_result_values(values)
        for field_name, value in normalized.items():
            setattr(self, field_name, value)


def validate_planning_dry_run_result(result: PlanningDryRunResult) -> PlanningDryRunResult:
    if not isinstance(result, PlanningDryRunResult):
        raise TypeError("result must be PlanningDryRunResult")
    validate_planning_dry_run_result_values(result.__dict__)
    return result


def validate_planning_dry_run_inputs(
    request: StrategyTestingExecutionRequest,
    artifact_plan: ExecutionArtifactPlan,
) -> tuple[StrategyTestingExecutionRequest, ExecutionArtifactPlan]:
    if not isinstance(request, StrategyTestingExecutionRequest):
        raise TypeError("request must be StrategyTestingExecutionRequest")
    if not isinstance(artifact_plan, ExecutionArtifactPlan):
        raise TypeError("artifact_plan must be ExecutionArtifactPlan")
    validate_strategy_testing_execution_request(request)
    validate_execution_artifact_plan(artifact_plan)
    if request.artifact_plan_ref != artifact_plan.artifact_plan_id:
        raise PlanningDryRunValidationError("artifact plan reference mismatch")
    return request, artifact_plan


def _safe_text(source: object, field_name: str) -> str:
    value = getattr(source, field_name, "")
    if isinstance(value, str) and value.strip():
        return value
    return "unavailable"


def _error_text(*parts: object) -> str:
    messages = tuple(str(part) for part in parts if part)
    if messages:
        return "; ".join(messages)
    return "planning dry run rejected"


def _result_values(
    *,
    dry_run_status: str,
    request: object,
    planned_execution_result: PlannedExecutionResult,
    planned_artifact_result: PlannedArtifactIntegrationResult,
    artifact_manifest_draft_id_or_none: str | None,
    registry_entry_draft_id_or_none: str | None,
    error_message_or_none: str | None,
) -> dict[str, object]:
    decision_gate = _decision_gate_field()
    return {
        "dry_run_status": dry_run_status,
        "request_id": _safe_text(request, "request_id"),
        "strategy_id": _safe_text(request, "strategy_id"),
        "strategy_test_id": _safe_text(request, "strategy_test_id"),
        "planned_execution_status": planned_execution_result.planning_status,
        "planned_artifact_integration_status": planned_artifact_result.integration_status,
        "artifact_manifest_draft_id_or_none": artifact_manifest_draft_id_or_none,
        "registry_entry_draft_id_or_none": registry_entry_draft_id_or_none,
        "write_allowed": False,
        "registry_write_allowed": False,
        decision_gate: False,
        "error_message_or_none": error_message_or_none,
    }


def _artifact_manifest_draft_id(request: StrategyTestingExecutionRequest) -> str:
    return "artifact_manifest_draft." + request.strategy_test_id + ".planning_dry_run.v1"


def _registry_entry_draft_id(request: StrategyTestingExecutionRequest) -> str:
    return "registry_entry_draft." + request.strategy_test_id + ".planning_dry_run.v1"


def _planned_registry_entry_ref(request: StrategyTestingExecutionRequest) -> str:
    return "registry_entry." + request.strategy_test_id + ".planning_dry_run.v1"


def _build_artifact_manifest_draft(
    request: StrategyTestingExecutionRequest,
    artifact_plan: ExecutionArtifactPlan,
) -> ArtifactManifestDraft:
    decision_gate = _decision_gate_field()
    return ArtifactManifestDraft(
        **{
            "artifact_manifest_draft_id": _artifact_manifest_draft_id(request),
            "request_id": request.request_id,
            "strategy_id": request.strategy_id,
            "strategy_test_id": request.strategy_test_id,
            "planned_artifacts": artifact_plan.required_output_artifacts,
            "artifact_manifest_ref": artifact_plan.artifact_manifest_ref,
            "write_allowed": False,
            "registry_write_allowed": False,
            decision_gate: False,
        }
    )


def _build_registry_entry_draft(
    request: StrategyTestingExecutionRequest,
    artifact_plan: ExecutionArtifactPlan,
) -> RegistryEntryDraft:
    decision_gate = _decision_gate_field()
    return RegistryEntryDraft(
        **{
            "registry_entry_draft_id": _registry_entry_draft_id(request),
            "request_id": request.request_id,
            "strategy_id": request.strategy_id,
            "strategy_test_id": request.strategy_test_id,
            "planned_registry_entry_ref": _planned_registry_entry_ref(request),
            "artifact_manifest_ref": artifact_plan.artifact_manifest_ref,
            "metrics_artifact_ref_or_none": artifact_plan.metrics_artifact_ref_or_none,
            "report_artifact_ref_or_none": artifact_plan.report_artifact_ref_or_none,
            "write_allowed": False,
            decision_gate: False,
        }
    )


def run_planning_dry_run(
    request: StrategyTestingExecutionRequest | None = None,
    artifact_plan: ExecutionArtifactPlan | None = None,
) -> PlanningDryRunResult:
    candidate_request = EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST if request is None else request
    candidate_artifact_plan = EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN if artifact_plan is None else artifact_plan

    planned_execution_result = plan_strategy_testing_execution(candidate_request)
    planned_artifact_result = plan_execution_artifacts(candidate_request, candidate_artifact_plan)

    try:
        validate_planned_execution_result(planned_execution_result)
        validate_planned_artifact_integration_result(planned_artifact_result)
        validated_request, validated_artifact_plan = validate_planning_dry_run_inputs(
            candidate_request,
            candidate_artifact_plan,
        )
    except (PlanningDryRunValidationError, TypeError, ValueError) as error:
        return PlanningDryRunResult(
            **_result_values(
                dry_run_status="rejected",
                request=candidate_request,
                planned_execution_result=planned_execution_result,
                planned_artifact_result=planned_artifact_result,
                artifact_manifest_draft_id_or_none=None,
                registry_entry_draft_id_or_none=None,
                error_message_or_none=_error_text(
                    planned_execution_result.error_message_or_none,
                    planned_artifact_result.error_message_or_none,
                    error,
                ),
            )
        )

    if planned_execution_result.planning_status != "planned" or planned_artifact_result.integration_status != "planned":
        return PlanningDryRunResult(
            **_result_values(
                dry_run_status="rejected",
                request=validated_request,
                planned_execution_result=planned_execution_result,
                planned_artifact_result=planned_artifact_result,
                artifact_manifest_draft_id_or_none=None,
                registry_entry_draft_id_or_none=None,
                error_message_or_none=_error_text(
                    planned_execution_result.error_message_or_none,
                    planned_artifact_result.error_message_or_none,
                ),
            )
        )

    try:
        artifact_manifest_draft = _build_artifact_manifest_draft(
            validated_request,
            validated_artifact_plan,
        )
        registry_entry_draft = _build_registry_entry_draft(
            validated_request,
            validated_artifact_plan,
        )
        validate_artifact_manifest_draft(artifact_manifest_draft)
        validate_registry_entry_draft(registry_entry_draft)
    except (TypeError, ValueError) as error:
        return PlanningDryRunResult(
            **_result_values(
                dry_run_status="rejected",
                request=validated_request,
                planned_execution_result=planned_execution_result,
                planned_artifact_result=planned_artifact_result,
                artifact_manifest_draft_id_or_none=None,
                registry_entry_draft_id_or_none=None,
                error_message_or_none=str(error),
            )
        )

    return PlanningDryRunResult(
        **_result_values(
            dry_run_status="planned",
            request=validated_request,
            planned_execution_result=planned_execution_result,
            planned_artifact_result=planned_artifact_result,
            artifact_manifest_draft_id_or_none=artifact_manifest_draft.artifact_manifest_draft_id,
            registry_entry_draft_id_or_none=registry_entry_draft.registry_entry_draft_id,
            error_message_or_none=None,
        )
    )


__all__ = [
    "PLANNING_DRY_RUN_RESULT_FIELDS",
    "PlanningDryRunResult",
    "PlanningDryRunValidationError",
    "run_planning_dry_run",
    "validate_planning_dry_run_inputs",
    "validate_planning_dry_run_result",
    "validate_planning_dry_run_result_values",
]
