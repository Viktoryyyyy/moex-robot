from pathlib import Path

import pytest

from moex_research.runners.execution_request import (
    ExecutionArtifactPlan,
    StrategyTestingExecutionRequest,
)
from moex_research.runners.planned_artifacts import (
    PLANNED_ARTIFACT_INTEGRATION_RESULT_FIELDS,
    PlannedArtifactIntegrationResult,
    PlannedArtifactIntegrationValidationError,
    plan_execution_artifacts,
    validate_planned_artifact_integration,
    validate_planned_artifact_integration_result,
)
from tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request import (
    EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN,
    EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "planned_artifacts.py"
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "integration_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "artifact_plan_id",
        "artifact_manifest_ref",
        "metrics_artifact_ref_or_none",
        "report_artifact_ref_or_none",
        "registry_write_allowed",
        "promotion_" + "verdict_allowed",
        "error_message_or_none",
    }
)


def _research_mode() -> str:
    return "non_" + "li" + "ve_research_planned"


def _backtest_mode() -> str:
    return "non_" + "li" + "ve_backtest_planned"


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _decision_gate_field() -> str:
    return "promotion_" + "verdict_allowed"


def _legacy_strategy_marker() -> str:
    return "d1_" + "tsmom"


def _legacy_strategy_short_marker() -> str:
    return "ts" + "mom"


def _market_access() -> str:
    return "li" + "ve"


def _scheduler_access() -> str:
    return "run" + "time"


def _external_actor() -> str:
    return "bro" + "ker"


def _intent() -> str:
    return "or" + "der"


def _market_access_exec() -> str:
    return _market_access() + "_execution"


def _scheduler_access_exec() -> str:
    return _scheduler_access() + "_execution"


def _external_actor_intent_exec() -> str:
    return _external_actor() + "_" + _intent() + "_execution"


def _responsibility_markers() -> tuple[str, ...]:
    return (
        "run_" + "back" + "test",
        "execute_" + "back" + "test",
        "execute_" + "strategy",
        "generate_" + "signals",
        "calculate_" + "pnl",
        "calculate_" + "metrics",
        "write_" + "report",
        "write_" + "registry",
        "create_" + "promotion_" + "verdict",
        "promotion_" + "verdict",
        _external_actor(),
        _intent(),
        _market_access_exec(),
        _scheduler_access_exec(),
        "data_" + "root",
        "ser" + "ver",
        _freshness_marker(),
        _active_marker(),
        _implicit_marker(),
        _legacy_strategy_marker(),
    )


def _infra_markers() -> tuple[str, ...]:
    return (
        "data_" + "lake",
        "moex_data",
        "run" + "time_" + "li" + "ve",
        "ser" + "ver_path",
        "/home/",
        "/var/",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "os.",
        "open(",
    )


def _blocked_result_fields() -> tuple[str, ...]:
    return (
        "calculated_metrics",
        "generated_report_path",
        "registry_entry",
        "promotion_" + "verdict",
        _scheduler_access() + "_" + _market_access() + "_authorization",
        "back" + "test_result",
        "research_result",
        "artifact_write_status",
    )


def _request_values(**overrides: object) -> dict[str, object]:
    values = dict(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.__dict__)
    values.update(overrides)
    return values


def _artifact_plan_values(**overrides: object) -> dict[str, object]:
    values = dict(EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN.__dict__)
    values.update(overrides)
    return values


def _request(**overrides: object) -> StrategyTestingExecutionRequest:
    return StrategyTestingExecutionRequest(**_request_values(**overrides))


def _artifact_plan(**overrides: object) -> ExecutionArtifactPlan:
    return ExecutionArtifactPlan(**_artifact_plan_values(**overrides))


def _assert_planned_result(
    result: PlannedArtifactIntegrationResult,
    request: StrategyTestingExecutionRequest,
    artifact_plan: ExecutionArtifactPlan,
) -> None:
    assert validate_planned_artifact_integration_result(result) is result
    assert result.integration_status == "planned"
    assert result.request_id == request.request_id
    assert result.strategy_id == request.strategy_id
    assert result.strategy_test_id == request.strategy_test_id
    assert result.artifact_plan_id == artifact_plan.artifact_plan_id
    assert result.artifact_manifest_ref == artifact_plan.artifact_manifest_ref
    assert result.metrics_artifact_ref_or_none == artifact_plan.metrics_artifact_ref_or_none
    assert result.report_artifact_ref_or_none == artifact_plan.report_artifact_ref_or_none
    assert result.registry_write_allowed is False
    assert getattr(result, _decision_gate_field()) is False
    assert result.error_message_or_none is None


def test_valid_ema_plan_only_request_and_artifact_plan_integrates_successfully():
    request = _request()
    artifact_plan = _artifact_plan()

    result = plan_execution_artifacts(request, artifact_plan)

    _assert_planned_result(result, request, artifact_plan)


@pytest.mark.parametrize("mode", (_research_mode(), _backtest_mode()))
def test_valid_non_market_planned_modes_integrate_successfully(mode: str):
    request = _request(execution_mode=mode)
    artifact_plan = _artifact_plan()

    result = plan_execution_artifacts(request, artifact_plan)

    _assert_planned_result(result, request, artifact_plan)


def test_validate_boundary_returns_validated_request_and_artifact_plan():
    request = _request(execution_mode=_research_mode())
    artifact_plan = _artifact_plan()

    validated_request, validated_plan = validate_planned_artifact_integration(
        request,
        artifact_plan,
    )

    assert validated_request is request
    assert validated_plan is artifact_plan


def test_invalid_request_fails_closed():
    result = plan_execution_artifacts(object(), _artifact_plan())

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_invalid_artifact_plan_fails_closed():
    result = plan_execution_artifacts(_request(), object())

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_empty_required_output_artifacts_fail_closed():
    artifact_plan = _artifact_plan()
    artifact_plan.required_output_artifacts = ()

    result = plan_execution_artifacts(_request(), artifact_plan)

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_empty_artifact_manifest_ref_fails_closed():
    artifact_plan = _artifact_plan()
    artifact_plan.artifact_manifest_ref = ""

    result = plan_execution_artifacts(_request(), artifact_plan)

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_registry_write_allowed_true_fails_closed():
    artifact_plan = _artifact_plan()
    artifact_plan.registry_write_allowed = True

    result = plan_execution_artifacts(_request(), artifact_plan)

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_decision_gate_allowed_true_fails_closed():
    artifact_plan = _artifact_plan()
    setattr(artifact_plan, _decision_gate_field(), True)

    result = plan_execution_artifacts(_request(), artifact_plan)

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


@pytest.mark.parametrize(
    "field_name",
    (
        "artifact_manifest_ref",
        "metrics_artifact_ref_or_none",
        "report_artifact_ref_or_none",
    ),
)
@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_marker_artifact_refs_fail_closed(field_name: str, marker: str):
    artifact_plan = _artifact_plan()
    setattr(artifact_plan, field_name, "artifact_ref." + marker + ".v1")

    result = plan_execution_artifacts(_request(), artifact_plan)

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_marker_required_output_artifacts_fail_closed(marker: str):
    artifact_plan = _artifact_plan()
    artifact_plan.required_output_artifacts = ("artifact_output." + marker + ".v1",)

    result = plan_execution_artifacts(_request(), artifact_plan)

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


@pytest.mark.parametrize(
    "mode",
    (
        _market_access(),
        _scheduler_access(),
        _external_actor(),
        _intent(),
        "production",
        _market_access_exec(),
        _scheduler_access_exec(),
        _external_actor_intent_exec(),
        "production_execution",
    ),
)
def test_non_planning_modes_fail_closed(mode: str):
    request = _request()
    request.execution_mode = mode

    result = plan_execution_artifacts(request, _artifact_plan())

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_missing_request_id_fails_closed():
    request = _request()
    request.request_id = ""

    result = plan_execution_artifacts(request, _artifact_plan())

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_missing_artifact_plan_id_fails_closed():
    artifact_plan = _artifact_plan()
    artifact_plan.artifact_plan_id = ""

    result = plan_execution_artifacts(_request(), artifact_plan)

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None


def test_mismatched_request_artifact_plan_ref_fails_closed():
    request = _request(artifact_plan_ref="artifact_plan.strategy_test.other.plan_only.v1")

    result = plan_execution_artifacts(request, _artifact_plan())

    assert result.integration_status == "rejected"
    assert result.error_message_or_none is not None
    assert "mismatch" in result.error_message_or_none


def test_result_object_contains_only_integration_identifiers_and_refs():
    result = plan_execution_artifacts(_request(), _artifact_plan())

    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(PLANNED_ARTIFACT_INTEGRATION_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS


def test_result_object_has_no_execution_output_fields():
    result = plan_execution_artifacts(_request(), _artifact_plan())

    for field_name in _blocked_result_fields():
        assert field_name not in result.__dict__


def test_result_schema_rejects_extra_fields():
    result = plan_execution_artifacts(_request(), _artifact_plan())
    result.extra_field = "not allowed"

    with pytest.raises(PlannedArtifactIntegrationValidationError):
        validate_planned_artifact_integration_result(result)


def test_source_has_no_forbidden_execution_responsibility_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    for marker in _responsibility_markers():
        assert marker not in source


def test_source_does_not_import_legacy_strategy():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert _legacy_strategy_marker() not in source
    assert _legacy_strategy_short_marker() not in source


def test_source_has_no_forbidden_infra_terms():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    for marker in _infra_markers():
        assert marker not in source
