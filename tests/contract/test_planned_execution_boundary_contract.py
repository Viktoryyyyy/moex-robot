from pathlib import Path

import pytest

from moex_research.runners.execution_request import (
    StrategyTestingExecutionRequest,
    StrategyTestingExecutionRequestValidationError,
)
from moex_research.runners.planned_execution import (
    PLANNED_EXECUTION_RESULT_FIELDS,
    PlannedExecutionBoundaryValidationError,
    PlannedExecutionResult,
    plan_strategy_testing_execution,
    validate_planned_execution_request,
    validate_planned_execution_result,
)
from tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request import (
    EMA_3_19_ARTIFACT_PLAN_ID,
    EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "planned_execution.py"
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "planning_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "execution_mode",
        "artifact_plan_ref",
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
        "pathlib",
        "os.",
        "open(",
    )


def _blocked_result_fields() -> tuple[str, ...]:
    return (
        "metrics",
        "report_path",
        "registry_entry",
        "promotion_" + "verdict",
        _scheduler_access() + "_" + _market_access() + "_authorization",
        "back" + "test_result",
        "research_result",
    )


def _request_values(**overrides: object) -> dict[str, object]:
    values = dict(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.__dict__)
    values.update(overrides)
    return values


def _request_with_mode(mode: str) -> StrategyTestingExecutionRequest:
    return StrategyTestingExecutionRequest(**_request_values(execution_mode=mode))


@pytest.mark.parametrize("mode", ("plan_only", _research_mode(), _backtest_mode()))
def test_valid_planned_modes_return_planned_result(mode: str):
    request = _request_with_mode(mode)

    result = plan_strategy_testing_execution(request)

    assert isinstance(result, PlannedExecutionResult)
    assert validate_planned_execution_result(result) is result
    assert result.planning_status == "planned"
    assert result.request_id == request.request_id
    assert result.strategy_id == request.strategy_id
    assert result.strategy_test_id == request.strategy_test_id
    assert result.execution_mode == mode
    assert result.artifact_plan_ref == EMA_3_19_ARTIFACT_PLAN_ID
    assert result.error_message_or_none is None


def test_validate_request_accepts_only_this_boundary_modes():
    request = _request_with_mode(_research_mode())

    assert validate_planned_execution_request(request) is request


def test_dry_validation_mode_is_rejected_as_wrong_boundary():
    request = _request_with_mode("dry_validation_only")

    with pytest.raises(PlannedExecutionBoundaryValidationError):
        validate_planned_execution_request(request)

    result = plan_strategy_testing_execution(request)

    assert result.planning_status == "rejected"
    assert result.execution_mode == "dry_validation_only"
    assert result.error_message_or_none is not None
    assert "wrong boundary" in result.error_message_or_none


def test_invalid_request_shape_fails_closed():
    result = plan_strategy_testing_execution(object())

    assert result.planning_status == "rejected"
    assert result.error_message_or_none is not None


@pytest.mark.parametrize(
    "mode",
    [
        _market_access(),
        _scheduler_access(),
        _external_actor(),
        _intent(),
        "production",
        _market_access_exec(),
        _scheduler_access_exec(),
        _external_actor_intent_exec(),
        "production_execution",
    ],
)
def test_non_planning_modes_fail_closed(mode: str):
    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(**_request_values(execution_mode=mode))


@pytest.mark.parametrize(
    "marker",
    [_freshness_marker(), _active_marker(), _implicit_marker()],
)
def test_selection_markers_fail_closed(marker: str):
    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(
            **_request_values(package_ref="strategy_test_package." + marker + ".v1")
        )


def test_result_object_contains_only_planning_status_and_identifiers():
    result = plan_strategy_testing_execution(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST)

    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(PLANNED_EXECUTION_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS


def test_result_object_has_no_execution_output_fields():
    result = plan_strategy_testing_execution(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST)

    for field_name in _blocked_result_fields():
        assert field_name not in result.__dict__


def test_result_schema_rejects_extra_fields():
    result = plan_strategy_testing_execution(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST)
    result.extra_field = "not allowed"

    with pytest.raises(PlannedExecutionBoundaryValidationError):
        validate_planned_execution_result(result)


def test_source_has_no_forbidden_responsibility_markers():
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
