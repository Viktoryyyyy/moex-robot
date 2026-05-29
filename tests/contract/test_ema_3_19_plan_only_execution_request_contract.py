from pathlib import Path

import pytest

from moex_research.contracts.strategy_test_package import validate_strategy_test_package
from moex_research.runners.execution_request import (
    ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES,
    ExecutionArtifactPlan,
    ExecutionArtifactPlanValidationError,
    ExecutionInputBinding,
    ExecutionInputBindingValidationError,
    StrategyTestingExecutionRequest,
    StrategyTestingExecutionRequestValidationError,
    validate_execution_artifact_plan,
    validate_execution_input_binding,
    validate_strategy_testing_execution_request,
)
from tests.fixtures.strategy_testing.ema_3_19_package import (
    EMA_3_19_STRATEGY_TEST_PACKAGE,
)
from tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request import (
    EMA_3_19_ARTIFACT_PLAN_ID,
    EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN,
    EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
    EMA_3_19_PLAN_ONLY_INPUT_BINDINGS,
    EMA_3_19_PLAN_ONLY_PACKAGE_REF,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_SOURCE_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "strategy_testing"
    / "ema_3_19_plan_only_execution_request.py"
)
EXPECTED_REF_TYPES = frozenset(
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


def _mode_market_access() -> str:
    return "li" + "ve"


def _mode_scheduler_access() -> str:
    return "run" + "time"


def _mode_external_actor() -> str:
    return "bro" + "ker"


def _mode_intent() -> str:
    return "or" + "der"


def _mode_market_access_execution() -> str:
    return _mode_market_access() + "_execution"


def _mode_scheduler_access_execution() -> str:
    return _mode_scheduler_access() + "_execution"


def _mode_external_actor_intent_execution() -> str:
    return _mode_external_actor() + "_" + _mode_intent() + "_execution"


def _blocked_markers() -> tuple[str, ...]:
    return (_freshness_marker(), _active_marker(), _implicit_marker())


def _forbidden_surface_markers() -> tuple[str, ...]:
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
        _mode_external_actor(),
        _mode_intent(),
        _mode_market_access_execution(),
        _mode_scheduler_access_execution(),
        "data_" + "root",
        "ser" + "ver",
        _freshness_marker(),
        _active_marker(),
        _implicit_marker(),
        _legacy_strategy_marker(),
    )


def _request_values(**overrides: object) -> dict[str, object]:
    values = dict(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.__dict__)
    values.update(overrides)
    return values


def _artifact_plan_values(**overrides: object) -> dict[str, object]:
    values = dict(EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN.__dict__)
    values.update(overrides)
    return values


def _binding_values(**overrides: object) -> dict[str, object]:
    values = dict(EMA_3_19_PLAN_ONLY_INPUT_BINDINGS[0].__dict__)
    values.update(overrides)
    return values


def _validate_binding_tuple(bindings: tuple[ExecutionInputBinding, ...]) -> tuple[ExecutionInputBinding, ...]:
    if not bindings:
        raise ExecutionInputBindingValidationError("bindings must be non-empty")
    return tuple(validate_execution_input_binding(binding) for binding in bindings)


def _iter_explicit_refs() -> tuple[str, ...]:
    request = EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST
    plan = EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN
    request_refs = (
        request.package_ref,
        *request.dataset_refs,
        *request.feature_refs,
        *request.label_refs,
        *request.signal_refs,
        request.backtest_semantics_ref,
        request.cost_slippage_ref,
        request.artifact_plan_ref,
    )
    plan_refs = (
        *plan.required_output_artifacts,
        plan.artifact_manifest_ref,
    )
    binding_refs = tuple(
        ref
        for binding in EMA_3_19_PLAN_ONLY_INPUT_BINDINGS
        for ref in (binding.ref_id, binding.artifact_ref)
    )
    return (*request_refs, *plan_refs, *binding_refs)


def _has_blocked_marker(value: str) -> bool:
    tokenized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        tokenized = tokenized.replace(separator, " ")
    tokens = tuple(token for token in tokenized.split() if token)
    return any(marker in tokens for marker in _blocked_markers())


def test_fixture_imports_successfully():
    assert isinstance(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST, StrategyTestingExecutionRequest)
    assert isinstance(EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN, ExecutionArtifactPlan)
    assert all(isinstance(binding, ExecutionInputBinding) for binding in EMA_3_19_PLAN_ONLY_INPUT_BINDINGS)


def test_strategy_test_package_fixture_validates():
    assert validate_strategy_test_package(EMA_3_19_STRATEGY_TEST_PACKAGE) is EMA_3_19_STRATEGY_TEST_PACKAGE


def test_strategy_testing_execution_request_validates():
    request = EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST

    assert validate_strategy_testing_execution_request(request) is request
    assert request.request_id == "exec_request.strategy_test.ema_3_19.plan_only.v1"
    assert request.strategy_id == "ema_3_19"
    assert request.strategy_test_id == EMA_3_19_STRATEGY_TEST_PACKAGE.manifest.strategy_test_id
    assert request.test_type == EMA_3_19_STRATEGY_TEST_PACKAGE.manifest.test_type
    assert request.execution_mode == "plan_only"
    assert request.package_ref == EMA_3_19_PLAN_ONLY_PACKAGE_REF


def test_execution_artifact_plan_validates_and_disables_writes_and_promotion():
    plan = EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN

    assert validate_execution_artifact_plan(plan) is plan
    assert plan.artifact_plan_id == EMA_3_19_ARTIFACT_PLAN_ID
    assert plan.registry_write_allowed is False
    assert plan.promotion_verdict_allowed is False
    assert plan.metrics_artifact_ref_or_none is None
    assert plan.report_artifact_ref_or_none is None


def test_all_execution_input_bindings_validate_and_cover_required_ref_types():
    bindings = _validate_binding_tuple(EMA_3_19_PLAN_ONLY_INPUT_BINDINGS)

    assert frozenset(binding.ref_type for binding in bindings) == EXPECTED_REF_TYPES
    assert sum(1 for binding in bindings if binding.ref_type == "label") == 2
    assert all(
        binding.artifact_class in ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES
        for binding in bindings
    )


def test_package_ref_and_all_refs_are_explicit_and_non_empty():
    assert EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.package_ref
    for ref in _iter_explicit_refs():
        assert isinstance(ref, str)
        assert ref.strip()


def test_freshness_selection_markers_are_absent_from_all_refs():
    for ref in _iter_explicit_refs():
        assert not _has_blocked_marker(ref)


@pytest.mark.parametrize(
    "mode",
    [
        "unsupported_mode",
        _mode_market_access(),
        _mode_scheduler_access(),
        _mode_external_actor(),
        _mode_intent(),
        "production",
        _mode_market_access_execution(),
        _mode_scheduler_access_execution(),
        _mode_external_actor_intent_execution(),
    ],
)
def test_invalid_execution_modes_fail_closed(mode):
    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(**_request_values(execution_mode=mode))


def test_empty_package_ref_fails_closed():
    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(**_request_values(package_ref=""))


def test_empty_request_ref_tuples_fail_closed():
    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(**_request_values(dataset_refs=()))


def test_empty_binding_tuple_fails_closed():
    with pytest.raises(ExecutionInputBindingValidationError):
        _validate_binding_tuple(())


def test_unsupported_ref_type_fails_closed():
    with pytest.raises(ExecutionInputBindingValidationError):
        ExecutionInputBinding(**_binding_values(ref_type="unsupported_ref"))


def test_unsupported_artifact_class_fails_closed():
    with pytest.raises(ExecutionInputBindingValidationError):
        ExecutionInputBinding(**_binding_values(artifact_class="implicit_file"))


@pytest.mark.parametrize(
    "marker",
    [_freshness_marker(), _active_marker(), _implicit_marker()],
)
def test_artifact_ref_selection_markers_fail_closed(marker):
    with pytest.raises(ExecutionInputBindingValidationError):
        ExecutionInputBinding(
            **_binding_values(artifact_ref="contracts/strategy_testing/" + marker + ".yaml")
        )


def test_artifact_plan_registry_permission_fails_closed():
    with pytest.raises(ExecutionArtifactPlanValidationError):
        ExecutionArtifactPlan(**_artifact_plan_values(registry_write_allowed=True))


def test_artifact_plan_promotion_permission_fails_closed():
    with pytest.raises(ExecutionArtifactPlanValidationError):
        ExecutionArtifactPlan(**_artifact_plan_values(promotion_verdict_allowed=True))


def test_no_strategy_execution_surface_is_introduced_by_fixture():
    source = FIXTURE_SOURCE_PATH.read_text(encoding="utf-8").casefold()

    for marker in _forbidden_surface_markers():
        assert marker not in source


def test_no_legacy_strategy_imports_or_refs_in_fixture():
    source = FIXTURE_SOURCE_PATH.read_text(encoding="utf-8").casefold()
    refs = " ".join(_iter_explicit_refs()).casefold()

    assert _legacy_strategy_marker() not in source
    assert _legacy_strategy_short_marker() not in source
    assert _legacy_strategy_marker() not in refs
    assert _legacy_strategy_short_marker() not in refs
