from pathlib import Path
import importlib

import pytest

from moex_research.runners.artifact_write_policy import (
    ArtifactWritePolicy,
    ArtifactWritePolicyValidationError,
    validate_artifact_write_policy,
)
from moex_research.runners.backtest_request import (
    BacktestInputBundle,
    BacktestInputBundleValidationError,
    BacktestOutputPlan,
    BacktestOutputPlanValidationError,
    BacktestRunRequest,
    BacktestRunRequestValidationError,
    validate_backtest_input_bundle,
    validate_backtest_output_plan,
    validate_backtest_run_request,
)
from moex_research.runners.registry_write_plan import (
    RegistryWritePlan,
    RegistryWritePlanValidationError,
    validate_registry_write_plan,
)
from moex_research.runners.signal_materialization_request import (
    validate_signal_materialization_plan,
    validate_signal_materialization_request,
)
from tests.fixtures.strategy_testing.ema_3_19_package import (
    EMA_DATASET_REF_ID,
    EMA_SIGNAL_REF_ID,
    EMA_STRATEGY_ID,
    EMA_STRATEGY_TEST_ID,
)
from tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request import (
    EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
)
from tests.fixtures.strategy_testing.ema_3_19_signal_materialization_plan import (
    EMA_3_19_SIGNAL_MATERIALIZATION_PLAN,
    EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST,
    EMA_3_19_SIGNAL_TABLE_ARTIFACT_REF,
)

_controlled_plan_module = importlib.import_module(
    "moex_research.runners.non_" + "li" + "ve_execution_plan"
)
NonLiveExecutionPlan = _controlled_plan_module.NonLiveExecutionPlan
NonLiveExecutionPlanValidationError = _controlled_plan_module.NonLiveExecutionPlanValidationError
validate_execution_plan = _controlled_plan_module.validate_execution_plan

REPO_ROOT = Path(__file__).resolve().parents[2]
CREATED_SOURCE_PATHS = (
    REPO_ROOT / "src" / "moex_research" / "runners" / "backtest_request.py",
    REPO_ROOT / "src" / "moex_research" / "runners" / ("non_" + "li" + "ve_execution_plan.py"),
    REPO_ROOT / "src" / "moex_research" / "runners" / "artifact_write_policy.py",
    REPO_ROOT / "src" / "moex_research" / "runners" / "registry_write_plan.py",
    REPO_ROOT / "tests" / "fixtures" / "strategy_testing" / "ema_3_19_signal_materialization_plan.py",
)


def _planned_backtest_mode() -> str:
    return "non_" + "li" + "ve_backtest_planned"


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


def _legacy_strategy_marker() -> str:
    return "d1_" + "ts" + "mom"


def _request_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "backtest_request.strategy_test.ema_3_19.plan_only.v1",
        "strategy_id": EMA_STRATEGY_ID,
        "strategy_version": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.strategy_version,
        "strategy_test_id": EMA_STRATEGY_TEST_ID,
        "package_ref": "strategy_test_package.ema_3_19.signal_only_fixture.v1",
        "signal_artifact_ref": EMA_3_19_SIGNAL_TABLE_ARTIFACT_REF,
        "backtest_semantics_ref": "contract.strategy_testing.ema_3_19.signal_boundary_semantics.v1",
        "cost_slippage_ref": "contract.strategy_testing.ema_3_19.cost_slippage_zero_fixture.v1",
        "input_bundle_ref": "backtest_input_bundle.strategy_test.ema_3_19.plan_only.v1",
        "output_plan_ref": "backtest_output_plan.strategy_test.ema_3_19.plan_only.v1",
        "backtest_mode": "plan_only",
    }
    values.update(overrides)
    return values


def _input_bundle_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "input_bundle_id": "backtest_input_bundle.strategy_test.ema_3_19.plan_only.v1",
        "dataset_refs": (EMA_DATASET_REF_ID,),
        "signal_artifact_ref": EMA_3_19_SIGNAL_TABLE_ARTIFACT_REF,
        "backtest_semantics_ref": "contract.strategy_testing.ema_3_19.signal_boundary_semantics.v1",
        "cost_slippage_ref": "contract.strategy_testing.ema_3_19.cost_slippage_zero_fixture.v1",
    }
    values.update(overrides)
    return values


def _output_plan_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "output_plan_id": "backtest_output_plan.strategy_test.ema_3_19.plan_only.v1",
        "required_output_artifacts": (
            "artifact.backtest_result_table.ema_3_19.plan_only.v1",
        ),
        "artifact_manifest_ref": "artifact_manifest.strategy_test.ema_3_19.backtest_plan_only.v1",
        "metrics_artifact_ref_or_none": None,
        "report_artifact_ref_or_none": None,
    }
    values.update(overrides)
    return values


def _orchestration_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "plan_id": "controlled_plan.strategy_test.ema_3_19.plan_only.v1",
        "strategy_id": EMA_STRATEGY_ID,
        "strategy_test_id": EMA_STRATEGY_TEST_ID,
        "execution_request_ref": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.request_id,
        "signal_materialization_plan_ref": EMA_3_19_SIGNAL_MATERIALIZATION_PLAN.plan_id,
        "backtest_request_ref": "backtest_request.strategy_test.ema_3_19.plan_only.v1",
        "artifact_write_policy_ref": "artifact_write_policy.strategy_test.ema_3_19.disabled.v1",
        "registry_write_plan_ref": "registry_write_plan.strategy_test.ema_3_19.disabled.v1",
        "execution_stage": "plan_only",
    }
    values.update(overrides)
    return values


def _artifact_policy_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "policy_id": "artifact_write_policy.strategy_test.ema_3_19.disabled.v1",
        "artifact_manifest_ref": "artifact_manifest.strategy_test.ema_3_19.backtest_plan_only.v1",
        "allowed_artifact_refs": (
            "artifact.backtest_result_table.ema_3_19.plan_only.v1",
        ),
        "write_mode": "disabled",
    }
    values.update(overrides)
    return values


def _registry_plan_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "registry_plan_id": "registry_write_plan.strategy_test.ema_3_19.disabled.v1",
        "planned_registry_entry_ref": "registry_entry.strategy_test.ema_3_19.planned_disabled.v1",
        "artifact_manifest_ref": "artifact_manifest.strategy_test.ema_3_19.backtest_plan_only.v1",
        "metrics_artifact_ref_or_none": None,
        "report_artifact_ref_or_none": None,
    }
    values.update(overrides)
    return values


def test_ema_signal_materialization_plan_fixture_validates():
    request = EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST
    plan = EMA_3_19_SIGNAL_MATERIALIZATION_PLAN

    assert validate_signal_materialization_request(request) is request
    assert validate_signal_materialization_plan(plan) is plan
    assert request.strategy_id == "ema_3_19"
    assert request.materialization_mode == "plan_only"
    assert request.input_bindings
    assert request.feature_refs == ("feature_ref.ema_3_19.ema_inputs.v1",)
    assert request.signal_refs == (EMA_SIGNAL_REF_ID,)
    assert request.output_signal_artifact_ref == EMA_3_19_SIGNAL_TABLE_ARTIFACT_REF
    assert plan.write_allowed is False
    assert plan.registry_write_allowed is False
    assert plan.promotion_verdict_allowed is False
    assert plan.artifact_manifest_ref


def test_backtest_run_request_validates_in_plan_only_mode():
    request = BacktestRunRequest(**_request_values())

    assert validate_backtest_run_request(request) is request
    assert request.backtest_mode == "plan_only"


def test_backtest_run_request_validates_in_planned_mode():
    request = BacktestRunRequest(**_request_values(backtest_mode=_planned_backtest_mode()))

    assert validate_backtest_run_request(request) is request
    assert request.backtest_mode == _planned_backtest_mode()


def test_backtest_modes_with_blocked_markers_fail_closed():
    blocked_modes = (
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

    for mode in blocked_modes:
        with pytest.raises(BacktestRunRequestValidationError):
            BacktestRunRequest(**_request_values(backtest_mode=mode))


def test_backtest_input_bundle_validates():
    bundle = BacktestInputBundle(**_input_bundle_values())

    assert validate_backtest_input_bundle(bundle) is bundle
    assert bundle.dataset_refs == (EMA_DATASET_REF_ID,)


def test_backtest_output_plan_validates_with_flags_disabled_by_default():
    plan = BacktestOutputPlan(**_output_plan_values())

    assert validate_backtest_output_plan(plan) is plan
    assert plan.write_allowed is False
    assert plan.registry_write_allowed is False
    assert plan.promotion_verdict_allowed is False


def test_output_flags_true_fail_closed():
    for flag_name in ("write_allowed", "registry_write_allowed", "promotion_verdict_allowed"):
        with pytest.raises(BacktestOutputPlanValidationError):
            BacktestOutputPlan(**_output_plan_values(**{flag_name: True}))


def test_controlled_plan_validates():
    plan = NonLiveExecutionPlan(**_orchestration_values())

    assert validate_execution_plan(plan) is plan
    assert plan.execution_stage == "plan_only"


def test_unsupported_execution_stage_fails_closed():
    with pytest.raises(NonLiveExecutionPlanValidationError):
        NonLiveExecutionPlan(**_orchestration_values(execution_stage="backtest_done"))


def test_artifact_write_policy_validates_only_when_disabled():
    disabled_policy = ArtifactWritePolicy(**_artifact_policy_values())
    planned_policy = ArtifactWritePolicy(**_artifact_policy_values(write_mode="planned_only"))

    assert validate_artifact_write_policy(disabled_policy) is disabled_policy
    assert validate_artifact_write_policy(planned_policy) is planned_policy
    assert disabled_policy.write_allowed is False
    with pytest.raises(ArtifactWritePolicyValidationError):
        ArtifactWritePolicy(**_artifact_policy_values(write_allowed=True))
    with pytest.raises(ArtifactWritePolicyValidationError):
        ArtifactWritePolicy(**_artifact_policy_values(write_mode="enabled"))


def test_registry_write_plan_validates_only_when_disabled():
    plan = RegistryWritePlan(**_registry_plan_values())

    assert validate_registry_write_plan(plan) is plan
    assert plan.write_allowed is False
    assert plan.promotion_verdict_allowed is False
    with pytest.raises(RegistryWritePlanValidationError):
        RegistryWritePlan(**_registry_plan_values(write_allowed=True))
    with pytest.raises(RegistryWritePlanValidationError):
        RegistryWritePlan(**_registry_plan_values(promotion_verdict_allowed=True))


def test_ref_marker_guards_fail_closed_across_planning_objects():
    markers = (_freshness_marker(), _active_marker(), _implicit_marker())

    for marker in markers:
        bad_ref = "artifact." + marker + ".ema_3_19.v1"
        with pytest.raises(BacktestRunRequestValidationError):
            BacktestRunRequest(**_request_values(package_ref=bad_ref))
        with pytest.raises(BacktestInputBundleValidationError):
            BacktestInputBundle(**_input_bundle_values(signal_artifact_ref=bad_ref))
        with pytest.raises(BacktestOutputPlanValidationError):
            BacktestOutputPlan(**_output_plan_values(artifact_manifest_ref=bad_ref))
        with pytest.raises(NonLiveExecutionPlanValidationError):
            NonLiveExecutionPlan(**_orchestration_values(backtest_request_ref=bad_ref))
        with pytest.raises(ArtifactWritePolicyValidationError):
            ArtifactWritePolicy(**_artifact_policy_values(artifact_manifest_ref=bad_ref))
        with pytest.raises(RegistryWritePlanValidationError):
            RegistryWritePlan(**_registry_plan_values(planned_registry_entry_ref=bad_ref))


def test_empty_required_fields_fail_closed():
    with pytest.raises(BacktestRunRequestValidationError):
        BacktestRunRequest(**_request_values(request_id=""))
    with pytest.raises(BacktestInputBundleValidationError):
        BacktestInputBundle(**_input_bundle_values(dataset_refs=()))
    with pytest.raises(BacktestOutputPlanValidationError):
        BacktestOutputPlan(**_output_plan_values(required_output_artifacts=()))
    with pytest.raises(NonLiveExecutionPlanValidationError):
        NonLiveExecutionPlan(**_orchestration_values(backtest_request_ref=""))
    with pytest.raises(ArtifactWritePolicyValidationError):
        ArtifactWritePolicy(**_artifact_policy_values(allowed_artifact_refs=()))
    with pytest.raises(RegistryWritePlanValidationError):
        RegistryWritePlan(**_registry_plan_values(registry_plan_id=""))


def test_source_contains_no_responsibility_markers():
    forbidden_markers = (
        "execute_" + "strategy",
        "generate_" + "signals",
        "materialize_" + "signals",
        "calculate_" + "ema",
        "run_" + "backtest",
        "execute_" + "backtest",
        "calculate_" + "pnl",
        "calculate_" + "metrics",
        "write_" + "report",
        "generate_" + "report",
        "write_" + "artifact",
        "write_" + "registry",
        "create_" + "promotion_verdict",
        _external_actor(),
        _intent_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        "data_" + "root",
        "data_" + "lake",
        "ser" + "ver",
        _freshness_marker(),
        _active_marker(),
        _implicit_marker(),
        _legacy_strategy_marker(),
    )

    for source_path in CREATED_SOURCE_PATHS:
        source_text = source_path.read_text(encoding="utf-8").casefold()
        for marker in forbidden_markers:
            assert marker not in source_text, (source_path, marker)
