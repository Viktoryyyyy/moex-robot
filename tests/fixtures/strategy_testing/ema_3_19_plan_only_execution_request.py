from __future__ import annotations

from moex_research.runners.execution_request import (
    ExecutionArtifactPlan,
    ExecutionInputBinding,
    StrategyTestingExecutionRequest,
)
from tests.fixtures.strategy_testing.ema_3_19_package import (
    EMA_3_19_STRATEGY_TEST_PACKAGE,
    EMA_DATASET_REF_ID,
    EMA_FEATURE_REF_ID,
    EMA_PRIMARY_LABEL_REF_ID,
    EMA_SECONDARY_LABEL_REF_ID,
    EMA_SIGNAL_REF_ID,
    EMA_STRATEGY_ID,
    EMA_STRATEGY_TEST_ID,
)


EMA_3_19_PLAN_ONLY_REQUEST_ID = "exec_request.strategy_test.ema_3_19.plan_only.v1"
EMA_3_19_PLAN_ONLY_PACKAGE_REF = "strategy_test_package.ema_3_19.signal_only_fixture.v1"
EMA_3_19_ARTIFACT_PLAN_ID = "artifact_plan.strategy_test.ema_3_19.plan_only.v1"
EMA_3_19_PLANNED_RUN_ID = "planned_run.strategy_test.ema_3_19.plan_only.v1"
EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF = (
    "artifact_manifest.strategy_test.ema_3_19.plan_only.v1"
)


def build_ema_3_19_plan_only_artifact_plan() -> ExecutionArtifactPlan:
    return ExecutionArtifactPlan(
        artifact_plan_id=EMA_3_19_ARTIFACT_PLAN_ID,
        run_id_or_planned_run_id=EMA_3_19_PLANNED_RUN_ID,
        required_output_artifacts=(EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF,),
        artifact_manifest_ref=EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF,
        metrics_artifact_ref_or_none=None,
        report_artifact_ref_or_none=None,
        registry_write_allowed=False,
        promotion_verdict_allowed=False,
    )


def _binding(
    binding_id: str,
    ref_id: str,
    ref_type: str,
    artifact_class: str,
    artifact_ref: str,
    schema_version: str,
) -> ExecutionInputBinding:
    return ExecutionInputBinding(
        binding_id=binding_id,
        ref_id=ref_id,
        ref_type=ref_type,
        artifact_class=artifact_class,
        artifact_ref=artifact_ref,
        schema_version=schema_version,
    )


def build_ema_3_19_plan_only_input_bindings() -> tuple[ExecutionInputBinding, ...]:
    package = EMA_3_19_STRATEGY_TEST_PACKAGE
    manifest = package.manifest
    dataset_ref = package.dataset_refs[0]
    feature_ref = package.feature_refs[0]
    primary_label_ref = package.label_refs[0]
    secondary_label_ref = package.label_refs[1]
    signal_ref = package.signal_refs[0]

    return (
        _binding(
            binding_id="binding.strategy_test.ema_3_19.dataset.v1",
            ref_id=EMA_DATASET_REF_ID,
            ref_type="dataset",
            artifact_class=dataset_ref.artifact_class,
            artifact_ref="contract.strategy_testing.ema_3_19.dataset_input_pattern.v1",
            schema_version=dataset_ref.schema_version,
        ),
        _binding(
            binding_id="binding.strategy_test.ema_3_19.feature.v1",
            ref_id=EMA_FEATURE_REF_ID,
            ref_type="feature",
            artifact_class="repo_relative",
            artifact_ref="tests/fixtures/strategy_testing/ema_3_19_package.py::EMA_FEATURE_REF_ID",
            schema_version=feature_ref.feature_version,
        ),
        _binding(
            binding_id="binding.strategy_test.ema_3_19.primary_label.v1",
            ref_id=EMA_PRIMARY_LABEL_REF_ID,
            ref_type="label",
            artifact_class="repo_relative",
            artifact_ref=(
                "tests/fixtures/strategy_testing/ema_3_19_package.py::"
                "EMA_PRIMARY_LABEL_REF_ID"
            ),
            schema_version=primary_label_ref.label_version,
        ),
        _binding(
            binding_id="binding.strategy_test.ema_3_19.secondary_label.v1",
            ref_id=EMA_SECONDARY_LABEL_REF_ID,
            ref_type="label",
            artifact_class="repo_relative",
            artifact_ref=(
                "tests/fixtures/strategy_testing/ema_3_19_package.py::"
                "EMA_SECONDARY_LABEL_REF_ID"
            ),
            schema_version=secondary_label_ref.label_version,
        ),
        _binding(
            binding_id="binding.strategy_test.ema_3_19.signal.v1",
            ref_id=EMA_SIGNAL_REF_ID,
            ref_type="signal",
            artifact_class="repo_relative",
            artifact_ref="tests/fixtures/strategy_testing/ema_3_19_package.py::EMA_SIGNAL_REF_ID",
            schema_version=signal_ref.signal_version,
        ),
        _binding(
            binding_id="binding.strategy_test.ema_3_19.backtest_semantics.v1",
            ref_id=manifest.backtest_semantics_ref,
            ref_type="backtest_semantics",
            artifact_class="repo_relative",
            artifact_ref=(
                "tests/fixtures/strategy_testing/ema_3_19_package.py::"
                "backtest_semantics_ref"
            ),
            schema_version="strategy_testing_backtest_semantics.v1",
        ),
        _binding(
            binding_id="binding.strategy_test.ema_3_19.cost_slippage.v1",
            ref_id=manifest.cost_slippage_ref,
            ref_type="cost_slippage",
            artifact_class="repo_relative",
            artifact_ref=(
                "tests/fixtures/strategy_testing/ema_3_19_package.py::"
                "cost_slippage_ref"
            ),
            schema_version="strategy_testing_cost_slippage.v1",
        ),
        _binding(
            binding_id="binding.strategy_test.ema_3_19.artifact_contract.v1",
            ref_id=manifest.artifact_contract_ref,
            ref_type="artifact_contract",
            artifact_class="repo_relative",
            artifact_ref=(
                "tests/fixtures/strategy_testing/ema_3_19_package.py::"
                "artifact_contract_ref"
            ),
            schema_version="strategy_testing_artifact_contract.v1",
        ),
    )


def build_ema_3_19_plan_only_execution_request() -> StrategyTestingExecutionRequest:
    package = EMA_3_19_STRATEGY_TEST_PACKAGE
    manifest = package.manifest

    return StrategyTestingExecutionRequest(
        request_id=EMA_3_19_PLAN_ONLY_REQUEST_ID,
        strategy_id=EMA_STRATEGY_ID,
        strategy_version=manifest.strategy_version,
        strategy_test_id=EMA_STRATEGY_TEST_ID,
        test_type=manifest.test_type,
        package_ref=EMA_3_19_PLAN_ONLY_PACKAGE_REF,
        dataset_refs=tuple(ref.ref_id for ref in package.dataset_refs),
        feature_refs=tuple(ref.ref_id for ref in package.feature_refs),
        label_refs=tuple(ref.ref_id for ref in package.label_refs),
        signal_refs=tuple(ref.ref_id for ref in package.signal_refs),
        backtest_semantics_ref=manifest.backtest_semantics_ref,
        cost_slippage_ref=manifest.cost_slippage_ref,
        artifact_plan_ref=EMA_3_19_ARTIFACT_PLAN_ID,
        execution_mode="plan_only",
    )


EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN = build_ema_3_19_plan_only_artifact_plan()
EMA_3_19_PLAN_ONLY_INPUT_BINDINGS = build_ema_3_19_plan_only_input_bindings()
EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST = build_ema_3_19_plan_only_execution_request()


__all__ = [
    "EMA_3_19_ARTIFACT_PLAN_ID",
    "EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF",
    "EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN",
    "EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST",
    "EMA_3_19_PLAN_ONLY_INPUT_BINDINGS",
    "EMA_3_19_PLAN_ONLY_PACKAGE_REF",
    "EMA_3_19_PLAN_ONLY_REQUEST_ID",
    "EMA_3_19_PLANNED_RUN_ID",
    "build_ema_3_19_plan_only_artifact_plan",
    "build_ema_3_19_plan_only_execution_request",
    "build_ema_3_19_plan_only_input_bindings",
]
