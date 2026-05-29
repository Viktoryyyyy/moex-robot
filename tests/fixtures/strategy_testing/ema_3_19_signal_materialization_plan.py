from __future__ import annotations

from moex_research.runners.signal_materialization_request import (
    SignalMaterializationPlan,
    SignalMaterializationRequest,
)
from tests.fixtures.strategy_testing.ema_3_19_package import (
    EMA_3_19_STRATEGY_TEST_PACKAGE,
    EMA_FEATURE_REF_ID,
    EMA_SIGNAL_REF_ID,
    EMA_STRATEGY_ID,
    EMA_STRATEGY_TEST_ID,
)
from tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request import (
    EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
    EMA_3_19_PLAN_ONLY_INPUT_BINDINGS,
    EMA_3_19_PLAN_ONLY_PACKAGE_REF,
)

EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST_ID = (
    "signal_materialization_request.strategy_test.ema_3_19.plan_only.v1"
)
EMA_3_19_SIGNAL_MATERIALIZATION_PLAN_ID = (
    "signal_materialization_plan.strategy_test.ema_3_19.plan_only.v1"
)
EMA_3_19_SIGNAL_TABLE_ARTIFACT_REF = "artifact.signal_table.ema_3_19.plan_only.v1"
EMA_3_19_SIGNAL_MATERIALIZATION_ARTIFACT_MANIFEST_REF = (
    "artifact_manifest.strategy_test.ema_3_19.signal_materialization.plan_only.v1"
)


def build_ema_3_19_signal_materialization_request() -> SignalMaterializationRequest:
    package = EMA_3_19_STRATEGY_TEST_PACKAGE
    manifest = package.manifest

    return SignalMaterializationRequest(
        request_id=EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST_ID,
        strategy_id=EMA_STRATEGY_ID,
        strategy_version=manifest.strategy_version,
        strategy_test_id=EMA_STRATEGY_TEST_ID,
        package_ref=EMA_3_19_PLAN_ONLY_PACKAGE_REF,
        input_bindings=tuple(binding.binding_id for binding in EMA_3_19_PLAN_ONLY_INPUT_BINDINGS),
        feature_refs=(EMA_FEATURE_REF_ID,),
        signal_refs=(EMA_SIGNAL_REF_ID,),
        output_signal_artifact_ref=EMA_3_19_SIGNAL_TABLE_ARTIFACT_REF,
        materialization_mode="plan_only",
    )


def build_ema_3_19_signal_materialization_plan() -> SignalMaterializationPlan:
    request = EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST

    return SignalMaterializationPlan(
        plan_id=EMA_3_19_SIGNAL_MATERIALIZATION_PLAN_ID,
        request_id=request.request_id,
        strategy_id=request.strategy_id,
        strategy_test_id=request.strategy_test_id,
        signal_table_artifact_ref=request.output_signal_artifact_ref,
        artifact_manifest_ref=EMA_3_19_SIGNAL_MATERIALIZATION_ARTIFACT_MANIFEST_REF,
        write_allowed=False,
        registry_write_allowed=False,
        promotion_verdict_allowed=False,
    )


EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST = build_ema_3_19_signal_materialization_request()
EMA_3_19_SIGNAL_MATERIALIZATION_PLAN = build_ema_3_19_signal_materialization_plan()

assert EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.strategy_id == EMA_STRATEGY_ID


__all__ = [
    "EMA_3_19_SIGNAL_MATERIALIZATION_ARTIFACT_MANIFEST_REF",
    "EMA_3_19_SIGNAL_MATERIALIZATION_PLAN",
    "EMA_3_19_SIGNAL_MATERIALIZATION_PLAN_ID",
    "EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST",
    "EMA_3_19_SIGNAL_MATERIALIZATION_REQUEST_ID",
    "EMA_3_19_SIGNAL_TABLE_ARTIFACT_REF",
    "build_ema_3_19_signal_materialization_plan",
    "build_ema_3_19_signal_materialization_request",
]
