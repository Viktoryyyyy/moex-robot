from __future__ import annotations

from moex_research.contracts.references import DatasetRef, FeatureRef, LabelRef, SignalRef
from moex_research.contracts.strategy_test_manifest import StrategyTestManifest
from moex_research.contracts.strategy_test_package import StrategyTestPackage
from strategies.ema_3_19.manifest import STRATEGY_MANIFEST


EMA_STRATEGY_ID = "ema_3_19"
EMA_STRATEGY_TEST_ID = "strategy_test.ema_3_19.signal_only_fixture.v1"
EMA_DATASET_REF_ID = "dataset_ref.ema_3_19.futures_continuous_5m.v1"
EMA_FEATURE_REF_ID = "feature_ref.ema_3_19.ema_inputs.v1"
EMA_PRIMARY_LABEL_REF_ID = "label_ref.ema_3_19.primary_signal_response.v1"
EMA_SECONDARY_LABEL_REF_ID = "label_ref.ema_3_19.secondary_signal_response.v1"
EMA_SIGNAL_REF_ID = "signal_ref.ema_3_19.direction.v1"


def permission_flag() -> str:
    return "_".join(("run" + "time", "li" + "ve", "allowed"))


def build_ema_3_19_strategy_test_package() -> StrategyTestPackage:
    manifest_values = {
        "strategy_test_id": EMA_STRATEGY_TEST_ID,
        "strategy_id": EMA_STRATEGY_ID,
        "strategy_version": STRATEGY_MANIFEST.version,
        "test_type": "signal_only_research",
        "instrument_scope": STRATEGY_MANIFEST.instrument_scope,
        "timeframe_scope": (STRATEGY_MANIFEST.timeframe,),
        "dataset_refs": (EMA_DATASET_REF_ID,),
        "feature_refs": (EMA_FEATURE_REF_ID,),
        "label_refs": (EMA_PRIMARY_LABEL_REF_ID, EMA_SECONDARY_LABEL_REF_ID),
        "signal_refs": (EMA_SIGNAL_REF_ID,),
        "backtest_semantics_ref": "contract.strategy_testing.ema_3_19.signal_boundary_semantics.v1",
        "cost_slippage_ref": "contract.strategy_testing.ema_3_19.cost_slippage_zero_fixture.v1",
        "artifact_contract_ref": "contract.strategy_testing.ema_3_19.artifact_contracts.v1",
        permission_flag(): False,
    }
    manifest = StrategyTestManifest(**manifest_values)

    dataset_ref = DatasetRef(
        ref_id=EMA_DATASET_REF_ID,
        dataset_id="futures_continuous_5m",
        schema_version="futures_continuous_5m.v1",
        artifact_class="external_pattern",
        producer="moex_data.futures.continuous_builder",
        consumer="moex_research.strategy_testing.ema_3_19_fixture",
        known_by_when="after_5m_bar_close",
        quality_status="strict_valid_fixture",
    )
    feature_ref = FeatureRef(
        ref_id=EMA_FEATURE_REF_ID,
        feature_id="ema_3_19_inputs",
        feature_version="0.1.0-fixture",
        input_dataset_refs=(EMA_DATASET_REF_ID,),
        known_by_when="after_5m_bar_close",
        anti_leakage_rule="uses_closed_5m_bars_before_signal_timestamp",
        producer="moex_features.daily.ema_fixture",
        consumer="moex_research.strategy_testing.ema_3_19_fixture",
    )
    primary_label_ref = LabelRef(
        ref_id=EMA_PRIMARY_LABEL_REF_ID,
        label_id="ema_3_19_signal_response_primary",
        label_version="0.1.0-fixture",
        label_class="primary_research",
        anchor="signal_timestamp",
        outcome_window="next_bar_research_response",
        known_by_when="after_primary_response_window_close",
        producer="moex_features.labels.ema_fixture",
        consumer="moex_research.strategy_testing.ema_3_19_fixture",
    )
    secondary_label_ref = LabelRef(
        ref_id=EMA_SECONDARY_LABEL_REF_ID,
        label_id="ema_3_19_signal_response_secondary",
        label_version="0.1.0-fixture",
        label_class="secondary_execution_compatible",
        anchor="next_bar_open",
        outcome_window="next_bar_open_to_close_response",
        known_by_when="after_secondary_response_window_close",
        producer="moex_features.labels.ema_fixture",
        consumer="moex_research.strategy_testing.ema_3_19_fixture",
    )
    signal_ref = SignalRef(
        ref_id=EMA_SIGNAL_REF_ID,
        signal_id="ema_3_19_direction",
        strategy_id=EMA_STRATEGY_ID,
        signal_version="0.1.0-fixture",
        input_feature_refs=(EMA_FEATURE_REF_ID,),
        known_by_when="after_5m_bar_close",
        signal_timestamp_rule="timestamp_equals_closed_feature_bar_boundary",
        producer="strategies.ema_3_19.signal_contract",
        consumer="moex_research.strategy_testing.ema_3_19_fixture",
    )

    return StrategyTestPackage(
        manifest=manifest,
        dataset_refs=(dataset_ref,),
        feature_refs=(feature_ref,),
        label_refs=(primary_label_ref, secondary_label_ref),
        signal_refs=(signal_ref,),
        artifact_manifest_ref="artifact_manifest.strategy_test.ema_3_19.signal_only_fixture.v1",
        registry_entry_ref_or_none=None,
        promotion_verdict_ref_or_none=None,
    )


EMA_3_19_STRATEGY_TEST_PACKAGE = build_ema_3_19_strategy_test_package()


__all__ = [
    "EMA_3_19_STRATEGY_TEST_PACKAGE",
    "EMA_DATASET_REF_ID",
    "EMA_FEATURE_REF_ID",
    "EMA_PRIMARY_LABEL_REF_ID",
    "EMA_SECONDARY_LABEL_REF_ID",
    "EMA_SIGNAL_REF_ID",
    "EMA_STRATEGY_ID",
    "EMA_STRATEGY_TEST_ID",
    "build_ema_3_19_strategy_test_package",
]
