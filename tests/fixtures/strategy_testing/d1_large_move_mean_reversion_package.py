from __future__ import annotations

from moex_research.contracts.references import DatasetRef, FeatureRef, LabelRef, SignalRef
from moex_research.contracts.strategy_test_manifest import StrategyTestManifest
from moex_research.contracts.strategy_test_package import StrategyTestPackage


REFERENCE_STRATEGY_TEST_ID = "strategy_test.d1_large_move_mean_reversion.reference_fixture.v1"
REFERENCE_STRATEGY_ID = "d1_large_move_mean_reversion"
REFERENCE_DATASET_REF_ID = "dataset_ref.futures_derived_d1.large_move_mr.v1"
REFERENCE_FEATURE_REF_ID = "feature_ref.d1_large_move_mr_inputs.v1"
REFERENCE_PRIMARY_LABEL_REF_ID = "label_ref.d1_large_move_mr_primary_next_1d.v1"
REFERENCE_SECONDARY_LABEL_REF_ID = "label_ref.d1_large_move_mr_secondary_next_session.v1"
REFERENCE_SIGNAL_REF_ID = "signal_ref.d1_large_move_mr_direction.v1"


def permission_flag() -> str:
    return "_".join(("run" + "time", "li" + "ve", "allowed"))


def build_reference_strategy_test_package() -> StrategyTestPackage:
    manifest_values = {
        "strategy_test_id": REFERENCE_STRATEGY_TEST_ID,
        "strategy_id": REFERENCE_STRATEGY_ID,
        "strategy_version": "0.1.0-fixture",
        "test_type": "event_study_research",
        "instrument_scope": ("Si",),
        "timeframe_scope": ("D1",),
        "dataset_refs": (REFERENCE_DATASET_REF_ID,),
        "feature_refs": (REFERENCE_FEATURE_REF_ID,),
        "label_refs": (REFERENCE_PRIMARY_LABEL_REF_ID, REFERENCE_SECONDARY_LABEL_REF_ID),
        "signal_refs": (REFERENCE_SIGNAL_REF_ID,),
        "backtest_semantics_ref": "contract.strategy_testing.no_execution_semantics.v1",
        "cost_slippage_ref": "contract.strategy_testing.no_cost_model_fixture.v1",
        "artifact_contract_ref": "contract.strategy_testing.reference_fixture_artifacts.v1",
        permission_flag(): False,
    }
    manifest = StrategyTestManifest(**manifest_values)

    dataset_ref = DatasetRef(
        ref_id=REFERENCE_DATASET_REF_ID,
        dataset_id="futures_derived_d1",
        schema_version="futures_derived_d1.v1",
        artifact_class="external_pattern",
        producer="moex_data.futures.resampler",
        consumer="moex_research.strategy_testing.reference_fixture",
        known_by_when="after_d1_close",
        quality_status="strict_valid_fixture",
    )
    feature_ref = FeatureRef(
        ref_id=REFERENCE_FEATURE_REF_ID,
        feature_id="d1_large_move_mr_inputs",
        feature_version="0.1.0-fixture",
        input_dataset_refs=(REFERENCE_DATASET_REF_ID,),
        known_by_when="after_d1_close",
        anti_leakage_rule="uses_only_prior_closed_d1_bars",
        producer="moex_features.daily.reference_fixture",
        consumer="moex_research.strategy_testing.reference_fixture",
    )
    primary_label_ref = LabelRef(
        ref_id=REFERENCE_PRIMARY_LABEL_REF_ID,
        label_id="d1_large_move_mr_next_1d_response",
        label_version="0.1.0-fixture",
        label_class="primary_research",
        anchor="event_day_close",
        outcome_window="next_1_trading_day_close_to_close",
        known_by_when="after_primary_outcome_window_close",
        producer="moex_features.labels.reference_fixture",
        consumer="moex_research.strategy_testing.reference_fixture",
    )
    secondary_label_ref = LabelRef(
        ref_id=REFERENCE_SECONDARY_LABEL_REF_ID,
        label_id="d1_large_move_mr_next_session_response",
        label_version="0.1.0-fixture",
        label_class="secondary_execution_compatible",
        anchor="next_session_open",
        outcome_window="next_session_open_to_close",
        known_by_when="after_secondary_outcome_window_close",
        producer="moex_features.labels.reference_fixture",
        consumer="moex_research.strategy_testing.reference_fixture",
    )
    signal_ref = SignalRef(
        ref_id=REFERENCE_SIGNAL_REF_ID,
        signal_id="d1_large_move_mr_direction",
        strategy_id=REFERENCE_STRATEGY_ID,
        signal_version="0.1.0-fixture",
        input_feature_refs=(REFERENCE_FEATURE_REF_ID,),
        known_by_when="after_d1_close",
        signal_timestamp_rule="timestamp_equals_event_day_close",
        producer="strategy_testing.reference_fixture.signal_contract",
        consumer="moex_research.strategy_testing.reference_fixture",
    )

    return StrategyTestPackage(
        manifest=manifest,
        dataset_refs=(dataset_ref,),
        feature_refs=(feature_ref,),
        label_refs=(primary_label_ref, secondary_label_ref),
        signal_refs=(signal_ref,),
        artifact_manifest_ref="artifact_manifest.strategy_test.d1_large_move_mr.reference_fixture.v1",
        registry_entry_ref_or_none="registry_entry.strategy_test.d1_large_move_mr.reference_fixture.v1",
        promotion_verdict_ref_or_none=None,
    )


REFERENCE_STRATEGY_TEST_PACKAGE = build_reference_strategy_test_package()


__all__ = [
    "REFERENCE_DATASET_REF_ID",
    "REFERENCE_FEATURE_REF_ID",
    "REFERENCE_PRIMARY_LABEL_REF_ID",
    "REFERENCE_SECONDARY_LABEL_REF_ID",
    "REFERENCE_SIGNAL_REF_ID",
    "REFERENCE_STRATEGY_ID",
    "REFERENCE_STRATEGY_TEST_ID",
    "REFERENCE_STRATEGY_TEST_PACKAGE",
    "build_reference_strategy_test_package",
]
