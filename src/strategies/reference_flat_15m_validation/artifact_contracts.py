from __future__ import annotations

from src.moex_strategy_sdk.artifact_contracts import ArtifactContract


ARTIFACT_CONTRACTS = (
    ArtifactContract(
        artifact_id="reference_flat_15m_validation_signal_state",
        artifact_role="state",
        contract_class="external_pattern",
        producer="moex_runtime",
        consumers=("reference_flat_15m_validation.live_adapter",),
        format="json",
        schema_version=1,
        partitioning_rule="by_trade_date",
        retention_policy="retain_last_trade_date",
        locator_ref="data/state/reference_flat_15m_validation_signal_state_{trade_date}.json",
    ),
    ArtifactContract(
        artifact_id="reference_flat_15m_validation_trade_log",
        artifact_role="output",
        contract_class="external_pattern",
        producer="moex_runtime",
        consumers=("runtime_operator",),
        format="csv",
        schema_version=1,
        partitioning_rule="by_trade_date",
        retention_policy=None,
        locator_ref="data/signals/reference_flat_15m_validation_realtime_{trade_date}.csv",
    ),
)
