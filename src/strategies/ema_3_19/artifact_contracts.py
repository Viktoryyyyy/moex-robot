from __future__ import annotations

from moex_strategy_sdk import ArtifactContract, validate_artifact_contract

ARTIFACT_CONTRACTS = tuple(
    validate_artifact_contract(contract)
    for contract in (
        ArtifactContract(
            artifact_id="ema_3_19.input_features.v1",
            artifact_class="input",
            producer="moex_features.daily",
            consumer="strategies.ema_3_19.signal_engine",
            format="table",
            schema_version="ema_3_19.input_features.v1",
            contract_class="external_pattern",
        ),
        ArtifactContract(
            artifact_id="ema_3_19.signal_output.v1",
            artifact_class="signal_output",
            producer="strategies.ema_3_19.signal_engine",
            consumer="moex_backtest.engine",
            format="table",
            schema_version="ema_3_19.signal_output.v1",
            contract_class="external_pattern",
        ),
        ArtifactContract(
            artifact_id="ema_3_19.report.v1",
            artifact_class="report",
            producer="moex_backtest.reports",
            consumer="moex_research.publishers",
            format="json",
            schema_version="ema_3_19.report.v1",
            contract_class="external_pattern",
        ),
    )
)

__all__ = ["ARTIFACT_CONTRACTS"]
