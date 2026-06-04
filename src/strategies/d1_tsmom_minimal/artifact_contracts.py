from __future__ import annotations

from moex_strategy_sdk import ArtifactContract, validate_artifact_contract

from .manifest import STRATEGY_ID

ARTIFACT_CONTRACTS = tuple(
    validate_artifact_contract(contract)
    for contract in (
        ArtifactContract(
            artifact_id="d1_tsmom_minimal.input_features.v1",
            artifact_class="input_contract",
            producer="moex_features.daily.tsmom_minimal",
            consumer=f"strategies.{STRATEGY_ID}.signal_engine",
            format="mapping.rows",
            schema_version="d1_tsmom_minimal_features.v1",
            contract_class="external_pattern",
            locator_ref="${MOEX_DATA_ROOT}/futures/features/d1_tsmom_minimal/family={FAMILY}/part.parquet",
        ),
        ArtifactContract(
            artifact_id="d1_tsmom_minimal.output_signals.v1",
            artifact_class="output_contract",
            producer=f"strategies.{STRATEGY_ID}.signal_engine",
            consumer=f"strategies.{STRATEGY_ID}.backtest_adapter",
            format="tuple.mapping",
            schema_version="d1_tsmom_minimal_signals.v1",
            contract_class="repo_relative",
            locator_ref="src/strategies/d1_tsmom_minimal/signal_engine.py",
        ),
        ArtifactContract(
            artifact_id="d1_tsmom_minimal.backtest_input.v1",
            artifact_class="adapter_contract",
            producer=f"strategies.{STRATEGY_ID}.backtest_adapter",
            consumer="moex_backtest.engine.canonical.CanonicalBacktestEngine",
            format="CanonicalBacktestInput",
            schema_version="canonical_backtest_input.v1",
            contract_class="repo_relative",
            locator_ref="src/strategies/d1_tsmom_minimal/backtest_adapter.py",
        ),
        ArtifactContract(
            artifact_id="d1_tsmom_minimal.live_blocked.v1",
            artifact_class="state_contract",
            producer=f"strategies.{STRATEGY_ID}.live_adapter",
            consumer="moex_runtime.blocked",
            format="UnsupportedModeError",
            schema_version="d1_tsmom_minimal_live_blocked.v1",
            contract_class="repo_relative",
            locator_ref="src/strategies/d1_tsmom_minimal/live_adapter.py",
        ),
    )
)

__all__ = ["ARTIFACT_CONTRACTS"]
