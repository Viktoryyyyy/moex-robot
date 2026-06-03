from __future__ import annotations

from moex_strategy_sdk import ArtifactContract, validate_artifact_contract

from .manifest import STRATEGY_ID

ARTIFACT_CONTRACTS = tuple(
    validate_artifact_contract(contract)
    for contract in (
        ArtifactContract(
            artifact_id="reference_fixture_strategy.input_features.v1",
            artifact_class="input_contract",
            producer="moex_features.fixture",
            consumer=f"strategies.{STRATEGY_ID}.signal_engine",
            format="mapping.rows",
            schema_version="reference_fixture_features.v1",
            contract_class="cli_argument",
            locator_ref="features.rows",
        ),
        ArtifactContract(
            artifact_id="reference_fixture_strategy.output_signals.v1",
            artifact_class="output_contract",
            producer=f"strategies.{STRATEGY_ID}.signal_engine",
            consumer=f"strategies.{STRATEGY_ID}.backtest_adapter",
            format="tuple.mapping",
            schema_version="reference_fixture_signals.v1",
            contract_class="repo_relative",
            locator_ref="src/strategies/reference_fixture_strategy/signal_engine.py",
        ),
        ArtifactContract(
            artifact_id="reference_fixture_strategy.backtest_input.v1",
            artifact_class="adapter_contract",
            producer=f"strategies.{STRATEGY_ID}.backtest_adapter",
            consumer="moex_backtest.engine.canonical.CanonicalBacktestEngine",
            format="CanonicalBacktestInput",
            schema_version="canonical_backtest_input.v1",
            contract_class="repo_relative",
            locator_ref="src/strategies/reference_fixture_strategy/backtest_adapter.py",
        ),
        ArtifactContract(
            artifact_id="reference_fixture_strategy.live_blocked.v1",
            artifact_class="state_contract",
            producer=f"strategies.{STRATEGY_ID}.live_adapter",
            consumer="moex_runtime.blocked",
            format="UnsupportedModeError",
            schema_version="reference_fixture_live_blocked.v1",
            contract_class="repo_relative",
            locator_ref="src/strategies/reference_fixture_strategy/live_adapter.py",
        ),
    )
)

__all__ = ["ARTIFACT_CONTRACTS"]
