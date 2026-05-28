from __future__ import annotations

from typing import Final

STRATEGY_TESTING_CONTRACT_DOC_PATHS: Final[tuple[str, ...]] = (
    "contracts/strategy_testing/strategy_test_manifest.v1.md",
    "contracts/strategy_testing/dataset_reference_contract.v1.md",
    "contracts/strategy_testing/feature_contract.v1.md",
    "contracts/strategy_testing/label_contract.v1.md",
    "contracts/strategy_testing/signal_contract.v1.md",
    "contracts/strategy_testing/canonical_backtest_semantics.v1.md",
    "contracts/strategy_testing/cost_slippage_contract.v1.md",
    "contracts/strategy_testing/result_artifact_contract.v1.md",
    "contracts/strategy_testing/artifact_manifest.v1.md",
    "contracts/strategy_testing/experiment_registry_entry.v1.md",
    "contracts/strategy_testing/promotion_verdict.v1.md",
)

__all__ = ["STRATEGY_TESTING_CONTRACT_DOC_PATHS"]
