from __future__ import annotations

from typing import Final

REQUIRED_CANONICAL_BACKTEST_ARTIFACT_NAMES: Final[tuple[str, ...]] = (
    "backtest_run_metadata.json",
    "backtest_semantics_contract.json",
    "backtest_fill_table.parquet",
    "backtest_cost_slippage_table.parquet",
    "backtest_position_path.parquet",
    "backtest_metrics.json",
    "backtest_report.md",
)


def validate_required_backtest_artifact_names(artifact_names: tuple[str, ...]) -> tuple[str, ...]:
    missing = set(REQUIRED_CANONICAL_BACKTEST_ARTIFACT_NAMES).difference(artifact_names)
    if missing:
        raise ValueError("missing required canonical backtest artifacts")
    return artifact_names
