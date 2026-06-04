from __future__ import annotations

from moex_strategy_sdk import StrategyManifest, validate_strategy_manifest

STRATEGY_ID = "d1_tsmom_minimal"

MANIFEST = validate_strategy_manifest(
    StrategyManifest(
        strategy_id=STRATEGY_ID,
        version="0.1.0",
        instrument_scope=("Si", "USDRUBF"),
        timeframe="D1",
        required_datasets=("dataset.futures_derived_d1.v1",),
        required_features=("feature.d1_tsmom_minimal_returns.v1",),
        required_labels=("label.d1_tsmom_minimal_forward_return.v1",),
        supports_backtest=True,
        supports_live=False,
        report_schema_version="d1_tsmom_minimal_report.v1",
        artifact_contract_version="d1_tsmom_minimal_artifacts.v1",
        tags=("real_data_ready", "research_only", "platform_run_ref"),
        owner="moex_bot_platform",
        default_portfolio_group="portfolio.single_strategy_research.v1",
        default_risk_profile="blocked_live",
    )
)

__all__ = ["MANIFEST", "STRATEGY_ID"]
