from __future__ import annotations

from moex_strategy_sdk import StrategyManifest, validate_strategy_manifest

STRATEGY_ID = "reference_fixture_strategy"

MANIFEST = validate_strategy_manifest(
    StrategyManifest(
        strategy_id=STRATEGY_ID,
        version="0.1.0",
        instrument_scope=("Si",),
        timeframe="D1",
        required_datasets=("dataset.futures_derived_d1.v1",),
        required_features=("feature.d1_tsmom_inputs.v1",),
        required_labels=("label.reference_fixture_none.v1",),
        supports_backtest=True,
        supports_live=False,
        report_schema_version="reference_fixture_report.v1",
        artifact_contract_version="reference_fixture_artifacts.v1",
        tags=("fixture", "contract_only", "no_market_hypothesis"),
        owner="moex_bot_platform",
        default_portfolio_group="portfolio.single_strategy_research.v1",
        default_risk_profile="blocked_live",
    )
)

__all__ = ["MANIFEST", "STRATEGY_ID"]
