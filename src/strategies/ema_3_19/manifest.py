from __future__ import annotations

from moex_strategy_sdk import StrategyManifest, validate_strategy_manifest

STRATEGY_ID = "ema_3_19"

STRATEGY_MANIFEST = validate_strategy_manifest(
    StrategyManifest(
        strategy_id=STRATEGY_ID,
        version="0.1.0",
        instrument_scope=("Si",),
        timeframe="15m",
        required_datasets=("futures_continuous_5m",),
        required_features=("ema_3", "ema_19"),
        required_labels=("signal_direction",),
        supports_backtest=True,
        supports_live=False,
        report_schema_version="strategy_report.v1",
        artifact_contract_version="strategy_artifacts.v1",
        tags=("framework_validation", "skeleton"),
        owner="moex_bot_pm",
    )
)

__all__ = ["STRATEGY_ID", "STRATEGY_MANIFEST"]
