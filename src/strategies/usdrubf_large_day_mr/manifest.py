from __future__ import annotations

from src.moex_strategy_sdk.manifest import StrategyManifest


STRATEGY_MANIFEST = StrategyManifest(
    strategy_id="usdrubf_large_day_mr",
    version="1.0.0",
    instrument_scope=("usdrubf",),
    timeframe="1d",
    required_datasets=("research_usdrubf_5m_full_history",),
    required_features=("research_usdrubf_large_day_mr_day_pairs",),
    required_labels=(),
    supports_backtest=True,
    supports_live=False,
    report_schema_version=1,
    artifact_contract_version=1,
    tags=("phase9", "usdrubf", "mean_reversion", "backtest_only"),
)
