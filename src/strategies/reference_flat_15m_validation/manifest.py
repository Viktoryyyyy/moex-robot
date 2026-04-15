from __future__ import annotations

from src.moex_strategy_sdk.manifest import StrategyManifest


STRATEGY_MANIFEST = StrategyManifest(
    strategy_id="reference_flat_15m_validation",
    version="1.0.0",
    instrument_scope=("si",),
    timeframe="15m",
    required_datasets=("si_fo_5m_intraday",),
    required_features=("si_15m_ohlc_from_5m",),
    required_labels=(),
    supports_backtest=False,
    supports_live=True,
    report_schema_version=1,
    artifact_contract_version=1,
    tags=("reference_slice", "validation", "flat", "15m"),
)
