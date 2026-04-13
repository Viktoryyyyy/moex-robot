from __future__ import annotations

from src.moex_strategy_sdk.manifest import StrategyManifest


STRATEGY_MANIFEST = StrategyManifest(
    strategy_id="ema_3_19_15m",
    version="1.0.0",
    instrument_scope=("si",),
    timeframe="15m",
    required_datasets=("si_fo_5m_intraday",),
    required_features=("si_15m_ohlc_from_5m",),
    required_labels=(),
    supports_backtest=True,
    supports_live=False,
    report_schema_version=1,
    artifact_contract_version=1,
    tags=("reference_slice", "ema", "15m"),
)
