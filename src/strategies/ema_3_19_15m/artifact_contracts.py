from __future__ import annotations

from src.moex_strategy_sdk.artifact_contracts import ArtifactContract


ARTIFACT_CONTRACTS = (
    ArtifactContract(
        artifact_id="si_15m_ohlc_feature_frame",
        artifact_role="input",
        contract_class="external_pattern",
        producer="moex_features",
        consumers=("ema_3_19_15m.signal_engine", "ema_3_19_15m.backtest_adapter", "ema_3_19_15m.live_adapter"),
        format="parquet",
        schema_version=1,
        partitioning_rule="by_instrument_trade_date",
        retention_policy=None,
        locator_ref="data/features/si_15m_ohlc_from_5m_{trade_date}.parquet",
    ),
    ArtifactContract(
        artifact_id="ema_3_19_15m_backtest_day_metrics",
        artifact_role="output",
        contract_class="external_pattern",
        producer="moex_backtest",
        consumers=("backtest_verdict_layer",),
        format="csv",
        schema_version=1,
        partitioning_rule="by_trade_date",
        retention_policy=None,
        locator_ref="data/backtests/ema_3_19_15m_day_metrics_{run_id}.csv",
    ),
    ArtifactContract(
        artifact_id="ema_3_19_15m_signal_state",
        artifact_role="state",
        contract_class="external_pattern",
        producer="moex_runtime",
        consumers=("ema_3_19_15m.live_adapter",),
        format="json",
        schema_version=1,
        partitioning_rule="by_trade_date",
        retention_policy="retain_last_trade_date",
        locator_ref="data/state/ema_3_19_15m_signal_state_{trade_date}.json",
    ),
    ArtifactContract(
        artifact_id="ema_3_19_15m_trade_log",
        artifact_role="output",
        contract_class="external_pattern",
        producer="moex_runtime",
        consumers=("runtime_operator",),
        format="csv",
        schema_version=1,
        partitioning_rule="by_trade_date",
        retention_policy=None,
        locator_ref="data/signals/ema_3_19_15m_realtime_{trade_date}.csv",
    ),
)
