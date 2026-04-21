from __future__ import annotations

from src.moex_strategy_sdk.artifact_contracts import ArtifactContract


ARTIFACT_CONTRACTS = (
    ArtifactContract(
        artifact_id="research_usdrubf_5m_full_history_dataset",
        artifact_role="input",
        contract_class="repo_relative",
        producer="src.api.futures.fo_5m_period_paged",
        consumers=("moex_features.daily.usdrubf_large_day_mr_day_pairs",),
        format="csv",
        schema_version=1,
        partitioning_rule="unpartitioned",
        retention_policy=None,
        locator_ref="data/master/usdrubf_5m_2022-04-26_2026-04-06.csv",
    ),
    ArtifactContract(
        artifact_id="research_usdrubf_large_day_mr_day_pairs_feature_frame",
        artifact_role="input",
        contract_class="repo_relative",
        producer="moex_features",
        consumers=("usdrubf_large_day_mr.signal_engine", "usdrubf_large_day_mr.backtest_adapter"),
        format="csv",
        schema_version=1,
        partitioning_rule="unpartitioned",
        retention_policy=None,
        locator_ref="data/research/usdrubf_large_day_mr_day_pairs.csv",
    ),
    ArtifactContract(
        artifact_id="usdrubf_large_day_mr_backtest_day_metrics",
        artifact_role="output",
        contract_class="external_pattern",
        producer="moex_backtest",
        consumers=("backtest_verdict_layer",),
        format="csv",
        schema_version=1,
        partitioning_rule="by_run_id",
        retention_policy=None,
        locator_ref="data/backtests/usdrubf_large_day_mr_day_metrics_{run_id}.csv",
    ),
)
