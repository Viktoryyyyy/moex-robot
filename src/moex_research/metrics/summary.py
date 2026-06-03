from __future__ import annotations

from moex_backtest.engine.interfaces import BacktestResult
from moex_research.metrics.schemas import MetricRecord, MetricsSummary, validate_metrics_summary

_PRODUCER_COMPONENT = "moex_research.metrics.summary"
_CONSUMER_COMPONENT = "PM_L3_DELIVERY_VALIDATION_OWNER"


def build_backtest_metrics_summary(
    *,
    run_id: str,
    strategy_id: str,
    result_status: str,
    canonicality_status: str,
    metrics_artifact_ref: str,
    backtest_result: BacktestResult,
) -> MetricsSummary:
    if not isinstance(backtest_result, BacktestResult):
        raise TypeError("backtest_result must be BacktestResult")
    metrics = backtest_result.metrics
    summary = MetricsSummary(
        run_id=run_id,
        strategy_id=strategy_id,
        test_type="fixture_dry_run_research_runner",
        scope_level="single_strategy_fixture",
        result_status=result_status,
        canonicality_status=canonicality_status,
        metric_schema_version="research_runner_metrics.v1",
        metric_records=(
            _record("signal_count", metrics["signal_count"], "count"),
            _record("trade_count", metrics["trade_count"], "count"),
            _record("rejected_signal_count", metrics["rejected_signal_count"], "count"),
            _record("ending_equity", metrics["ending_equity"], "currency_units"),
            _record("total_pnl", metrics["total_pnl"], "currency_units"),
            _record("total_cost", metrics["total_cost"], "currency_units"),
        ),
        artifact_ref=metrics_artifact_ref,
    )
    return validate_metrics_summary(summary)


def _record(metric_name: str, metric_value: object, metric_unit: str) -> MetricRecord:
    return MetricRecord(
        metric_id="research_runner." + metric_name,
        metric_name=metric_name,
        metric_value=metric_value,
        metric_unit=metric_unit,
        scope="single_strategy_fixture",
        gross_or_net="net",
        producer=_PRODUCER_COMPONENT,
        consumer=_CONSUMER_COMPONENT,
    )


__all__ = ["build_backtest_metrics_summary"]
