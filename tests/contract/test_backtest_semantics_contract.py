import pytest

from moex_backtest.costs.models import ALLOWED_COST_MODEL_IDS, ALLOWED_SLIPPAGE_MODEL_IDS, CostSlippageModelSpec
from moex_backtest.fills.models import ALLOWED_FILL_MODEL_IDS, FillModelSpec
from moex_backtest.reports.artifacts import REQUIRED_CANONICAL_BACKTEST_ARTIFACT_NAMES
from moex_backtest.semantics.contracts import BacktestSemanticsContract, REQUIRED_BACKTEST_SEMANTICS_FIELDS


def _semantics_kwargs():
    return {
        "signal_timestamp_rule": "signals are timestamped at setup bar close",
        "known_by_when_rule": "all signal inputs are known by setup bar close",
        "execution_delay_rule": "execution starts no earlier than the next tradable bar",
        "execution_price_rule": "default execution price is the next bar open",
        "fill_rule": "fills are simulated only by a declared fill model id",
        "position_transition_rule": "positions transition through the canonical position path builder",
        "reversal_rule": "reversals flatten before opening the opposite side unless a contract says otherwise",
        "sizing_rule": "sizing uses declared target exposure inputs only",
        "cost_slippage_rule": "cost and slippage are applied after fills through declared model ids",
        "terminal_close_rule": "open positions are force-closed by the configured terminal close rule",
        "missing_bar_rule": "missing execution bars fail closed unless explicitly skipped by contract",
        "invalid_data_rule": "invalid OHLC or non-monotonic data fails closed",
        "calendar_session_rule": "session membership is resolved by the canonical calendar/session contract",
        "aggregation_rule": "reported metrics aggregate from canonical position path outputs only",
        "anti_leakage_invariants": (
            "signals must be timestamped at the decision point only",
            "features used for a decision must be known no later than known_by_when_rule",
            "execution prices must occur at or after the configured execution delay",
        ),
    }


def test_backtest_semantics_contract_declares_required_fields():
    assert REQUIRED_BACKTEST_SEMANTICS_FIELDS == (
        "signal_timestamp_rule",
        "known_by_when_rule",
        "execution_delay_rule",
        "execution_price_rule",
        "fill_rule",
        "position_transition_rule",
        "reversal_rule",
        "sizing_rule",
        "cost_slippage_rule",
        "terminal_close_rule",
        "missing_bar_rule",
        "invalid_data_rule",
        "calendar_session_rule",
        "aggregation_rule",
        "anti_leakage_invariants",
    )

    contract = BacktestSemanticsContract(**_semantics_kwargs())

    for field_name in REQUIRED_BACKTEST_SEMANTICS_FIELDS:
        assert hasattr(contract, field_name)
    assert contract.anti_leakage_invariants


@pytest.mark.parametrize("field_name", REQUIRED_BACKTEST_SEMANTICS_FIELDS)
def test_backtest_semantics_contract_rejects_empty_required_fields(field_name):
    kwargs = _semantics_kwargs()
    kwargs[field_name] = () if field_name == "anti_leakage_invariants" else ""

    with pytest.raises(ValueError):
        BacktestSemanticsContract(**kwargs)


def test_fill_model_skeleton_allows_declared_fill_model_ids_only():
    assert ALLOWED_FILL_MODEL_IDS == frozenset({"next_bar_open", "next_bar_close", "next_tick"})
    assert FillModelSpec(fill_model_id="next_bar_open").fill_model_id == "next_bar_open"

    with pytest.raises(ValueError):
        FillModelSpec(fill_model_id="strategy_specific_fill")


def test_cost_slippage_model_skeleton_allows_declared_model_ids_only():
    assert ALLOWED_COST_MODEL_IDS == frozenset({"zero_cost", "fixed_bps", "commission_bps"})
    assert ALLOWED_SLIPPAGE_MODEL_IDS == frozenset({"zero_slippage", "fixed_bps", "spread_bps"})
    assert CostSlippageModelSpec(cost_model_id="zero_cost", slippage_model_id="zero_slippage")

    with pytest.raises(ValueError):
        CostSlippageModelSpec(cost_model_id="strategy_specific_cost", slippage_model_id="zero_slippage")
    with pytest.raises(ValueError):
        CostSlippageModelSpec(cost_model_id="zero_cost", slippage_model_id="strategy_specific_slippage")


def test_report_artifacts_skeleton_declares_required_canonical_outputs():
    assert REQUIRED_CANONICAL_BACKTEST_ARTIFACT_NAMES == (
        "backtest_run_metadata.json",
        "backtest_semantics_contract.json",
        "backtest_fill_table.parquet",
        "backtest_cost_slippage_table.parquet",
        "backtest_position_path.parquet",
        "backtest_metrics.json",
        "backtest_report.md",
    )
