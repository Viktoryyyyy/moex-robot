import pytest

from moex_backtest.engine.canonical import (
    CanonicalBacktestEngine,
    CanonicalBacktestInput,
    CostConfig,
    ExecutionConfig,
)


def _bar(timestamp, open_price, close_price=None, valid=True):
    return {
        "timestamp": timestamp,
        "open": open_price,
        "close": open_price if close_price is None else close_price,
        "valid": valid,
    }


def _signal(timestamp, target_position):
    return {"timestamp": timestamp, "target_position": target_position}


def test_next_bar_open_execution_and_no_lookahead_invariant():
    bars = (
        _bar(0, 100.0),
        _bar(1, 110.0, 111.0),
        _bar(2, 900.0, 999.0),
    )
    signals = (_signal(0, 1),)
    request = CanonicalBacktestInput(
        bars=bars,
        signals=signals,
        execution_config=ExecutionConfig(terminal_close=False),
    )

    result = CanonicalBacktestEngine().run(request)

    assert result.fills[0]["execution_timestamp"] == 1
    assert result.fills[0]["raw_price"] == 110.0
    assert result.fills[0]["fill_price"] == 110.0
    assert result.fills[0]["position_after"] == 1
    assert result.metrics["final_position"] == 1

    changed_future_bars = (
        _bar(0, 100.0),
        _bar(1, 110.0, 111.0),
        _bar(2, 1_000_000.0, 1_000_000.0),
    )
    changed_result = CanonicalBacktestEngine().run(
        CanonicalBacktestInput(
            bars=changed_future_bars,
            signals=signals,
            execution_config=ExecutionConfig(terminal_close=False),
        )
    )

    assert changed_result.fills[0]["execution_timestamp"] == result.fills[0]["execution_timestamp"]
    assert changed_result.fills[0]["fill_price"] == result.fills[0]["fill_price"]


def test_commission_and_slippage_models_are_applied_to_trade_records():
    bars = (
        _bar(0, 100.0),
        _bar(1, 100.0, 101.0),
    )
    result = CanonicalBacktestEngine().run(
        CanonicalBacktestInput(
            bars=bars,
            signals=(_signal(0, 1),),
            cost_config=CostConfig(commission_bps=10.0, slippage_bps=5.0),
            execution_config=ExecutionConfig(terminal_close=False),
        )
    )

    assert result.fills[0]["raw_price"] == 100.0
    assert result.fills[0]["fill_price"] == pytest.approx(100.05)
    assert result.costs[0]["commission"] == pytest.approx(0.10)
    assert result.costs[0]["slippage_cost"] == pytest.approx(0.05)
    assert result.metrics["total_commission"] == pytest.approx(0.10)
    assert result.metrics["total_slippage_cost"] == pytest.approx(0.05)


def test_flat_long_short_flat_and_reversal_transitions_are_trade_records():
    bars = tuple(_bar(timestamp, 100.0 + timestamp, 100.0 + timestamp) for timestamp in range(8))
    signals = (
        _signal(0, 1),
        _signal(1, 0),
        _signal(2, -1),
        _signal(3, 1),
        _signal(4, -1),
        _signal(5, 0),
    )

    result = CanonicalBacktestEngine().run(
        CanonicalBacktestInput(
            bars=bars,
            signals=signals,
            execution_config=ExecutionConfig(terminal_close=False),
        )
    )

    assert tuple(fill["transition_type"] for fill in result.fills) == (
        "flat_to_long",
        "long_to_flat",
        "flat_to_short",
        "short_to_long_reversal",
        "long_to_short_reversal",
        "short_to_flat",
    )
    assert tuple(fill["quantity"] for fill in result.fills) == (1, -1, -1, 2, -2, 1)
    assert result.metrics["final_position"] == 0


def test_forced_terminal_close_uses_last_valid_bar_close_and_flattens_position():
    bars = (
        _bar(0, 100.0, 100.0),
        _bar(1, 101.0, 102.0),
        _bar(2, 103.0, 104.0),
    )

    result = CanonicalBacktestEngine().run(
        CanonicalBacktestInput(
            bars=bars,
            signals=(_signal(0, 1),),
            execution_config=ExecutionConfig(terminal_close=True),
        )
    )

    assert len(result.fills) == 2
    assert result.fills[0]["reason"] == "signal"
    assert result.fills[1]["reason"] == "forced_terminal_close"
    assert result.fills[1]["execution_timestamp"] == 2
    assert result.fills[1]["raw_price"] == 104.0
    assert result.fills[1]["transition_type"] == "long_to_flat"
    assert result.metrics["forced_close_count"] == 1
    assert result.metrics["final_position"] == 0


def test_invalid_next_bar_and_missing_next_bar_are_rejected_fail_closed():
    bars = (
        _bar(0, 100.0),
        _bar(1, None, 101.0),
        _bar(2, 102.0),
    )
    signals = (
        _signal(0, 1),
        _signal(2, -1),
    )

    result = CanonicalBacktestEngine().run(
        CanonicalBacktestInput(
            bars=bars,
            signals=signals,
            execution_config=ExecutionConfig(terminal_close=True),
        )
    )

    assert result.fills == ()
    assert result.costs == ()
    assert result.position_path == ()
    assert result.metrics["rejected_signal_count"] == 2
    assert tuple(item["reject_reason"] for item in result.artifacts["rejected_signals"]) == (
        "invalid_next_bar",
        "missing_next_bar",
    )


def test_result_object_is_structured_not_stdout_only():
    result = CanonicalBacktestEngine().run(
        CanonicalBacktestInput(
            bars=(_bar(0, 100.0), _bar(1, 101.0, 102.0)),
            signals=(_signal(0, 1),),
            execution_config=ExecutionConfig(terminal_close=False),
        )
    )

    assert result.fills
    assert result.costs
    assert result.position_path
    assert result.metrics["engine_id"] == "canonical_backtest_engine_minimal_v1"
    assert result.artifacts["execution_semantics"]["execution_delay_rule"] == "strict next bar after signal timestamp"
    assert result.artifacts["trade_records"] == result.fills
