def test_strategy_sdk_interfaces_contract_smoke():
    from moex_strategy_sdk.interfaces import BacktestAdapter, LiveAdapter, SignalEngine
    assert hasattr(SignalEngine, "generate_signals")
    assert hasattr(BacktestAdapter, "to_backtest_inputs")
    assert hasattr(LiveAdapter, "to_live_intents")
