import inspect

from moex_strategy_sdk.interfaces import BacktestAdapter, LiveAdapter, SignalEngine


FORBIDDEN_SIGNAL_ENGINE_NAMES = {
    "open",
    "read",
    "write",
    "glob",
    "path",
    "artifact",
    "fill",
    "pnl",
    "lock",
    "state",
    "request",
    "network",
}


def test_signal_engine_exposes_deterministic_signal_generation_only():
    public_methods = [name for name, value in inspect.getmembers(SignalEngine) if not name.startswith("_") and callable(value)]

    assert public_methods == ["generate_signals"]

    signature = inspect.signature(SignalEngine.generate_signals)
    assert "features" in signature.parameters
    assert "config" in signature.parameters

    names = set(SignalEngine.generate_signals.__code__.co_names)
    assert names.isdisjoint(FORBIDDEN_SIGNAL_ENGINE_NAMES)


def test_backtest_adapter_maps_outputs_to_canonical_backtest_inputs_only():
    public_methods = [name for name, value in inspect.getmembers(BacktestAdapter) if not name.startswith("_") and callable(value)]

    assert public_methods == ["to_backtest_inputs"]

    signature = inspect.signature(BacktestAdapter.to_backtest_inputs)
    assert "signals" in signature.parameters
    assert "context" in signature.parameters


def test_live_adapter_exists_without_authorizing_runtime_or_live():
    public_methods = [name for name, value in inspect.getmembers(LiveAdapter) if not name.startswith("_") and callable(value)]

    assert public_methods == ["to_live_intents"]

    signature = inspect.signature(LiveAdapter.to_live_intents)
    assert "signals" in signature.parameters
    assert "context" in signature.parameters
    assert "authorize" not in LiveAdapter.to_live_intents.__code__.co_names
    assert "enable" not in LiveAdapter.to_live_intents.__code__.co_names
