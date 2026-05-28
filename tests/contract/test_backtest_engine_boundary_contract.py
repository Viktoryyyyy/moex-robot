import inspect

from moex_backtest.engine.interfaces import BacktestEngine, BacktestRequest, BacktestResult


FORBIDDEN_BACKTEST_ENGINE_NAMES = {
    "generate_signals",
    "signal_math",
    "discover",
    "glob",
    "latest",
    "server",
    "runtime",
    "live",
    "authorize",
    "enable",
    "strategy_pnl",
}


def test_backtest_engine_owns_required_platform_boundaries():
    public_methods = [name for name, value in inspect.getmembers(BacktestEngine) if not name.startswith("_") and callable(value)]

    assert public_methods == [
        "apply_costs_and_slippage",
        "build_position_path",
        "build_report_artifacts",
        "run_backtest",
        "simulate_fills",
    ]

    assert "fill" in BacktestEngine.simulate_fills.__name__
    assert "costs" in BacktestEngine.apply_costs_and_slippage.__name__
    assert "slippage" in BacktestEngine.apply_costs_and_slippage.__name__
    assert "position_path" in BacktestEngine.build_position_path.__name__
    assert "artifacts" in BacktestEngine.build_report_artifacts.__name__


def test_backtest_engine_interface_excludes_forbidden_responsibilities():
    method_names = {name for name, value in inspect.getmembers(BacktestEngine) if not name.startswith("_") and callable(value)}
    method_code_names = set()
    for method_name in method_names:
        method_code_names.update(getattr(BacktestEngine, method_name).__code__.co_names)

    assert method_names.isdisjoint(FORBIDDEN_BACKTEST_ENGINE_NAMES)
    assert method_code_names.isdisjoint(FORBIDDEN_BACKTEST_ENGINE_NAMES)


def test_backtest_engine_request_result_keep_artifact_boundary_explicit():
    request_fields = set(BacktestRequest.__dataclass_fields__)
    result_fields = set(BacktestResult.__dataclass_fields__)

    assert request_fields == {"strategy_id", "strategy_version", "signals", "market_data", "semantics", "context"}
    assert result_fields == {"fills", "costs", "position_path", "metrics", "artifacts"}
