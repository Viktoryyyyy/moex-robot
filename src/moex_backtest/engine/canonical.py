from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Mapping, Sequence

from moex_backtest.engine.interfaces import BacktestResult


BacktestRows = Sequence[Mapping[str, object]]
BacktestTable = tuple[dict[str, object], ...]


class BacktestValidationError(ValueError):
    pass


@dataclass(frozen=True)
class CostConfig:
    commission_bps: float = 0.0
    slippage_bps: float = 0.0

    def __post_init__(self) -> None:
        commission_bps = _as_non_negative_float(self.commission_bps, "commission_bps")
        slippage_bps = _as_non_negative_float(self.slippage_bps, "slippage_bps")
        object.__setattr__(self, "commission_bps", commission_bps)
        object.__setattr__(self, "slippage_bps", slippage_bps)


@dataclass(frozen=True)
class ExecutionConfig:
    fill_model_id: str = "next_bar_open"
    terminal_close: bool = True
    initial_cash: float = 0.0

    def __post_init__(self) -> None:
        if self.fill_model_id != "next_bar_open":
            raise BacktestValidationError("only next_bar_open execution is implemented in minimal slice")
        if not isinstance(self.terminal_close, bool):
            raise BacktestValidationError("terminal_close must be boolean")
        initial_cash = _as_float(self.initial_cash, "initial_cash")
        object.__setattr__(self, "initial_cash", initial_cash)


@dataclass(frozen=True)
class CanonicalBacktestInput:
    bars: BacktestRows
    signals: BacktestRows
    cost_config: CostConfig = field(default_factory=CostConfig)
    execution_config: ExecutionConfig = field(default_factory=ExecutionConfig)


@dataclass(frozen=True)
class _Bar:
    timestamp: object
    order: int
    open: float
    close: float
    valid: bool


@dataclass(frozen=True)
class _Signal:
    timestamp: object
    order: int
    target_position: int


@dataclass(frozen=True)
class _TradeCost:
    fill_price: float
    commission: float
    slippage_cost: float


class CanonicalBacktestEngine:
    engine_id = "canonical_backtest_engine_minimal_v1"

    def run(self, contract: CanonicalBacktestInput) -> BacktestResult:
        if not isinstance(contract, CanonicalBacktestInput):
            raise BacktestValidationError("contract must be CanonicalBacktestInput")

        bars = _normalize_bars(contract.bars)
        signals = _normalize_signals(contract.signals)
        return self._run_normalized(bars, signals, contract.cost_config, contract.execution_config)

    def run_backtest(self, request: Any) -> BacktestResult:
        context = getattr(request, "context", {}) or {}
        cost_config = _coerce_cost_config(context.get("cost_config", CostConfig()))
        execution_config = _coerce_execution_config(context.get("execution_config", ExecutionConfig()))
        return self.run(
            CanonicalBacktestInput(
                bars=getattr(request, "market_data"),
                signals=getattr(request, "signals"),
                cost_config=cost_config,
                execution_config=execution_config,
            )
        )

    def _run_normalized(
        self,
        bars: tuple[_Bar, ...],
        signals: tuple[_Signal, ...],
        cost_config: CostConfig,
        execution_config: ExecutionConfig,
    ) -> BacktestResult:
        cash = execution_config.initial_cash
        position = 0
        fills: list[dict[str, object]] = []
        costs: list[dict[str, object]] = []
        position_path: list[dict[str, object]] = []
        rejected_signals: list[dict[str, object]] = []
        total_commission = 0.0
        total_slippage_cost = 0.0
        peak_equity = execution_config.initial_cash
        max_drawdown = 0.0

        for signal in signals:
            next_bar = _next_bar_after_signal(signal, bars)
            if next_bar is None:
                rejected_signals.append(_rejected_signal(signal, "missing_next_bar"))
                continue
            if not next_bar.valid:
                rejected_signals.append(_rejected_signal(signal, "invalid_next_bar"))
                continue
            if signal.target_position == position:
                continue

            fill, cost, cash, position = self._execute_transition(
                signal_timestamp=signal.timestamp,
                execution_timestamp=next_bar.timestamp,
                raw_price=next_bar.open,
                mark_price=next_bar.close,
                position_before=position,
                target_position=signal.target_position,
                cash=cash,
                reason="signal",
                cost_config=cost_config,
            )
            fills.append(fill)
            costs.append(cost)
            total_commission += cost["commission"]
            total_slippage_cost += cost["slippage_cost"]
            path_row = _position_path_row(fill, cash, position, next_bar.close)
            position_path.append(path_row)
            peak_equity = max(peak_equity, path_row["equity"])
            max_drawdown = max(max_drawdown, peak_equity - path_row["equity"])

        if execution_config.terminal_close and position != 0:
            terminal_bar = _last_valid_bar(bars)
            if terminal_bar is None:
                rejected_signals.append(
                    {
                        "signal_timestamp": None,
                        "target_position": 0,
                        "reject_reason": "missing_valid_terminal_bar",
                    }
                )
            else:
                fill, cost, cash, position = self._execute_transition(
                    signal_timestamp=None,
                    execution_timestamp=terminal_bar.timestamp,
                    raw_price=terminal_bar.close,
                    mark_price=terminal_bar.close,
                    position_before=position,
                    target_position=0,
                    cash=cash,
                    reason="forced_terminal_close",
                    cost_config=cost_config,
                )
                fills.append(fill)
                costs.append(cost)
                total_commission += cost["commission"]
                total_slippage_cost += cost["slippage_cost"]
                path_row = _position_path_row(fill, cash, position, terminal_bar.close)
                position_path.append(path_row)
                peak_equity = max(peak_equity, path_row["equity"])
                max_drawdown = max(max_drawdown, peak_equity - path_row["equity"])

        final_equity = cash if not position_path else position_path[-1]["equity"]
        metrics = {
            "engine_id": self.engine_id,
            "fill_model_id": execution_config.fill_model_id,
            "signal_count": len(signals),
            "trade_count": len(fills),
            "rejected_signal_count": len(rejected_signals),
            "forced_close_count": sum(1 for fill in fills if fill["reason"] == "forced_terminal_close"),
            "final_position": position,
            "initial_cash": execution_config.initial_cash,
            "ending_cash": cash,
            "ending_equity": final_equity,
            "total_pnl": final_equity - execution_config.initial_cash,
            "total_commission": total_commission,
            "total_slippage_cost": total_slippage_cost,
            "total_cost": total_commission + total_slippage_cost,
            "max_drawdown": max_drawdown,
        }
        artifacts = {
            "engine_id": self.engine_id,
            "execution_semantics": {
                "signal_timestamp_rule": "signal row timestamp is decision timestamp",
                "execution_delay_rule": "strict next bar after signal timestamp",
                "execution_price_rule": "next bar open for signal trades; terminal bar close for forced close",
                "position_transition_rule": "target position delta creates one trade record",
                "terminal_close_rule": "non-flat terminal position is closed at last valid bar close",
                "missing_bar_rule": "missing strict next bar rejects the signal",
                "invalid_data_rule": "invalid strict next bar rejects the signal",
            },
            "trade_records": tuple(fills),
            "cost_slippage_records": tuple(costs),
            "rejected_signals": tuple(rejected_signals),
            "equity_summary": metrics,
        }
        return BacktestResult(
            fills=tuple(fills),
            costs=tuple(costs),
            position_path=tuple(position_path),
            metrics=metrics,
            artifacts=artifacts,
        )

    def _execute_transition(
        self,
        signal_timestamp: object,
        execution_timestamp: object,
        raw_price: float,
        mark_price: float,
        position_before: int,
        target_position: int,
        cash: float,
        reason: str,
        cost_config: CostConfig,
    ) -> tuple[dict[str, object], dict[str, object], float, int]:
        quantity = target_position - position_before
        if quantity == 0:
            raise BacktestValidationError("zero-quantity transition must not be executed")

        trade_cost = _apply_costs(raw_price, quantity, cost_config)
        commission = trade_cost.commission
        slippage_cost = trade_cost.slippage_cost
        cash_after = cash - quantity * trade_cost.fill_price - commission
        transition_type = _transition_type(position_before, target_position)
        fill = {
            "engine_id": self.engine_id,
            "signal_timestamp": signal_timestamp,
            "execution_timestamp": execution_timestamp,
            "raw_price": raw_price,
            "fill_price": trade_cost.fill_price,
            "mark_price": mark_price,
            "quantity": quantity,
            "side": "buy" if quantity > 0 else "sell",
            "position_before": position_before,
            "position_after": target_position,
            "transition_type": transition_type,
            "reason": reason,
        }
        cost = {
            "engine_id": self.engine_id,
            "execution_timestamp": execution_timestamp,
            "quantity": quantity,
            "raw_price": raw_price,
            "fill_price": trade_cost.fill_price,
            "commission": commission,
            "slippage_cost": slippage_cost,
            "total_cost": commission + slippage_cost,
            "reason": reason,
        }
        return fill, cost, cash_after, target_position


def _coerce_cost_config(value: object) -> CostConfig:
    if isinstance(value, CostConfig):
        return value
    if isinstance(value, Mapping):
        return CostConfig(
            commission_bps=value.get("commission_bps", 0.0),
            slippage_bps=value.get("slippage_bps", 0.0),
        )
    raise BacktestValidationError("cost_config must be CostConfig or mapping")


def _coerce_execution_config(value: object) -> ExecutionConfig:
    if isinstance(value, ExecutionConfig):
        return value
    if isinstance(value, Mapping):
        return ExecutionConfig(
            fill_model_id=str(value.get("fill_model_id", "next_bar_open")),
            terminal_close=bool(value.get("terminal_close", True)),
            initial_cash=value.get("initial_cash", 0.0),
        )
    raise BacktestValidationError("execution_config must be ExecutionConfig or mapping")


def _normalize_bars(rows: BacktestRows) -> tuple[_Bar, ...]:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise BacktestValidationError("bars must be a sequence of mappings")
    bars: list[_Bar] = []
    for order, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise BacktestValidationError("bar row must be a mapping")
        timestamp = _required(row, "timestamp")
        valid = row.get("valid", True)
        if not isinstance(valid, bool):
            raise BacktestValidationError("bar valid flag must be boolean")
        open_price = _optional_price(row.get("open"), "open")
        close_price = _optional_price(row.get("close"), "close")
        row_is_valid = valid and open_price is not None and close_price is not None
        bars.append(
            _Bar(
                timestamp=timestamp,
                order=order,
                open=0.0 if open_price is None else open_price,
                close=0.0 if close_price is None else close_price,
                valid=row_is_valid,
            )
        )
    if not bars:
        raise BacktestValidationError("bars must be non-empty")
    sorted_bars = tuple(sorted(bars, key=lambda item: item.timestamp))
    for previous, current in zip(sorted_bars, sorted_bars[1:]):
        if previous.timestamp == current.timestamp:
            raise BacktestValidationError("bar timestamps must be unique")
    return sorted_bars


def _normalize_signals(rows: BacktestRows) -> tuple[_Signal, ...]:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise BacktestValidationError("signals must be a sequence of mappings")
    signals: list[_Signal] = []
    for order, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise BacktestValidationError("signal row must be a mapping")
        timestamp = _required(row, "timestamp")
        target_position = row.get("target_position")
        if isinstance(target_position, bool) or target_position not in (-1, 0, 1):
            raise BacktestValidationError("target_position must be one of -1, 0, 1")
        signals.append(_Signal(timestamp=timestamp, order=order, target_position=int(target_position)))
    return tuple(sorted(signals, key=lambda item: (item.timestamp, item.order)))


def _required(row: Mapping[str, object], field_name: str) -> object:
    if field_name not in row:
        raise BacktestValidationError(f"{field_name} is required")
    return row[field_name]


def _optional_price(value: object, field_name: str) -> float | None:
    if value is None:
        return None
    price = _as_float(value, field_name)
    if price <= 0:
        return None
    return price


def _as_non_negative_float(value: object, field_name: str) -> float:
    number = _as_float(value, field_name)
    if number < 0:
        raise BacktestValidationError(f"{field_name} must be non-negative")
    return number


def _as_float(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BacktestValidationError(f"{field_name} must be numeric")
    number = float(value)
    if not isfinite(number):
        raise BacktestValidationError(f"{field_name} must be finite")
    return number


def _next_bar_after_signal(signal: _Signal, bars: tuple[_Bar, ...]) -> _Bar | None:
    for bar in bars:
        if bar.timestamp > signal.timestamp:
            return bar
    return None


def _last_valid_bar(bars: tuple[_Bar, ...]) -> _Bar | None:
    for bar in reversed(bars):
        if bar.valid:
            return bar
    return None


def _rejected_signal(signal: _Signal, reason: str) -> dict[str, object]:
    return {
        "signal_timestamp": signal.timestamp,
        "target_position": signal.target_position,
        "reject_reason": reason,
    }


def _transition_type(position_before: int, target_position: int) -> str:
    if position_before == 0 and target_position == 1:
        return "flat_to_long"
    if position_before == 0 and target_position == -1:
        return "flat_to_short"
    if position_before == 1 and target_position == 0:
        return "long_to_flat"
    if position_before == -1 and target_position == 0:
        return "short_to_flat"
    if position_before == 1 and target_position == -1:
        return "long_to_short_reversal"
    if position_before == -1 and target_position == 1:
        return "short_to_long_reversal"
    raise BacktestValidationError("unsupported position transition")


def _apply_costs(raw_price: float, quantity: int, cost_config: CostConfig) -> _TradeCost:
    slippage_delta = raw_price * cost_config.slippage_bps / 10_000.0
    fill_price = raw_price + slippage_delta if quantity > 0 else raw_price - slippage_delta
    commission = abs(quantity) * raw_price * cost_config.commission_bps / 10_000.0
    slippage_cost = abs(quantity) * abs(fill_price - raw_price)
    return _TradeCost(fill_price=fill_price, commission=commission, slippage_cost=slippage_cost)


def _position_path_row(fill: Mapping[str, object], cash: float, position: int, mark_price: float) -> dict[str, object]:
    equity = cash + position * mark_price
    return {
        "engine_id": fill["engine_id"],
        "execution_timestamp": fill["execution_timestamp"],
        "position": position,
        "cash": cash,
        "mark_price": mark_price,
        "equity": equity,
        "reason": fill["reason"],
    }


__all__ = [
    "BacktestValidationError",
    "CanonicalBacktestEngine",
    "CanonicalBacktestInput",
    "CostConfig",
    "ExecutionConfig",
]
