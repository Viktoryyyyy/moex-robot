from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

REQUIRED_BACKTEST_SEMANTICS_FIELDS: Final[tuple[str, ...]] = (
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

DEFAULT_ANTI_LEAKAGE_INVARIANTS: Final[tuple[str, ...]] = (
    "signals must be timestamped at the decision point only",
    "features used for a decision must be known no later than known_by_when_rule",
    "execution prices must occur at or after the configured execution delay",
    "future outcome labels must not be inputs to fill, cost, sizing, or position transitions",
    "calendar and aggregation rules must not shift future bars into prior sessions",
)


@dataclass(frozen=True)
class BacktestSemanticsContract:
    signal_timestamp_rule: str
    known_by_when_rule: str
    execution_delay_rule: str
    execution_price_rule: str
    fill_rule: str
    position_transition_rule: str
    reversal_rule: str
    sizing_rule: str
    cost_slippage_rule: str
    terminal_close_rule: str
    missing_bar_rule: str
    invalid_data_rule: str
    calendar_session_rule: str
    aggregation_rule: str
    anti_leakage_invariants: tuple[str, ...] = field(default_factory=lambda: DEFAULT_ANTI_LEAKAGE_INVARIANTS)

    def __post_init__(self) -> None:
        for field_name in REQUIRED_BACKTEST_SEMANTICS_FIELDS:
            value = getattr(self, field_name)
            if field_name == "anti_leakage_invariants":
                if isinstance(value, (str, bytes)) or not tuple(value):
                    raise ValueError("anti_leakage_invariants must be a non-empty tuple of strings")
                if any(not isinstance(item, str) or not item.strip() for item in value):
                    raise ValueError("anti_leakage_invariants must contain non-empty strings")
                object.__setattr__(self, field_name, tuple(value))
            elif not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} is required")


def validate_backtest_semantics_contract(contract: BacktestSemanticsContract) -> BacktestSemanticsContract:
    if not isinstance(contract, BacktestSemanticsContract):
        raise TypeError("contract must be BacktestSemanticsContract")
    return contract
