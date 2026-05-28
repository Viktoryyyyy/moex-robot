# Canonical Backtest Semantics Contract v1

contract_id: canonical_backtest_semantics.v1
schema_version: v1
artifact_class: backtest_semantics_contract
producer: moex_backtest.semantics
consumer: moex_backtest.engine / strategies / moex_research.runners / PM review

## purpose

Defines the single canonical execution semantics layer for strategy testing.

## required_fields

- signal_timestamp_rule
- known_by_when_rule
- execution_delay_rule
- execution_price_rule
- fill_rule
- position_transition_rule
- reversal_rule
- sizing_rule
- cost_slippage_rule
- terminal_close_rule
- missing_bar_rule
- invalid_data_rule
- calendar_session_rule
- aggregation_rule
- anti_leakage_invariants

## validation_rules

- signal_timestamp_rule and known_by_when_rule must define the ex-ante decision boundary.
- execution_delay_rule must define the number of bars or sessions between signal availability and order eligibility.
- execution_price_rule must name the price field or modeled fill price used.
- fill_rule must state full, partial, skipped, or rejected fill behavior.
- position_transition_rule must define flat-to-long, flat-to-short, hold, close, and no-op transitions.
- reversal_rule must define whether close-and-open happens atomically or across delayed steps.
- sizing_rule must define unit, notional, volatility, or portfolio sizing semantics.
- cost_slippage_rule must reference cost_slippage_contract.v1 or a later approved version.
- terminal_close_rule must define forced close behavior at test end or contract/session boundary.
- missing_bar_rule and invalid_data_rule must define fail-closed or skip behavior.
- calendar_session_rule must bind to canonical MOEX/ISS session semantics.
- aggregation_rule must define daily, multi-day, and portfolio aggregation.
- anti_leakage_invariants must prohibit future-dependent fills or same-bar impossible execution.

## forbidden_patterns

- Strategy-local custom PnL engines that bypass canonical semantics.
- Same-bar execution unless explicitly proven available by known_by_when_rule.
- Cost-free promotion metrics unless the contract explicitly states gross-only diagnostic use.
- Calendar/session guessing from observed file gaps.
- Runtime/live assumptions derived from research-only backtests.
